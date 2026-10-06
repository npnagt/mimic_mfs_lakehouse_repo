# Standalone PySpark script: derive tobacco/alcohol/obesity levels + a tobacco-cessation
# code from the discharge notes with the open-source medSpaCy clinical NLP library.
# Runs as a script-mode Glue job (see etl/create_medspacy_nlp_job.py).
#
# medSpaCy runs in-process, so there is no S3 input round-trip and no async batch job:
#   1. Read mimic4_db_raw.discharge_note_raw.
#   2. mapPartitions -> load a medSpaCy pipeline once per partition (PyRuSH sentence
#      splitter + rule-based TargetMatcher + ConText negation/historical/hypothetical/
#      family), tag each note, emit normalized entity spans.
#   3. Optionally persist one JSON per note to s3://<nlp_bucket>/<results_prefix>/, then
#      aggregate to (subject_id, hadm_id), derive the levels from the entities + ConText
#      attributes, keep the full medSpaCy output, and overwrite
#      glue_catalog.<gold_db>.fact_discharge_note_nlp.
#
# recognized_entities is a JSON STRING (see the DDL comment re: VARIANT).
#
# Resumable, partition-aligned batching: medSpaCy's per-document NLP is compute-heavy
# enough that a real run (331,793 discharge notes, the default --note_limit 1000000 does
# NOT bind here) exceeded the Glue job's 120-minute Timeout on a small (2x G.1X) worker
# allocation with ZERO partial progress saved -- the original code ran one
# mapPartitions(...).collect() over the entire corpus and only wrote anything at the very
# end. Every write now processes and writes NUM_BUCKETS-many groups of admissions, grouped
# by hadm_bucket = hadm_id % NUM_BUCKETS -- a real column, matching
# fact_discharge_note_nlp's PARTITIONED BY (hadm_bucket) plain IDENTITY partition (see the
# DDL comment for why a materialized column instead of an Iceberg bucket() transform:
# glue_catalog's SparkCatalog here doesn't implement Iceberg's function catalog, confirmed
# live via "AnalysisException: Catalog glue_catalog does not support functions"). Because
# each write's rows fall in one hadm_bucket value, Iceberg's overwritePartitions()
# replaces ONLY that partition -- every other bucket (all previously-written admissions)
# is left untouched, so cost stays proportional to bucket size regardless of how much of
# the backfill has already completed (unlike a naive "read whole table minus touched,
# union, overwrite" against an unpartitioned table, which would make every later batch
# rewrite an ever-growing carried-forward set). If a Timeout hits mid-run, only the
# in-flight bucket's work is lost; resuming the job (see run_lakehouse_pipeline.py's
# printed resume command) re-reads TARGET_TABLE, skips every admission a prior attempt
# already wrote (left-anti join), and continues -- so repeated resumes make real forward
# progress instead of restarting from zero. Resume-skip only applies when
# refresh_mode=auto with no watermark yet (a fresh or interrupted backfill);
# refresh_mode=full means "redo everything" and must not skip anything.
import json
import re
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ARGS = getResolvedOptions(
    sys.argv,
    [
        "raw_database",
        "gold_database",
        "gold_s3_bucket",
        "nlp_bucket",
        "medspacy_results_prefix",
        "note_limit",
        "region",
        "write_note_json",
        "refresh_mode",
        "num_buckets",
    ],
)
RAW_DB = ARGS["raw_database"]
GOLD_DB = ARGS["gold_database"]
NLP_BUCKET = ARGS["nlp_bucket"]
RESULTS_PREFIX = ARGS["medspacy_results_prefix"].strip("/")
REGION = ARGS["region"]
WRITE_NOTE_JSON = ARGS["write_note_json"].strip().lower() == "true"
_raw_limit = ARGS["note_limit"].strip().lower()
NOTE_LIMIT = None if _raw_limit in ("", "0", "all", "-1") else int(_raw_limit)
# Must match fact_discharge_note_nlp's PARTITIONED BY (bucket(N, hadm_id)) -- see module
# docstring. Only affects batching/observability granularity if it doesn't (Iceberg's
# dynamic overwrite is correct either way, computed from the table's own partition spec),
# but the write-cost benefit only holds when a batch's rows land in one physical bucket.
NUM_BUCKETS = int(ARGS["num_buckets"])

MAX_DOC_CHARS = 100000  # medSpaCy is local; only guard against pathological outliers.
TARGET_TABLE = f"glue_catalog.{GOLD_DB}.fact_discharge_note_nlp"
REFRESH_MODE = ARGS["refresh_mode"].strip().lower()  # "auto" | "full"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------------------------------------------------------------------------------
# Incremental refresh via mimic4_db_business.etl_control  (proposition P7)
#
# Same watermark pattern the agg/OBT jobs use (etl/create_agg_visual_etl_jobs.py): keep a
# per-target row in etl_control holding the max source note timestamp already processed.
#   auto : first run (no watermark row) -> full backfill; later runs -> reprocess only the
#          admissions that have a discharge note newer than the watermark, and MERGE those
#          rows into the target (every other admission's row is left untouched).
#   full : reprocess every note regardless of watermark, then advance it.
# A --note_limit sample run never advances the watermark (so it can't poison it).
# ---------------------------------------------------------------------------------------
from pyspark.sql import functions as _F  # noqa: E402

ETL_CONTROL = f"glue_catalog.{GOLD_DB}.etl_control"
CONTROL_KEY = "fact_discharge_note_nlp"
# a note we cannot date is processed once on the backfill, then never re-selected
NOTE_TS = _F.coalesce(_F.to_timestamp("storetime"), _F.to_timestamp("charttime"),
                      _F.lit("1900-01-01 00:00:00").cast("timestamp"))


def read_watermark():
    try:
        r = (spark.table(ETL_CONTROL)
             .filter(_F.col("aggregate_table") == CONTROL_KEY)
             .select(_F.max("last_processed_ts").alias("wm")).collect())
        return r[0]["wm"] if r and r[0]["wm"] is not None else None
    except Exception as exc:  # noqa: BLE001 -- etl_control absent on a fresh bootstrap
        print(f"etl_control not readable ({exc}) -- treating as first run.")
        return None


def advance_watermark(new_ts):
    if new_ts is None:
        return
    (spark.createDataFrame([(CONTROL_KEY, new_ts)], ["aggregate_table", "last_processed_ts"])
     .withColumn("updated_ts", _F.current_timestamp())
     .writeTo(ETL_CONTROL).option("fanout-enabled", "true").overwritePartitions())
    print(f"etl_control: {CONTROL_KEY} watermark advanced to {new_ts}")

# ---------------------------------------------------------------------------------------
# Rule-based target vocabulary (plain data so it pickles into the Spark closure; the
# medSpaCy TargetRule objects are built inside each executor). Labels:
#   TOBACCO | ALCOHOL | OBESITY | CESSATION_COUNSEL | CESSATION_PHARM
# ---------------------------------------------------------------------------------------
TARGET_PATTERNS = [
    ("tobacco", "TOBACCO"), ("tobacco use", "TOBACCO"), ("tobacco abuse", "TOBACCO"),
    ("tobacco dependence", "TOBACCO"), ("smoking", "TOBACCO"), ("smoker", "TOBACCO"),
    ("smokes", "TOBACCO"), ("cigarette", "TOBACCO"), ("cigarettes", "TOBACCO"),
    ("cigar", "TOBACCO"), ("nicotine", "TOBACCO"), ("nicotine dependence", "TOBACCO"),
    ("pack year", "TOBACCO"), ("pack-year", "TOBACCO"), ("pack years", "TOBACCO"),
    ("packs per day", "TOBACCO"), ("ppd", "TOBACCO"), ("chewing tobacco", "TOBACCO"),
    ("snuff", "TOBACCO"), ("vaping", "TOBACCO"), ("e-cigarette", "TOBACCO"),
    ("alcohol", "ALCOHOL"), ("alcohol use", "ALCOHOL"), ("alcohol abuse", "ALCOHOL"),
    ("alcohol dependence", "ALCOHOL"), ("alcoholic", "ALCOHOL"), ("alcoholism", "ALCOHOL"),
    ("etoh", "ALCOHOL"), ("drinking", "ALCOHOL"), ("binge drinking", "ALCOHOL"),
    ("beer", "ALCOHOL"), ("wine", "ALCOHOL"), ("liquor", "ALCOHOL"),
    ("obesity", "OBESITY"), ("obese", "OBESITY"), ("morbid obesity", "OBESITY"),
    ("morbidly obese", "OBESITY"), ("severe obesity", "OBESITY"),
    ("class iii obesity", "OBESITY"), ("adiposity", "OBESITY"), ("bmi", "OBESITY"),
    ("smoking cessation", "CESSATION_COUNSEL"), ("tobacco cessation", "CESSATION_COUNSEL"),
    ("cessation counseling", "CESSATION_COUNSEL"), ("quit smoking", "CESSATION_COUNSEL"),
    ("quit date", "CESSATION_COUNSEL"), ("advised to quit", "CESSATION_COUNSEL"),
    ("counseled on smoking", "CESSATION_COUNSEL"),
    ("varenicline", "CESSATION_PHARM"), ("chantix", "CESSATION_PHARM"),
    ("bupropion", "CESSATION_PHARM"), ("zyban", "CESSATION_PHARM"),
    ("nicotine patch", "CESSATION_PHARM"), ("nicotine gum", "CESSATION_PHARM"),
    ("nicotine lozenge", "CESSATION_PHARM"), ("nicotine replacement", "CESSATION_PHARM"),
    ("nicotine replacement therapy", "CESSATION_PHARM"), ("nrt", "CESSATION_PHARM"),
]

MORBID_RX = re.compile(r"(?i)morbid|severe|class\s*(?:iii|3)\b|BMI\s*(?:[4-9]\d|\d{3})")


def process_partition(rows):
    """Executor-side: tag each note's text with medSpaCy, yield normalized rows."""
    import medspacy
    from medspacy.ner import TargetRule

    try:
        nlp = medspacy.load()  # blank 'en' + pyrush + target_matcher + context
    except Exception:  # noqa: BLE001 -- PyRuSH unavailable in the runtime: fall back
        nlp = medspacy.load(medspacy_disable=["medspacy_pyrush"])
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer", first=True)

    matcher = nlp.get_pipe("medspacy_target_matcher")
    matcher.add([TargetRule(literal=lit, category=cat) for lit, cat in TARGET_PATTERNS])

    for r in rows:
        text = (r["text"] or "")[:MAX_DOC_CHARS]
        doc = nlp(text)
        entities = [
            {
                "text": ent.text,
                "label": ent.label_,
                "start_char": ent.start_char,
                "end_char": ent.end_char,
                "negated": bool(ent._.is_negated),
                "historical": bool(ent._.is_historical),
                "hypothetical": bool(ent._.is_hypothetical),
                "family": bool(ent._.is_family),
                "uncertain": bool(getattr(ent._, "is_uncertain", False)),
                "sentence": ent.sent.text if ent.sent is not None else "",
            }
            for ent in doc.ents
        ]
        yield (
            int(r["subject_id"]),
            int(r["hadm_id"]),
            r["admit_provider_id"],
            r["admit_date_key"],
            str(r["note_id"]),
            text,
            json.dumps(entities),
        )


# ---------------------------------------------------------------------------------------
# Level derivation from the tagged entities + ConText attributes:
#   tobacco_use / alcohol_use : NOT_MENTIONED | DENIES | FORMER | CURRENT
#   obesity_level             : NONE | OBESE | MORBID
#   tobacco_cessation_cd      : NA | NONE | COUNSELED | PHARMACOTHERAPY
# ---------------------------------------------------------------------------------------
def _status(matches):
    real = [m for m in matches if not m["hypothetical"] and not m["family"]]
    if not real:
        return "NOT_MENTIONED"
    non_negated = [m for m in real if not m["negated"]]
    if not non_negated:
        return "DENIES"
    if any(not m["historical"] for m in non_negated):
        return "CURRENT"
    return "FORMER"


def derive_levels(entities, note_text):
    tobacco_use = _status([e for e in entities if e["label"] == "TOBACCO"])
    alcohol_use = _status([e for e in entities if e["label"] == "ALCOHOL"])

    obesity = [
        e
        for e in entities
        if e["label"] == "OBESITY" and not e["negated"] and not e["hypothetical"] and not e["family"]
    ]
    if not obesity:
        obesity_level = "NONE"
    elif any(MORBID_RX.search(e["text"]) or MORBID_RX.search(e["sentence"]) for e in obesity) or MORBID_RX.search(note_text or ""):
        obesity_level = "MORBID"
    else:
        obesity_level = "OBESE"

    if any(e["label"] == "CESSATION_PHARM" and not e["negated"] for e in entities):
        tobacco_cessation_cd = "PHARMACOTHERAPY"
    elif any(e["label"] == "CESSATION_COUNSEL" and not e["negated"] for e in entities):
        tobacco_cessation_cd = "COUNSELED"
    elif tobacco_use in ("CURRENT", "FORMER"):
        tobacco_cessation_cd = "NONE"
    else:
        tobacco_cessation_cd = "NA"

    return tobacco_use, alcohol_use, obesity_level, tobacco_cessation_cd


SCHEMA = StructType(
    [
        StructField("subject_id", LongType()),
        StructField("hadm_id", LongType()),
        StructField("admit_provider_id", StringType()),
        StructField("admit_date_key", IntegerType()),
        StructField("hadm_bucket", IntegerType()),
        StructField("tobacco_use", StringType()),
        StructField("alcohol_use", StringType()),
        StructField("obesity_level", StringType()),
        StructField("tobacco_cessation_cd", StringType()),
        StructField("recognized_entities", StringType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
        StructField("created_by", StringType()),
        StructField("updated_by", StringType()),
    ]
)


def tag_and_derive(batch_notes_df):
    """Phase 2+3 for one batch: medSpaCy on the executors, aggregate to
    (subject_id, hadm_id), derive the levels. Returns a DataFrame (possibly 0 rows)."""
    tagged = batch_notes_df.rdd.mapPartitions(process_partition).collect()
    if not tagged:
        return spark.createDataFrame([], SCHEMA)

    s3 = boto3.client("s3", region_name=REGION)
    per_admission = {}
    for subject_id, hadm_id, admit_provider_id, admit_date_key, note_id, text, entities_json in tagged:
        entities = json.loads(entities_json)
        if WRITE_NOTE_JSON:
            s3.put_object(
                Bucket=NLP_BUCKET,
                Key=f"{RESULTS_PREFIX}/{subject_id}_{hadm_id}_{note_id}.json",
                Body=json.dumps({"note_id": note_id, "entities": entities}).encode("utf-8"),
            )
        agg = per_admission.setdefault(
            (subject_id, hadm_id),
            {"entities": [], "texts": [], "notes": [], "admit_provider_id": admit_provider_id, "admit_date_key": admit_date_key},
        )
        agg["entities"].extend(entities)
        agg["texts"].append(text)
        agg["notes"].append({"note_id": note_id, "entities": entities})

    now = datetime.now(timezone.utc)
    rows = []
    for (subject_id, hadm_id), agg in per_admission.items():
        combined_text = "\n".join(agg["texts"])
        tobacco_use, alcohol_use, obesity_level, tobacco_cessation_cd = derive_levels(agg["entities"], combined_text)
        recognized_entities = json.dumps(
            {"nlp_engine": "medspacy", "note_count": len(agg["notes"]), "notes": agg["notes"]},
            default=str,
        )
        rows.append(
            Row(
                subject_id=subject_id,
                hadm_id=hadm_id,
                admit_provider_id=agg["admit_provider_id"],
                admit_date_key=agg["admit_date_key"],
                hadm_bucket=hadm_id % NUM_BUCKETS,
                tobacco_use=tobacco_use,
                alcohol_use=alcohol_use,
                obesity_level=obesity_level,
                tobacco_cessation_cd=tobacco_cessation_cd,
                recognized_entities=recognized_entities,
                created_ts=now,
                updated_ts=now,
                created_by="glue_nlp_medspacy",
                updated_by="glue_nlp_medspacy",
            )
        )
    return spark.createDataFrame(rows, SCHEMA)


def write_batch(result_df) -> int:
    """Write one bucket's worth of results into TARGET_TABLE. TARGET_TABLE is PARTITIONED
    BY (hadm_bucket) and every row's hadm_bucket = hadm_id % NUM_BUCKETS is fixed by
    tag_and_derive above, matching the single value the main loop below groups by, so
    overwritePartitions() only ever replaces the one partition this batch's rows fall in
    -- every other bucket (every previously-written admission) is left completely
    untouched by Iceberg's dynamic partition overwrite. No manual "read current table,
    filter out touched, union, overwrite the whole table" carry-forward dance is needed
    (that would only be required against an unpartitioned table).

    coalesce(1): result_df is built via spark.createDataFrame(rows, SCHEMA) from a plain
    Python list (after the per-note NLP work is collected to the driver in tag_and_derive),
    which Spark spreads across its default parallelism's worth of partitions regardless of
    how few rows a bucket actually holds. Without this, every bucket's write produces
    several small Parquet files instead of one -- one file per Spark partition, most nearly
    empty -- and NUM_BUCKETS buckets' worth of that compounds into thousands of tiny files
    plus one Iceberg manifest/snapshot per bucket, each file still paying Parquet's fixed
    per-file footer/statistics cost regardless of row count (confirmed empirically: this
    table measured close to one physical file per admission before this fix)."""
    result_df.coalesce(1).writeTo(TARGET_TABLE).overwritePartitions()
    return result_df.count()


# ---------------------------------------------------------------------------------------
# Phase 1 -- decide the note set (incremental vs full), read notes
# ---------------------------------------------------------------------------------------
# admit_provider_id / admit_date_key are pulled from fact_admission so this table shares
# fact_admission's grain, matching every other archetype instance in the Multimodal Fusion
# Schema -- same value for every note of a given admission.
admission_keys = spark.table(f"glue_catalog.{GOLD_DB}.fact_admission").select(
    "hadm_id", "admit_provider_id", "admit_date_key"
)
src = (
    spark.table(f"{RAW_DB}.discharge_note_raw")
    .select("subject_id", "hadm_id", "note_id", "text", NOTE_TS.alias("_note_ts"))
    .where("hadm_id IS NOT NULL")
    .join(admission_keys, "hadm_id", "left")
)

watermark = None if REFRESH_MODE == "full" else read_watermark()
watermark_to_set = None

if watermark is not None:
    new_notes = src.filter(_F.col("_note_ts") > _F.lit(watermark).cast("timestamp"))
    touched_hadm = [r["hadm_id"] for r in new_notes.select("hadm_id").distinct().collect()]
    if not touched_hadm:
        # Nothing to do. A plain sys.exit(0) is reported as FAILED by Glue's script
        # runner (it surfaces the SystemExit); os._exit(0) ends the process cleanly with
        # code 0 -> the run shows SUCCEEDED, which is what a no-op refresh should be.
        print(f"medspacy_nlp: no discharge note newer than watermark {watermark} -- nothing to refresh.")
        import os as _os
        _os._exit(0)
    print(f"medspacy_nlp: incremental -- {len(touched_hadm)} admission(s) have a new discharge note "
          f"since {watermark}; reprocessing all notes for those admissions.")
    notes_df = src.where(_F.col("hadm_id").isin(touched_hadm))
    watermark_to_set = new_notes.select(_F.max("_note_ts").alias("m")).collect()[0]["m"]
else:
    print(f"medspacy_nlp: full {'refresh' if REFRESH_MODE == 'full' else 'backfill (no watermark)'}.")
    notes_df = src
    watermark_to_set = src.select(_F.max("_note_ts").alias("m")).collect()[0]["m"]

notes_df = notes_df.select("subject_id", "hadm_id", "admit_provider_id", "admit_date_key", "note_id", "text")
if NOTE_LIMIT is not None:
    notes_df = notes_df.orderBy("note_id").limit(NOTE_LIMIT)
    watermark_to_set = None  # a capped sample must not move the watermark

# Resume-skip only makes sense for a fresh-or-interrupted backfill (refresh_mode=auto,
# no watermark yet): refresh_mode=full means "redo everything" and must not skip rows
# just because they're already there. NOTE_LIMIT does NOT gate this -- the default
# --note_limit 1000000 is itself the multi-hundred-thousand-note scale that timed out,
# not a small sample (a genuinely tiny --note-limit for a quick manual test also benefits
# from resume-skip/bucketed writes; both are correct and cheap even for a handful of rows).
USE_RESUME_SKIP = watermark is None and REFRESH_MODE != "full"

if USE_RESUME_SKIP:
    try:
        already_done_df = spark.table(TARGET_TABLE).select("hadm_id").distinct()
        already_done_count = already_done_df.count()
    except Exception as exc:  # noqa: BLE001 -- target table unreadable: treat as empty
        print(f"{TARGET_TABLE} not readable yet ({exc}) -- treating as empty (first attempt).")
        already_done_df, already_done_count = None, 0
    if already_done_count:
        before = notes_df.select("hadm_id").distinct().count()
        notes_df = notes_df.join(already_done_df, on="hadm_id", how="left_anti")
        after = notes_df.select("hadm_id").distinct().count()
        print(f"medspacy_nlp: resuming backfill -- {already_done_count} admission(s) already in "
              f"{TARGET_TABLE} from a prior attempt; {before} -> {after} admission(s) remaining to process.")

# ---------------------------------------------------------------------------------------
# Phase 2+3 -- medSpaCy on the executors, aggregate, write -- one bucket (partition) at a
# time, see module docstring and write_batch's docstring for why this is what makes each
# write's cost independent of how much of the backfill has already completed.
# ---------------------------------------------------------------------------------------
notes_df = notes_df.withColumn("hadm_bucket", _F.col("hadm_id") % NUM_BUCKETS)
bucket_ids = [r["hadm_bucket"] for r in notes_df.select("hadm_bucket").distinct().orderBy("hadm_bucket").collect()]
if not bucket_ids:
    print("medspacy_nlp: nothing to process -- either no notes matched, or a prior attempt already "
          "covered every admission.")
    advance_watermark(watermark_to_set)
    import os as _os
    _os._exit(0)

print(f"medspacy_nlp: processing {len(bucket_ids)} of {NUM_BUCKETS} hadm_id bucket(s).")
run_start = datetime.now(timezone.utc)
total_written = 0
for i, bucket_id in enumerate(bucket_ids, start=1):
    batch_start = datetime.now(timezone.utc)
    batch_notes_df = notes_df.where(_F.col("hadm_bucket") == bucket_id).drop("hadm_bucket").repartition(8)
    result_df = tag_and_derive(batch_notes_df)
    written = write_batch(result_df)
    total_written += written
    now_ts = datetime.now(timezone.utc)
    print(f"medspacy_nlp: bucket {i}/{len(bucket_ids)} (id={bucket_id}) -- wrote {written} admission(s) in "
          f"{(now_ts - batch_start).total_seconds():.0f}s "
          f"(total {total_written} written, {(now_ts - run_start).total_seconds():.0f}s elapsed).")
advance_watermark(watermark_to_set)
print(f"phase 3: complete -- wrote {total_written} admission(s) across {len(bucket_ids)} bucket(s) "
      f"to {TARGET_TABLE}.")
