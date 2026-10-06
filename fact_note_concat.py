# Standalone PySpark script: collapse a note-level raw table (grain = note_id) into an
# admission-level Gold fact (grain = subject_id, hadm_id) -- one parameterized job shared by
# fact_discharge_note (source discharge_note_raw) and fact_radiology_note (source
# radiology_note_raw). Runs as a script-mode Glue job (see
# etl/create_fact_note_concat_job.py).
#
# WHY THIS TABLE EXISTS: discharge_note_raw / radiology_note_raw are at note_id grain --
# an admission with multiple notes has multiple raw rows -- which does not match every
# other archetype in the Multimodal Fusion Schema (Source Data Fact, Computed Structured
# Fact, Unstructured Features Fact, Inference Fact are all one row per subject_id/hadm_id).
# fact_discharge_note / fact_radiology_note give the Unstructured Data Fact archetype a
# Gold-layer instance at that SAME grain, so every archetype in the schema now joins on the
# identical key. discharge_note_raw / radiology_note_raw are unchanged and remain the
# canonical note-id-grain source; this table is a rollup, not a replacement.
#
# TRANSFORM: for each (subject_id, hadm_id):
#   text          = every note's text, ordered by note_seq, concatenated with a blank-line
#                   separator (sort_array on a (note_seq, text) struct -- deterministic
#                   regardless of Spark's read order, unlike collect_list alone).
#   notes_count   = count(*) over that admission's notes.
#   last_charttime, last_storetime = max(to_timestamp(...)) over that admission's notes
#                   (charttime/storetime are STRING in the raw table; NULL/unparseable
#                   values are ignored by max(), matching every other date-parsing path in
#                   this repo -- see NOTE_TS in medspacy_nlp.py / radiology_nlp.py).
#
# NOT incremental: this is a plain full-reload overwritePartitions() write, matching the
# majority of this repo's fact tables (etl/create_fact_visual_etl_jobs.py's FACTS list),
# not the resumable-batched/etl_control-watermarked pattern medspacy_nlp.py and
# radiology_nlp.py use -- text concatenation is cheap relative to medSpaCy's per-document
# NLP cost, so neither the Timeout risk nor the incentive for incremental refresh that
# motivated that pattern applies here. PARTITIONED BY (bucket(N, hadm_id)) is therefore an
# ordinary Iceberg hidden partition transform (same as fact_admission), not the
# materialized hadm_bucket column the two NLP fact tables need for their per-bucket
# resumable writes.
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ARGS = getResolvedOptions(sys.argv, ["raw_database", "gold_database", "source_table", "target_table"])
RAW_DB = ARGS["raw_database"]
GOLD_DB = ARGS["gold_database"]
SOURCE_TABLE = ARGS["source_table"]
TARGET_TABLE = ARGS["target_table"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print(f"spark={spark.version}; fact_note_concat: {RAW_DB}.{SOURCE_TABLE} -> glue_catalog.{GOLD_DB}.{TARGET_TABLE}")

raw = (
    spark.table(f"{RAW_DB}.{SOURCE_TABLE}")
    .where("subject_id IS NOT NULL AND hadm_id IS NOT NULL")
    .withColumn("_charttime_ts", F.to_timestamp("charttime"))
    .withColumn("_storetime_ts", F.to_timestamp("storetime"))
)

# admit_provider_id / admit_date_key are pulled from fact_admission so this table shares
# fact_admission's grain (hadm_id, subject_id, admit_provider_id, admit_date_key), matching
# every other archetype instance in the Multimodal Fusion Schema.
admission_keys = spark.table(f"glue_catalog.{GOLD_DB}.fact_admission").select(
    "hadm_id", "admit_provider_id", "admit_date_key"
)

# sort_array on (note_seq, text) structs orders deterministically by note_seq regardless of
# the read's physical row order, then transform() pulls just the text back out in that
# order for concat_ws -- the DataFrame equivalent of "ORDER BY note_seq" inside a group.
result = (
    raw.groupBy("subject_id", "hadm_id")
    .agg(
        F.count(F.lit(1)).alias("notes_count"),
        F.max("_charttime_ts").alias("last_charttime"),
        F.max("_storetime_ts").alias("last_storetime"),
        F.concat_ws(
            "\n\n",
            F.transform(
                F.sort_array(F.collect_list(F.struct(F.col("note_seq"), F.col("text")))),
                lambda note: note["text"],
            ),
        ).alias("text"),
    )
    .join(admission_keys, "hadm_id", "left")
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
    .withColumn("created_by", F.lit("fact_note_concat"))
    .withColumn("updated_by", F.lit("fact_note_concat"))
    .select(
        "subject_id", "hadm_id", "admit_provider_id", "admit_date_key",
        "text", "notes_count", "last_charttime", "last_storetime",
        "created_ts", "updated_ts", "created_by", "updated_by",
    )
)

n = result.count()
result.writeTo(f"glue_catalog.{GOLD_DB}.{TARGET_TABLE}").option("fanout-enabled", "true").overwritePartitions()
print(f"fact_note_concat: wrote {n:,} row(s) to glue_catalog.{GOLD_DB}.{TARGET_TABLE}")
