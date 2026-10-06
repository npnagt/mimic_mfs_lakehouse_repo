# Standalone PySpark script (Glue 6.0 / Spark 4.1 / Iceberg 1.11): combine the two
# note-type-specific STRING+JSON NLP fact tables -- fact_discharge_note_nlp and
# fact_radiology_note_nlp -- into ONE VARIANT-typed Inference Fact, fact_clinician_note_nlp_v,
# at the shared (subject_id, hadm_id) admission grain both sources already share.
#
# Why a second Inference Fact instance alongside fact_radiology_note_nlp_v (which this job
# does not replace or touch): that table only ever covers radiology notes. This one answers
# "what does the model know about this admission's clinical notes, of either type, in one
# row" -- the schema's fact_admission-grain, all-modalities inference view. An admission with
# only a discharge note, only a radiology note, or both, still gets exactly one row here;
# has_discharge_note / has_radiology_note flag which source(s) contributed.
#
# Why a separate job on Glue 6.0: same reason as radiology_nlp_variant.py -- a VARIANT
# column requires Iceberg format-version 3, which Athena engine v3 cannot read at all.
#
# This job does NO NLP -- it reads the two already-populated STRING tables, FULL OUTER JOINs
# them on hadm_id, and parse_json()s the JSON-shaped columns from whichever side is present.
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as _F

ARGS = getResolvedOptions(
    sys.argv, ["gold_database", "gold_s3_bucket", "target_table", "refresh_mode"]
)
GOLD_DB = ARGS["gold_database"]
GOLD_BUCKET = ARGS["gold_s3_bucket"]
TARGET = f"glue_catalog.{GOLD_DB}.{ARGS['target_table']}"
TARGET_LOCATION = f"s3://{GOLD_BUCKET}/mimic_bus/{ARGS['target_table']}/"
REFRESH_MODE = ARGS["refresh_mode"].strip().lower()  # "auto" | "full"

DISCHARGE_SOURCE = f"glue_catalog.{GOLD_DB}.fact_discharge_note_nlp"
RADIOLOGY_SOURCE = f"glue_catalog.{GOLD_DB}.fact_radiology_note_nlp"

spark = GlueContext(SparkContext()).spark_session
print(f"spark={spark.version}; {DISCHARGE_SOURCE} + {RADIOLOGY_SOURCE} -> {TARGET} "
      f"(VARIANT, format-version 3), refresh={REFRESH_MODE}")

# ---------------------------------------------------------------------------------------
# Incremental refresh via mimic4_db_business.etl_control  (proposition P7) -- same pattern
# as radiology_nlp_variant.py, extended to two sources: an admission is "touched" if EITHER
# source's row for it changed since the watermark, and a touched admission's row is always
# recomputed from BOTH sources' CURRENT data (not just the side that changed), since the
# full outer join result depends on both.
# ---------------------------------------------------------------------------------------
ETL_CONTROL = f"glue_catalog.{GOLD_DB}.etl_control"
CONTROL_KEY = ARGS["target_table"]  # "fact_clinician_note_nlp_v"


def read_watermark():
    try:
        r = (spark.table(ETL_CONTROL)
             .filter(_F.col("aggregate_table") == CONTROL_KEY)
             .select(_F.max("last_processed_ts").alias("wm")).collect())
        return r[0]["wm"] if r and r[0]["wm"] is not None else None
    except Exception as exc:  # noqa: BLE001
        print(f"etl_control not readable ({exc}) -- treating as first run.")
        return None


def advance_watermark(new_ts):
    if new_ts is None:
        return
    (spark.createDataFrame([(CONTROL_KEY, new_ts)], ["aggregate_table", "last_processed_ts"])
     .withColumn("updated_ts", _F.current_timestamp())
     .writeTo(ETL_CONTROL).option("fanout-enabled", "true").overwritePartitions())
    print(f"etl_control: {CONTROL_KEY} watermark advanced to {new_ts}")


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET} (
        subject_id BIGINT,
        hadm_id BIGINT,
        admit_provider_id STRING,
        admit_date_key INT,
        has_discharge_note BOOLEAN,
        has_radiology_note BOOLEAN,
        tobacco_use STRING,
        alcohol_use STRING,
        obesity_level STRING,
        tobacco_cessation_cd STRING,
        recognized_entities VARIANT,
        radiology_note_count INT,
        radiology_procedure_types VARIANT,
        radiology_reasons VARIANT,
        symptoms VARIANT,
        disorders VARIANT,
        icd_codes VARIANT,
        procedures VARIANT,
        findings_summary VARIANT,
        indication_summary VARIANT,
        conclusion VARIANT,
        ner_json VARIANT,
        created_ts TIMESTAMP,
        updated_ts TIMESTAMP,
        created_by STRING,
        updated_by STRING
    )
    USING iceberg
    LOCATION '{TARGET_LOCATION}'
    TBLPROPERTIES ('format-version' = '3', 'write.data.path' = '{TARGET_LOCATION}')
    """
)

discharge_all = spark.table(DISCHARGE_SOURCE)
radiology_all = spark.table(RADIOLOGY_SOURCE)

watermark = None if REFRESH_MODE == "full" else read_watermark()

if watermark is not None:
    wm_ts = _F.lit(watermark).cast("timestamp")
    d_changed = discharge_all.filter(_F.col("updated_ts") > wm_ts)
    r_changed = radiology_all.filter(_F.col("updated_ts") > wm_ts)
    touched_ids = d_changed.select("hadm_id").union(r_changed.select("hadm_id")).distinct()
    touched_count = touched_ids.count()
    if touched_count == 0:
        # os._exit(0), not sys.exit(0): Glue reports a raised SystemExit as FAILED.
        print(f"clinician_note_nlp_variant: no row changed in either source since "
              f"watermark {watermark} -- nothing to project.")
        import os as _os
        _os._exit(0)
    print(f"clinician_note_nlp_variant: incremental -- {touched_count} admission(s) touched "
          f"since {watermark}.")
    discharge_src = discharge_all.join(touched_ids, "hadm_id")
    radiology_src = radiology_all.join(touched_ids, "hadm_id")
    d_max = d_changed.select(_F.max("updated_ts").alias("m")).collect()[0]["m"]
    r_max = r_changed.select(_F.max("updated_ts").alias("m")).collect()[0]["m"]
    new_max_ts = max(ts for ts in (d_max, r_max) if ts is not None)
else:
    print("clinician_note_nlp_variant: full backfill (no watermark).")
    discharge_src = discharge_all
    radiology_src = radiology_all
    d_max = discharge_all.select(_F.max("updated_ts").alias("m")).collect()[0]["m"]
    r_max = radiology_all.select(_F.max("updated_ts").alias("m")).collect()[0]["m"]
    new_max_ts = max(ts for ts in (d_max, r_max) if ts is not None)

discharge_src.createOrReplaceTempView("_disch_src")
radiology_src.createOrReplaceTempView("_rad_src")

# FULL OUTER JOIN: an admission with only one note type still gets exactly one row, with
# the other side's columns NULL (parse_json(NULL) -> NULL, standard null propagation).
projected = spark.sql(
    """
    SELECT
      coalesce(d.subject_id, r.subject_id) AS subject_id,
      coalesce(d.hadm_id, r.hadm_id) AS hadm_id,
      coalesce(d.admit_provider_id, r.admit_provider_id) AS admit_provider_id,
      coalesce(d.admit_date_key, r.admit_date_key) AS admit_date_key,
      (d.hadm_id IS NOT NULL) AS has_discharge_note,
      (r.hadm_id IS NOT NULL) AS has_radiology_note,
      d.tobacco_use AS tobacco_use,
      d.alcohol_use AS alcohol_use,
      d.obesity_level AS obesity_level,
      d.tobacco_cessation_cd AS tobacco_cessation_cd,
      parse_json(d.recognized_entities) AS recognized_entities,
      r.note_count AS radiology_note_count,
      parse_json(r.radiology_procedure_types) AS radiology_procedure_types,
      parse_json(r.radiology_reasons) AS radiology_reasons,
      parse_json(r.symptoms) AS symptoms,
      parse_json(r.disorders) AS disorders,
      parse_json(r.icd_codes) AS icd_codes,
      parse_json(r.procedures) AS procedures,
      parse_json(r.findings_summary) AS findings_summary,
      parse_json(r.indication_summary) AS indication_summary,
      parse_json(r.conclusion) AS conclusion,
      parse_json(r.ner_json) AS ner_json,
      current_timestamp() AS created_ts,
      current_timestamp() AS updated_ts,
      'clinician_note_nlp_variant' AS created_by,
      'clinician_note_nlp_variant' AS updated_by
    FROM _disch_src d
    FULL OUTER JOIN _rad_src r ON d.hadm_id = r.hadm_id
    """
)

if watermark is not None:
    # carry untouched admissions forward, replace only the ones recomputed above
    touched_hadm = [r["hadm_id"] for r in projected.select("hadm_id").distinct().collect()]
    kept = spark.table(TARGET).filter(~_F.col("hadm_id").isin(touched_hadm)).localCheckpoint(eager=True)
    projected = kept.unionByName(projected)
    print(f"clinician_note_nlp_variant: {kept.count()} admission(s) carried forward unchanged.")

# writeTo(...).overwrite(condition) resolves columns by NAME -- see radiology_nlp_variant.py's
# identical comment for why this matters once ALTER TABLE ADD COLUMNS is ever used here.
projected.writeTo(TARGET).overwrite(_F.lit(True))
advance_watermark(new_max_ts)

check = spark.sql(
    f"""
    SELECT
      count(*) AS rows,
      sum(CASE WHEN has_discharge_note AND has_radiology_note THEN 1 ELSE 0 END) AS both_notes,
      sum(CASE WHEN has_discharge_note AND NOT has_radiology_note THEN 1 ELSE 0 END) AS discharge_only,
      sum(CASE WHEN has_radiology_note AND NOT has_discharge_note THEN 1 ELSE 0 END) AS radiology_only
    FROM {TARGET}
    """
).collect()[0]
print(
    f"wrote {check['rows']} row(s) to {TARGET}; both_notes={check['both_notes']}, "
    f"discharge_only={check['discharge_only']}, radiology_only={check['radiology_only']}"
)
