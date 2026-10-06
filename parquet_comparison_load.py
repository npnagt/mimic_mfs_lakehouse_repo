# Standalone PySpark script (Glue 6.0 / Spark 4.1 / Iceberg 1.11): populate the plain-Parquet
# comparison tables defined in ddl/gold/mimic_iv_ddl_parquet_comparison.sql from their Iceberg
# Gold-table counterparts, for the added research question "How does the storage taken by
# Iceberg compare to the storage taken by plain Parquet table format for the Multimodal
# Fusion Schema?"
#
# Requires Glue 6.0 (not 4.0/5.0) because one source table, fact_clinician_note_nlp_v, is
# Iceberg format-version 3 (VARIANT columns) -- only Glue 6.0's Spark/Iceberg build can open
# it at all. The other six source tables are ordinary format-v2 Iceberg, readable on any
# Glue version, so running everything on 6.0 keeps this to one job.
#
# Does NO transformation beyond flattening VARIANT back to a JSON string (plain Parquet/Hive
# has no VARIANT type): every other table is a straight, unmodified copy of its Iceberg
# source, so a byte-for-byte storage comparison isolates table FORMAT as the only variable.
#
# Prerequisite: ddl/gold/mimic_iv_ddl_parquet_comparison.sql has been applied (the *_parquet
# external tables must already exist in the Glue Data Catalog) -- this job only writes data
# files to the S3 locations those tables were created against; it does not issue DDL, except
# for fact_clinician_note_nlp_v_parquet's SOURCE table, which this job does not create either
# (clinician_note_nlp_variant.py owns that DDL, since it is format-v3-only and Athena cannot
# create format-v3 tables -- see that job's comments).
#
# Usage (via the job creator): python run_parquet_comparison_load.py --create-role --run-now
#
# Supports the paper's RQ1 ("Multimodal Fusion Schema" paper). See the README's "Research
# paper: RQ1 and RQ2" section.
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

CODEC = "zstd"
S3_PREFIX = "parquet_compare"

ARGS = getResolvedOptions(sys.argv, ["gold_database", "gold_s3_bucket", "tables"])
GOLD_DB = ARGS["gold_database"]
GOLD_BUCKET = ARGS["gold_s3_bucket"]
# "all" (default) or a comma-separated subset of target (*_parquet) or source table names,
# so a single table can be re-populated without re-copying all five -- e.g. after fixing a
# defect in one archetype's data without paying to re-copy the other four.
_raw_filter = ARGS.get("tables", "all") or "all"
WANTED = {t.strip() for t in _raw_filter.split(",") if t.strip()}
ALL_WANTED = WANTED == {"all"}

spark = GlueContext(SparkContext()).spark_session
print(f"spark={spark.version}; parquet_comparison_load -> {GOLD_DB} (tables={_raw_filter})")

# ---------------------------------------------------------------------------------------
# Plain copies: identical schema, identical rows, Iceberg -> Parquet only.
# ---------------------------------------------------------------------------------------
PLAIN_COPIES = [
    ("fact_admission", "fact_admission_parquet"),
    ("obt_admission_features", "obt_admission_features_parquet"),
    ("fact_discharge_note", "fact_discharge_note_parquet"),
    ("fact_radiology_note", "fact_radiology_note_parquet"),
    ("fact_discharge_note_nlp", "fact_discharge_note_nlp_parquet"),
    ("fact_radiology_note_nlp", "fact_radiology_note_nlp_parquet"),
]


def wanted(source_table: str, target_table: str) -> bool:
    return ALL_WANTED or target_table in WANTED or source_table in WANTED


def copy_plain(source_table: str, target_table: str) -> None:
    if not wanted(source_table, target_table):
        print(f"skip {source_table} -> {target_table} (not in --tables filter)")
        return
    target_location = f"s3://{GOLD_BUCKET}/{S3_PREFIX}/{target_table}/"
    df = spark.table(f"glue_catalog.{GOLD_DB}.{source_table}")
    n = df.count()
    df.write.mode("overwrite").option("compression", CODEC).parquet(target_location)
    print(f"wrote {n:,} row(s): {source_table} (Iceberg) -> {target_table} (Parquet {CODEC}) at {target_location}")


for src, tgt in PLAIN_COPIES:
    copy_plain(src, tgt)

# ---------------------------------------------------------------------------------------
# VARIANT flatten: fact_clinician_note_nlp_v (Iceberg format-v3, VARIANT columns) has no
# Parquet-representable equivalent for its defining feature, so every VARIANT column is
# serialized back to a JSON string with to_json(). This makes its row of the comparison
# Iceberg-VARIANT-vs-Parquet-STRING, not a same-type format swap; report it as such.
# ---------------------------------------------------------------------------------------
def copy_variant_flatten(source_table: str, target_table: str, json_cols: list[str], all_cols: list[str]) -> None:
    if not wanted(source_table, target_table):
        print(f"skip {source_table} -> {target_table} (not in --tables filter)")
        return
    target_location = f"s3://{GOLD_BUCKET}/{S3_PREFIX}/{target_table}/"
    src_df = spark.table(f"glue_catalog.{GOLD_DB}.{source_table}")
    select_sql = ", ".join((f"to_json({c}) AS {c}" if c in json_cols else c) for c in all_cols)
    view_name = f"_variant_src_{target_table}"
    src_df.createOrReplaceTempView(view_name)
    flattened = spark.sql(f"SELECT {select_sql} FROM {view_name}")
    n = flattened.count()
    flattened.write.mode("overwrite").option("compression", CODEC).parquet(target_location)
    print(f"wrote {n:,} row(s): {source_table} (Iceberg VARIANT) -> {target_table} "
          f"(Parquet {CODEC} STRING) at {target_location}")


CLINICIAN_JSON_COLS = [
    "recognized_entities", "radiology_procedure_types", "radiology_reasons", "symptoms",
    "disorders", "icd_codes", "procedures", "findings_summary", "indication_summary",
    "conclusion", "ner_json",
]
copy_variant_flatten(
    "fact_clinician_note_nlp_v", "fact_clinician_note_nlp_v_parquet",
    CLINICIAN_JSON_COLS,
    ["subject_id", "hadm_id", "admit_provider_id", "admit_date_key",
     "has_discharge_note", "has_radiology_note",
     "tobacco_use", "alcohol_use", "obesity_level", "tobacco_cessation_cd",
     "recognized_entities", "radiology_note_count",
     *[c for c in CLINICIAN_JSON_COLS if c != "recognized_entities"],
     "created_ts", "updated_ts", "created_by", "updated_by"],
)

print("parquet_comparison_load: done.")
