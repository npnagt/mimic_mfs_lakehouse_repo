# Standalone PySpark script (Glue 6.0 / Spark 4.1 / Iceberg 1.11): populate the plain-CSV
# comparison tables defined in ddl/gold/mimic_iv_ddl_csv_comparison.sql from their Iceberg
# Gold-table counterparts -- the CSV leg of the three-way Iceberg/Parquet/CSV storage
# comparison (see that DDL's header for the research question and design decisions).
#
# Same source set and same Glue-6.0-for-VARIANT requirement as parquet_comparison_load.py.
#
# Every *_csv table is declared with STRING-only columns (Athena's OpenCSVSerDe requirement
# -- see design decision 1 in the DDL), so every column here is explicitly cast to string
# before write, not left to Spark's CSV writer to infer.
#
# Embedded newlines in `text` (fact_discharge_note / fact_radiology_note) are escaped to the
# literal two-character sequences \n / \r before write -- see design decision 2 in the DDL:
# Hive/Presto/Athena's CSV record splitting happens on physical newline bytes before the
# SerDe ever parses a line, so a raw embedded newline would silently corrupt row boundaries
# regardless of RFC4180 quote-escaping.
#
# Output files are gzip-compressed (.csv.gz) -- this is what a real-world CSV export
# ordinarily looks like, rather than leaving the comparison's CSV leg as uncompressed plain
# text while Iceberg/Parquet both compress their data (ZSTD and Snappy respectively).
#
# Prerequisite: ddl/gold/mimic_iv_ddl_csv_comparison.sql has been applied.
#
# Usage (via the job creator): python run_csv_comparison_load.py --create-role --run-now
#
# Supports the paper's RQ1 ("Multimodal Fusion Schema" paper). See the README's "Research
# paper: RQ1 and RQ2" section.
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ARGS = getResolvedOptions(sys.argv, ["gold_database", "gold_s3_bucket", "tables"])
GOLD_DB = ARGS["gold_database"]
GOLD_BUCKET = ARGS["gold_s3_bucket"]
_raw_filter = ARGS.get("tables", "all") or "all"
WANTED = {t.strip() for t in _raw_filter.split(",") if t.strip()}
ALL_WANTED = WANTED == {"all"}

spark = GlueContext(SparkContext()).spark_session
print(f"spark={spark.version}; csv_comparison_load -> {GOLD_DB} (tables={_raw_filter})")

# gzip-compressed: Hive/Presto/Athena's TextInputFormat auto-detects and transparently
# decompresses a .gz file by extension on read, so no DDL change is needed for this --
# STORED AS TEXTFILE already covers it.
CSV_OPTIONS = {"sep": ",", "quote": '"', "escape": "\\", "header": "false", "compression": "gzip"}

# Columns whose raw value can contain an embedded newline and must be escaped before write
# (see design decision 2). JSON-shaped STRING columns need no such handling: json.dumps()
# already escapes embedded newlines to the literal two-character \n, never a raw byte.
NEWLINE_UNSAFE_COLS = {"text"}


def _stringify(df):
    cols = []
    for name, dtype in df.dtypes:
        c = F.col(name).cast("string")
        if name in NEWLINE_UNSAFE_COLS:
            c = F.regexp_replace(F.regexp_replace(c, "\r\n", "\\\\r\\\\n"), "\n", "\\\\n")
        cols.append(c.alias(name))
    return df.select(*cols)


def wanted(source_table: str, target_table: str) -> bool:
    return ALL_WANTED or target_table in WANTED or source_table in WANTED


# ---------------------------------------------------------------------------------------
# Plain copies: identical schema (stringified), identical rows, Iceberg -> CSV.
# ---------------------------------------------------------------------------------------
PLAIN_COPIES = [
    ("fact_admission", "fact_admission_csv"),
    ("obt_admission_features", "obt_admission_features_csv"),
    ("fact_discharge_note", "fact_discharge_note_csv"),
    ("fact_radiology_note", "fact_radiology_note_csv"),
    ("fact_discharge_note_nlp", "fact_discharge_note_nlp_csv"),
    ("fact_radiology_note_nlp", "fact_radiology_note_nlp_csv"),
]


def copy_plain(source_table: str, target_table: str) -> None:
    if not wanted(source_table, target_table):
        print(f"skip {source_table} -> {target_table} (not in --tables filter)")
        return
    target_location = f"s3://{GOLD_BUCKET}/csv_compare/{target_table}/"
    df = _stringify(spark.table(f"glue_catalog.{GOLD_DB}.{source_table}"))
    n = df.count()
    df.write.mode("overwrite").options(**CSV_OPTIONS).csv(target_location)
    print(f"wrote {n:,} row(s): {source_table} (Iceberg) -> {target_table} (CSV) at {target_location}")


for src, tgt in PLAIN_COPIES:
    copy_plain(src, tgt)

# ---------------------------------------------------------------------------------------
# VARIANT flatten: fact_clinician_note_nlp_v -> fact_clinician_note_nlp_v_csv. Same
# to_json() flatten as parquet_comparison_load.py, then stringified/written as CSV.
# ---------------------------------------------------------------------------------------
def copy_variant_flatten(source_table: str, target_table: str, json_cols: list, all_cols: list) -> None:
    if not wanted(source_table, target_table):
        print(f"skip {source_table} -> {target_table} (not in --tables filter)")
        return
    target_location = f"s3://{GOLD_BUCKET}/csv_compare/{target_table}/"
    src_df = spark.table(f"glue_catalog.{GOLD_DB}.{source_table}")
    select_sql = ", ".join((f"to_json({c}) AS {c}" if c in json_cols else c) for c in all_cols)
    view_name = f"_variant_src_csv_{target_table}"
    src_df.createOrReplaceTempView(view_name)
    flattened = _stringify(spark.sql(f"SELECT {select_sql} FROM {view_name}"))
    n = flattened.count()
    flattened.write.mode("overwrite").options(**CSV_OPTIONS).csv(target_location)
    print(f"wrote {n:,} row(s): {source_table} (Iceberg VARIANT) -> {target_table} (CSV) at {target_location}")


CLINICIAN_JSON_COLS = [
    "recognized_entities", "radiology_procedure_types", "radiology_reasons", "symptoms",
    "disorders", "icd_codes", "procedures", "findings_summary", "indication_summary",
    "conclusion", "ner_json",
]
copy_variant_flatten(
    "fact_clinician_note_nlp_v", "fact_clinician_note_nlp_v_csv",
    CLINICIAN_JSON_COLS,
    ["subject_id", "hadm_id", "admit_provider_id", "admit_date_key",
     "has_discharge_note", "has_radiology_note",
     "tobacco_use", "alcohol_use", "obesity_level", "tobacco_cessation_cd",
     "recognized_entities", "radiology_note_count",
     *[c for c in CLINICIAN_JSON_COLS if c != "recognized_entities"],
     "created_ts", "updated_ts", "created_by", "updated_by"],
)

print("csv_comparison_load: done.")
