# Standalone PySpark script for ingesting MIMIC-IV-Note CSVs into the raw layer --
# run as a script-mode Glue job (see etl/create_notes_ingest_job.py), not a Visual
# ETL job.
#
# WHY THIS EXISTS: discharge.csv.gz / radiology.csv.gz have a free-text `text`
# column containing embedded newlines (and commas, quotes) inside RFC-4180 quoted
# fields. Every Hive/Athena CSV SerDe reads through Hadoop TextInputFormat, which
# splits on \n, so one note record is shredded across hundreds of physical lines and
# the typed columns (subject_id BIGINT ...) throw BAD_DATA. Spark's DataFrame CSV
# reader with multiLine=true parses records correctly across newlines; this job does
# that once and rewrites the result as Parquet, which is safe to catalog and query.
#
# Job arguments (set by etl/create_notes_ingest_job.py):
#   --raw_s3_bucket   S3 bucket holding the raw prefixes (default: mimic4-datalake-v3-2)
#   --raw_database    Glue database for the raw tables (default: mimic4_db_raw)
#   --demo_only       "true"/"false" -- filter notes to mimic4_db_raw.demo_subject_raw
#                     so the note tables stay demo-scoped like the rest of the repo
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["raw_s3_bucket", "raw_database", "demo_only"])
RAW_BUCKET = args["raw_s3_bucket"]
RAW_DB = args["raw_database"]
DEMO_ONLY = args["demo_only"].strip().lower() == "true"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# discharge and radiology share the same column layout (note_id, subject_id, hadm_id,
# note_type, note_seq, charttime, storetime, text). The *_detail files (note_id,
# subject_id, field_name, field_value, field_ordinal) are handled the same way if
# present -- add them here when they are downloaded.
INT_COLS = {"subject_id", "hadm_id", "note_seq", "field_ordinal"}
NOTE_SOURCES = [
    {"name": "discharge", "src_prefix": "discharge_raw", "out_prefix": "discharge_note_raw"},
    {"name": "radiology", "src_prefix": "radiology_raw", "out_prefix": "radiology_note_raw"},
    {"name": "discharge_detail", "src_prefix": "discharge_detail_raw", "out_prefix": "discharge_detail_note_raw"},
    {"name": "radiology_detail", "src_prefix": "radiology_detail_raw", "out_prefix": "radiology_detail_note_raw"},
]

demo_ids = None
if DEMO_ONLY:
    demo_ids = spark.table(f"{RAW_DB}.demo_subject_raw").select("subject_id").distinct()
    print(f"demo_only: restricting to {demo_ids.count()} subject_id(s) from {RAW_DB}.demo_subject_raw")


def source_has_objects(prefix: str) -> bool:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(f"s3://{RAW_BUCKET}/{prefix}/")
    fs = path.getFileSystem(hadoop_conf)
    try:
        return bool(fs.exists(path)) and len(fs.listStatus(path)) > 0
    except Exception:  # noqa: BLE001 -- missing path / transient list error -> treat as absent
        return False


for src in NOTE_SOURCES:
    name, src_prefix, out_prefix = src["name"], src["src_prefix"], src["out_prefix"]
    input_path = f"s3://{RAW_BUCKET}/{src_prefix}/"
    output_path = f"s3://{RAW_BUCKET}/{out_prefix}/"

    if not source_has_objects(src_prefix):
        print(f"{name}: no objects under {input_path} -- skipping.")
        continue

    df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(input_path)
    )
    # Keep raw timestamp columns as strings (raw-layer convention across this repo);
    # cast only the known integer keys.
    for col in df.columns:
        if col in INT_COLS:
            df = df.withColumn(col, F.col(col).cast("long"))

    if demo_ids is not None and "subject_id" in df.columns:
        df = df.join(F.broadcast(demo_ids), on="subject_id", how="left_semi")

    # A join-on "subject_id" moves the key to column 0; restore the source column
    # order so the Parquet schema matches the raw CSV header (and the raw-layer DDL).
    preferred = ["note_id", "subject_id", "hadm_id", "note_type", "note_seq",
                 "charttime", "storetime", "text",
                 "field_name", "field_value", "field_ordinal"]
    ordered = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df.select(*ordered)

    df = df.persist()
    row_count = df.count()
    df.repartition(1).write.mode("overwrite").parquet(output_path)
    df.unpersist()

    spark.sql(f"DROP TABLE IF EXISTS {RAW_DB}.{out_prefix}")
    spark.sql(f"CREATE TABLE {RAW_DB}.{out_prefix} USING parquet LOCATION '{output_path}'")
    print(f"{name}: wrote {row_count} row(s) -> {output_path} and catalogued {RAW_DB}.{out_prefix}")

print("notes_ingest: done.")
