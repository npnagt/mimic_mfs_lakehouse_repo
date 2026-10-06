# Standalone PySpark snippet for seeding dim_date -- run as a script-mode Glue job,
# not a Visual ETL job (no built-in "generate date sequence" visual node exists).
#
# Structured as importable functions guarded by `if __name__ == "__main__"` so the
# module can be imported (e.g. by tests) without a live Spark/Glue runtime; Glue runs
# the uploaded script as __main__, so main() still fires there.
import sys

from pyspark.sql import functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# --gold_database is passed by etl/create_dim_date_seed_job.py; the default keeps the
# old behaviour if the job is somehow run without it.
try:
    from awsglue.utils import getResolvedOptions

    _GOLD_DB = getResolvedOptions(sys.argv, ["gold_database"])["gold_database"]
except Exception:  # noqa: BLE001 -- arg absent, or not running under Glue (tests / import)
    _GOLD_DB = "mimic4_db_business"
TARGET_TABLE = f"glue_catalog.{_GOLD_DB}.dim_date"

# INSERT OVERWRITE ... SELECT * binds columns positionally, not by name -- this order
# must match the mimic4_db_business.dim_date column order exactly (see
# ddl/gold/mimic_iv_ddl_gold_combined_v4.sql), or values land in the wrong columns.
DIM_DATE_COLUMNS = [
    "date_key", "full_date", "day_of_week", "day_name", "day_name_short",
    "day_of_month", "day_of_year", "week_of_year", "week_start_date", "week_end_date",
    "month_number", "month_name", "month_name_short", "first_day_of_month", "last_day_of_month",
    "quarter_number", "quarter_name", "year", "is_weekday", "is_weekend",
    "is_holiday", "holiday_name", "fiscal_month", "fiscal_quarter", "fiscal_year",
    "created_ts", "updated_ts", "created_by", "updated_by",
]


def create_spark_session():
    return GlueContext(SparkContext()).spark_session


def build_dim_date(spark):
    df = spark.sql(
        # 2100..2230 -- MIMIC-IV de-identification date-shifting pushes some full-dataset
    # events to ~2215 (max dischtime 2214-12-24); the old 2210 endpoint dropped ~275
    # admissions on a dim_date join. 2230 leaves margin; the extra rows cost ~nothing.
    "SELECT explode(sequence(to_date('2100-01-01'), to_date('2230-12-31'), interval 1 day)) AS full_date"
    )
    df = (
        df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
          .withColumn("day_of_week", F.dayofweek("full_date"))
          .withColumn("day_name", F.date_format("full_date", "EEEE"))
          .withColumn("day_name_short", F.date_format("full_date", "EEE"))
          .withColumn("day_of_month", F.dayofmonth("full_date"))
          .withColumn("day_of_year", F.dayofyear("full_date"))
          .withColumn("week_of_year", F.weekofyear("full_date"))
          .withColumn("week_start_date", F.date_sub("full_date", F.dayofweek("full_date") - 1))
          .withColumn("week_end_date", F.date_add(F.col("week_start_date"), 6))
          .withColumn("month_number", F.month("full_date"))
          .withColumn("month_name", F.date_format("full_date", "MMMM"))
          .withColumn("month_name_short", F.date_format("full_date", "MMM"))
          .withColumn("first_day_of_month", F.trunc("full_date", "month"))
          .withColumn("last_day_of_month", F.last_day("full_date"))
          .withColumn("quarter_number", F.quarter("full_date"))
          .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("full_date")))
          .withColumn("year", F.year("full_date"))
          .withColumn("is_weekday", F.dayofweek("full_date").between(2, 6))
          .withColumn("is_weekend", ~F.dayofweek("full_date").between(2, 6))
          .withColumn("is_holiday", F.lit(False))   # see Gold DDL note: shifted dates, not meaningful
          .withColumn("holiday_name", F.lit(None).cast("string"))
          .withColumn("fiscal_month", F.month("full_date"))     # adjust if fiscal year != calendar year
          .withColumn("fiscal_quarter", F.quarter("full_date"))
          .withColumn("fiscal_year", F.year("full_date"))
          .withColumn("created_ts", F.current_timestamp())
          .withColumn("updated_ts", F.current_timestamp())
          .withColumn("created_by", F.lit("glue_visual_etl"))
          .withColumn("updated_by", F.lit("glue_visual_etl"))
    )
    return df.select(*DIM_DATE_COLUMNS)


def main():
    spark = create_spark_session()
    df = build_dim_date(spark)
    df.createOrReplaceTempView("dim_date_seed")
    spark.sql(f"INSERT OVERWRITE {TARGET_TABLE} SELECT * FROM dim_date_seed")


if __name__ == "__main__":
    main()
