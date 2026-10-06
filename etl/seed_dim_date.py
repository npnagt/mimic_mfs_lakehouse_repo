#!/usr/bin/env python3
"""Generate a standalone Glue PySpark seed script for the dim_date dimension."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mimic_lakehouse import config  # noqa: E402


def build_dim_date_snippet(
    start_date: str = "2100-01-01",
    end_date: str = "2230-12-31",
    gold_database: str = "mimic4_db_business",
) -> str:
    """Return a standalone Glue PySpark snippet to seed dim_date.

    MIMIC-IV timestamps are date-shifted into the future for de-identification, so the
    date range must cover the shifted study period, not the real calendar years.
    The snippet is intended to run as a Glue script job.
    """
    return f'''
# Standalone PySpark snippet for seeding dim_date -- run as a script-mode Glue job,
# not a Visual ETL job (no built-in "generate date sequence" visual node exists).
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.sql(
    "SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date"
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

# INSERT OVERWRITE ... SELECT * binds columns positionally, not by name -- this
# order must match the mimic4_db_business.dim_date column order exactly (see
# ddl/gold/mimic_iv_ddl_gold_combined_v4.sql), or values land in the wrong columns.
df = df.select(
    "date_key", "full_date", "day_of_week", "day_name", "day_name_short",
    "day_of_month", "day_of_year", "week_of_year", "week_start_date", "week_end_date",
    "month_number", "month_name", "month_name_short", "first_day_of_month", "last_day_of_month",
    "quarter_number", "quarter_name", "year", "is_weekday", "is_weekend",
    "is_holiday", "holiday_name", "fiscal_month", "fiscal_quarter", "fiscal_year",
    "created_ts", "updated_ts", "created_by", "updated_by",
)

df.createOrReplaceTempView("dim_date_seed")
spark.sql(
    "INSERT OVERWRITE glue_catalog.{gold_database}.dim_date SELECT * FROM dim_date_seed"
)
'''


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="OUTPUT_PATH",
        help="Write the PySpark dim_date seed snippet to OUTPUT_PATH",
    )
    parser.add_argument(
        "--start-date",
        default="2100-01-01",
        help="Start date for the dim_date range (default: 2100-01-01)",
    )
    parser.add_argument(
        "--end-date",
        default="2230-12-31",
        help="End date for the dim_date range (default: 2230-12-31)",
    )
    config.add_dataset_arg(parser)
    parser.add_argument(
        "--gold-database",
        default=None,
        help="Gold Glue database for the INSERT OVERWRITE target (default: from --dataset)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    output_path = args.output
    if not output_path:
        parser.error("--output is required")

    gold_database = args.gold_database or config.dataset_profile(args.dataset).gold_database
    output_file = Path(output_path)
    output_file.write_text(
        build_dim_date_snippet(args.start_date, args.end_date, gold_database),
        encoding="utf-8",
    )
    print(f"Wrote dim_date seeding snippet to {output_file}")


if __name__ == "__main__":
    main()
