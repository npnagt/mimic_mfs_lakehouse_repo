#!/usr/bin/env python3
"""Driver for the Iceberg-vs-Parquet comparison table load job generator in `etl/`.

Forwards arguments to `etl/create_parquet_comparison_job.py`. Standalone -- does not
require running run_lakehouse_pipeline.py first, only that the Gold tables it copies from
already have data and that ddl/gold/mimic_iv_ddl_parquet_comparison.sql has been applied.

Usage:
  python run_parquet_comparison_load.py --dataset fulldataset --create-role --run-now
"""
import os
import runpy
import sys


def main():
    script_path = os.path.join(os.path.dirname(__file__), "etl", "create_parquet_comparison_job.py")
    if not os.path.exists(script_path):
        print(f"ETL script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)
    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        raise SystemExit(e.code)


if __name__ == "__main__":
    main()
