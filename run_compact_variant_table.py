#!/usr/bin/env python3
"""Driver for the format-v3 Iceberg compaction job generator in `etl/`.

Forwards arguments to `etl/create_compact_variant_table_job.py`. Standalone -- run any time
against fact_radiology_note_nlp_v (or any other format-v3 table); does not require running
run_lakehouse_pipeline.py first.

Usage:
  python run_compact_variant_table.py --dataset fulldataset --create-role --run-now \\
      --tables fact_radiology_note_nlp_v
"""
import os
import runpy
import sys


def main():
    script_path = os.path.join(os.path.dirname(__file__), "etl", "create_compact_variant_table_job.py")
    if not os.path.exists(script_path):
        print(f"ETL script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)
    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        raise SystemExit(e.code)


if __name__ == "__main__":
    main()
