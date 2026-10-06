#!/usr/bin/env python3
"""Driver for the remove_orphan_files job generator in `etl/`.

Forwards arguments to `etl/create_remove_orphan_files_job.py`. Run AFTER
compact_gold_tables.py (OPTIMIZE + VACUUM) on the same tables.

Usage:
  python run_remove_orphan_files.py --dataset fulldataset --create-role --run-now \\
      --tables fact_admission,obt_admission_features,fact_discharge_note_nlp,fact_radiology_note_nlp
"""
import os
import runpy
import sys


def main():
    script_path = os.path.join(os.path.dirname(__file__), "etl", "create_remove_orphan_files_job.py")
    if not os.path.exists(script_path):
        print(f"ETL script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)
    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        raise SystemExit(e.code)


if __name__ == "__main__":
    main()
