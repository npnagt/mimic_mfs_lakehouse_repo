#!/usr/bin/env python3
"""Driver for the combined clinician-note VARIANT-projection job generator in `etl/`.

Forwards arguments to `etl/create_clinician_note_variant_job.py`. Run AFTER both
run_medspacy_nlp.py and run_radiology_nlp.py -- it reads both jobs' output tables.
"""
import os
import runpy
import sys


def main():
    script_path = os.path.join(os.path.dirname(__file__), "etl", "create_clinician_note_variant_job.py")
    if not os.path.exists(script_path):
        print(f"ETL script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)
    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        raise SystemExit(e.code)


if __name__ == "__main__":
    main()
