#!/usr/bin/env python3
"""Driver script to invoke the aggregate-refresh Visual ETL job generator in `etl/`.

This forwards command-line arguments to `etl/create_agg_visual_etl_jobs.py` and
propagates its exit code.
"""
import os
import runpy
import sys


def main():
    repo_root = os.path.dirname(__file__)
    script_path = os.path.join(repo_root, "etl", "create_agg_visual_etl_jobs.py")
    if not os.path.exists(script_path):
        print(f"ETL script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)

    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        raise SystemExit(e.code)


if __name__ == "__main__":
    main()
