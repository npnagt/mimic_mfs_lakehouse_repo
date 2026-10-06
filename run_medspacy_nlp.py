#!/usr/bin/env python3
"""Driver script to invoke the medSpaCy NLP job generator in `etl/`.

Forwards command-line arguments to `etl/create_medspacy_nlp_job.py` and propagates
its exit code. Defaults for bucket names / region come from `.env` (see .env.example).
Rule-based, local -- no managed service, no per-character billing.
"""
import os
import runpy
import sys


def main():
    repo_root = os.path.dirname(__file__)
    script_path = os.path.join(repo_root, "etl", "create_medspacy_nlp_job.py")
    if not os.path.exists(script_path):
        print(f"ETL script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)

    try:
        runpy.run_path(script_path, run_name="__main__")
    except SystemExit as e:
        raise SystemExit(e.code)


if __name__ == "__main__":
    main()
