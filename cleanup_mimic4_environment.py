#!/usr/bin/env python3
"""Entry point for the full environment cleanup workflow.

Drops every AWS object created by this project's setup/ETL scripts (Glue databases and
tables, jobs, crawlers, classifiers, and IAM roles) and empties the S3 buckets, without
deleting the buckets themselves. Defaults to a dry run -- pass --yes to actually apply.

Run whenever a complete environment reset is needed before re-running the setup workflow.
"""

from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mimic_lakehouse.cleanup import main


if __name__ == "__main__":
    raise SystemExit(main())
