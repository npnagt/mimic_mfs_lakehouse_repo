#!/usr/bin/env python3
"""Compatibility wrapper for the AWS S3 + Glue setup workflow."""

from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mimic_lakehouse.aws_workflow import main


if __name__ == "__main__":
    raise SystemExit(main())
