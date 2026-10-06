#!/usr/bin/env python3
"""Driver script to generate the standalone dim_date seed script."""

import os
import runpy
import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    script_path = repo_root / "etl" / "seed_dim_date.py"
    normalized_path = str(script_path).replace(os.sep, "/")

    if not script_path.exists():
        print(f"Seed script not found: {script_path}", file=sys.stderr)
        raise SystemExit(2)

    try:
        runpy.run_path(normalized_path, run_name="__main__")
    except SystemExit as exc:
        raise SystemExit(exc.code)


if __name__ == "__main__":
    main()
