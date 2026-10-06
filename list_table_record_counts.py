#!/usr/bin/env python3
"""List every table in the raw and Gold Glue databases for one --dataset, with its live
record count -- dimensions first, then facts, then everything else (raw source tables,
aggregates/OBTs, etl_control/etl_process_log, NLP facts, and the Parquet/CSV
format-comparison tables).

Classification is a plain table-name prefix check (dim_* / fact_* / other). Raw-schema
tables keep their native CSV-derived names (patients, admissions, discharge_note_raw, ...)
and so fall into "other" alongside Gold's agg_*/obt_*/etl_* tables -- this script does not
try to map a raw table to the dim/fact it feeds.

Row counts: Iceberg tables are counted via their "$files" system table's record_count sum
(cheap, metadata-only, no data scan -- the same technique measure_storage_footprint.py and
measure_parquet_comparison.py already use). Every other table (raw crawled CSV->Parquet
tables in mimic4_db_raw, and the plain Parquet/CSV format-comparison tables in Gold) has no
such metadata table and is counted with a full SELECT count(*). fact_radiology_note_nlp_v
(Iceberg format-version 3) shows "n/a" -- Athena engine v3 cannot read it or its "$files"
table at all; see its Parquet/CSV siblings for the same count instead.

Usage:
    python list_table_record_counts.py --dataset fulldataset
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
from mimic_lakehouse import config as _cfg  # noqa: E402


def _make_console_utf8_safe() -> None:
    enc = (sys.stdout.encoding or "").lower()
    if enc and enc != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def list_tables(glue, database: str) -> list[dict]:
    tables = []
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        tables.extend(page["TableList"])
    return tables


def is_iceberg(table: dict) -> bool:
    return table.get("Parameters", {}).get("table_type", "").upper() == "ICEBERG"


def category(table_name: str) -> tuple[int, str]:
    """(sort_rank, label) -- dimensions first, then facts, then everything else."""
    if table_name.startswith("dim_"):
        return 0, "dimension"
    if table_name.startswith("fact_"):
        return 1, "fact"
    return 2, "other"


def run_athena(athena, sql: str, output_location: str, workgroup: str, poll: float = 1.5):
    qid = athena.start_query_execution(
        QueryString=sql, ResultConfiguration={"OutputLocation": output_location}, WorkGroup=workgroup,
    )["QueryExecutionId"]
    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll)
    if state != "SUCCEEDED":
        return None
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return rows[1]["Data"][0].get("VarCharValue") if len(rows) > 1 else None


def row_count(athena, output_location: str, workgroup: str, database: str, table: dict):
    name = table["Name"]
    if is_iceberg(table):
        sql = f'SELECT coalesce(sum(record_count), 0) FROM "{database}"."{name}$files"'
    else:
        sql = f'SELECT count(*) FROM "{database}"."{name}"'
    val = run_athena(athena, sql, output_location, workgroup)
    return int(val) if val is not None else None


def main() -> int:
    _make_console_utf8_safe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _cfg.add_dataset_arg(p)
    p.add_argument("--region", default=_cfg.get("AWS_REGION", "us-east-2"))
    p.add_argument("--athena-workgroup", default=_cfg.get("ATHENA_WORKGROUP", "primary"))
    args = p.parse_args()

    profile = _cfg.dataset_profile(args.dataset)
    glue = _cfg.RefreshingClient("glue", args.region)
    athena = _cfg.RefreshingClient("athena", args.region)
    output_location = f"s3://{profile.gold_bucket}/athena-results/"

    databases = [profile.raw_database, profile.gold_database]
    entries = []
    for db in databases:
        for t in list_tables(glue, db):
            rank, label = category(t["Name"])
            entries.append({"database": db, "name": t["Name"], "rank": rank,
                            "category": label, "table": t})
    entries.sort(key=lambda e: (e["rank"], e["database"], e["name"]))

    print(f"Counting rows for {len(entries)} table(s) across {', '.join(databases)}...", file=sys.stderr)
    for i, e in enumerate(entries, 1):
        e["row_count"] = row_count(athena, output_location, args.athena_workgroup, e["database"], e["table"])
        n = f"{e['row_count']:,}" if e["row_count"] is not None else "n/a"
        print(f"  ({i}/{len(entries)}) {e['database']}.{e['name']}: {n}", file=sys.stderr)

    print()
    print(f"{'Database':26} {'Table':42} {'Row count':>14}")
    print("-" * 84)
    current_cat = None
    category_total = 0
    grand_total = 0
    for e in entries:
        if e["category"] != current_cat:
            if current_cat is not None:
                print(f"{'':26} {'(' + current_cat + ' subtotal)':42} {category_total:>14,}")
                print()
            current_cat = e["category"]
            category_total = 0
            print(f"-- {current_cat.upper()} --")
        n = e["row_count"]
        n_str = f"{n:,}" if n is not None else "n/a"
        print(f"{e['database']:26} {e['name']:42} {n_str:>14}")
        if n is not None:
            category_total += n
            grand_total += n
    if current_cat is not None:
        print(f"{'':26} {'(' + current_cat + ' subtotal)':42} {category_total:>14,}")
    print("-" * 84)
    print(f"{'':26} {'TOTAL':42} {grand_total:>14,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
