#!/usr/bin/env python3
"""Compact every Iceberg table in mimic4_db_business via Athena OPTIMIZE, and
optionally VACUUM to reclaim the physical S3 bytes.

`OPTIMIZE <t> REWRITE DATA USING BIN_PACK` rewrites the current snapshot's data
files into fewer, larger files -- fixing the small-file fragmentation that inflates
this project's Gold layer (see docs/storage_footprint_analysis.md). It does NOT
delete the superseded files; `VACUUM` does that by expiring old snapshots.

Athena OPTIMIZE processes at most 100 partitions per call and returns
ICEBERG_OPTIMIZE_MORE_RUNS_NEEDED when more remain -- this script re-runs each
table's OPTIMIZE until it reports done (--max-optimize-rounds, default 30).

This MUTATES the tables (a new snapshot per OPTIMIZE) and scans data via Athena
(a real, billed cost -- roughly the live-snapshot size of each table). It is
reversible via Iceberg time-travel until VACUUM runs.

Usage
-----
  python compact_gold_tables.py --dry-run          # print the plan, run nothing
  python compact_gold_tables.py                    # OPTIMIZE every Iceberg table
  python compact_gold_tables.py --only fact_admission,dim_diagnosis
  python compact_gold_tables.py --skip fact_chart_observation,fact_input_event
  python compact_gold_tables.py --vacuum --vacuum-max-age 300   # + reclaim S3 bytes now
  python compact_gold_tables.py --measure          # run measure_storage_footprint.py before & after

fact_clinician_note_nlp_v is Iceberg format-version 3 -- Athena engine v3 cannot
OPTIMIZE it; it is skipped with a note (compact it from Glue 6.0 / Spark, or via
compact_variant_table.py -- see that script's module docstring for the full
rewrite/expire/orphan-removal sequence).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
from mimic_lakehouse import config as _cfg  # noqa: E402


def env(key, default=None):
    return _cfg.get(key, default)

DEFAULT_VACUUM_MAX_AGE = 432000  # Iceberg / Athena default: 5 days


def human(nbytes) -> str:
    n = float(nbytes or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{int(n):,} B"
        n /= 1024
    return f"{n:.1f} TiB"


def athena(client, sql: str, output: str, workgroup: str, poll: float = 3.0) -> dict:
    """Run one statement. Returns {state, scanned_bytes, elapsed_ms, rows, reason}."""
    t0 = time.time()
    qid = client.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": output},
        WorkGroup=workgroup,
    )["QueryExecutionId"]
    while True:
        ex = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll)
    stats = ex.get("Statistics", {})
    out = {
        "query_id": qid,
        "state": state,
        "scanned_bytes": stats.get("DataScannedInBytes", 0),
        "engine_ms": stats.get("EngineExecutionTimeInMillis", 0),
        "wall_ms": int((time.time() - t0) * 1000),
        "reason": ex["Status"].get("StateChangeReason"),
        "rows": [],
    }
    if state == "SUCCEEDED":
        try:
            rs = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
            if len(rs) >= 2:
                hdr = [c.get("VarCharValue") for c in rs[0]["Data"]]
                val = [c.get("VarCharValue") for c in rs[1]["Data"]]
                out["rows"] = dict(zip(hdr, val))
        except ClientError:
            pass
    return out


def list_iceberg_tables(glue, database: str) -> list[str]:
    names: list[str] = []
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for t in page["TableList"]:
            params = t.get("Parameters", {}) or {}
            if params.get("table_type", "").upper() == "ICEBERG" or "metadata_location" in params:
                names.append(t["Name"])
    return sorted(names)


# Athena engine v3 cannot OPTIMIZE / read an Iceberg format-version-3 table.
KNOWN_FORMAT_V3 = {"fact_clinician_note_nlp_v"}


def table_format_version(glue, s3, database: str, name: str) -> int | None:
    """Iceberg format-version from the table's metadata.json (Glue params rarely carry it)."""
    if name in KNOWN_FORMAT_V3:
        return 3
    try:
        params = glue.get_table(DatabaseName=database, Name=name)["Table"].get("Parameters", {})
    except ClientError:
        return None
    if params.get("format-version"):
        return int(params["format-version"])
    loc = params.get("metadata_location")
    if not loc or not loc.startswith("s3://"):
        return None
    bucket, key = loc[len("s3://"):].split("/", 1)
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return int(json.loads(body).get("format-version", 2))
    except (ClientError, ValueError, KeyError):
        return None


def run_measure(label: str, region: str | None) -> None:
    script = REPO_ROOT / "measure_storage_footprint.py"
    if not script.exists():
        print(f"  ({label}) measure_storage_footprint.py not found -- skipping", file=sys.stderr)
        return
    cmd = [sys.executable, str(script), "--quiet"]
    if region:
        cmd += ["--region", region]
    print(f"\n=== storage footprint {label} compaction ===", file=sys.stderr)
    subprocess.run(cmd, check=False)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _cfg.add_dataset_arg(p)
    p.add_argument("--gold-database", default=None, help="Iceberg gold database (default: from --dataset)")
    p.add_argument("--athena-workgroup", default=env("ATHENA_WORKGROUP", "primary"))
    p.add_argument("--athena-output", default=None,
                   help="default: s3://<GOLD_BUCKET>/athena-results/")
    p.add_argument("--gold-bucket", default=None, help="Iceberg gold bucket (default: from --dataset)")
    p.add_argument("--region", default=env("AWS_REGION"))
    p.add_argument("--only", default=None, help="comma-separated table names to include (default: all)")
    p.add_argument("--skip", default="", help="comma-separated table names to exclude")
    p.add_argument("--max-optimize-rounds", type=int, default=30,
                   help="Athena OPTIMIZE processes <=100 partitions/call; repeat up to this "
                        "many times per table until it reports done (default: 30)")
    p.add_argument("--vacuum", action="store_true", help="also VACUUM each table after OPTIMIZE")
    p.add_argument("--vacuum-max-age", type=int, default=None,
                   help="seconds -- temporarily set vacuum_max_snapshot_age_seconds this low so "
                        "VACUUM reclaims the just-superseded files (e.g. 300); restored afterwards")
    p.add_argument("--measure", action="store_true",
                   help="run measure_storage_footprint.py before and after")
    p.add_argument("--dry-run", action="store_true", help="print the plan, execute nothing")
    p.add_argument("--out-dir", default=str(REPO_ROOT / "docs" / "storage_footprint"))
    args = p.parse_args()

    profile = _cfg.dataset_profile(args.dataset)
    args.gold_database = args.gold_database or profile.gold_database
    args.gold_bucket = args.gold_bucket or profile.gold_bucket

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    region = session.region_name
    glue = session.client("glue")
    ath = session.client("athena")
    s3 = session.client("s3")
    output = args.athena_output or f"s3://{args.gold_bucket}/athena-results/"

    tables = list_iceberg_tables(glue, args.gold_database)
    if args.only:
        wanted = {t.strip() for t in args.only.split(",") if t.strip()}
        tables = [t for t in tables if t in wanted]
    skip = {t.strip() for t in args.skip.split(",") if t.strip()}
    tables = [t for t in tables if t not in skip]

    # partition off tables Athena cannot OPTIMIZE (Iceberg format-version 3)
    runnable, unsupported = [], []
    for t in tables:
        fv = table_format_version(glue, s3, args.gold_database, t)
        (unsupported if fv == 3 else runnable).append(t)

    print(f"database        : {args.gold_database}")
    print(f"iceberg tables  : {len(tables)}  ({len(runnable)} OPTIMIZE-able, "
          f"{len(unsupported)} format-v3 skipped)")
    print(f"vacuum          : {'yes' if args.vacuum else 'no'}"
          + (f" (max-age {args.vacuum_max_age}s)" if args.vacuum_max_age else ""))
    print(f"athena output   : {output}")
    if unsupported:
        print(f"skipped (fmt-v3): {', '.join(unsupported)}  -- compact from Glue 6.0 / Spark")
    print()

    if args.dry_run:
        for t in runnable:
            print(f"OPTIMIZE {args.gold_database}.{t} REWRITE DATA USING BIN_PACK;")
            if args.vacuum:
                print(f"VACUUM {args.gold_database}.{t};")
        return 0

    if args.measure:
        run_measure("BEFORE", region)

    started = datetime.now(timezone.utc)
    results = []
    for i, t in enumerate(runnable, 1):
        fq = f"{args.gold_database}.{t}"
        entry = {"table": t, "optimize": None, "optimize_rounds": [], "vacuum": None}
        print(f"[{i}/{len(runnable)}] OPTIMIZE {fq} ...", end=" ", flush=True)
        # Athena OPTIMIZE processes <=100 partitions per call and returns
        # ICEBERG_OPTIMIZE_MORE_RUNS_NEEDED when more remain -- loop until done.
        rnd = 0
        while True:
            rnd += 1
            r = athena(ath, f"OPTIMIZE {fq} REWRITE DATA USING BIN_PACK", output, args.athena_workgroup)
            entry["optimize_rounds"].append({"round": rnd, **{k: r[k] for k in ("state", "scanned_bytes", "wall_ms")}})
            entry["optimize"] = r
            more = r["state"] == "FAILED" and "MORE_RUNS_NEEDED" in (r["reason"] or "")
            if not more or rnd >= args.max_optimize_rounds:
                break
            print(f"round {rnd} +100 parts,", end=" ", flush=True)
        if r["state"] == "SUCCEEDED":
            rw = r["rows"] or {}
            secs = sum(x["wall_ms"] for x in entry["optimize_rounds"]) / 1000
            print(f"ok  {rnd} round(s)  {secs:.0f}s  scanned "
                  f"{human(sum(x['scanned_bytes'] for x in entry['optimize_rounds']))}"
                  + (f"  {rw.get('rewritten_data_files_count','?')}->{rw.get('added_data_files_count','?')} files" if rw else ""))
        elif "MORE_RUNS_NEEDED" in (r["reason"] or ""):
            print(f"INCOMPLETE  (still more partitions after {rnd} rounds -- raise --max-optimize-rounds)")
        else:
            print(f"FAILED  ({r['reason']})")

        if args.vacuum and r["state"] == "SUCCEEDED":
            if args.vacuum_max_age is not None:
                athena(ath, f"ALTER TABLE {fq} SET TBLPROPERTIES "
                            f"('vacuum_max_snapshot_age_seconds' = '{args.vacuum_max_age}')",
                       output, args.athena_workgroup)
            v = athena(ath, f"VACUUM {fq}", output, args.athena_workgroup)
            entry["vacuum"] = v
            print(f"        VACUUM {fq} ... {'ok' if v['state']=='SUCCEEDED' else 'FAILED: '+str(v['reason'])}"
                  f"  {v['wall_ms']/1000:.0f}s")
            if args.vacuum_max_age is not None:
                athena(ath, f"ALTER TABLE {fq} SET TBLPROPERTIES "
                            f"('vacuum_max_snapshot_age_seconds' = '{DEFAULT_VACUUM_MAX_AGE}')",
                       output, args.athena_workgroup)
        results.append(entry)

    # summary
    def state_of(e):
        r = e["optimize"]
        if r["state"] == "SUCCEEDED":
            return "ok"
        return "incomplete" if "MORE_RUNS_NEEDED" in (r["reason"] or "") else "failed"

    ok = [e["table"] for e in results if state_of(e) == "ok"]
    incomplete = [e["table"] for e in results if state_of(e) == "incomplete"]
    failed = [e["table"] for e in results if state_of(e) == "failed"]
    total_scanned = sum(x["scanned_bytes"] for e in results for x in e["optimize_rounds"])
    total_wall = sum(x["wall_ms"] for e in results for x in e["optimize_rounds"]) + sum(
        (e["vacuum"] or {}).get("wall_ms", 0) for e in results
    )
    total_rounds = sum(len(e["optimize_rounds"]) for e in results)
    print()
    print(f"OPTIMIZE: {len(ok)}/{len(runnable)} complete in {total_rounds} rounds"
          + (f"; INCOMPLETE (raise --max-optimize-rounds): {', '.join(incomplete)}" if incomplete else "")
          + (f"; FAILED: {', '.join(failed)}" if failed else ""))
    print(f"total data scanned : {human(total_scanned)}   (~$"
          f"{total_scanned / 1_099_511_627_776 * 5:.2f} at $5/TiB)")
    print(f"total wall time    : {total_wall/1000:.0f}s")

    ts = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "gold_database": args.gold_database,
        "region": region,
        "vacuum": args.vacuum,
        "vacuum_max_age": args.vacuum_max_age,
        "unsupported_format_v3": unsupported,
        "optimize_complete": ok,
        "optimize_incomplete": incomplete,
        "optimize_failed": failed,
        "total_optimize_rounds": total_rounds,
        "total_data_scanned_bytes": total_scanned,
        "total_wall_ms": total_wall,
        "tables": results,
    }
    (out_dir / f"compaction_{ts}.json").write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")
    (out_dir / "compaction_latest.json").write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")
    print(f"log                : {out_dir / f'compaction_{ts}.json'}")

    if args.measure:
        run_measure("AFTER", region)

    return 0 if not (failed or incomplete) else 1


if __name__ == "__main__":
    raise SystemExit(main())
