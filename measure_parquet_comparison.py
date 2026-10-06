#!/usr/bin/env python3
"""Measure raw storage/row facts for the Iceberg-vs-Parquet-vs-CSV comparison tables --
supports the research question "How does the storage taken by Iceberg (ZSTD) compare to the
storage taken by plain Parquet (ZSTD and Snappy) / gzip-compressed plain CSV table format
for the Multimodal Fusion Schema?"

Standalone: reads only S3 (list_objects_v2) and Athena ("$files" + count(*)). Does not
require run_lakehouse_pipeline.py or run_measurement_suite.py -- run it any time after
ddl/gold/mimic_iv_ddl_parquet_comparison.sql, mimic_iv_ddl_parquet_snappy_comparison.sql,
and mimic_iv_ddl_csv_comparison.sql have been applied and parquet_comparison_load.py /
parquet_snappy_comparison_load.py / csv_comparison_load.py have populated the *_parquet /
*_parquet_snappy / *_csv tables.

This script only MEASURES and reports raw per-table facts (physical bytes, live-snapshot
bytes, row counts) for each of the 7 archetypes across 4 table-format/codec combinations
(Iceberg/ZSTD, Parquet/ZSTD, Parquet/Snappy, CSV/gzip) -- it does not compute ratios or an
interpreted finding. Run compare_parquet_iceberg.py afterward (it reads this script's JSON
output) for the actual comparison; the two are separate scripts, mirroring
measure_aws_resources.py / compare_environments.py elsewhere in this repo, so the
comparison can be re-rendered without re-querying AWS.

Usage:
    python measure_parquet_comparison.py --dataset fulldataset
    python measure_parquet_comparison.py --dataset fulldataset --skip-athena

Supports the paper's RQ1 ("Multimodal Fusion Schema" paper). See the README's "Research
paper: RQ1 and RQ2" section and compare_parquet_iceberg.py, which interprets this script's
output.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
from mimic_lakehouse import config as _cfg  # noqa: E402

SCRIPT_VERSION = "2.0.0"


def _make_console_utf8_safe() -> None:
    enc = (sys.stdout.encoding or "").lower()
    if enc and enc != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def human(nbytes: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(nbytes) < 1024 or unit == "TiB":
            return f"{nbytes:,.2f} {unit}" if unit != "B" else f"{int(nbytes):,} B"
        nbytes /= 1024
    return f"{nbytes:.2f} TiB"


def list_prefix_bytes(s3, bucket: str, prefix: str) -> dict:
    """Physical bytes/objects for every key under s3://<bucket>/<prefix> (no trailing-slash
    assumption -- callers pass the exact prefix each table's LOCATION uses)."""
    total_bytes = 0
    total_objects = 0
    data_bytes = 0
    meta_bytes = 0
    paginator = s3.get_paginator("list_objects_v2")
    norm = prefix if prefix.endswith("/") else prefix + "/"
    for page in paginator.paginate(Bucket=bucket, Prefix=norm):
        for obj in page.get("Contents", []):
            total_bytes += obj["Size"]
            total_objects += 1
            if "/metadata/" in obj["Key"]:
                meta_bytes += obj["Size"]
            else:
                data_bytes += obj["Size"]
    return {"physical_bytes": total_bytes, "objects": total_objects,
            "data_bytes": data_bytes, "metadata_bytes": meta_bytes}


def run_athena(athena, sql: str, output_location: str, workgroup: str, poll: float = 2.0):
    import time
    qid = athena.start_query_execution(
        QueryString=sql, ResultConfiguration={"OutputLocation": output_location}, WorkGroup=workgroup,
    )["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = st["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll)
    if state != "SUCCEEDED":
        raise RuntimeError(st.get("StateChangeReason", state))
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]


# ---------------------------------------------------------------------------------------
# The 7 archetypes, each measured across 4 table-format/codec combinations. All seven are
# at fact_admission's grain (hadm_id, subject_id, admit_provider_id, admit_date_key).
# Every archetype has a real Iceberg instantiation. athena_readable=False marks the one
# format-version-3 (VARIANT) Iceberg table, fact_clinician_note_nlp_v, which Athena engine
# v3 cannot open at all (verified elsewhere in this repo) -- its Parquet/CSV siblings ARE
# Athena-readable (VARIANT flattened to STRING before write), so only the Iceberg side of
# that archetype is unmeasurable via Athena row count.
# ---------------------------------------------------------------------------------------
def build_archetypes(profile) -> list[dict]:
    gold_bucket = profile.gold_bucket
    gold_db = profile.gold_database

    def _formats(name: str, iceberg_athena_readable: bool) -> list[dict]:
        formats = [
            {"name": "iceberg", "label": "Iceberg", "table": name, "database": gold_db,
             "bucket": gold_bucket, "prefix": f"mimic_bus/{name}",
             "athena_readable": iceberg_athena_readable},
            {"name": "parquet_zstd", "label": "Parquet (ZSTD)", "table": f"{name}_parquet",
             "database": gold_db, "bucket": gold_bucket,
             "prefix": f"parquet_compare/{name}_parquet", "athena_readable": True},
            {"name": "parquet_snappy", "label": "Parquet (Snappy)", "table": f"{name}_parquet_snappy",
             "database": gold_db, "bucket": gold_bucket,
             "prefix": f"parquet_compare_snappy/{name}_parquet_snappy", "athena_readable": True},
            {"name": "csv_gzip", "label": "CSV (gzip)", "table": f"{name}_csv",
             "database": gold_db, "bucket": gold_bucket,
             "prefix": f"csv_compare/{name}_csv", "athena_readable": True},
        ]
        if not iceberg_athena_readable:
            formats[0]["athena_error_default"] = "Iceberg format-version 3 -- not readable by Athena engine v3"
        return formats

    def _archetype(archetype: str, name: str, iceberg_athena_readable: bool = True, note: str | None = None) -> dict:
        d = {"archetype": archetype, "name": name,
             "formats": _formats(name, iceberg_athena_readable)}
        if note:
            d["note"] = note
        return d

    return [
        _archetype("Source Data Fact", "fact_admission"),
        _archetype("Computed Structured Fact", "obt_admission_features"),
        _archetype("Unstructured Data Fact", "fact_discharge_note"),
        _archetype("Unstructured Data Fact", "fact_radiology_note"),
        _archetype("Unstructured Features Fact", "fact_discharge_note_nlp"),
        _archetype("Unstructured Features Fact", "fact_radiology_note_nlp"),
        _archetype(
            "Inference Fact", "fact_clinician_note_nlp_v", iceberg_athena_readable=False,
            note="Combines fact_discharge_note_nlp + fact_radiology_note_nlp via a full "
                 "outer join on hadm_id. Iceberg side is VARIANT-typed (format-v3); "
                 "Parquet/CSV sides are the same columns flattened to STRING (neither format "
                 "has a VARIANT type) -- not a same-type comparison for this row.",
        ),
    ]


def measure_format(s3, athena, athena_output: str | None, workgroup: str, fmt: dict) -> dict:
    result = list_prefix_bytes(s3, fmt["bucket"], fmt["prefix"])
    if athena is None:
        return result
    if not fmt.get("athena_readable", True):
        result["athena_error"] = fmt.get("athena_error_default", "not readable by Athena")
        return result
    try:
        if fmt["name"] == "iceberg":
            r = run_athena(
                athena,
                f'SELECT count(*), coalesce(sum(file_size_in_bytes),0), coalesce(sum(record_count),0) '
                f'FROM "{fmt["database"]}"."{fmt["table"]}$files"',
                athena_output, workgroup,
            )[0]
            result["live_files"] = int(r[0])
            result["live_bytes"] = int(r[1])
            result["live_records"] = int(r[2])
        cnt = run_athena(athena, f'SELECT count(*) FROM "{fmt["database"]}"."{fmt["table"]}"',
                         athena_output, workgroup)[0][0]
        result["row_count"] = int(cnt)
    except Exception as exc:  # noqa: BLE001
        result["athena_error"] = str(exc)
    return result


def measure_archetype(s3, athena, athena_output: str | None, workgroup: str, entry: dict) -> dict:
    result = {k: v for k, v in entry.items() if k != "formats"}
    result["formats"] = [
        {**fmt, **measure_format(s3, athena, athena_output, workgroup, fmt)}
        for fmt in entry["formats"]
    ]
    return result


def render_markdown(report: dict) -> str:
    L = []
    w = L.append
    m = report["meta"]
    w("# Iceberg (ZSTD) vs Parquet (ZSTD) vs Parquet (Snappy) vs CSV (gzip) table-format storage -- raw measurement")
    w("")
    w(f"- **Generated:** {m['generated_utc']}  ·  region `{m['region']}`  ·  dataset `{m['dataset']}`")
    w("- Raw per-table facts only -- see the companion compare_parquet_iceberg.py report for ratios and findings.")
    w("")

    fmt_meta = report["archetypes"][0]["formats"]
    headers, aligns = ["Archetype"], ["---"]
    for fm in fmt_meta:
        headers += [f"{fm['label']} table", f"{fm['label']} physical"]
        aligns += ["---", "--:"]
        if fm["name"] == "iceberg":
            headers.append(f"{fm['label']} live")
            aligns.append("--:")
        headers.append(f"{fm['label']} rows")
        aligns.append("--:")
    headers.append("Note")
    aligns.append("---")
    w("| " + " | ".join(headers) + " |")
    w("|" + "|".join(aligns) + "|")

    for entry in report["archetypes"]:
        cells = [entry["archetype"]]
        for fmt in entry["formats"]:
            cells.append(f"`{fmt['table']}`")
            cells.append(human(fmt["physical_bytes"]))
            if fmt["name"] == "iceberg":
                live = human(fmt["live_bytes"]) if fmt.get("live_bytes") is not None else ("n/a" if fmt.get("athena_error") else "—")
                cells.append(live)
            rows = f"{fmt['row_count']:,}" if fmt.get("row_count") is not None else ("n/a" if fmt.get("athena_error") else "—")
            cells.append(rows)
        cells.append(entry.get("note", ""))
        w("| " + " | ".join(cells) + " |")

    w("")
    w("## Reproduce")
    w("")
    w("```")
    w(f"python measure_parquet_comparison.py --dataset {m['dataset']} --region {m['region']}")
    w("```")
    w("")
    w(f"_Machine-readable companion: `{m['json_filename']}`. Next: `python compare_parquet_iceberg.py "
      f"--dataset {m['dataset']}` to render the interpreted comparison._")
    return "\n".join(L) + "\n"


def main() -> int:
    _make_console_utf8_safe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _cfg.add_dataset_arg(p)
    p.add_argument("--region", default=_cfg.get("AWS_REGION", "us-east-2"))
    p.add_argument("--athena-workgroup", default=_cfg.get("ATHENA_WORKGROUP", "primary"))
    p.add_argument("--athena-output", default=None)
    p.add_argument("--out-dir", default=None,
                   help="output dir (default: docs/parquet_comparison/<demo|full> for the --dataset)")
    p.add_argument("--skip-athena", action="store_true", help="S3 physical bytes only; no row counts / live-snapshot")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    profile = _cfg.dataset_profile(args.dataset)
    if args.out_dir is None:
        args.out_dir = str(REPO_ROOT / "docs" / "parquet_comparison" / profile.mode)
    athena_output = args.athena_output or f"s3://{profile.gold_bucket}/athena-results/"

    log = (lambda *a: None) if args.quiet else (lambda *a: print(*a, file=sys.stderr))

    s3 = _cfg.RefreshingClient("s3", args.region)
    athena = None if args.skip_athena else _cfg.RefreshingClient("athena", args.region)

    archetypes = build_archetypes(profile)
    log(f"[dataset] {args.dataset} -> gold={profile.gold_database}/{profile.gold_bucket}, "
        f"raw={profile.raw_database}/{profile.raw_bucket}")
    log(f"Measuring {len(archetypes)} archetype(s) x {len(archetypes[0]['formats'])} format(s)...")
    measured = []
    for i, entry in enumerate(archetypes, 1):
        fmt_names = " vs ".join(f["table"] for f in entry["formats"])
        log(f"  ({i}/{len(archetypes)}) {entry['archetype']}: {fmt_names}")
        measured.append(measure_archetype(s3, athena, athena_output, args.athena_workgroup, entry))

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_name = f"parquet_comparison_measurement_{ts}.json"
    report = {
        "meta": {
            "script_version": SCRIPT_VERSION,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "dataset": args.dataset,
            "region": args.region,
            "gold_database": profile.gold_database,
            "gold_bucket": profile.gold_bucket,
            "raw_database": profile.raw_database,
            "raw_bucket": profile.raw_bucket,
            "athena_workgroup": args.athena_workgroup,
            "athena_output": athena_output,
            "skip_athena": args.skip_athena,
            "json_filename": json_name,
        },
        "archetypes": measured,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / json_name).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out_dir / "latest.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md = render_markdown(report)
    (out_dir / f"parquet_comparison_measurement_{ts}.md").write_text(md, encoding="utf-8")
    (out_dir / "latest.md").write_text(md, encoding="utf-8")

    if not args.quiet:
        print(md)
    log(f"written: {out_dir / 'latest.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
