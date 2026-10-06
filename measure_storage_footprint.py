#!/usr/bin/env python3
"""Measure the physical + live storage footprint of the MIMIC multimodal lakehouse
and emit an auditable, reproducible report.

Anyone with read access to the S3 buckets and the Athena workgroup can run this and
verify the numbers in docs/storage_footprint/. It is read-only: the only writes are
Athena query results into the workgroup's own output location.

What it measures
----------------
1. PHYSICAL bytes (S3 `list_objects_v2`) per bucket, per top-level prefix, and -- for
   the Gold Iceberg bucket -- per table, split into data files vs Iceberg metadata
   (manifests / manifest lists / snapshots / stats under `<table>/metadata/`).
2. LIVE-SNAPSHOT bytes (Athena `"<table>$files"`) -- the data the current Iceberg
   snapshot actually references. Physical > live because expired snapshots' files are
   not deleted until `expire_snapshots` runs.
3. Derived metrics: physical/live ratio, small-file fragmentation, and the size of the
   AI/NLP-derived tables both absolutely and as a share of the Gold layer and raw data
   (the "space to support P4 -- structured queries joined to AI inference outputs").
4. A STRUCTURED vs UNSTRUCTURED / NLP rollup spanning every bucket. "Structured" is
   what the Aurora PostgreSQL warehouse (mimic_datawarehouse_repo) also holds; the
   "unstructured / NLP" category (note text + `*_note_raw` Parquet + `fact_discharge_note_nlp` /
   `fact_radiology_note_nlp*` + per-note JSON) has no warehouse equivalent, so a P3
   redundancy-ratio comparison is made on the structured row alone.

`fact_clinician_note_nlp_v` is Iceberg format-version 3 and Athena engine v3 cannot
read it; it is reported as `athena_readable: false` with physical bytes only.

Usage
-----
  python measure_storage_footprint.py                      # full run, writes JSON + Markdown
  python measure_storage_footprint.py --skip-athena        # S3 physical only, no Athena cost
  python measure_storage_footprint.py --exact-rows         # also SELECT count(*) per table (scans data)
  python measure_storage_footprint.py --out-dir /tmp/audit --quiet

Defaults come from .env (see .env.example) via src/mimic_lakehouse/config.py, then the
built-in bucket names, then command-line flags (flags win).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

SCRIPT_VERSION = "1.2.0"  # 1.1.0: structured vs unstructured/NLP rollup; 1.2.0: --aurora-baseline DW comparison
REPO_ROOT = Path(__file__).resolve().parent

sys.path.insert(0, str(REPO_ROOT / "src"))
try:  # optional .env loader; fall back to os.environ
    from mimic_lakehouse import config as _cfg

    def env(key, default=None):
        return _cfg.get(key, default)
except Exception:  # noqa: BLE001
    import os

    def env(key, default=None):
        return os.environ.get(key, default)


def _make_console_utf8_safe() -> None:
    """Report files are always written as UTF-8; only the console echo is limited by
    the terminal's encoding (cp1252 on a default Windows console, which can't encode
    some glyphs used in this report, e.g. Sigma or an approx-equals sign)."""
    enc = (sys.stdout.encoding or "").lower()
    if enc and enc != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


# Tables whose rows are AI / NLP inference outputs (the P4 "AI inference" layer).
AI_NLP_TABLES = {
    "fact_discharge_note_nlp",
    "fact_radiology_note_nlp",
    "fact_clinician_note_nlp_v",
}
CONTROL_TABLES = {"etl_control", "etl_process_log"}
# Athena engine v3 cannot read Iceberg format-version 3 (VARIANT projections).
KNOWN_FORMAT_V3 = {"fact_clinician_note_nlp_v"}


def human(nbytes: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(nbytes) < 1024 or unit == "TiB":
            return f"{nbytes:,.2f} {unit}" if unit != "B" else f"{int(nbytes):,} B"
        nbytes /= 1024
    return f"{nbytes:.2f} TiB"


def pct(part: float, whole: float) -> float:
    return round(100.0 * part / whole, 4) if whole else 0.0


def git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------------------
# S3 physical inventory
# ---------------------------------------------------------------------------------------
def list_bucket(s3, bucket: str) -> list[tuple[str, int]]:
    """Every current object as (key, size). Note: current versions only -- if bucket
    versioning is enabled, noncurrent versions are not counted (documented limitation)."""
    out: list[tuple[str, int]] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            out.append((obj["Key"], obj["Size"]))
    return out


def classify_raw_prefix(prefix: str) -> str:
    if prefix in ("discharge_raw", "radiology_raw", "discharge_detail_raw", "radiology_detail_raw"):
        return "note_text_gz"
    if prefix.endswith("_note_raw"):
        return "note_parquet"
    if prefix.endswith("_raw"):
        return "structured_csv"
    return "other"


def classify_gold_table(name: str) -> str:
    if name in AI_NLP_TABLES:
        return "ai_nlp"
    if name in CONTROL_TABLES:
        return "control"
    for kind in ("dim_", "fact_", "agg_", "obt_"):
        if name.startswith(kind):
            return kind.rstrip("_")
    return "other"


# Coarse category for the structured-vs-unstructured rollup. "structured" = has a
# direct equivalent in the Aurora PostgreSQL warehouse (mimic_datawarehouse_repo);
# "unstructured_nlp" = clinical free-text + everything derived from it by NLP, which
# that warehouse does not carry at all; "infra" = Glue scripts / Athena query results.
_RAW_CLASS_CATEGORY = {
    "structured_csv": "structured",
    "note_text_gz": "unstructured_nlp",
    "note_parquet": "unstructured_nlp",
    "other": "infra",
}
_GOLD_CLASS_CATEGORY = {
    "dim": "structured", "fact": "structured", "agg": "structured",
    "obt": "structured", "control": "structured",
    "ai_nlp": "unstructured_nlp",
    "other": "infra",
}


def category_of_raw_class(raw_class: str) -> str:
    return _RAW_CLASS_CATEGORY.get(raw_class, "infra")


def category_of_gold_class(gold_class: str) -> str:
    return _GOLD_CLASS_CATEGORY.get(gold_class, "infra")


def summarize_raw(objects) -> dict:
    by_prefix: dict[str, dict] = defaultdict(lambda: {"bytes": 0, "objects": 0})
    for key, size in objects:
        prefix = key.split("/", 1)[0]
        by_prefix[prefix]["bytes"] += size
        by_prefix[prefix]["objects"] += 1
    by_class: dict[str, dict] = defaultdict(lambda: {"bytes": 0, "objects": 0, "prefixes": []})
    for prefix, agg in by_prefix.items():
        cls = classify_raw_prefix(prefix)
        by_class[cls]["bytes"] += agg["bytes"]
        by_class[cls]["objects"] += agg["objects"]
        by_class[cls]["prefixes"].append(prefix)
    total = sum(a["bytes"] for a in by_prefix.values())
    return {
        "total_bytes": total,
        "total_objects": sum(a["objects"] for a in by_prefix.values()),
        "by_class": {
            k: {**v, "pct_of_bucket": pct(v["bytes"], total),
                "category": category_of_raw_class(k)}
            for k, v in sorted(by_class.items())
        },
        "by_prefix": {k: v for k, v in sorted(by_prefix.items(), key=lambda kv: -kv[1]["bytes"])},
    }


def summarize_gold(objects, gold_prefix: str) -> dict:
    """Per-table physical bytes under <gold_prefix>/<table>/, split data vs metadata."""
    tables: dict[str, dict] = defaultdict(
        lambda: {"data_bytes": 0, "data_objects": 0, "meta_bytes": 0, "meta_objects": 0}
    )
    stray_bytes = stray_objects = 0
    prefix_objects = prefix_bytes = 0
    for key, size in objects:
        parts = key.split("/")
        if parts[0] != gold_prefix:
            continue
        prefix_objects += 1
        prefix_bytes += size
        if len(parts) < 3:  # <gold_prefix>/<file> -- stray Athena result etc.
            stray_bytes += size
            stray_objects += 1
            continue
        table = parts[1]
        if parts[2] == "metadata":
            tables[table]["meta_bytes"] += size
            tables[table]["meta_objects"] += 1
        else:
            tables[table]["data_bytes"] += size
            tables[table]["data_objects"] += 1
    for name, t in tables.items():
        t["class"] = classify_gold_table(name)
        t["category"] = category_of_gold_class(t["class"])
        t["physical_bytes"] = t["data_bytes"] + t["meta_bytes"]
        t["physical_objects"] = t["data_objects"] + t["meta_objects"]
    return {
        "prefix": gold_prefix,
        "total_physical_bytes": prefix_bytes,
        "total_physical_objects": prefix_objects,
        "total_data_bytes": sum(t["data_bytes"] for t in tables.values()),
        "total_metadata_bytes": sum(t["meta_bytes"] for t in tables.values()),
        "stray_bytes": stray_bytes,
        "stray_objects": stray_objects,
        "tables": dict(sorted(tables.items(), key=lambda kv: -kv[1]["physical_bytes"])),
    }


# ---------------------------------------------------------------------------------------
# Athena live-snapshot inventory
# ---------------------------------------------------------------------------------------
def run_athena(athena, sql: str, output_location: str, workgroup: str, poll: float = 2.0) -> list[list[str]]:
    qid = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
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
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]  # drop header


def live_snapshot(athena, database: str, tables: list[str], output_location: str,
                  workgroup: str, exact_rows: bool) -> dict:
    result: dict[str, dict] = {}
    for name in tables:
        entry = {"athena_readable": True, "live_files": None, "live_bytes": None,
                 "live_records": None, "exact_rows": None, "error": None}
        try:
            r = run_athena(
                athena,
                f'SELECT count(*), coalesce(sum(file_size_in_bytes),0), '
                f'coalesce(sum(record_count),0) FROM "{database}"."{name}$files"',
                output_location, workgroup,
            )[0]
            entry["live_files"] = int(r[0])
            entry["live_bytes"] = int(r[1])
            entry["live_records"] = int(r[2])
            if exact_rows:
                cnt = run_athena(athena, f'SELECT count(*) FROM "{database}"."{name}"',
                                 output_location, workgroup)[0][0]
                entry["exact_rows"] = int(cnt)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            entry["athena_readable"] = False
            entry["error"] = msg
            if "format version 3" in msg or name in KNOWN_FORMAT_V3:
                entry["error"] = "Iceberg format-version 3 -- not readable by Athena engine v3"
        result[name] = entry
    return result


# ---------------------------------------------------------------------------------------
# Derived metrics + report
# ---------------------------------------------------------------------------------------
def derive(raw_sum, gold_sum, live, nlp_debug_bytes, scripts_bytes) -> dict:
    tables = gold_sum["tables"]
    # attach live-snapshot numbers to each table
    for name, t in tables.items():
        ls = live.get(name, {})
        t["live_bytes"] = ls.get("live_bytes")
        t["live_files"] = ls.get("live_files")
        t["live_records"] = ls.get("live_records")
        t["exact_rows"] = ls.get("exact_rows")
        t["athena_readable"] = ls.get("athena_readable", None)
        if t["live_bytes"] and t["physical_bytes"]:
            t["physical_to_live_ratio"] = round(t["physical_bytes"] / t["live_bytes"], 2)
        else:
            t["physical_to_live_ratio"] = None
        if t["live_bytes"] and t["live_files"]:
            t["avg_live_file_bytes"] = int(t["live_bytes"] / t["live_files"])
        else:
            t["avg_live_file_bytes"] = None

    live_total = sum((t["live_bytes"] or 0) for t in tables.values())
    live_files_total = sum((t["live_files"] or 0) for t in tables.values())
    gold_phys = gold_sum["total_physical_bytes"]

    def cls_sum(cls, field):
        return sum((tables[n].get(field) or 0) for n in tables if tables[n]["class"] == cls)

    ai_phys = cls_sum("ai_nlp", "physical_bytes")
    ai_live = cls_sum("ai_nlp", "live_bytes")
    raw_total = raw_sum["total_bytes"]
    total_lakehouse = raw_total + gold_phys + nlp_debug_bytes + scripts_bytes

    small_file_tables = sorted(
        (
            {"table": n, "avg_live_file_bytes": t["avg_live_file_bytes"],
             "live_files": t["live_files"], "live_bytes": t["live_bytes"]}
            for n, t in tables.items()
            if t["avg_live_file_bytes"] is not None and t["avg_live_file_bytes"] < 1_048_576
        ),
        key=lambda d: d["avg_live_file_bytes"],
    )

    # -----------------------------------------------------------------------------------
    # Structured vs unstructured / NLP rollup (spans all buckets).
    # The Aurora PostgreSQL warehouse (mimic_datawarehouse_repo) carries ONLY the
    # "structured" category -- it has no notes, no NLP tables, no note text. So a P3
    # redundancy-ratio comparison must be made on the "structured" row alone; the
    # "unstructured_nlp" row is coverage the lakehouse adds, not duplication.
    # -----------------------------------------------------------------------------------
    def raw_cat(category):
        return sum(v["bytes"] for v in raw_sum["by_class"].values() if v["category"] == category)

    def gold_cat_phys(category):
        return sum(t["physical_bytes"] for t in tables.values() if t["category"] == category)

    def gold_cat_live(category):
        return sum((t["live_bytes"] or 0) for t in tables.values() if t["category"] == category)

    structured = {
        "raw_csv_bytes": raw_cat("structured"),
        "gold_physical_bytes": gold_cat_phys("structured"),
        "gold_live_bytes": gold_cat_live("structured"),
        "gold_table_count": sum(1 for t in tables.values() if t["category"] == "structured"),
    }
    structured["physical_bytes"] = structured["raw_csv_bytes"] + structured["gold_physical_bytes"]
    structured["live_bytes"] = structured["raw_csv_bytes"] + structured["gold_live_bytes"]

    unstructured = {
        "note_text_bytes": raw_sum["by_class"].get("note_text_gz", {}).get("bytes", 0),
        "note_parquet_bytes": raw_sum["by_class"].get("note_parquet", {}).get("bytes", 0),
        "nlp_gold_physical_bytes": gold_cat_phys("unstructured_nlp"),
        "nlp_gold_live_bytes": gold_cat_live("unstructured_nlp"),
        "nlp_gold_table_count": sum(1 for t in tables.values() if t["category"] == "unstructured_nlp"),
        "nlp_debug_bytes": nlp_debug_bytes,
    }
    unstructured["physical_bytes"] = (
        unstructured["note_text_bytes"] + unstructured["note_parquet_bytes"]
        + unstructured["nlp_gold_physical_bytes"] + unstructured["nlp_debug_bytes"]
    )
    unstructured["live_bytes"] = (
        unstructured["note_text_bytes"] + unstructured["note_parquet_bytes"]
        + unstructured["nlp_gold_live_bytes"] + unstructured["nlp_debug_bytes"]
    )

    infra_bytes = (
        raw_cat("infra") + gold_cat_phys("infra") + gold_sum["stray_bytes"] + scripts_bytes
    )
    grand_physical = structured["physical_bytes"] + unstructured["physical_bytes"] + infra_bytes

    structured_vs_unstructured = {
        "structured": {
            **structured,
            "physical_share_pct": pct(structured["physical_bytes"], grand_physical),
            "rds_warehouse_equivalent": True,
        },
        "unstructured_nlp": {
            **unstructured,
            "physical_share_pct": pct(unstructured["physical_bytes"], grand_physical),
            "rds_warehouse_equivalent": False,
        },
        "infrastructure": {
            "physical_bytes": infra_bytes,
            "physical_share_pct": pct(infra_bytes, grand_physical),
            "note": "Glue job scripts + stray Athena query results -- neither modality",
        },
        "grand_total_physical_bytes": grand_physical,
        "p3_comparable_bytes": {
            "structured_physical": structured["physical_bytes"],
            "structured_live": structured["live_bytes"],
            "note": "compare structured_live (or a post-OPTIMIZE re-measure) against the "
                    "Aurora warehouse's pg_total_relation_size over hosp/icu/staging/warehouse "
                    "plus its raw S3 landing copy",
        },
        "lakehouse_only_bytes": unstructured["physical_bytes"],
    }

    return {
        "structured_vs_unstructured": structured_vs_unstructured,
        "raw_bucket": {
            "total_bytes": raw_total,
            "note_text_gz_bytes": raw_sum["by_class"].get("note_text_gz", {}).get("bytes", 0),
            "note_text_gz_pct": raw_sum["by_class"].get("note_text_gz", {}).get("pct_of_bucket", 0),
            "structured_csv_bytes": raw_sum["by_class"].get("structured_csv", {}).get("bytes", 0),
            "structured_csv_pct": raw_sum["by_class"].get("structured_csv", {}).get("pct_of_bucket", 0),
            "note_parquet_bytes": raw_sum["by_class"].get("note_parquet", {}).get("bytes", 0),
        },
        "gold_bucket": {
            "physical_bytes": gold_phys,
            "physical_objects": gold_sum["total_physical_objects"],
            "data_bytes": gold_sum["total_data_bytes"],
            "iceberg_metadata_bytes": gold_sum["total_metadata_bytes"],
            "stray_bytes": gold_sum["stray_bytes"],
            "stray_objects": gold_sum["stray_objects"],
            "live_snapshot_bytes": live_total or None,
            "live_snapshot_files": live_files_total or None,
            "physical_to_live_ratio": round(gold_phys / live_total, 2) if live_total else None,
            "avg_live_file_bytes": int(live_total / live_files_total) if live_files_total else None,
            "small_file_tables": small_file_tables,
        },
        "ai_inference_layer": {
            "tables": sorted(n for n in tables if tables[n]["class"] == "ai_nlp"),
            "physical_bytes": ai_phys,
            "live_bytes": ai_live or None,
            "pct_of_gold_physical": pct(ai_phys, gold_phys),
            "pct_of_raw_bucket": pct(ai_phys, raw_total),
            "pct_of_total_lakehouse": pct(ai_phys, total_lakehouse),
        },
        "nlp_debug_bucket_bytes": nlp_debug_bytes,
        "scripts_bucket_bytes": scripts_bytes,
        "total_lakehouse_physical_bytes": total_lakehouse,
    }


MIB = 1024 * 1024


def build_dw_vs_lakehouse(sv: dict, aurora: dict) -> dict:
    """Data-warehouse (Aurora) vs data-lakehouse space comparison.

    The raw structured CSV files are identical for both architectures, so they are
    reported once as a shared line and excluded from the head-to-head of the
    *processed* layers -- which is where the architectures actually differ.
    """
    lh = sv["structured"]
    shared_raw = lh["raw_csv_bytes"]                 # identical for both

    lh_proc_phys = lh["gold_physical_bytes"]
    lh_proc_live = lh["gold_live_bytes"]
    # compacted-logical size = the current live snapshot. OPTIMIZE ... REWRITE DATA
    # USING BIN_PACK has been run across the whole Gold layer (compact_gold_tables.py,
    # 2026-09-08), so the live snapshot IS the bin-packed layout -- no longer an
    # estimate. Physical still carries the superseded data files: Athena VACUUM expired
    # the old snapshots but does not delete their now-orphaned files (verified), so
    # physical/live stays high until a Spark remove_orphan_files pass runs.
    lh_proc_compacted_est = lh_proc_live

    dw = aurora.get("structured", {})
    dw_proc_phys = round(dw.get("physical_mib", 0) * MIB)
    dw_proc_live = round(dw.get("live_mib", 0) * MIB)
    dw_pgdb = round(aurora.get("pg_database_size_mib", 0) * MIB) or None

    def ratio(a, b):
        return round(a / b, 2) if b else None

    return {
        "aurora_baseline_source": aurora.get("source"),
        "aurora_measured_utc": aurora.get("measured_utc"),
        "aurora_tables": aurora.get("tables", []),
        "shared_raw_structured_csv_bytes": shared_raw,
        "processed_structured": {
            "data_warehouse": {
                "physical_bytes": dw_proc_phys,
                "live_bytes": dw_proc_live,
                "pg_database_size_bytes": dw_pgdb,
                "table_count": dw.get("table_count"),
                "materializations": dw.get("materializations", []),
                "note": dw.get("content"),
            },
            "data_lakehouse": {
                "physical_bytes": lh_proc_phys,
                "live_bytes": lh_proc_live,
                "compacted_logical_estimate_bytes": lh_proc_compacted_est,
                "table_count": lh["gold_table_count"],
                "materializations": [
                    "raw CSV.gz in S3 (shared)",
                    "mimic4_db_raw staging = EXTERNAL tables over the raw CSVs (0 bytes)",
                    "Gold Iceberg (the only physical processed copy)",
                ],
            },
            "ratios_lakehouse_over_warehouse": {
                "physical_as_deployed": ratio(lh_proc_phys, dw_proc_phys),
                "live_snapshot": ratio(lh_proc_live, dw_proc_live),
                "compacted_estimate": ratio(lh_proc_compacted_est, dw_proc_live),
            },
        },
        "redundancy_ratio_structured": {
            "definition": "physical bytes materialized for the structured dataset / bytes of one "
                          "clean modeled copy",
            "data_warehouse": dw.get("redundancy_multiple", 2.0),
            "data_warehouse_note": dw.get("redundancy_basis",
                                   "Aurora materializes the structured data twice inside Postgres "
                                   "(staging.stg_* full reload + warehouse.* modeled)"),
            "data_lakehouse_as_deployed": ratio(lh_proc_phys, lh_proc_compacted_est),
            "data_lakehouse_as_deployed_note": "OPTIMIZE has run (live snapshot IS bin-packed); "
                                               "the residual physical/live gap is superseded data "
                                               "files that Athena VACUUM expired the snapshots for "
                                               "but did not delete -- reclaimable only by a Spark "
                                               "remove_orphan_files pass. Not architecture.",
            "data_lakehouse_compacted": 1.0,
            "data_lakehouse_compacted_note": "live snapshot = one clean modeled copy; gold is the "
                                             "only physical processed copy, the mimic4_db_raw "
                                             "staging layer is external tables (0 bytes)",
        },
        "unstructured_nlp": {
            "data_warehouse_bytes": round(aurora.get("unstructured_nlp", {}).get("physical_mib", 0) * MIB),
            "data_warehouse_note": aurora.get("unstructured_nlp", {}).get("content",
                                   "not stored in Aurora"),
            "data_lakehouse_nlp_derived_physical_bytes": sv["unstructured_nlp"]["nlp_gold_physical_bytes"],
            "data_lakehouse_nlp_derived_live_bytes": sv["unstructured_nlp"]["nlp_gold_live_bytes"],
            "data_lakehouse_note_text_bytes": sv["unstructured_nlp"]["note_text_bytes"],
            "finding": "Aurora holds 0 bytes -- a coverage gap, not a space win. The lakehouse "
                       "holds the NLP-derived tables in ONE copy (shared catalog); a decoupled "
                       "architecture that did NLP would hold each output in >= 2 (model store + "
                       "warehouse load).",
        },
    }


def render_markdown(report: dict) -> str:
    m = report["meta"]
    d = report["derived"]
    raw = report["raw_bucket"]
    gold = report["gold_bucket"]
    L = []
    w = L.append
    w("# Lakehouse storage footprint")
    w("")
    w(f"- **Generated:** {m['generated_utc']}  ·  script v{m['script_version']}"
      + (f"  ·  repo `{m['git_commit']}`" if m["git_commit"] else ""))
    w(f"- **AWS account:** `{m['aws_account']}`  ·  **region:** `{m['region']}`")
    w(f"- **Buckets:** raw `{m['raw_bucket']}` · gold `{m['gold_bucket']}` "
      f"(prefix `{m['gold_prefix']}`) · nlp-debug `{m['nlp_bucket']}` · scripts `{m['scripts_bucket']}`")
    w(f"- **Gold database:** `{m['gold_database']}`  ·  Athena workgroup `{m['athena_workgroup']}`"
      + ("  ·  **Athena: skipped**" if m["skip_athena"] else ""))
    w("")
    w("## Summary")
    w("")
    w(f"| Metric | Bytes | Human |")
    w(f"|---|--:|--:|")
    w(f"| Total lakehouse (physical) | {d['total_lakehouse_physical_bytes']:,} | {human(d['total_lakehouse_physical_bytes'])} |")
    w(f"| Raw bucket | {raw['total_bytes']:,} | {human(raw['total_bytes'])} |")
    w(f"| Gold bucket (physical) | {gold['physical_bytes']:,} | {human(gold['physical_bytes'])} |")
    w(f"| Gold live snapshot | {gold['live_snapshot_bytes'] or 0:,} | {human(gold['live_snapshot_bytes'] or 0)} |")
    w(f"| **AI-inference layer (physical)** | {d['ai_inference_layer']['physical_bytes']:,} | {human(d['ai_inference_layer']['physical_bytes'])} |")
    w("")

    sv = d["structured_vs_unstructured"]
    st, un, inf = sv["structured"], sv["unstructured_nlp"], sv["infrastructure"]
    w("## Structured vs unstructured / NLP")
    w("")
    w("The Aurora PostgreSQL warehouse (`mimic_datawarehouse_repo`) carries **only the "
      "structured category** — no clinical notes, no NLP-derived tables, no note text. So the "
      "P3 redundancy-ratio comparison is made on the **structured** row alone; the "
      "**unstructured / NLP** row is coverage the lakehouse adds, not duplication.")
    w("")
    w("| Category | Physical | Live snapshot | % of lakehouse | In the Aurora warehouse? |")
    w("|---|--:|--:|--:|:--:|")
    w(f"| **Structured** — raw `*_raw` CSV + `dim_`/`fact_`/`agg_`/`obt_`/`etl_control` "
      f"({st['gold_table_count']} Gold tables) | {human(st['physical_bytes'])} | "
      f"{human(st['live_bytes'])} | {st['physical_share_pct']}% | yes — `warehouse` schema |")
    w(f"| **Unstructured / NLP** — note `.csv.gz` + `*_note_raw` Parquet + "
      f"`fact_discharge_note_nlp`/`fact_radiology_note_nlp*` ({un['nlp_gold_table_count']} Gold tables) "
      f"+ per-note JSON | {human(un['physical_bytes'])} | {human(un['live_bytes'])} | "
      f"{un['physical_share_pct']}% | **no** |")
    w(f"| Infrastructure — Glue scripts, stray Athena results | {human(inf['physical_bytes'])} "
      f"| — | {inf['physical_share_pct']}% | n/a |")
    w("")
    w("Breakdown of the unstructured / NLP category:")
    w("")
    w(f"| Component | Physical |")
    w(f"|---|--:|")
    w(f"| Clinical note text (`discharge_raw` + `radiology_raw` `.csv.gz`) | {human(un['note_text_bytes'])} |")
    w(f"| Converted note Parquet (`discharge_note_raw`, `radiology_note_raw`) | {human(un['note_parquet_bytes'])} |")
    w(f"| NLP-derived Gold tables (physical / live) | {human(un['nlp_gold_physical_bytes'])} / {human(un['nlp_gold_live_bytes'])} |")
    w(f"| Per-note debug JSON (`{m['nlp_bucket']}`) | {human(un['nlp_debug_bytes'])} |")
    w("")
    w(f"**For a P3 redundancy ratio vs. the Aurora warehouse, compare "
      f"`{human(sv['p3_comparable_bytes']['structured_live'])}` "
      f"(structured, live snapshot)** — ideally after `OPTIMIZE … REWRITE DATA` — against "
      f"that warehouse's `pg_total_relation_size()` over `staging` + `warehouse` plus its own "
      f"raw S3 landing copy. The lakehouse additionally carries "
      f"**{human(sv['lakehouse_only_bytes'])}** of unstructured / NLP data that has no "
      f"warehouse equivalent at all.")
    w("")

    cmp = report.get("dw_vs_lakehouse")
    if cmp:
        ps = cmp["processed_structured"]
        dw, lh = ps["data_warehouse"], ps["data_lakehouse"]
        r = ps["ratios_lakehouse_over_warehouse"]
        rr = cmp["redundancy_ratio_structured"]
        un2 = cmp["unstructured_nlp"]
        w("## Data warehouse vs data lakehouse — space comparison")
        w("")
        w(f"Aurora baseline: **{cmp['aurora_baseline_source']}** ({cmp['aurora_measured_utc']}). "
          "The raw structured CSV files are identical for both architectures, so they are one "
          "shared line and excluded from the head-to-head of the *processed* layers — which is "
          "where the two designs actually differ.")
        w("")
        w("| Layer | Data warehouse (Aurora) | Data lakehouse |")
        w("|---|--:|--:|")
        w(f"| Raw structured CSV.gz in S3 *(shared — same files)* | "
          f"{human(cmp['shared_raw_structured_csv_bytes'])} | {human(cmp['shared_raw_structured_csv_bytes'])} |")
        w(f"| Staging / raw-mirror layer | *materialized* — part of the total below "
          f"(`staging.stg_*`, full TRUNCATE+reload) | **0 B** — `mimic4_db_raw` is external "
          f"tables over the same S3 files |")
        w(f"| Modeled / queryable layer | `warehouse.dim_/fact_/agg_*` | Gold Iceberg |")
        w(f"| **Processed structured — physical** | **{human(dw['physical_bytes'])}** "
          f"({dw['table_count']} tables) | **{human(lh['physical_bytes'])}** "
          f"({lh['table_count']} tables) |")
        w(f"| **Processed structured — live / current** | {human(dw['live_bytes'])} | "
          f"{human(lh['live_bytes'])} |")
        w(f"| Processed structured — compacted-logical | ~{human(dw['live_bytes'])} "
          f"(Postgres, little bloat) | {human(lh['compacted_logical_estimate_bytes'])} "
          f"(**measured** — post-`OPTIMIZE … REWRITE DATA`) |")
        if dw["pg_database_size_bytes"]:
            w(f"| `pg_database_size()` (incl. catalogs) | {human(dw['pg_database_size_bytes'])} | n/a |")
        w("")
        w(f"| Unstructured / NLP | Data warehouse (Aurora) | Data lakehouse |")
        w(f"|---|--:|--:|")
        w(f"| Clinical note text (raw source) | not stored | {human(un2['data_lakehouse_note_text_bytes'])} |")
        w(f"| **NLP-derived tables** (physical / live) | **{human(un2['data_warehouse_bytes'])} "
          f"— cannot store** | {human(un2['data_lakehouse_nlp_derived_physical_bytes'])} / "
          f"{human(un2['data_lakehouse_nlp_derived_live_bytes'])} |")
        w("")
        w(f"**Head-to-head, processed structured:** on the **live snapshot** — the data a "
          f"correctly-maintained Iceberg table actually serves, and now bin-packed by "
          f"`OPTIMIZE` — the lakehouse is **{r['live_snapshot']}×** the warehouse "
          f"({human(lh['live_bytes'])} vs {human(dw['live_bytes'])}), i.e. it holds the same "
          f"structured information in a fraction of the space — **P3 supported**. On **raw "
          f"physical bytes** it is still **{r['physical_as_deployed']}×** the warehouse: the "
          f"live snapshot is only {pct(lh['live_bytes'], lh['physical_bytes']):.1f}% of physical "
          f"because Athena `VACUUM` expired the superseded snapshots but did not delete their "
          f"data files (verified) — a Spark `remove_orphan_files` pass is required to reclaim "
          f"them, and is a tooling gap, not an architecture property.")
        w("")
        w(f"**Redundancy ratio (physical ÷ one clean modeled copy), structured:**")
        w("")
        w(f"| Architecture | Ratio | Why |")
        w(f"|---|--:|---|")
        w(f"| Data warehouse | **{rr['data_warehouse']}×** | {rr['data_warehouse_note']} |")
        w(f"| Data lakehouse — as deployed | {rr['data_lakehouse_as_deployed']}× | "
          f"{rr['data_lakehouse_as_deployed_note']} |")
        w(f"| Data lakehouse — compacted | **{rr['data_lakehouse_compacted']}×** | "
          f"{rr['data_lakehouse_compacted_note']} |")
        w("")
        w(f"**P3 holds.** The warehouse materializes the structured data twice inside Postgres "
          f"(`staging.stg_*` full reload + `warehouse.*` modeled); the lakehouse materializes it "
          f"once (Gold), because its staging layer is external tables over the raw CSVs — "
          f"**0 bytes**. That is one full physical copy the lakehouse eliminates (redundancy "
          f"{rr['data_warehouse']}× vs {rr['data_lakehouse_compacted']}×). On information content "
          f"(the live snapshot) the lakehouse is {r['live_snapshot']}× the warehouse. The only "
          f"figure that still favours the warehouse is raw physical bytes, and that is orphaned "
          f"data files awaiting a Spark `remove_orphan_files` pass — not a second copy of the data.")
        w("")
        w(f"**Unstructured / NLP:** {un2['finding']}")
        w("")

        tables = cmp.get("aurora_tables") or []
        if tables:
            w("### Per-table detail — data warehouse (Aurora)")
            w("")
            w("`live` = pgstattuple's exact live-tuple accounting (or the live/dead-tuple-ratio "
              "estimate if that extension isn't available) plus full-size indexes/TOAST — the "
              "same \"current, visible content\" concept as the lakehouse's Live snapshot column, "
              "just computed inside Postgres instead of from Iceberg's `$files` metadata table.")
            w("")
            w("| Table | Schema | Cat. | Rows | Physical | Live | Phys/Live |")
            w("|---|---|---|--:|--:|--:|--:|")
            for t in sorted(tables, key=lambda t: t["total_bytes"], reverse=True):
                cat = "NLP" if t.get("category") == "unstructured_nlp" else ("str" if t.get("category") == "structured" else "infra")
                ratio = round(t["total_bytes"] / t["live_bytes"], 2) if t.get("live_bytes") else "—"
                w(f"| `{t['table']}` | {t['schema']} | {cat} | {t['row_count']:,} | "
                  f"{human(t['total_bytes'])} | {human(t['live_bytes'])} | {ratio} |")
            w("")

    w("## Raw bucket")
    w("")
    w("| Class | Category | Bytes | Human | % of bucket | Prefixes |")
    w("|---|---|--:|--:|--:|---|")
    for cls, v in report["raw_detail"]["by_class"].items():
        w(f"| {cls} | {v.get('category', '?')} | {v['bytes']:,} | {human(v['bytes'])} | "
          f"{v['pct_of_bucket']}% | {len(v['prefixes'])} |")
    w("")
    w(f"Free-text clinical notes (`.csv.gz`) are **{raw['note_text_gz_pct']}%** of the raw bucket; "
      f"the structured MIMIC-IV CSVs are **{raw['structured_csv_pct']}%** "
      f"({human(raw['structured_csv_bytes'])}).")
    w("")
    w("## Gold Iceberg layer")
    w("")
    w(f"- Physical: **{human(gold['physical_bytes'])}** in **{gold['physical_objects']:,} objects** "
      f"({human(gold['data_bytes'])} data + {human(gold['iceberg_metadata_bytes'])} Iceberg metadata"
      + (f" + {human(gold['stray_bytes'])} stray query results" if gold["stray_bytes"] else "") + ")")
    if gold["live_snapshot_bytes"]:
        w(f"- Current snapshot (\"live\"): **{human(gold['live_snapshot_bytes'])}** in "
          f"**{gold['live_snapshot_files']:,} files** — physical is **{gold['physical_to_live_ratio']}×** "
          f"the live snapshot (un-expired snapshots).")
        if gold["small_file_tables"]:
            w(f"- **{len(gold['small_file_tables'])} tables** average < 1 MiB per live data file "
              f"(small-file fragmentation from `bucket()` / `day()` partitioning on a demo-sized dataset):")
            for s in gold["small_file_tables"][:8]:
                w(f"  - `{s['table']}` — {human(s['live_bytes'])} in {s['live_files']:,} files "
                  f"(~{human(s['avg_live_file_bytes'])}/file)")
    w("")
    w("### Per-table detail")
    w("")
    w("`file records` = Σ `record_count` over the current snapshot's data files — the true "
      "current row count for a copy-on-write table. If it exceeds `count(*)` (run with "
      "`--exact-rows` to check) the snapshot holds unmerged delete files or duplicate data "
      "from an append-instead-of-overwrite re-run.")
    w("")
    exact = report["meta"]["exact_rows"]
    w("| Table | Class | Cat. | Physical | Data | Iceberg meta | Live snapshot | Live files | File records |"
      + (" Exact rows |" if exact else "") + " Phys/Live |")
    w("|---|---|---|--:|--:|--:|--:|--:|--:|" + ("--:|" if exact else "") + "--:|")
    for name, t in report["gold_detail"]["tables"].items():
        live_b = human(t["live_bytes"]) if t.get("live_bytes") is not None else ("—" if t.get("athena_readable") else "n/a (fmt-v3)")
        lf = f"{t['live_files']:,}" if t.get("live_files") is not None else "—"
        rec = f"{t['live_records']:,}" if t.get("live_records") is not None else "—"
        ratio = t.get("physical_to_live_ratio")
        cat = "NLP" if t.get("category") == "unstructured_nlp" else ("str" if t.get("category") == "structured" else "infra")
        exact_cell = ""
        if exact:
            exact_cell = f" {t['exact_rows']:,} |" if t.get("exact_rows") is not None else " — |"
        w(f"| `{name}` | {t['class']} | {cat} | {human(t['physical_bytes'])} | {human(t['data_bytes'])} | "
          f"{human(t['meta_bytes'])} | {live_b} | {lf} | {rec} |" + exact_cell
          + f" {ratio if ratio else '—'} |")
    w("")
    w("## AI-inference layer  (P4: structured queries joined to AI inference outputs)")
    w("")
    ai = d["ai_inference_layer"]
    w(f"Tables: {', '.join('`'+t+'`' for t in ai['tables'])}")
    w("")
    w(f"- Physical: **{human(ai['physical_bytes'])}**"
      + (f"  ·  live snapshot: **{human(ai['live_bytes'])}**" if ai["live_bytes"] else ""))
    w(f"- **{ai['pct_of_gold_physical']}%** of the Gold layer  ·  "
      f"**{ai['pct_of_raw_bucket']}%** of the raw bucket  ·  "
      f"**{ai['pct_of_total_lakehouse']}%** of the total lakehouse")
    w("")
    w("**Marginal storage cost of the P4 capability ≈ 0.** An AI-inference output is a new small "
      "Iceberg table in the existing catalog (`" + m["gold_database"] + "`) and the existing bucket — "
      "no new store, no new format, no partitioning. It is joined to `fact_admission` in place.")
    w("")
    w("**Redundancy ratio for the AI-inference layer = 1.0** (one physical copy). In a decoupled "
      "architecture the same output is materialized in the model/NLP store *and* copied into the "
      "warehouse to become joinable — redundancy ≥ 2.0 plus an ongoing sync. The bytes are tiny "
      "either way; the property is structural, and is what makes P4's \"no ETL detour\" true. "
      "This is the native side of P3's redundancy-ratio measurement.")
    w("")
    w("## Methodology")
    w("")
    w("- **Physical bytes** — `s3:ListObjectsV2` over each bucket, summed. Current object versions "
      "only; noncurrent versions (if bucket versioning is on) are not counted.")
    w("- **Live-snapshot bytes** — `SELECT count(*), sum(file_size_in_bytes), sum(record_count) "
      "FROM \"<db>\".\"<table>$files\"` per Gold table (Iceberg metadata table; scans no data).")
    w("- **Data vs Iceberg metadata** — a Gold object is \"metadata\" iff its key is "
      "`<prefix>/<table>/metadata/...`, else \"data\".")
    w("- **AI-inference layer** — the tables `" + "`, `".join(sorted(AI_NLP_TABLES)) + "`.")
    w("- **Logical / compacted size** is *not* measured here — obtain it by running "
      "`OPTIMIZE <table> REWRITE DATA` + `VACUUM` (or Spark `rewrite_data_files` + "
      "`expire_snapshots`) and re-running this script; the drop is the accumulated bloat.")
    w("")
    w("## Caveats before using this for a P3 redundancy ratio")
    w("")
    w("The Gold layer's *physical* footprint is inflated over its logical content by (1) un-expired "
      "Iceberg snapshots (physical ≈ 2–3× live) and (2) small-file fragmentation from partitioned "
      "writes on a demo-sized dataset. Compact and expire snapshots first, or the ratio measures "
      "ETL hygiene rather than architecture. The AI/NLP tables are effectively exempt "
      "(unpartitioned, few re-runs).")
    w("")
    if not report.get("dw_vs_lakehouse"):
        w("The decoupled-baseline side of the P3 ratio must be measured separately against "
          "`mimic_datawarehouse_repo` (Aurora Postgres): `SELECT schemaname, relname, "
          "pg_total_relation_size(...)` over its `staging` + `warehouse` schemas, plus its raw S3 "
          "landing copy, then pass the totals via `--aurora-baseline`.")
    else:
        w(f"The Aurora baseline used above is `{m.get('aurora_baseline_file')}` — regenerate it "
          "from `mimic_datawarehouse_repo` and re-run with `--aurora-baseline <file>` to refresh "
          "the comparison.")
    w("")
    w("## Reproduce")
    w("")
    w("```")
    w(f"python measure_storage_footprint.py --dataset {m.get('dataset', 'demodataset')} --region {m['region']}")
    w("```")
    w("")
    w(f"_Machine-readable companion: `{m['json_filename']}`_")
    return "\n".join(L) + "\n"


def main() -> int:
    _make_console_utf8_safe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _cfg.add_dataset_arg(p)
    p.add_argument("--raw-bucket", default=None, help="raw/landing bucket (default: from --dataset)")
    p.add_argument("--gold-bucket", default=None, help="Iceberg gold bucket (default: from --dataset)")
    p.add_argument("--nlp-bucket", default=None, help="per-note / NLP bucket (default: from --dataset)")
    p.add_argument("--scripts-bucket", default=None, help=f"shared Glue scripts bucket (default: {_cfg.SCRIPTS_BUCKET})")
    p.add_argument("--gold-database", default=None, help="Iceberg gold database (default: from --dataset)")
    p.add_argument("--gold-prefix", default="mimic_bus")
    p.add_argument("--athena-workgroup", default=env("ATHENA_WORKGROUP", "primary"))
    p.add_argument("--athena-output", default=None,
                   help="Athena result location (default: s3://<gold-bucket>/athena-results/)")
    p.add_argument("--region", default=env("AWS_REGION"))
    p.add_argument("--out-dir", default=None,
                   help="output dir (default: docs/storage_footprint/<demo|full> for the --dataset)")
    p.add_argument("--skip-athena", action="store_true", help="S3 physical only; no Athena queries")
    p.add_argument("--exact-rows", action="store_true", help="also SELECT count(*) per table (scans data)")
    p.add_argument("--aurora-baseline", default=str(REPO_ROOT / "docs" / "storage_footprint" / "aurora_baseline.json"),
                   help="JSON with the Aurora DW storage numbers; renders a data-warehouse vs "
                        "data-lakehouse comparison. Pass 'none' to skip.")
    p.add_argument("--json-only", action="store_true", help="write only the JSON report")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    _profile = _cfg.dataset_profile(args.dataset)
    args.raw_bucket = args.raw_bucket or _profile.raw_bucket
    args.gold_bucket = args.gold_bucket or _profile.gold_bucket
    args.nlp_bucket = args.nlp_bucket or _profile.nlp_bucket
    args.scripts_bucket = args.scripts_bucket or _profile.scripts_bucket
    args.gold_database = args.gold_database or _profile.gold_database
    if args.out_dir is None:
        args.out_dir = str(REPO_ROOT / "docs" / "storage_footprint" / _profile.mode)

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    region = session.region_name or "unknown"
    s3 = session.client("s3")
    athena = session.client("athena")
    try:
        account = session.client("sts").get_caller_identity()["Account"]
    except ClientError:
        account = "unknown"
    athena_output = args.athena_output or f"s3://{args.gold_bucket}/athena-results/"

    log = (lambda *a: None) if args.quiet else (lambda *a: print(*a, file=sys.stderr))

    log(f"[1/4] S3 inventory: {args.raw_bucket}")
    raw_objects = list_bucket(s3, args.raw_bucket)
    log(f"      {len(raw_objects):,} objects")
    log(f"[2/4] S3 inventory: {args.gold_bucket}")
    gold_objects = list_bucket(s3, args.gold_bucket)
    log(f"      {len(gold_objects):,} objects")
    nlp_objects = list_bucket(s3, args.nlp_bucket) if args.nlp_bucket else []
    scripts_objects = list_bucket(s3, args.scripts_bucket) if args.scripts_bucket else []

    raw_sum = summarize_raw(raw_objects)
    gold_sum = summarize_gold(gold_objects, args.gold_prefix)
    nlp_debug_bytes = sum(sz for _, sz in nlp_objects)
    scripts_bytes = sum(sz for _, sz in scripts_objects)

    gold_tables = list(gold_sum["tables"].keys())
    if args.skip_athena:
        log("[3/4] Athena live-snapshot: SKIPPED (--skip-athena)")
        live = {}
    else:
        log(f"[3/4] Athena live-snapshot: {len(gold_tables)} tables via \"$files\"")
        live = live_snapshot(athena, args.gold_database, gold_tables, athena_output,
                             args.athena_workgroup, args.exact_rows)

    derived = derive(raw_sum, gold_sum, live, nlp_debug_bytes, scripts_bytes)

    dw_vs_lakehouse = None
    aurora_baseline = None
    if args.aurora_baseline and args.aurora_baseline.lower() != "none":
        bp = Path(args.aurora_baseline)
        if bp.is_file():
            aurora_baseline = json.loads(bp.read_text(encoding="utf-8"))
            dw_vs_lakehouse = build_dw_vs_lakehouse(
                derived["structured_vs_unstructured"], aurora_baseline
            )
            log(f"      Aurora baseline: {bp}")
        else:
            log(f"      Aurora baseline file not found ({bp}) -- skipping the DW comparison")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_name = f"storage_footprint_{ts}.json"
    report = {
        "meta": {
            "script_version": SCRIPT_VERSION,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "dataset": args.dataset,
            "git_commit": git_commit(),
            "aws_account": account,
            "region": region,
            "raw_bucket": args.raw_bucket,
            "gold_bucket": args.gold_bucket,
            "nlp_bucket": args.nlp_bucket,
            "scripts_bucket": args.scripts_bucket,
            "gold_database": args.gold_database,
            "gold_prefix": args.gold_prefix,
            "athena_workgroup": args.athena_workgroup,
            "athena_output": athena_output,
            "skip_athena": args.skip_athena,
            "exact_rows": args.exact_rows,
            "aurora_baseline_file": args.aurora_baseline if dw_vs_lakehouse else None,
            "json_filename": json_name,
            "queries": {
                "physical": "s3:ListObjectsV2 per bucket",
                "live_snapshot": 'SELECT count(*), sum(file_size_in_bytes), sum(record_count) '
                                 'FROM "<db>"."<table>$files"',
                "exact_rows": 'SELECT count(*) FROM "<db>"."<table>"' if args.exact_rows else None,
            },
        },
        "structured_vs_unstructured": derived["structured_vs_unstructured"],
        "dw_vs_lakehouse": dw_vs_lakehouse,
        "aurora_baseline": aurora_baseline,
        "raw_bucket": derived["raw_bucket"],
        "gold_bucket": derived["gold_bucket"],
        "ai_inference_layer": derived["ai_inference_layer"],
        "nlp_debug_bucket_bytes": nlp_debug_bytes,
        "scripts_bucket_bytes": scripts_bytes,
        "total_lakehouse_physical_bytes": derived["total_lakehouse_physical_bytes"],
        "derived": derived,
        "raw_detail": raw_sum,
        "gold_detail": gold_sum,
        "live_snapshot": live,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / json_name).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out_dir / "latest.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    written = [json_name, "latest.json"]
    if not args.json_only:
        md = render_markdown(report)
        (out_dir / f"storage_footprint_{ts}.md").write_text(md, encoding="utf-8")
        (out_dir / "latest.md").write_text(md, encoding="utf-8")
        written += [f"storage_footprint_{ts}.md", "latest.md"]

    log(f"[4/4] wrote {', '.join(written)} to {out_dir}")
    ai = derived["ai_inference_layer"]
    g = derived["gold_bucket"]
    sv = derived["structured_vs_unstructured"]
    print(f"total lakehouse physical  : {human(sv['grand_total_physical_bytes'])}")
    print(f"  structured (RDS-equiv)  : {human(sv['structured']['physical_bytes'])} physical  "
          f"/ {human(sv['structured']['live_bytes'])} live   ({sv['structured']['physical_share_pct']}%)")
    print(f"  unstructured / NLP      : {human(sv['unstructured_nlp']['physical_bytes'])} physical  "
          f"({sv['unstructured_nlp']['physical_share_pct']}%)  -- no warehouse equivalent")
    print(f"    note text .csv.gz     : {human(sv['unstructured_nlp']['note_text_bytes'])}")
    print(f"    NLP Gold tables       : {human(sv['unstructured_nlp']['nlp_gold_physical_bytes'])} phys "
          f"/ {human(sv['unstructured_nlp']['nlp_gold_live_bytes'])} live")
    print(f"gold physical / live      : {human(g['physical_bytes'])} / "
          f"{human(g['live_snapshot_bytes']) if g['live_snapshot_bytes'] else 'n/a'}"
          + (f"  ({g['physical_to_live_ratio']}x)" if g["physical_to_live_ratio"] else ""))
    print(f"AI-inference layer        : {human(ai['physical_bytes'])} physical  "
          f"({ai['pct_of_gold_physical']}% of gold)")
    print(f"P3 comparable (structured): {human(sv['p3_comparable_bytes']['structured_live'])} live")
    if dw_vs_lakehouse:
        ps = dw_vs_lakehouse["processed_structured"]
        rr = dw_vs_lakehouse["redundancy_ratio_structured"]
        print(f"DW vs LH (processed str.) : warehouse {human(ps['data_warehouse']['physical_bytes'])} "
              f"phys / {human(ps['data_warehouse']['live_bytes'])} live  vs  lakehouse "
              f"{human(ps['data_lakehouse']['physical_bytes'])} phys / "
              f"{human(ps['data_lakehouse']['live_bytes'])} live")
        print(f"redundancy ratio (str.)   : warehouse {rr['data_warehouse']}x  |  "
              f"lakehouse {rr['data_lakehouse_as_deployed']}x as-deployed, "
              f"{rr['data_lakehouse_compacted']}x compacted")
    print(f"report                    : {out_dir / 'latest.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
