#!/usr/bin/env python3
"""Compare every Gold Iceberg table with the raw source .csv.gz archive it was loaded from.

For each Gold table loaded straight from a MIMIC-IV source CSV (every dimension and fact),
pairs the raw `<source>.csv.gz` object in the raw bucket with the Iceberg table it became,
and reports both as-loaded and per-row sizes. Note-text tables (fact_discharge_note /
fact_radiology_note, rolled up from discharge.csv.gz / radiology.csv.gz to admission grain)
are compared on bytes only. Gold tables with no raw source of their own (aggregates, OBTs,
NLP inference tables, etl_* control tables) are listed separately.

Why per-row: on the full dataset every fact table over 1M rows is loaded as a 1M-row
sample (--row-limit), while the raw .csv.gz always holds the whole table -- so as-loaded
bytes compare a sample against the complete file. Per-row bytes (raw: gz bytes / raw rows;
Iceberg: live bytes / Gold rows) compare like with like, and "Iceberg, full-row equivalent"
extrapolates the Iceberg size to every raw row (assumes the loaded sample is representative
-- --row-limit takes the first N rows, not a random sample).

Inputs:
  * docs/storage_footprint/<demo|full>/latest.json (measure_storage_footprint.py) -- raw
    .csv.gz object sizes and each Gold table's live-snapshot bytes / rows and physical bytes.
  * <gold_database>.etl_process_log via Athena -- the full raw row count each dim/fact job
    logged (raw_row_count) on its most recent run. --skip-athena omits it (no per-row figures).
  * docs/parquet_comparison/<demo|full>/latest_comparison.json (compare_parquet_iceberg.py,
    optional) -- Parquet (ZSTD/Snappy) and CSV copy sizes for the representative table per
    archetype, merged into the all-tables list and shown as a four-format section.

The report opens with a summary and one consolidated list of every Gold table (raw source,
raw and Gold row counts, Iceberg live/physical bytes, and Parquet/CSV copy sizes where they
exist), followed by the per-group detail tables.

Caveats the report repeats: the Gold schema is not the raw schema (adds surrogate date keys,
derived measures and four audit columns; types columns), and the codecs differ (Iceberg =
Parquet + ZSTD, columnar; raw = row-oriented CSV + gzip). The ratio is "what the modeled
Gold table costs relative to its source archive", not a pure file-format benchmark -- for
that, see compare_parquet_iceberg.py (identical content across formats).

Writes docs/iceberg_vs_raw/<demo|full>/iceberg_vs_raw_<UTC>.{json,md} + latest.{json,md}.

Usage:
    python compare_iceberg_raw_csv.py --dataset fulldataset
    python compare_iceberg_raw_csv.py --dataset fulldataset --skip-athena
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "etl"))
from mimic_lakehouse import config as _cfg  # noqa: E402
import create_dim_visual_etl_jobs as _dim  # noqa: E402
import create_fact_visual_etl_jobs as _fact  # noqa: E402

SCRIPT_VERSION = "1.0.0"

# note-text Gold tables -> the raw .csv.gz they come from (rolled up to admission grain, so
# rows aren't comparable one-to-one; bytes are)
NOTE_TABLES = {"fact_discharge_note": "discharge_raw", "fact_radiology_note": "radiology_raw"}


def _make_console_utf8_safe() -> None:
    enc = (sys.stdout.encoding or "").lower()
    if enc and enc != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def human(n) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{int(n):,} B" if unit == "B" else f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TiB"


def ratio(a, b, nd=3):
    return round(a / b, nd) if a is not None and b else None


def pct_change(base, other):
    """Signed % change of `other` vs `base` (negative = smaller than base)."""
    return round(100.0 * (other - base) / base, 1) if base and other is not None else None


def raw_row_counts(region: str, database: str, output: str, workgroup: str) -> dict[str, dict]:
    """process_name -> {raw_rows, gold_rows, full_run_id, start_ts} from each process's most
    recent etl_process_log row that carries a raw_row_count."""
    ath = boto3.Session(region_name=region).client("athena")
    sql = (f"SELECT process_name, raw_row_count, gold_row_count, full_run_id, CAST(start_ts AS VARCHAR) "
           f"FROM (SELECT *, row_number() OVER (PARTITION BY process_name ORDER BY start_ts DESC) rn "
           f"FROM {database}.etl_process_log WHERE raw_row_count IS NOT NULL) WHERE rn = 1")
    qid = ath.start_query_execution(QueryString=sql, WorkGroup=workgroup,
                                    ResultConfiguration={"OutputLocation": output})["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(f"etl_process_log query {st['State']}: {st.get('StateChangeReason')}")
    out = {}
    for row in ath.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]:
        v = [c.get("VarCharValue") for c in row["Data"]]
        out[v[0]] = {"raw_rows": int(v[1]) if v[1] else None, "gold_rows_logged": int(v[2]) if v[2] else None,
                     "full_run_id": int(v[3]) if v[3] else None, "logged_start_ts": v[4]}
    return out


def table_entry(name: str, kind: str, raw_table: str, raw_bytes: dict, gold: dict, rows: dict) -> dict:
    g = gold.get(name, {})
    r = rows.get(name, {})
    raw_gz = raw_bytes.get(raw_table)
    raw_rows = r.get("raw_rows")
    gold_rows = g.get("live_records")
    live = g.get("live_bytes")
    raw_bpr = raw_gz / raw_rows if raw_gz and raw_rows else None
    ice_bpr = live / gold_rows if live and gold_rows else None
    full_equiv = round(ice_bpr * raw_rows) if ice_bpr and raw_rows else None
    return {
        "table": name, "kind": kind, "raw_table": raw_table,
        "raw_csv_gz_bytes": raw_gz,
        "raw_rows": raw_rows,
        "gold_rows": gold_rows,
        "row_delta": (gold_rows - raw_rows) if gold_rows is not None and raw_rows is not None else None,
        "row_coverage_pct": round(100.0 * gold_rows / raw_rows, 2) if gold_rows and raw_rows else None,
        "iceberg_live_bytes": live,
        "iceberg_live_files": g.get("live_files"),
        "iceberg_physical_bytes": g.get("physical_bytes"),
        "raw_bytes_per_row": round(raw_bpr, 2) if raw_bpr else None,
        "iceberg_bytes_per_row": round(ice_bpr, 2) if ice_bpr else None,
        "iceberg_full_row_equivalent_bytes": full_equiv,
        "per_row_ratio_iceberg_over_raw": ratio(ice_bpr, raw_bpr),
        "per_row_change_pct": pct_change(raw_bpr, ice_bpr),
        "as_loaded_ratio_iceberg_over_raw": ratio(live, raw_gz),
        "logged_full_run_id": r.get("full_run_id"),
    }


def group_summary(entries: list[dict]) -> dict:
    def s(field):
        vals = [e[field] for e in entries if e.get(field) is not None]
        return sum(vals) if vals else None
    comparable = [e for e in entries if e["iceberg_full_row_equivalent_bytes"] is not None and e["raw_csv_gz_bytes"]]
    raw_c = sum(e["raw_csv_gz_bytes"] for e in comparable)
    eq_c = sum(e["iceberg_full_row_equivalent_bytes"] for e in comparable)
    smaller = sum(1 for e in comparable if e["per_row_ratio_iceberg_over_raw"] < 1)
    return {
        "tables": len(entries),
        "tables_per_row_comparable": len(comparable),
        "tables_iceberg_smaller_per_row": smaller,
        "raw_csv_gz_bytes": s("raw_csv_gz_bytes"),
        "raw_rows": s("raw_rows"),
        "gold_rows": s("gold_rows"),
        "iceberg_live_bytes_as_loaded": s("iceberg_live_bytes"),
        "iceberg_physical_bytes_as_loaded": s("iceberg_physical_bytes"),
        "iceberg_full_row_equivalent_bytes": eq_c if comparable else None,
        "raw_csv_gz_bytes_comparable": raw_c if comparable else None,
        "full_row_ratio_iceberg_over_raw": ratio(eq_c, raw_c),
        "full_row_change_pct": pct_change(raw_c, eq_c),
        "as_loaded_ratio_iceberg_over_raw": ratio(s("iceberg_live_bytes"), s("raw_csv_gz_bytes")),
    }


def format_entries(fmt: dict | None) -> dict[str, dict]:
    """Iceberg table name -> its four-format figures from compare_parquet_iceberg.py's report
    (only the representative table per archetype has Parquet/CSV copies)."""
    out = {}
    for e in (fmt or {}).get("entries", []):
        out[e["iceberg_table"]] = {
            "archetype": e.get("archetype"),
            "iceberg_basis_bytes": e.get("iceberg_basis_bytes"),
            "iceberg_metadata_bytes": e.get("iceberg_metadata_bytes"),
            "parquet_zstd_bytes": e.get("parquet_zstd_physical_bytes"),
            "parquet_snappy_bytes": e.get("parquet_snappy_physical_bytes"),
            "csv_copy_bytes": e.get("csv_physical_bytes"),
            "pct_iceberg_vs_parquet_zstd": e.get("reduction_pct_iceberg_vs_parquet_zstd"),
            "pct_iceberg_vs_csv": e.get("reduction_pct_iceberg_vs_csv"),
            "pct_parquet_zstd_vs_snappy": e.get("reduction_pct_parquet_zstd_vs_snappy"),
        }
    return out


def all_tables(gold: dict, groups: dict[str, list[dict]], fmt_by_table: dict[str, dict]) -> list[dict]:
    """One row per Gold table: group, raw source + rows, Gold rows, Iceberg bytes, and the
    four-format figures where a Parquet/CSV copy exists."""
    by_name = {e["table"]: (g, e) for g, es in groups.items() for e in es}
    order = {"fact": 0, "dim": 1, "note_text": 2}
    out = []
    for name, t in gold.items():
        g, e = by_name.get(name, (t.get("class") or "derived", {}))
        out.append({
            "table": name, "group": g, "category": t.get("category"),
            "raw_table": e.get("raw_table"), "raw_csv_gz_bytes": e.get("raw_csv_gz_bytes"),
            "raw_rows": e.get("raw_rows"), "gold_rows": t.get("live_records"),
            "iceberg_live_bytes": t.get("live_bytes"), "iceberg_live_files": t.get("live_files"),
            "iceberg_physical_bytes": t.get("physical_bytes"),
            "athena_readable": t.get("athena_readable"),
            "format_comparison": fmt_by_table.get(name),
        })
    return sorted(out, key=lambda x: (order.get(x["group"], 3), x["group"], x["table"]))


def build(sf: dict, rows: dict, fmt: dict | None = None) -> dict:
    raw_bytes = {k: v["bytes"] for k, v in sf["raw_detail"]["by_prefix"].items()}
    gold = sf["gold_detail"]["tables"]
    facts = [table_entry(f["name"], "fact", f["raw_table"], raw_bytes, gold, rows) for f in _fact.FACTS]
    dims = [table_entry(d["name"], "dim", d["raw_table"], raw_bytes, gold, rows) for d in _dim.DIMENSIONS
            if d.get("raw_table")]
    notes = []
    for name, raw_table in NOTE_TABLES.items():
        e = table_entry(name, "note_text", raw_table, raw_bytes, gold, rows)
        # rolled up note -> admission grain: per-row figures would compare different grains
        for k in ("raw_bytes_per_row", "iceberg_bytes_per_row", "iceberg_full_row_equivalent_bytes",
                  "per_row_ratio_iceberg_over_raw", "per_row_change_pct", "row_coverage_pct", "row_delta"):
            e[k] = None
        e["as_loaded_change_pct"] = pct_change(e["raw_csv_gz_bytes"], e["iceberg_live_bytes"])
        notes.append(e)
    sourced = {e["table"] for e in facts + dims + notes}
    derived = [{"table": n, "class": t.get("class"), "category": t.get("category"),
                "iceberg_live_bytes": t.get("live_bytes"), "iceberg_live_rows": t.get("live_records"),
                "iceberg_physical_bytes": t.get("physical_bytes"),
                "athena_readable": t.get("athena_readable")}
               for n, t in sorted(gold.items()) if n not in sourced]
    gold_live_total = sum((t.get("live_bytes") or 0) for t in gold.values())
    gold_phys_total = sum((t.get("physical_bytes") or 0) for t in gold.values())
    raw_struct = sum(e["raw_csv_gz_bytes"] or 0 for e in facts + dims)
    fmt_by_table = format_entries(fmt)
    return {
        "all_tables": all_tables(gold, {"fact": facts, "dim": dims, "note_text": notes}, fmt_by_table),
        "format_comparison": {
            "generated_utc": ((fmt or {}).get("meta") or {}).get("generated_utc"),
            "tables": [{"table": n, **v} for n, v in fmt_by_table.items()],
            "summary": (fmt or {}).get("summary"),
        } if fmt else None,
        "facts": sorted(facts, key=lambda e: -(e["raw_csv_gz_bytes"] or 0)),
        "dimensions": sorted(dims, key=lambda e: -(e["raw_csv_gz_bytes"] or 0)),
        "note_text": notes,
        "derived_gold_tables": sorted(derived, key=lambda e: -(e["iceberg_live_bytes"] or 0)),
        "summary": {
            "facts": group_summary(facts),
            "dimensions": group_summary(dims),
            "all_source_loaded": group_summary(facts + dims),
            "note_text": {
                "raw_csv_gz_bytes": sum(e["raw_csv_gz_bytes"] or 0 for e in notes),
                "iceberg_live_bytes": sum(e["iceberg_live_bytes"] or 0 for e in notes),
                "as_loaded_ratio_iceberg_over_raw": ratio(sum(e["iceberg_live_bytes"] or 0 for e in notes),
                                                         sum(e["raw_csv_gz_bytes"] or 0 for e in notes)),
            },
            "derived": {"tables": len(derived),
                        "iceberg_live_bytes": sum(e["iceberg_live_bytes"] or 0 for e in derived),
                        "iceberg_physical_bytes": sum(e["iceberg_physical_bytes"] or 0 for e in derived)},
            "gold_layer": {"tables": len(gold), "iceberg_live_bytes": gold_live_total,
                           "iceberg_physical_bytes": gold_phys_total,
                           "raw_structured_csv_gz_bytes": raw_struct,
                           "live_over_raw_structured": ratio(gold_live_total, raw_struct)},
            "row_mismatches": [{"table": e["table"], "raw_rows": e["raw_rows"], "gold_rows": e["gold_rows"],
                                "row_delta": e["row_delta"]}
                               for e in facts + dims
                               if e["row_delta"] not in (None, 0) and e["row_coverage_pct"] and e["row_coverage_pct"] > 99],
        },
    }


def render_md(r: dict) -> str:
    m, s = r["meta"], r["summary"]
    w = []
    P = w.append

    def fx(v, suffix=""):
        return "n/a" if v is None else f"{v}{suffix}"

    def num(v):
        return "n/a" if v is None else f"{v:,}"

    P(f"# Gold Iceberg tables vs raw source CSV archives -- {m['dataset']}")
    P("")
    P(f"- **Generated:** {m['generated_utc']} · `{m['gold_database']}` vs `s3://{m['raw_bucket']}`")
    P(f"- **Storage measurement:** {m['storage_footprint_generated_utc']} "
      f"(`{m['storage_footprint_file']}`); raw row counts from `etl_process_log` "
      f"(full_run_id {', '.join(str(x) for x in m['row_count_full_run_ids']) or 'n/a'})")
    P("")
    P("## Summary")
    P("")
    P("Per-row comparison (the fair one -- fact tables over 1M rows hold a 1M-row sample, the "
      "raw archive holds every row). *Full-row equivalent* = Iceberg bytes/row × raw rows.")
    P("")
    P("| Group | Tables | Raw .csv.gz | Iceberg as loaded (live) | Iceberg, full-row equivalent | "
      "Iceberg / raw (full-row) | Change | Iceberg smaller per row |")
    P("|---|--:|--:|--:|--:|--:|--:|--:|")
    for label, key in (("Fact tables", "facts"), ("Dimensions", "dimensions"),
                       ("**All source-loaded**", "all_source_loaded")):
        g = s[key]
        P(f"| {label} | {g['tables']} | {human(g['raw_csv_gz_bytes'])} | {human(g['iceberg_live_bytes_as_loaded'])} | "
          f"{human(g['iceberg_full_row_equivalent_bytes'])} | {fx(g['full_row_ratio_iceberg_over_raw'], '×')} | "
          f"{fx(g['full_row_change_pct'], '%')} | {g['tables_iceberg_smaller_per_row']} of {g['tables_per_row_comparable']} |")
    nt = s["note_text"]
    P(f"| Note text (bytes only) | {len(r['note_text'])} | {human(nt['raw_csv_gz_bytes'])} | "
      f"{human(nt['iceberg_live_bytes'])} | -- | {fx(nt['as_loaded_ratio_iceberg_over_raw'], '×')} (as loaded) | -- | -- |")
    d, gl = s["derived"], s["gold_layer"]
    P(f"| Derived (no raw source) | {d['tables']} | -- | {human(d['iceberg_live_bytes'])} | -- | -- | -- | -- |")
    P("")
    P(f"Whole Gold layer: {gl['tables']} tables, {human(gl['iceberg_live_bytes'])} live "
      f"({human(gl['iceberg_physical_bytes'])} physical incl. superseded/orphaned files) vs "
      f"{human(gl['raw_structured_csv_gz_bytes'])} of raw structured .csv.gz "
      f"({fx(gl['live_over_raw_structured'], '×')} as loaded -- understated by the 1M-row cap).")
    P("")
    P(f"## All Gold tables ({len(r['all_tables'])})")
    P("")
    P("Every table in the Gold layer with its raw source, row counts and Iceberg size. *Parquet "
      "(ZSTD) / Parquet (Snappy) / CSV copy* are filled only for the representative table per "
      "archetype that has comparison copies (see the next section).")
    P("")
    P("| # | Table | Group | Raw source | Raw .csv.gz | Raw rows | Gold rows | Iceberg live | "
      "Iceberg physical | Parquet (ZSTD) | Parquet (Snappy) | CSV copy |")
    P("|--:|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for i, t in enumerate(r["all_tables"], 1):
        fc = t["format_comparison"] or {}
        live = human(t["iceberg_live_bytes"]) if t["athena_readable"] is not False else "n/a (format-v3)"
        P(f"| {i} | `{t['table']}` | {t['group']} | {('`' + t['raw_table'] + '`') if t['raw_table'] else '--'} | "
          f"{human(t['raw_csv_gz_bytes']) if t['raw_csv_gz_bytes'] else '--'} | {num(t['raw_rows']) if t['raw_rows'] else '--'} | "
          f"{num(t['gold_rows'])} | {live} | {human(t['iceberg_physical_bytes'])} | "
          f"{human(fc['parquet_zstd_bytes']) if fc else '--'} | {human(fc['parquet_snappy_bytes']) if fc else '--'} | "
          f"{human(fc['csv_copy_bytes']) if fc else '--'} |")
    P("")
    fcmp = r.get("format_comparison")
    if fcmp:
        P("## Iceberg vs Parquet (ZSTD, Snappy) vs CSV -- representative table per archetype")
        P("")
        P(f"From `docs/parquet_comparison/` (compare_parquet_iceberg.py, {fcmp['generated_utc']}). Identical "
          "rows and columns in every format; Iceberg = data + metadata, excluding orphaned files. "
          "Reduction % = (comparator - subject) / comparator; negative = the first-named format is larger.")
        P("")
        P("| Archetype | Table | Iceberg | Parquet (ZSTD) | Parquet (Snappy) | CSV (gzip) | "
          "Iceberg vs Parquet (ZSTD) | Iceberg vs CSV | Parquet ZSTD vs Snappy |")
        P("|---|---|--:|--:|--:|--:|--:|--:|--:|")
        for t in fcmp["tables"]:
            P(f"| {t['archetype']} | `{t['table']}` | {human(t['iceberg_basis_bytes'])} | {human(t['parquet_zstd_bytes'])} | "
              f"{human(t['parquet_snappy_bytes'])} | {human(t['csv_copy_bytes'])} | {fx(t['pct_iceberg_vs_parquet_zstd'], '%')} | "
              f"{fx(t['pct_iceberg_vs_csv'], '%')} | {fx(t['pct_parquet_zstd_vs_snappy'], '%')} |")
        fs = fcmp.get("summary") or {}
        if fs:
            P(f"| **Total** | {fs.get('archetype_count')} tables | **{human(fs.get('total_iceberg_basis_bytes'))}** | "
              f"**{human(fs.get('total_parquet_zstd_physical_bytes'))}** | **{human(fs.get('total_parquet_snappy_physical_bytes'))}** | "
              f"**{human(fs.get('total_csv_physical_bytes'))}** | **{fx(fs.get('reduction_pct_iceberg_vs_parquet_zstd'), '%')}** | "
              f"**{fx(fs.get('reduction_pct_iceberg_vs_csv'), '%')}** | **{fx(fs.get('reduction_pct_parquet_zstd_vs_snappy'), '%')}** |")
        P("")
    P("## Fact tables -- detail (one row per fact loaded from a source CSV archive)")
    P("")
    P("| Fact table | Raw source | Raw .csv.gz | Raw rows | Gold rows | Coverage | Raw B/row | "
      "Iceberg live | Iceberg B/row | Iceberg / raw per row | Change | Full-row equivalent | Iceberg physical |")
    P("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for e in r["facts"]:
        P(f"| `{e['table']}` | `{e['raw_table']}` | {human(e['raw_csv_gz_bytes'])} | {num(e['raw_rows'])} | "
          f"{num(e['gold_rows'])} | {fx(e['row_coverage_pct'], '%')} | {fx(e['raw_bytes_per_row'])} | "
          f"{human(e['iceberg_live_bytes'])} | {fx(e['iceberg_bytes_per_row'])} | {fx(e['per_row_ratio_iceberg_over_raw'], '×')} | "
          f"{fx(e['per_row_change_pct'], '%')} | {human(e['iceberg_full_row_equivalent_bytes'])} | {human(e['iceberg_physical_bytes'])} |")
    P("")
    P("## Dimensions -- detail")
    P("")
    P("| Dimension | Raw source | Raw .csv.gz | Raw rows | Gold rows | Raw B/row | Iceberg live | Iceberg B/row | "
      "Iceberg / raw per row | Change |")
    P("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for e in r["dimensions"]:
        P(f"| `{e['table']}` | `{e['raw_table']}` | {human(e['raw_csv_gz_bytes'])} | {num(e['raw_rows'])} | "
          f"{num(e['gold_rows'])} | {fx(e['raw_bytes_per_row'])} | {human(e['iceberg_live_bytes'])} | "
          f"{fx(e['iceberg_bytes_per_row'])} | {fx(e['per_row_ratio_iceberg_over_raw'], '×')} | {fx(e['per_row_change_pct'], '%')} |")
    P("")
    P("## Note text -- bytes only")
    P("")
    P("Notes are rolled up from one row per note to one row per admission, so rows are not "
      "comparable; the byte comparison is as loaded (no row cap applies to these tables).")
    P("")
    P("| Gold table | Raw source | Raw .csv.gz | Raw notes | Gold rows (admissions) | Iceberg live | Iceberg / raw | Change |")
    P("|---|---|--:|--:|--:|--:|--:|--:|")
    for e in r["note_text"]:
        P(f"| `{e['table']}` | `{e['raw_table']}` | {human(e['raw_csv_gz_bytes'])} | {num(e['raw_rows'])} | "
          f"{num(e['gold_rows'])} | {human(e['iceberg_live_bytes'])} | "
          f"{fx(e['as_loaded_ratio_iceberg_over_raw'], '×')} | {fx(e['as_loaded_change_pct'], '%')} |")
    P("")
    P("## Derived Gold tables (no raw source of their own)")
    P("")
    P("| Table | Class | Iceberg live | Live rows | Iceberg physical |")
    P("|---|---|--:|--:|--:|")
    for e in r["derived_gold_tables"]:
        live = human(e["iceberg_live_bytes"]) if e["athena_readable"] is not False else "n/a (format-v3)"
        P(f"| `{e['table']}` | {e['class']} | {live} | {num(e['iceberg_live_rows'])} | "
          f"{human(e['iceberg_physical_bytes'])} |")
    if s["row_mismatches"]:
        P("")
        P("## Row-count mismatches on fully loaded tables")
        P("")
        P("Tables loaded in full (not capped) whose Gold row count differs from the raw row count -- "
          "worth checking (e.g. a CSV header line loaded as a data row):")
        P("")
        P("| Table | Raw rows | Gold rows | Delta |")
        P("|---|--:|--:|--:|")
        for x in s["row_mismatches"]:
            P(f"| `{x['table']}` | {x['raw_rows']:,} | {x['gold_rows']:,} | {x['row_delta']:+,} |")
    P("")
    P("## How to read this")
    P("")
    P("- **Raw .csv.gz** = the source archive object in the raw bucket (row-oriented CSV, gzip). "
      "**Iceberg live** = bytes of the data files the table's current snapshot references (Parquet, "
      "ZSTD, columnar) -- excludes superseded and orphaned files. **Iceberg physical** = every object "
      "under the table's S3 prefix, including metadata and files awaiting orphan removal.")
    P("- **Per row** compares bytes per row on each side, so the 1M-row cap on large fact tables "
      "doesn't distort it. *Full-row equivalent* extrapolates linearly from the loaded rows -- "
      "`--row-limit` loads the first N rows, not a random sample.")
    P("- **Not a pure format benchmark.** Gold tables add surrogate date keys, derived measures and "
      "four audit columns, and type the columns; the codec also differs. For identical content "
      "across formats see `docs/parquet_comparison/` (compare_parquet_iceberg.py).")
    P("")
    P("## Reproduce")
    P("")
    P("```")
    P(f"python measure_storage_footprint.py --dataset {m['dataset']}")
    P(f"python compare_iceberg_raw_csv.py --dataset {m['dataset']}")
    P("```")
    return "\n".join(w) + "\n"


def main() -> int:
    _make_console_utf8_safe()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _cfg.add_dataset_arg(ap)
    ap.add_argument("--storage-file", default=None,
                    help="storage-footprint JSON (default: docs/storage_footprint/<demo|full>/latest.json)")
    ap.add_argument("--format-file", default=None,
                    help="Iceberg/Parquet/CSV comparison JSON from compare_parquet_iceberg.py "
                         "(default: docs/parquet_comparison/<demo|full>/latest_comparison.json; optional)")
    ap.add_argument("--skip-athena", action="store_true",
                    help="don't read raw row counts from etl_process_log (no per-row figures)")
    ap.add_argument("--workgroup", default=_cfg.ATHENA_WORKGROUP)
    ap.add_argument("--region", default=_cfg.get("AWS_REGION"))
    ap.add_argument("--out-dir", default=None, help="default: docs/iceberg_vs_raw/<demo|full>")
    args = ap.parse_args()

    profile = _cfg.dataset_profile(args.dataset)
    storage_file = Path(args.storage_file or REPO_ROOT / "docs" / "storage_footprint" / profile.mode / "latest.json")
    if not storage_file.is_file():
        raise SystemExit(f"{storage_file} not found -- run measure_storage_footprint.py --dataset {profile.dataset} first.")
    sf = json.loads(storage_file.read_text(encoding="utf-8"))

    rows = {} if args.skip_athena else raw_row_counts(
        args.region, profile.gold_database, f"s3://{profile.gold_bucket}/athena-results/", args.workgroup)

    format_file = Path(args.format_file or REPO_ROOT / "docs" / "parquet_comparison" / profile.mode / "latest_comparison.json")
    fmt = json.loads(format_file.read_text(encoding="utf-8")) if format_file.is_file() else None
    if fmt is None:
        print(f"note: {format_file} not found -- report omits the Parquet/CSV comparison columns")

    report = build(sf, rows, fmt)
    ts = datetime.now(timezone.utc)
    report["meta"] = {
        "script_version": SCRIPT_VERSION, "generated_utc": ts.isoformat(), "dataset": profile.dataset,
        "gold_database": profile.gold_database, "raw_bucket": profile.raw_bucket,
        "storage_footprint_file": str(storage_file.relative_to(REPO_ROOT)) if storage_file.is_relative_to(REPO_ROOT) else str(storage_file),
        "storage_footprint_generated_utc": (sf.get("meta") or {}).get("generated_utc"),
        "row_counts_source": None if args.skip_athena else f"{profile.gold_database}.etl_process_log (latest row per process)",
        "row_count_full_run_ids": sorted({e["logged_full_run_id"] for e in report["facts"] + report["dimensions"]
                                          if e["logged_full_run_id"] is not None}),
        "athena_workgroup": None if args.skip_athena else args.workgroup,
    }

    out = Path(args.out_dir or REPO_ROOT / "docs" / "iceberg_vs_raw" / profile.mode)
    out.mkdir(parents=True, exist_ok=True)
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    md = render_md(report)
    for name in (f"iceberg_vs_raw_{stamp}", "latest"):
        (out / f"{name}.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        (out / f"{name}.md").write_text(md, encoding="utf-8")

    a = report["summary"]["all_source_loaded"]
    print(f"source-loaded tables: {a['tables']}  raw .csv.gz {human(a['raw_csv_gz_bytes'])}  "
          f"Iceberg full-row equivalent {human(a['iceberg_full_row_equivalent_bytes'])}  "
          f"(Iceberg/raw {a['full_row_ratio_iceberg_over_raw']}x, {a['full_row_change_pct']}%)")
    print(f"written: {out / 'latest.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
