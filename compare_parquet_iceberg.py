#!/usr/bin/env python3
"""Interpret the raw facts measure_parquet_comparison.py collected -- per-archetype and
overall storage ratios across Iceberg (ZSTD), Parquet (ZSTD), Parquet (Snappy), and
gzip-compressed CSV for the Multimodal Fusion Schema. Supports two related questions:
"How does storage taken by Iceberg compare to Parquet / CSV?" and, since Parquet appears
in two codecs here, "How much of any Iceberg-vs-Parquet difference is the table format
itself, versus the compression codec each side happens to use?"

Standalone: reads only the JSON file measure_parquet_comparison.py already wrote. Does not
call AWS itself, so it can be re-run (e.g. after changing how a ratio is reported) without
re-measuring S3/Athena. Run measure_parquet_comparison.py first if latest.json is missing
or stale.

Usage:
    python compare_parquet_iceberg.py --dataset fulldataset
    python compare_parquet_iceberg.py --measurement-file docs/parquet_comparison/full/latest.json

Supports the paper's RQ1 ("Multimodal Fusion Schema" paper): "To what extent, if any, does
the Apache Iceberg open table format yield measurable storage efficiency relative to a raw
(CSV) table format and the Apache Parquet table format, when all three are populated with
the same multimodal fact archetypes at an identical grain?" See the README's "Research
paper: RQ1 and RQ2" section.
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

SCRIPT_VERSION = "3.0.0"


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


def ratio(a, b):
    return round(a / b, 3) if b else None


def reduction_pct(baseline, other):
    """Percent of `other`'s physical bytes that `baseline` saves: positive = baseline is
    smaller (a real reduction), negative = baseline is larger (baseline costs MORE than
    `other`, an increase, not a reduction)."""
    if not other:
        return None
    return round((other - baseline) / other * 100, 1)


def interpret_archetype(entry: dict) -> dict:
    fmts = {f["name"]: f for f in entry["formats"]}
    ice, pq_z, pq_s, csv = fmts["iceberg"], fmts["parquet_zstd"], fmts["parquet_snappy"], fmts["csv_gzip"]

    ice_phys = ice["physical_bytes"]
    ice_live = ice.get("live_bytes")
    ice_meta = ice.get("metadata_bytes")
    ice_rows = ice.get("row_count")

    # Orphan-excluded estimate: Iceberg's "<table>$files" metadata table lists exactly the
    # data files the CURRENT snapshot references (that's what live_bytes already is), so any
    # physical byte outside (live + current metadata) is -- by construction -- unreferenced
    # by that snapshot, i.e. what system.remove_orphan_files would eventually delete. Not
    # available when Athena can't read the table at all (format-v3) -- see
    # measure_parquet_comparison.py's athena_readable flag.
    ice_excl_orphans = (ice_live + ice_meta) if (ice_live is not None and ice_meta is not None) else None
    estimated_orphan_bytes = (ice_phys - ice_excl_orphans) if ice_excl_orphans is not None else None
    ice_basis = ice_excl_orphans if ice_excl_orphans is not None else ice_phys

    pq_z_phys, pq_s_phys, csv_phys = pq_z["physical_bytes"], pq_s["physical_bytes"], csv["physical_bytes"]
    pq_z_rows, pq_s_rows, csv_rows = pq_z.get("row_count"), pq_s.get("row_count"), csv.get("row_count")
    basis_rows = ice_rows or pq_z_rows or pq_s_rows or csv_rows  # same data, same row count on every side

    return {
        "archetype": entry["archetype"],
        "name": entry["name"],
        "note": entry.get("note"),
        "iceberg_table": ice["table"],
        "iceberg_physical_bytes": ice_phys,
        "iceberg_live_bytes": ice_live,
        "iceberg_metadata_bytes": ice_meta,
        "iceberg_physical_excl_orphans_bytes": ice_excl_orphans,
        "iceberg_basis_bytes": ice_basis,
        "estimated_orphan_bytes": estimated_orphan_bytes,
        "iceberg_row_count": ice_rows,
        "iceberg_athena_error": ice.get("athena_error"),
        "parquet_zstd_table": pq_z["table"],
        "parquet_zstd_physical_bytes": pq_z_phys,
        "parquet_zstd_row_count": pq_z_rows,
        "parquet_snappy_table": pq_s["table"],
        "parquet_snappy_physical_bytes": pq_s_phys,
        "parquet_snappy_row_count": pq_s_rows,
        "csv_table": csv["table"],
        "csv_physical_bytes": csv_phys,
        "csv_row_count": csv_rows,
        "reduction_pct_iceberg_vs_parquet_zstd": reduction_pct(ice_basis, pq_z_phys),
        "reduction_pct_iceberg_vs_csv": reduction_pct(ice_basis, csv_phys),
        "reduction_pct_parquet_zstd_vs_snappy": reduction_pct(pq_z_phys, pq_s_phys),
        "bytes_per_row_iceberg": ratio(ice_basis, basis_rows),
        "bytes_per_row_parquet_zstd": ratio(pq_z_phys, pq_z_rows),
        "bytes_per_row_parquet_snappy": ratio(pq_s_phys, pq_s_rows),
        "bytes_per_row_csv": ratio(csv_phys, csv_rows),
    }


def build_summary(entries: list[dict]) -> dict:
    total_ice_phys = sum(e["iceberg_physical_bytes"] for e in entries)
    total_ice_basis = sum(e["iceberg_basis_bytes"] for e in entries)
    total_orphan_est = sum(e["estimated_orphan_bytes"] for e in entries if e.get("estimated_orphan_bytes") is not None)
    total_pq_z = sum(e["parquet_zstd_physical_bytes"] for e in entries)
    total_pq_s = sum(e["parquet_snappy_physical_bytes"] for e in entries)
    total_csv = sum(e["csv_physical_bytes"] for e in entries)
    return {
        "archetype_count": len(entries),
        "total_iceberg_physical_bytes": total_ice_phys,
        "total_iceberg_basis_bytes": total_ice_basis,
        "total_estimated_orphan_bytes": total_orphan_est or None,
        "total_parquet_zstd_physical_bytes": total_pq_z,
        "total_parquet_snappy_physical_bytes": total_pq_s,
        "total_csv_physical_bytes": total_csv,
        "reduction_pct_iceberg_vs_parquet_zstd": reduction_pct(total_ice_basis, total_pq_z),
        "reduction_pct_iceberg_vs_csv": reduction_pct(total_ice_basis, total_csv),
        "reduction_pct_parquet_zstd_vs_snappy": reduction_pct(total_pq_z, total_pq_s),
    }


def render_markdown(report: dict) -> str:
    L = []
    w = L.append
    m = report["meta"]
    s = report["summary"]
    w("# Iceberg (ZSTD) vs Parquet (ZSTD) vs Parquet (Snappy) vs CSV (gzip) table-format storage -- comparison")
    w("")
    w(f"- **Generated:** {m['generated_utc']}  ·  dataset `{m['dataset']}`")
    w(f"- **Source measurement:** `{m['measurement_file']}` (generated {m['measurement_generated_utc']})")
    w("- Supports two related questions: how does storage taken by Iceberg compare to Parquet "
      "/ CSV, and -- since Parquet appears here in two codecs -- how much of any "
      "Iceberg-vs-Parquet difference is table format versus compression codec.")
    w("")
    w("## Per-archetype storage")
    w("")
    w("| Archetype | Iceberg table | Iceberg physical (raw) | Iceberg physical (excl. orphans, est.) | "
      "Parquet (ZSTD) table | Parquet (ZSTD) physical | Parquet (Snappy) table | Parquet (Snappy) physical | "
      "CSV table | CSV physical |")
    w("|---|---|--:|--:|---|--:|---|--:|---|--:|")
    for e in report["entries"]:
        ice_excl = human(e["iceberg_physical_excl_orphans_bytes"]) if e.get("iceberg_physical_excl_orphans_bytes") is not None else "n/a"
        w(f"| {e['archetype']} | `{e['iceberg_table']}` | {human(e['iceberg_physical_bytes'])} | {ice_excl} | "
          f"`{e['parquet_zstd_table']}` | {human(e['parquet_zstd_physical_bytes'])} | "
          f"`{e['parquet_snappy_table']}` | {human(e['parquet_snappy_physical_bytes'])} | "
          f"`{e['csv_table']}` | {human(e['csv_physical_bytes'])} |")
    w("")
    w("## Per-archetype ratios")
    w("")
    w("| Archetype | Reduction% Ice vs Pq(ZSTD) | Reduction% Ice vs CSV | Reduction% Pq(ZSTD) vs Pq(Snappy) | "
      "Bytes/row Ice | Bytes/row Pq(ZSTD) | Bytes/row Pq(Snappy) | Bytes/row CSV |")
    w("|---|--:|--:|--:|--:|--:|--:|--:|")
    for e in report["entries"]:
        def pct(v):
            return f"{v:+.1f}%" if v is not None else "n/a"
        def bpr(v):
            return human(v) if v else "n/a"
        w(f"| {e['archetype']} | {pct(e['reduction_pct_iceberg_vs_parquet_zstd'])} | "
          f"{pct(e['reduction_pct_iceberg_vs_csv'])} | {pct(e['reduction_pct_parquet_zstd_vs_snappy'])} | "
          f"{bpr(e['bytes_per_row_iceberg'])} | {bpr(e['bytes_per_row_parquet_zstd'])} | "
          f"{bpr(e['bytes_per_row_parquet_snappy'])} | {bpr(e['bytes_per_row_csv'])} |")
    w("")
    w("**Reduction%** = `(other - subject) / other * 100`: positive means the first-named "
      "side is smaller than the second (a real reduction); negative means it is LARGER (an "
      "increase, not a reduction).")
    w("")
    w("**\"Excl. orphans, est.\"** = live-snapshot data bytes (from `\"<table>$files\"`, i.e. "
      "exactly what the current Iceberg snapshot references) plus the table's current physical "
      "metadata bytes -- an estimate of what remains after `system.remove_orphan_files`, used as "
      "the Iceberg basis for every ratio above when available (falls back to raw physical bytes "
      "for tables Athena cannot read, e.g. format-v3 VARIANT tables, where this estimate cannot "
      "be computed).")
    w("")
    for e in report["entries"]:
        if e.get("note"):
            w(f"- **`{e['iceberg_table']}`:** {e['note']}")
    w("")
    w("## Summary")
    w("")
    w(f"- Archetypes compared: **{s['archetype_count']}**")
    w(f"- Total Iceberg physical, raw: **{human(s['total_iceberg_physical_bytes'])}**")
    w(f"- Total Iceberg physical, basis used for ratios (excl. orphans where measurable): "
      f"**{human(s['total_iceberg_basis_bytes'])}**"
      + (f"  (estimated orphaned bytes pending cleanup: {human(s['total_estimated_orphan_bytes'])})"
         if s.get("total_estimated_orphan_bytes") else ""))
    w(f"- Total Parquet (ZSTD) physical: **{human(s['total_parquet_zstd_physical_bytes'])}**")
    w(f"- Total Parquet (Snappy) physical: **{human(s['total_parquet_snappy_physical_bytes'])}**")
    w(f"- Total CSV (gzip) physical: **{human(s['total_csv_physical_bytes'])}**")
    w("")
    w(f"- **Overall reduction, Iceberg vs Parquet (ZSTD): {s['reduction_pct_iceberg_vs_parquet_zstd']:+.1f}%** "
      f"-- both sides use the same codec, so this isolates table format")
    w(f"- **Overall reduction, Iceberg vs CSV (gzip): {s['reduction_pct_iceberg_vs_csv']:+.1f}%**")
    w(f"- **Overall reduction, Parquet (ZSTD) vs Parquet (Snappy): {s['reduction_pct_parquet_zstd_vs_snappy']:+.1f}%** "
      f"-- both sides use the same format, so this isolates compression codec")
    w("")
    w("Read the per-archetype tables above before quoting an overall percentage alone: the "
      "Inference Fact row's Iceberg side is VARIANT-typed (format-v3) while its Parquet/CSV "
      "siblings flatten the same columns to STRING (neither format has a VARIANT type) -- not "
      "a same-type comparison for that one row -- and every CSV/Parquet column is "
      "string-encoded end to end for the CSV leg (see mimic_iv_ddl_csv_comparison.sql design "
      "decision 1), so a per-column byte comparison across formats would conflate the format "
      "difference with the encoding difference. Only the table-level totals are directly "
      "comparable.")
    w("")
    w("## Reproduce")
    w("")
    w("```")
    w(f"python measure_parquet_comparison.py --dataset {m['dataset']}")
    w(f"python compare_parquet_iceberg.py --dataset {m['dataset']}")
    w("```")
    return "\n".join(L) + "\n"


def main() -> int:
    _make_console_utf8_safe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _cfg.add_dataset_arg(p)
    p.add_argument("--measurement-file", default=None,
                   help="path to measure_parquet_comparison.py's JSON output "
                        "(default: docs/parquet_comparison/<demo|full>/latest.json)")
    p.add_argument("--out-dir", default=None,
                   help="output dir (default: docs/parquet_comparison/<demo|full> for the --dataset)")
    args = p.parse_args()

    profile = _cfg.dataset_profile(args.dataset)
    if args.out_dir is None:
        args.out_dir = str(REPO_ROOT / "docs" / "parquet_comparison" / profile.mode)
    measurement_file = args.measurement_file or str(Path(args.out_dir) / "latest.json")

    mp = Path(measurement_file)
    if not mp.is_file():
        print(f"ERROR: measurement file not found: {mp}\n"
              f"Run measure_parquet_comparison.py --dataset {args.dataset} first.", file=sys.stderr)
        return 1
    measurement = json.loads(mp.read_text(encoding="utf-8"))

    entries = [interpret_archetype(a) for a in measurement["archetypes"]]
    summary = build_summary(entries)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_name = f"parquet_comparison_{ts}.json"
    report = {
        "meta": {
            "script_version": SCRIPT_VERSION,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "dataset": args.dataset,
            "measurement_file": str(mp),
            "measurement_generated_utc": measurement["meta"]["generated_utc"],
            "json_filename": json_name,
        },
        "entries": entries,
        "summary": summary,
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / json_name).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out_dir / "latest_comparison.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md = render_markdown(report)
    (out_dir / f"parquet_comparison_{ts}.md").write_text(md, encoding="utf-8")
    (out_dir / "latest_comparison.md").write_text(md, encoding="utf-8")

    print(md)
    print(f"written: {out_dir / 'latest_comparison.md'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
