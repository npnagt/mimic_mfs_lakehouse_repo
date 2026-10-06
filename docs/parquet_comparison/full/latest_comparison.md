# Iceberg (ZSTD) vs Parquet (ZSTD) vs Parquet (Snappy) vs CSV (gzip) table-format storage -- comparison

- **Generated:** 2026-10-01T01:04:14.804724+00:00  ·  dataset `fulldataset`
- **Source measurement:** `docs/parquet_comparison/full/latest.json` (generated 2026-10-01T01:04:14.657235+00:00)
- Supports two related questions: how does storage taken by Iceberg compare to Parquet / CSV, and -- since Parquet appears here in two codecs -- how much of any Iceberg-vs-Parquet difference is table format versus compression codec.

## Per-archetype storage

| Archetype | Iceberg table | Iceberg physical (raw) | Iceberg physical (excl. orphans, est.) | Parquet (ZSTD) table | Parquet (ZSTD) physical | Parquet (Snappy) table | Parquet (Snappy) physical | CSV table | CSV physical |
|---|---|--:|--:|---|--:|---|--:|---|--:|
| Source Data Fact | `fact_admission` | 52.00 MiB | 25.65 MiB | `fact_admission_parquet` | 25.60 MiB | `fact_admission_parquet_snappy` | 32.30 MiB | `fact_admission_csv` | 32.04 MiB |
| Computed Structured Fact | `obt_admission_features` | 37.58 MiB | 37.58 MiB | `obt_admission_features_parquet` | 23.94 MiB | `obt_admission_features_parquet_snappy` | 30.58 MiB | `obt_admission_features_csv` | 27.31 MiB |
| Unstructured Data Fact | `fact_discharge_note` | 2.06 GiB | 1.03 GiB | `fact_discharge_note_parquet` | 1.03 GiB | `fact_discharge_note_parquet_snappy` | 1.85 GiB | `fact_discharge_note_csv` | 1.20 GiB |
| Unstructured Data Fact | `fact_radiology_note` | 649.46 MiB | 325.73 MiB | `fact_radiology_note_parquet` | 322.85 MiB | `fact_radiology_note_parquet_snappy` | 604.54 MiB | `fact_radiology_note_csv` | 376.80 MiB |
| Unstructured Features Fact | `fact_discharge_note_nlp` | 4.61 MiB | 4.61 MiB | `fact_discharge_note_nlp_parquet` | 1.18 MiB | `fact_discharge_note_nlp_parquet_snappy` | 1.94 MiB | `fact_discharge_note_nlp_csv` | 1.23 MiB |
| Unstructured Features Fact | `fact_radiology_note_nlp` | 15.31 MiB | 15.31 MiB | `fact_radiology_note_nlp_parquet` | 4.46 MiB | `fact_radiology_note_nlp_parquet_snappy` | 6.77 MiB | `fact_radiology_note_nlp_csv` | 3.42 MiB |
| Inference Fact | `fact_clinician_note_nlp_v` | 4.93 MiB | n/a | `fact_clinician_note_nlp_v_parquet` | 4.56 MiB | `fact_clinician_note_nlp_v_parquet_snappy` | 8.01 MiB | `fact_clinician_note_nlp_v_csv` | 4.79 MiB |

## Per-archetype ratios

| Archetype | Reduction% Ice vs Pq(ZSTD) | Reduction% Ice vs CSV | Reduction% Pq(ZSTD) vs Pq(Snappy) | Bytes/row Ice | Bytes/row Pq(ZSTD) | Bytes/row Pq(Snappy) | Bytes/row CSV |
|---|--:|--:|--:|--:|--:|--:|--:|
| Source Data Fact | -0.2% | +20.0% | +20.7% | 49 B | 49 B | 62 B | 61 B |
| Computed Structured Fact | -57.0% | -37.6% | +21.7% | 72 B | 45 B | 58 B | 52 B |
| Unstructured Data Fact | +0.4% | +14.2% | +44.4% | 3.24 KiB | 3.26 KiB | 5.86 KiB | 3.78 KiB |
| Unstructured Data Fact | -0.9% | +13.6% | +46.6% | 1.08 KiB | 1.07 KiB | 2.00 KiB | 1.25 KiB |
| Unstructured Features Fact | -291.3% | -273.4% | +39.3% | 483 B | 123 B | 203 B | 129 B |
| Unstructured Features Fact | -243.2% | -347.2% | +34.1% | 5.78 KiB | 1.68 KiB | 2.56 KiB | 1.29 KiB |
| Inference Fact | -8.2% | -3.0% | +43.1% | 497 B | 460 B | 808 B | 483 B |

**Reduction%** = `(other - subject) / other * 100`: positive means the first-named side is smaller than the second (a real reduction); negative means it is LARGER (an increase, not a reduction).

**"Excl. orphans, est."** = live-snapshot data bytes (from `"<table>$files"`, i.e. exactly what the current Iceberg snapshot references) plus the table's current physical metadata bytes -- an estimate of what remains after `system.remove_orphan_files`, used as the Iceberg basis for every ratio above when available (falls back to raw physical bytes for tables Athena cannot read, e.g. format-v3 VARIANT tables, where this estimate cannot be computed).

- **`fact_clinician_note_nlp_v`:** Combines fact_discharge_note_nlp + fact_radiology_note_nlp via a full outer join on hadm_id. Iceberg side is VARIANT-typed (format-v3); Parquet/CSV sides are the same columns flattened to STRING (neither format has a VARIANT type) -- not a same-type comparison for this row.

## Summary

- Archetypes compared: **7**
- Total Iceberg physical, raw: **2.80 GiB**
- Total Iceberg physical, basis used for ratios (excl. orphans where measurable): **1.43 GiB**  (estimated orphaned bytes pending cleanup: 1.37 GiB)
- Total Parquet (ZSTD) physical: **1.40 GiB**
- Total Parquet (Snappy) physical: **2.52 GiB**
- Total CSV (gzip) physical: **1.63 GiB**

- **Overall reduction, Iceberg vs Parquet (ZSTD): -1.9%** -- both sides use the same codec, so this isolates table format
- **Overall reduction, Iceberg vs CSV (gzip): +12.3%**
- **Overall reduction, Parquet (ZSTD) vs Parquet (Snappy): +44.3%** -- both sides use the same format, so this isolates compression codec

Read the per-archetype tables above before quoting an overall percentage alone: the Inference Fact row's Iceberg side is VARIANT-typed (format-v3) while its Parquet/CSV siblings flatten the same columns to STRING (neither format has a VARIANT type) -- not a same-type comparison for that one row -- and every CSV/Parquet column is string-encoded end to end for the CSV leg (see mimic_iv_ddl_csv_comparison.sql design decision 1), so a per-column byte comparison across formats would conflate the format difference with the encoding difference. Only the table-level totals are directly comparable.

## Reproduce

```
python measure_parquet_comparison.py --dataset fulldataset
python compare_parquet_iceberg.py --dataset fulldataset
```
