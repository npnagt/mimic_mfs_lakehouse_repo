# Iceberg (ZSTD) vs Parquet (ZSTD) vs Parquet (Snappy) vs CSV (gzip) table-format storage -- raw measurement

- **Generated:** 2026-10-01T01:04:14.657235+00:00  ·  region `us-east-2`  ·  dataset `fulldataset`
- Raw per-table facts only -- see the companion compare_parquet_iceberg.py report for ratios and findings.

| Archetype | Iceberg table | Iceberg physical | Iceberg live | Iceberg rows | Parquet (ZSTD) table | Parquet (ZSTD) physical | Parquet (ZSTD) rows | Parquet (Snappy) table | Parquet (Snappy) physical | Parquet (Snappy) rows | CSV (gzip) table | CSV (gzip) physical | CSV (gzip) rows | Note |
|---|---|--:|--:|--:|---|--:|--:|---|--:|--:|---|--:|--:|---|
| Source Data Fact | `fact_admission` | 52.00 MiB | 25.58 MiB | 546,029 | `fact_admission_parquet` | 25.60 MiB | 546,029 | `fact_admission_parquet_snappy` | 32.30 MiB | 546,029 | `fact_admission_csv` | 32.04 MiB | 546,029 |  |
| Computed Structured Fact | `obt_admission_features` | 37.58 MiB | 37.01 MiB | 546,031 | `obt_admission_features_parquet` | 23.94 MiB | 546,031 | `obt_admission_features_parquet_snappy` | 30.58 MiB | 546,031 | `obt_admission_features_csv` | 27.31 MiB | 546,031 |  |
| Unstructured Data Fact | `fact_discharge_note` | 2.06 GiB | 1.03 GiB | 331,793 | `fact_discharge_note_parquet` | 1.03 GiB | 331,793 | `fact_discharge_note_parquet_snappy` | 1.85 GiB | 331,793 | `fact_discharge_note_csv` | 1.20 GiB | 331,793 |  |
| Unstructured Data Fact | `fact_radiology_note` | 649.46 MiB | 325.68 MiB | 309,670 | `fact_radiology_note_parquet` | 322.85 MiB | 309,670 | `fact_radiology_note_parquet_snappy` | 604.54 MiB | 309,670 | `fact_radiology_note_csv` | 376.80 MiB | 309,670 |  |
| Unstructured Features Fact | `fact_discharge_note_nlp` | 4.61 MiB | 1.59 MiB | 10,000 | `fact_discharge_note_nlp_parquet` | 1.18 MiB | 10,000 | `fact_discharge_note_nlp_parquet_snappy` | 1.94 MiB | 10,000 | `fact_discharge_note_nlp_csv` | 1.23 MiB | 10,000 |  |
| Unstructured Features Fact | `fact_radiology_note_nlp` | 15.31 MiB | 6.18 MiB | 2,711 | `fact_radiology_note_nlp_parquet` | 4.46 MiB | 2,711 | `fact_radiology_note_nlp_parquet_snappy` | 6.77 MiB | 2,711 | `fact_radiology_note_nlp_csv` | 3.42 MiB | 2,711 |  |
| Inference Fact | `fact_clinician_note_nlp_v` | 4.93 MiB | n/a | n/a | `fact_clinician_note_nlp_v_parquet` | 4.56 MiB | 10,388 | `fact_clinician_note_nlp_v_parquet_snappy` | 8.01 MiB | 10,388 | `fact_clinician_note_nlp_v_csv` | 4.79 MiB | 10,388 | Combines fact_discharge_note_nlp + fact_radiology_note_nlp via a full outer join on hadm_id. Iceberg side is VARIANT-typed (format-v3); Parquet/CSV sides are the same columns flattened to STRING (neither format has a VARIANT type) -- not a same-type comparison for this row. |

## Reproduce

```
python measure_parquet_comparison.py --dataset fulldataset --region us-east-2
```

_Machine-readable companion: `parquet_comparison_measurement_20261001T010414Z.json`. Next: `python compare_parquet_iceberg.py --dataset fulldataset` to render the interpreted comparison._
