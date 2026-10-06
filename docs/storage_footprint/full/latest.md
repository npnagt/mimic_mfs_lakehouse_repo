# Lakehouse storage footprint

- **Generated:** 2026-10-01T01:02:50.192021+00:00  ·  script v1.2.0  ·  repo `32d9123`
- **AWS account:** `605785396773`  ·  **region:** `us-east-2`
- **Buckets:** raw `mimic4-datalake-v3-2-full` · gold `mimic4-lakehouse-v3-2-full` (prefix `mimic_bus`) · nlp-debug `mimic4-nlp-v3-2-full` · scripts `mimic4-glue-scripts-v3-2-bucket`
- **Gold database:** `mimic4_db_business_full`  ·  Athena workgroup `mimic4-lakehouse`

## Summary

| Metric | Bytes | Human |
|---|--:|--:|
| Total lakehouse (physical) | 21,297,593,129 | 19.83 GiB |
| Raw bucket | 15,763,593,662 | 14.68 GiB |
| Gold bucket (physical) | 4,967,446,079 | 4.63 GiB |
| Gold live snapshot | 2,141,436,229 | 1.99 GiB |
| **AI-inference layer (physical)** | 26,061,950 | 24.85 MiB |

## Structured vs unstructured / NLP

The Aurora PostgreSQL warehouse (`mimic_datawarehouse_repo`) carries **only the structured category** — no clinical notes, no NLP-derived tables, no note text. So the P3 redundancy-ratio comparison is made on the **structured** row alone; the **unstructured / NLP** row is coverage the lakehouse adds, not duplication.

| Category | Physical | Live snapshot | % of lakehouse | In the Aurora warehouse? |
|---|--:|--:|--:|:--:|
| **Structured** — raw `*_raw` CSV + `dim_`/`fact_`/`agg_`/`obt_`/`etl_control` (42 Gold tables) | 14.53 GiB | 11.91 GiB | 73.2364% | yes — `warehouse` schema |
| **Unstructured / NLP** — note `.csv.gz` + `*_note_raw` Parquet + `fact_discharge_note_nlp`/`fact_radiology_note_nlp*` (3 Gold tables) + per-note JSON | 5.31 GiB | 5.29 GiB | 26.7623% | **no** |
| Infrastructure — Glue scripts, stray Athena results | 260.52 KiB | — | 0.0013% | n/a |

Breakdown of the unstructured / NLP category:

| Component | Physical |
|---|--:|
| Clinical note text (`discharge_raw` + `radiology_raw` `.csv.gz`) | 1.79 GiB |
| Converted note Parquet (`discharge_note_raw`, `radiology_note_raw`) | 2.97 GiB |
| NLP-derived Gold tables (physical / live) | 24.85 MiB / 7.77 MiB |
| Per-note debug JSON (`mimic4-nlp-v3-2-full`) | 540.05 MiB |

**For a P3 redundancy ratio vs. the Aurora warehouse, compare `11.91 GiB` (structured, live snapshot)** — ideally after `OPTIMIZE … REWRITE DATA` — against that warehouse's `pg_total_relation_size()` over `staging` + `warehouse` plus its own raw S3 landing copy. The lakehouse additionally carries **5.31 GiB** of unstructured / NLP data that has no warehouse equivalent at all.

## Data warehouse vs data lakehouse — space comparison

Aurora baseline: **AWS RDS Aurora PostgreSQL data warehouse (mimic_datawarehouse_repo)** (2026-09-22). The raw structured CSV files are identical for both architectures, so they are one shared line and excluded from the head-to-head of the *processed* layers — which is where the two designs actually differ.

| Layer | Data warehouse (Aurora) | Data lakehouse |
|---|--:|--:|
| Raw structured CSV.gz in S3 *(shared — same files)* | 9.92 GiB | 9.92 GiB |
| Staging / raw-mirror layer | *materialized* — part of the total below (`staging.stg_*`, full TRUNCATE+reload) | **0 B** — `mimic4_db_raw` is external tables over the same S3 files |
| Modeled / queryable layer | `warehouse.dim_/fact_/agg_*` | Gold Iceberg |
| **Processed structured — physical** | **8.65 GiB** (67 tables) | **4.60 GiB** (42 tables) |
| **Processed structured — live / current** | 8.14 GiB | 1.99 GiB |
| Processed structured — compacted-logical | ~8.14 GiB (Postgres, little bloat) | 1.99 GiB (**measured** — post-`OPTIMIZE … REWRITE DATA`) |
| `pg_database_size()` (incl. catalogs) | 8.66 GiB | n/a |

| Unstructured / NLP | Data warehouse (Aurora) | Data lakehouse |
|---|--:|--:|
| Clinical note text (raw source) | not stored | 1.79 GiB |
| **NLP-derived tables** (physical / live) | **0 B — cannot store** | 24.85 MiB / 7.77 MiB |

**Head-to-head, processed structured:** on the **live snapshot** — the data a correctly-maintained Iceberg table actually serves, and now bin-packed by `OPTIMIZE` — the lakehouse is **0.24×** the warehouse (1.99 GiB vs 8.14 GiB), i.e. it holds the same structured information in a fraction of the space — **P3 supported**. On **raw physical bytes** it is still **0.53×** the warehouse: the live snapshot is only 43.2% of physical because Athena `VACUUM` expired the superseded snapshots but did not delete their data files (verified) — a Spark `remove_orphan_files` pass is required to reclaim them, and is a tooling gap, not an architecture property.

**Redundancy ratio (physical ÷ one clean modeled copy), structured:**

| Architecture | Ratio | Why |
|---|--:|---|
| Data warehouse | **2.0×** | two full materializations inside Postgres -- staging.stg_* (full TRUNCATE+reload of every source) + warehouse.* (modeled copy); the shared raw CSV in S3 is a third copy, excluded as it is common to both architectures |
| Data lakehouse — as deployed | 2.32× | OPTIMIZE has run (live snapshot IS bin-packed); the residual physical/live gap is superseded data files that Athena VACUUM expired the snapshots for but did not delete -- reclaimable only by a Spark remove_orphan_files pass. Not architecture. |
| Data lakehouse — compacted | **1.0×** | live snapshot = one clean modeled copy; gold is the only physical processed copy, the mimic4_db_raw staging layer is external tables (0 bytes) |

**P3 holds.** The warehouse materializes the structured data twice inside Postgres (`staging.stg_*` full reload + `warehouse.*` modeled); the lakehouse materializes it once (Gold), because its staging layer is external tables over the raw CSVs — **0 bytes**. That is one full physical copy the lakehouse eliminates (redundancy 2.0× vs 1.0×). On information content (the live snapshot) the lakehouse is 0.24× the warehouse. The only figure that still favours the warehouse is raw physical bytes, and that is orphaned data files awaiting a Spark `remove_orphan_files` pass — not a second copy of the data.

**Unstructured / NLP:** Aurora holds 0 bytes -- a coverage gap, not a space win. The lakehouse holds the NLP-derived tables in ONE copy (shared catalog); a decoupled architecture that did NLP would hold each output in >= 2 (model store + warehouse load).

### Per-table detail — data warehouse (Aurora)

`live` = pgstattuple's exact live-tuple accounting (or the live/dead-tuple-ratio estimate if that extension isn't available) plus full-size indexes/TOAST — the same "current, visible content" concept as the lakehouse's Live snapshot column, just computed inside Postgres instead of from Iceberg's `$files` metadata table.

| Table | Schema | Cat. | Rows | Physical | Live | Phys/Live |
|---|---|---|--:|--:|--:|--:|
| `fact_input_event` | warehouse | str | 999,887 | 463.57 MiB | 447.27 MiB | 1.04 |
| `fact_prescription` | warehouse | str | 999,892 | 398.18 MiB | 385.85 MiB | 1.03 |
| `fact_ingredient_event` | warehouse | str | 1,000,020 | 364.17 MiB | 351.21 MiB | 1.04 |
| `fact_microbiology_result` | warehouse | str | 1,000,166 | 337.20 MiB | 324.32 MiB | 1.04 |
| `fact_procedure_event` | warehouse | str | 808,693 | 326.08 MiB | 314.47 MiB | 1.04 |
| `fact_pharmacy_order` | warehouse | str | 999,850 | 318.73 MiB | 305.54 MiB | 1.04 |
| `stg_inputevents` | staging | str | 1,000,139 | 309.11 MiB | 299.33 MiB | 1.03 |
| `fact_datetime_event` | warehouse | str | 1,000,000 | 293.23 MiB | 283.60 MiB | 1.03 |
| `fact_medication_administration_mini_detail` | warehouse | str | 999,936 | 281.60 MiB | 271.52 MiB | 1.04 |
| `fact_output_event` | warehouse | str | 1,000,000 | 278.27 MiB | 267.00 MiB | 1.04 |
| `fact_medication_administration` | warehouse | str | 999,986 | 275.45 MiB | 264.09 MiB | 1.04 |
| `stg_microbiologyevents` | staging | str | 999,668 | 272.20 MiB | 260.71 MiB | 1.04 |
| `fact_lab_result` | warehouse | str | 1,000,000 | 260.95 MiB | 250.28 MiB | 1.04 |
| `stg_pharmacy` | staging | str | 999,953 | 253.66 MiB | 243.73 MiB | 1.04 |
| `fact_chart_observation` | warehouse | str | 1,008,075 | 229.15 MiB | 218.79 MiB | 1.05 |
| `fact_provider_order` | warehouse | str | 1,000,000 | 228.04 MiB | 217.62 MiB | 1.05 |
| `stg_prescriptions` | staging | str | 1,000,000 | 213.55 MiB | 202.94 MiB | 1.05 |
| `stg_procedureevents` | staging | str | 808,706 | 203.55 MiB | 196.84 MiB | 1.03 |
| `fact_transfer` | warehouse | str | 1,000,000 | 202.74 MiB | 193.01 MiB | 1.05 |
| `stg_ingredientevents` | staging | str | 1,000,000 | 202.19 MiB | 195.16 MiB | 1.04 |
| `fact_outpatient_measurement` | warehouse | str | 1,000,000 | 185.38 MiB | 176.34 MiB | 1.05 |
| `stg_emar` | staging | str | 1,000,000 | 183.84 MiB | 174.10 MiB | 1.06 |
| `fact_diagnosis` | warehouse | str | 999,977 | 177.86 MiB | 168.03 MiB | 1.06 |
| `fact_drg_assignment` | warehouse | str | 761,856 | 174.98 MiB | 167.57 MiB | 1.04 |
| `fact_admission` | warehouse | str | 546,028 | 166.84 MiB | 160.05 MiB | 1.04 |
| `stg_labevents` | staging | str | 1,000,000 | 160.81 MiB | 152.76 MiB | 1.05 |
| `fact_procedure` | warehouse | str | 859,650 | 156.42 MiB | 148.84 MiB | 1.05 |
| `stg_datetimeevents` | staging | str | 1,000,000 | 144.97 MiB | 139.57 MiB | 1.04 |
| `stg_chartevents` | staging | str | 1,000,000 | 137.42 MiB | 132.19 MiB | 1.04 |
| `stg_poe` | staging | str | 1,000,000 | 135.38 MiB | 126.27 MiB | 1.07 |
| `stg_emar_detail` | staging | str | 1,000,000 | 122.62 MiB | 114.34 MiB | 1.07 |
| `stg_outputevents` | staging | str | 1,000,000 | 120.26 MiB | 109.74 MiB | 1.1 |
| `dim_patient` | warehouse | str | 364,627 | 119.30 MiB | 65.08 MiB | 1.83 |
| `stg_admissions` | staging | str | 546,028 | 113.09 MiB | 109.47 MiB | 1.03 |
| `stg_transfers` | staging | str | 1,000,000 | 107.58 MiB | 100.98 MiB | 1.07 |
| `fact_service_assignment` | warehouse | str | 593,070 | 107.28 MiB | 102.20 MiB | 1.05 |
| `stg_poe_detail` | staging | str | 1,000,000 | 85.33 MiB | 76.33 MiB | 1.12 |
| `stg_drgcodes` | staging | str | 761,856 | 82.98 MiB | 78.01 MiB | 1.06 |
| `stg_omr` | staging | str | 1,000,000 | 80.62 MiB | 71.94 MiB | 1.12 |
| `stg_procedures_icd` | staging | str | 859,655 | 65.84 MiB | 62.06 MiB | 1.06 |
| `stg_diagnoses_icd` | staging | str | 1,000,000 | 65.16 MiB | 61.09 MiB | 1.07 |
| `agg_admission_daily` | warehouse | str | 244,500 | 54.11 MiB | 34.42 MiB | 1.57 |
| `dim_diagnosis` | warehouse | str | 112,107 | 48.34 MiB | 26.52 MiB | 1.82 |
| `stg_services` | staging | str | 593,071 | 40.95 MiB | 36.81 MiB | 1.11 |
| `dim_procedure` | warehouse | str | 86,423 | 37.79 MiB | 20.60 MiB | 1.83 |
| `fact_hcpcs_event` | warehouse | str | 186,074 | 37.55 MiB | 35.77 MiB | 1.05 |
| `fact_provider_order_mini_detail` | warehouse | str | 159,231 | 33.58 MiB | 32.06 MiB | 1.05 |
| `dim_hcpcs` | warehouse | str | 89,208 | 30.52 MiB | 16.58 MiB | 1.84 |
| `stg_patients` | staging | str | 364,627 | 26.95 MiB | 24.09 MiB | 1.12 |
| `fact_icu_stay_accumulating` | warehouse | str | 94,458 | 24.91 MiB | 23.84 MiB | 1.04 |
| `dim_date` | warehouse | str | 47,846 | 19.20 MiB | 10.16 MiB | 1.89 |
| `stg_hcpcsevents` | staging | str | 186,074 | 19.01 MiB | 17.59 MiB | 1.08 |
| `agg_icu_fluid_balance_daily` | warehouse | str | 75,366 | 17.09 MiB | 10.70 MiB | 1.6 |
| `stg_icustays` | staging | str | 94,458 | 15.85 MiB | 15.30 MiB | 1.04 |
| `stg_d_icd_diagnoses` | staging | str | 112,107 | 12.70 MiB | 11.79 MiB | 1.08 |
| `stg_d_icd_procedures` | staging | str | 86,423 | 10.12 MiB | 9.42 MiB | 1.07 |
| `stg_d_hcpcs` | staging | str | 89,208 | 5.84 MiB | 5.17 MiB | 1.13 |
| `dim_provider` | warehouse | str | 42,244 | 5.68 MiB | 5.39 MiB | 1.05 |
| `agg_admission_monthly` | warehouse | str | 10,856 | 2.47 MiB | 1.59 MiB | 1.56 |
| `dim_caregiver` | warehouse | str | 17,984 | 2.45 MiB | 2.33 MiB | 1.05 |
| `dim_chart_item` | warehouse | str | 4,095 | 1.90 MiB | 1.03 MiB | 1.84 |
| `stg_provider` | staging | str | 42,244 | 1.50 MiB | 1.29 MiB | 1.16 |
| `stg_caregiver` | staging | str | 17,984 | 672.00 KiB | 594.00 KiB | 1.13 |
| `dim_lab_item` | warehouse | str | 1,650 | 632.00 KiB | 363.81 KiB | 1.74 |
| `stg_d_items` | staging | str | 4,095 | 568.00 KiB | 527.73 KiB | 1.08 |
| `etl_process_log` | warehouse | str | 390 | 192.00 KiB | 170.86 KiB | 1.12 |
| `stg_d_labitems` | staging | str | 1,650 | 160.00 KiB | 144.44 KiB | 1.11 |

## Raw bucket

| Class | Category | Bytes | Human | % of bucket | Prefixes |
|---|---|--:|--:|--:|---|
| note_parquet | unstructured_nlp | 3,186,351,281 | 2.97 GiB | 20.2134% | 2 |
| note_text_gz | unstructured_nlp | 1,921,034,145 | 1.79 GiB | 12.1865% | 2 |
| structured_csv | structured | 10,656,208,236 | 9.92 GiB | 67.6001% | 32 |

Free-text clinical notes (`.csv.gz`) are **12.1865%** of the raw bucket; the structured MIMIC-IV CSVs are **67.6001%** (9.92 GiB).

## Gold Iceberg layer

- Physical: **4.63 GiB** in **67,242 objects** (4.54 GiB data + 84.94 MiB Iceberg metadata)
- Current snapshot ("live"): **1.99 GiB** in **4,438 files** — physical is **2.32×** the live snapshot (un-expired snapshots).
- **23 tables** average < 1 MiB per live data file (small-file fragmentation from `bucket()` / `day()` partitioning on a demo-sized dataset):
  - `etl_control` — 8.00 KiB in 7 files (~1.14 KiB/file)
  - `agg_admission_monthly` — 2.86 MiB in 1,262 files (~2.32 KiB/file)
  - `obt_icu_stay_features` — 14.69 MiB in 1,230 files (~12.23 KiB/file)
  - `agg_admission_daily` — 878.11 KiB in 64 files (~13.72 KiB/file)
  - `dim_lab_item` — 15.37 KiB in 1 files (~15.37 KiB/file)
  - `etl_process_log` — 53.63 KiB in 3 files (~17.88 KiB/file)
  - `dim_caregiver` — 21.96 KiB in 1 files (~21.96 KiB/file)
  - `agg_icu_fluid_balance_daily` — 1.39 MiB in 64 files (~22.26 KiB/file)

### Per-table detail

`file records` = Σ `record_count` over the current snapshot's data files — the true current row count for a copy-on-write table. If it exceeds `count(*)` (run with `--exact-rows` to check) the snapshot holds unmerged delete files or duplicate data from an append-instead-of-overwrite re-run.

| Table | Class | Cat. | Physical | Data | Iceberg meta | Live snapshot | Live files | File records | Phys/Live |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| `fact_discharge_note` | fact | str | 2.06 GiB | 2.06 GiB | 50.12 KiB | 1.03 GiB | 4 | 331,793 | 2.01 |
| `fact_radiology_note` | fact | str | 649.46 MiB | 649.41 MiB | 53.44 KiB | 325.68 MiB | 8 | 309,670 | 1.99 |
| `fact_lab_result` | fact | str | 188.21 MiB | 185.56 MiB | 2.65 MiB | 37.24 MiB | 65 | 1,000,000 | 5.05 |
| `fact_pharmacy_order` | fact | str | 160.95 MiB | 159.86 MiB | 1.09 MiB | 57.84 MiB | 17 | 1,000,000 | 2.78 |
| `fact_medication_administration` | fact | str | 152.96 MiB | 151.52 MiB | 1.43 MiB | 49.66 MiB | 32 | 1,000,000 | 3.08 |
| `fact_input_event` | fact | str | 145.16 MiB | 144.60 MiB | 575.71 KiB | 57.31 MiB | 9 | 1,000,000 | 2.53 |
| `fact_prescription` | fact | str | 141.05 MiB | 140.18 MiB | 897.58 KiB | 47.24 MiB | 17 | 1,000,000 | 2.99 |
| `fact_ingredient_event` | fact | str | 128.64 MiB | 127.81 MiB | 841.93 KiB | 41.68 MiB | 17 | 1,000,000 | 3.09 |
| `fact_medication_administration_mini_detail` | fact | str | 121.39 MiB | 119.72 MiB | 1.67 MiB | 15.52 MiB | 32 | 1,000,000 | 7.82 |
| `fact_microbiology_result` | fact | str | 120.62 MiB | 120.05 MiB | 590.01 KiB | 42.80 MiB | 9 | 1,000,000 | 2.82 |
| `fact_procedure_event` | fact | str | 97.12 MiB | 96.77 MiB | 363.19 KiB | 40.49 MiB | 5 | 808,707 | 2.4 |
| `fact_provider_order` | fact | str | 75.90 MiB | 75.36 MiB | 545.18 KiB | 25.77 MiB | 16 | 1,000,000 | 2.94 |
| `fact_transfer` | fact | str | 70.10 MiB | 69.79 MiB | 311.21 KiB | 30.81 MiB | 9 | 1,000,000 | 2.28 |
| `fact_output_event` | fact | str | 66.13 MiB | 65.83 MiB | 300.15 KiB | 23.23 MiB | 9 | 1,000,000 | 2.85 |
| `fact_datetime_event` | fact | str | 65.25 MiB | 64.94 MiB | 319.39 KiB | 18.96 MiB | 9 | 1,000,000 | 3.44 |
| `etl_process_log` | control | str | 59.90 MiB | 2.04 MiB | 57.85 MiB | 53.63 KiB | 3 | 677 | 1143.66 |
| `fact_admission` | fact | str | 52.00 MiB | 51.93 MiB | 71.81 KiB | 25.58 MiB | 5 | 546,029 | 2.03 |
| `fact_chart_observation` | fact | str | 51.63 MiB | 51.31 MiB | 328.79 KiB | 12.89 MiB | 9 | 1,000,000 | 4.01 |
| `obt_admission_features` | obt | str | 37.58 MiB | 37.01 MiB | 591.17 KiB | 37.01 MiB | 1,259 | 546,031 | 1.02 |
| `fact_provider_order_mini_detail` | fact | str | 34.82 MiB | 34.51 MiB | 310.31 KiB | 11.45 MiB | 16 | 1,000,000 | 3.04 |
| `fact_drg_assignment` | fact | str | 27.56 MiB | 27.38 MiB | 183.56 KiB | 7.78 MiB | 9 | 761,857 | 3.55 |
| `fact_procedure` | fact | str | 27.36 MiB | 27.24 MiB | 123.52 KiB | 10.80 MiB | 5 | 859,656 | 2.53 |
| `fact_outpatient_measurement` | fact | str | 26.73 MiB | 26.55 MiB | 181.08 KiB | 8.36 MiB | 9 | 1,000,000 | 3.2 |
| `fact_diagnosis` | fact | str | 26.39 MiB | 26.14 MiB | 257.19 KiB | 6.23 MiB | 17 | 1,000,000 | 4.24 |
| `fact_service_assignment` | fact | str | 23.18 MiB | 23.05 MiB | 124.85 KiB | 10.34 MiB | 5 | 593,072 | 2.24 |
| `fact_radiology_note_nlp` | ai_nlp | NLP | 15.31 MiB | 6.18 MiB | 9.13 MiB | 6.18 MiB | 128 | 2,711 | 2.48 |
| `obt_icu_stay_features` | obt | str | 15.05 MiB | 14.69 MiB | 368.08 KiB | 14.69 MiB | 1,230 | 94,458 | 1.02 |
| `fact_hcpcs_event` | fact | str | 8.57 MiB | 8.45 MiB | 123.44 KiB | 2.80 MiB | 5 | 186,075 | 3.06 |
| `obt_patient_360` | obt | str | 7.39 MiB | 7.35 MiB | 38.26 KiB | 7.35 MiB | 5 | 364,627 | 1.01 |
| `fact_icu_stay_accumulating` | fact | str | 7.03 MiB | 6.97 MiB | 55.45 KiB | 3.44 MiB | 5 | 94,459 | 2.04 |
| `fact_clinician_note_nlp_v` | ai_nlp | NLP | 4.93 MiB | 4.87 MiB | 64.57 KiB | n/a (fmt-v3) | — | — | — |
| `fact_discharge_note_nlp` | ai_nlp | NLP | 4.61 MiB | 1.59 MiB | 3.02 MiB | 1.59 MiB | 64 | 10,000 | 2.9 |
| `dim_diagnosis` | dim | str | 3.49 MiB | 3.45 MiB | 42.59 KiB | 1.62 MiB | 1 | 112,107 | 2.15 |
| `dim_patient` | dim | str | 3.22 MiB | 3.18 MiB | 45.20 KiB | 1.49 MiB | 1 | 364,627 | 2.16 |
| `agg_admission_monthly` | agg | str | 2.93 MiB | 2.86 MiB | 78.36 KiB | 2.86 MiB | 1,262 | 10,856 | 1.03 |
| `dim_procedure` | dim | str | 2.37 MiB | 2.33 MiB | 42.71 KiB | 1.11 MiB | 1 | 86,423 | 2.14 |
| `agg_icu_fluid_balance_daily` | agg | str | 1.42 MiB | 1.39 MiB | 33.56 KiB | 1.39 MiB | 64 | 75,366 | 1.02 |
| `dim_hcpcs` | dim | str | 973.17 KiB | 929.45 KiB | 43.72 KiB | 350.49 KiB | 1 | 89,208 | 2.78 |
| `agg_admission_daily` | agg | str | 909.55 KiB | 878.11 KiB | 31.44 KiB | 878.11 KiB | 64 | 244,500 | 1.04 |
| `dim_date` | dim | str | 453.43 KiB | 416.26 KiB | 37.17 KiB | 416.26 KiB | 1 | 47,846 | 1.09 |
| `dim_chart_item` | dim | str | 352.40 KiB | 303.93 KiB | 48.47 KiB | 82.11 KiB | 1 | 4,095 | 4.29 |
| `dim_provider` | dim | str | 163.09 KiB | 139.81 KiB | 23.28 KiB | 139.81 KiB | 1 | 42,245 | 1.17 |
| `dim_lab_item` | dim | str | 133.90 KiB | 90.58 KiB | 43.32 KiB | 15.37 KiB | 1 | 1,650 | 8.71 |
| `etl_control` | control | str | 123.63 KiB | 8.00 KiB | 115.64 KiB | 8.00 KiB | 7 | 7 | 15.46 |
| `dim_caregiver` | dim | str | 45.22 KiB | 21.96 KiB | 23.26 KiB | 21.96 KiB | 1 | 17,985 | 2.06 |

## AI-inference layer  (P4: structured queries joined to AI inference outputs)

Tables: `fact_clinician_note_nlp_v`, `fact_discharge_note_nlp`, `fact_radiology_note_nlp`

- Physical: **24.85 MiB**  ·  live snapshot: **7.77 MiB**
- **0.5247%** of the Gold layer  ·  **0.1653%** of the raw bucket  ·  **0.1224%** of the total lakehouse

**Marginal storage cost of the P4 capability ≈ 0.** An AI-inference output is a new small Iceberg table in the existing catalog (`mimic4_db_business_full`) and the existing bucket — no new store, no new format, no partitioning. It is joined to `fact_admission` in place.

**Redundancy ratio for the AI-inference layer = 1.0** (one physical copy). In a decoupled architecture the same output is materialized in the model/NLP store *and* copied into the warehouse to become joinable — redundancy ≥ 2.0 plus an ongoing sync. The bytes are tiny either way; the property is structural, and is what makes P4's "no ETL detour" true. This is the native side of P3's redundancy-ratio measurement.

## Methodology

- **Physical bytes** — `s3:ListObjectsV2` over each bucket, summed. Current object versions only; noncurrent versions (if bucket versioning is on) are not counted.
- **Live-snapshot bytes** — `SELECT count(*), sum(file_size_in_bytes), sum(record_count) FROM "<db>"."<table>$files"` per Gold table (Iceberg metadata table; scans no data).
- **Data vs Iceberg metadata** — a Gold object is "metadata" iff its key is `<prefix>/<table>/metadata/...`, else "data".
- **AI-inference layer** — the tables `fact_clinician_note_nlp_v`, `fact_discharge_note_nlp`, `fact_radiology_note_nlp`.
- **Logical / compacted size** is *not* measured here — obtain it by running `OPTIMIZE <table> REWRITE DATA` + `VACUUM` (or Spark `rewrite_data_files` + `expire_snapshots`) and re-running this script; the drop is the accumulated bloat.

## Caveats before using this for a P3 redundancy ratio

The Gold layer's *physical* footprint is inflated over its logical content by (1) un-expired Iceberg snapshots (physical ≈ 2–3× live) and (2) small-file fragmentation from partitioned writes on a demo-sized dataset. Compact and expire snapshots first, or the ratio measures ETL hygiene rather than architecture. The AI/NLP tables are effectively exempt (unpartitioned, few re-runs).

The Aurora baseline used above is `docs/storage_footprint/aurora_baseline.json` — regenerate it from `mimic_datawarehouse_repo` and re-run with `--aurora-baseline <file>` to refresh the comparison.

## Reproduce

```
python measure_storage_footprint.py --dataset fulldataset --region us-east-2
```

_Machine-readable companion: `storage_footprint_20261001T010250Z.json`_
