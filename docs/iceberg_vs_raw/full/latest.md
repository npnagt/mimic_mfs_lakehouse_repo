# Gold Iceberg tables vs raw source CSV archives -- fulldataset

- **Generated:** 2026-10-01T08:51:41.495974+00:00 · `mimic4_db_business_full` vs `s3://mimic4-datalake-v3-2-full`
- **Storage measurement:** 2026-10-01T01:02:50.192021+00:00 (`docs\storage_footprint\full\latest.json`); raw row counts from `etl_process_log` (full_run_id 18)

## Summary

Per-row comparison (the fair one -- fact tables over 1M rows hold a 1M-row sample, the raw archive holds every row). *Full-row equivalent* = Iceberg bytes/row × raw rows.

| Group | Tables | Raw .csv.gz | Iceberg as loaded (live) | Iceberg, full-row equivalent | Iceberg / raw (full-row) | Change | Iceberg smaller per row |
|---|--:|--:|--:|--:|--:|--:|--:|
| Fact tables | 23 | 9.92 GiB | 588.22 MiB | 19.90 GiB | 2.006× | 100.6% | 1 of 23 |
| Dimensions | 8 | 4.74 MiB | 4.82 MiB | 4.82 MiB | 1.018× | 1.8% | 3 of 8 |
| **All source-loaded** | 31 | 9.92 GiB | 593.05 MiB | 19.91 GiB | 2.006× | 100.6% | 4 of 31 |
| Note text (bytes only) | 2 | 1.79 GiB | 1.34 GiB | -- | 0.752× (as loaded) | -- | -- |
| Derived (no raw source) | 12 | -- | 72.39 MiB | -- | -- | -- | -- |

Whole Gold layer: 45 tables, 1.99 GiB live (4.63 GiB physical incl. superseded/orphaned files) vs 9.92 GiB of raw structured .csv.gz (0.201× as loaded -- understated by the 1M-row cap).

## All Gold tables (45)

Every table in the Gold layer with its raw source, row counts and Iceberg size. *Parquet (ZSTD) / Parquet (Snappy) / CSV copy* are filled only for the representative table per archetype that has comparison copies (see the next section).

| # | Table | Group | Raw source | Raw .csv.gz | Raw rows | Gold rows | Iceberg live | Iceberg physical | Parquet (ZSTD) | Parquet (Snappy) | CSV copy |
|--:|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | `fact_admission` | fact | `admissions_raw` | 19.00 MiB | 546,028 | 546,029 | 25.58 MiB | 52.00 MiB | 25.60 MiB | 32.30 MiB | 32.04 MiB |
| 2 | `fact_chart_observation` | fact | `chartevents_raw` | 3.26 GiB | 432,997,491 | 1,000,000 | 12.89 MiB | 51.63 MiB | -- | -- | -- |
| 3 | `fact_datetime_event` | fact | `datetimeevents_raw` | 60.54 MiB | 9,979,761 | 1,000,000 | 18.96 MiB | 65.25 MiB | -- | -- | -- |
| 4 | `fact_diagnosis` | fact | `diagnoses_icd_raw` | 32.01 MiB | 6,364,488 | 1,000,000 | 6.23 MiB | 26.39 MiB | -- | -- | -- |
| 5 | `fact_drg_assignment` | fact | `drgcodes_raw` | 9.29 MiB | 761,856 | 761,857 | 7.78 MiB | 27.56 MiB | -- | -- | -- |
| 6 | `fact_hcpcs_event` | fact | `hcpcsevents_raw` | 2.06 MiB | 186,074 | 186,075 | 2.80 MiB | 8.57 MiB | -- | -- | -- |
| 7 | `fact_icu_stay_accumulating` | fact | `icustays_raw` | 3.19 MiB | 94,458 | 94,459 | 3.44 MiB | 7.03 MiB | -- | -- | -- |
| 8 | `fact_ingredient_event` | fact | `ingredientevents_raw` | 297.21 MiB | 14,253,480 | 1,000,000 | 41.68 MiB | 128.64 MiB | -- | -- | -- |
| 9 | `fact_input_event` | fact | `inputevents_raw` | 382.51 MiB | 10,953,713 | 1,000,000 | 57.31 MiB | 145.16 MiB | -- | -- | -- |
| 10 | `fact_lab_result` | fact | `labevents_raw` | 2.41 GiB | 158,374,764 | 1,000,000 | 37.24 MiB | 188.21 MiB | -- | -- | -- |
| 11 | `fact_medication_administration` | fact | `emar_raw` | 773.72 MiB | 42,808,593 | 1,000,000 | 49.66 MiB | 152.96 MiB | -- | -- | -- |
| 12 | `fact_medication_administration_mini_detail` | fact | `emar_detail_raw` | 713.50 MiB | 87,371,064 | 1,000,000 | 15.52 MiB | 121.39 MiB | -- | -- | -- |
| 13 | `fact_microbiology_result` | fact | `microbiologyevents_raw` | 112.19 MiB | 3,988,224 | 1,000,000 | 42.80 MiB | 120.62 MiB | -- | -- | -- |
| 14 | `fact_outpatient_measurement` | fact | `omr_raw` | 42.03 MiB | 7,753,027 | 1,000,000 | 8.36 MiB | 26.73 MiB | -- | -- | -- |
| 15 | `fact_output_event` | fact | `outputevents_raw` | 47.02 MiB | 5,359,395 | 1,000,000 | 23.23 MiB | 66.13 MiB | -- | -- | -- |
| 16 | `fact_pharmacy_order` | fact | `pharmacy_raw` | 501.35 MiB | 17,847,567 | 1,000,000 | 57.84 MiB | 160.95 MiB | -- | -- | -- |
| 17 | `fact_prescription` | fact | `prescriptions_raw` | 578.21 MiB | 20,292,611 | 1,000,000 | 47.24 MiB | 141.05 MiB | -- | -- | -- |
| 18 | `fact_procedure` | fact | `procedures_icd_raw` | 7.42 MiB | 859,655 | 859,656 | 10.80 MiB | 27.36 MiB | -- | -- | -- |
| 19 | `fact_procedure_event` | fact | `procedureevents_raw` | 22.98 MiB | 808,706 | 808,707 | 40.49 MiB | 97.12 MiB | -- | -- | -- |
| 20 | `fact_provider_order` | fact | `poe_raw` | 635.71 MiB | 52,212,109 | 1,000,000 | 25.77 MiB | 75.90 MiB | -- | -- | -- |
| 21 | `fact_provider_order_mini_detail` | fact | `poe_detail_raw` | 52.71 MiB | 8,504,982 | 1,000,000 | 11.45 MiB | 34.82 MiB | -- | -- | -- |
| 22 | `fact_service_assignment` | fact | `services_raw` | 8.17 MiB | 593,071 | 593,072 | 10.34 MiB | 23.18 MiB | -- | -- | -- |
| 23 | `fact_transfer` | fact | `transfers_raw` | 44.05 MiB | 2,413,581 | 1,000,000 | 30.81 MiB | 70.10 MiB | -- | -- | -- |
| 24 | `dim_caregiver` | dim | `caregiver_raw` | 40.59 KiB | 17,985 | 17,985 | 21.96 KiB | 45.22 KiB | -- | -- | -- |
| 25 | `dim_chart_item` | dim | `d_items_raw` | 57.36 KiB | 4,095 | 4,095 | 82.11 KiB | 352.40 KiB | -- | -- | -- |
| 26 | `dim_date` | dim | -- | -- | -- | 47,846 | 416.26 KiB | 453.43 KiB | -- | -- | -- |
| 27 | `dim_diagnosis` | dim | `d_icd_diagnoses_raw` | 855.82 KiB | 112,107 | 112,107 | 1.62 MiB | 3.49 MiB | -- | -- | -- |
| 28 | `dim_hcpcs` | dim | `d_hcpcs_raw` | 417.53 KiB | 89,208 | 89,208 | 350.49 KiB | 973.17 KiB | -- | -- | -- |
| 29 | `dim_lab_item` | dim | `d_labitems_raw` | 12.86 KiB | 1,650 | 1,650 | 15.37 KiB | 133.90 KiB | -- | -- | -- |
| 30 | `dim_patient` | dim | `patients_raw` | 2.70 MiB | 364,627 | 364,627 | 1.49 MiB | 3.22 MiB | -- | -- | -- |
| 31 | `dim_procedure` | dim | `d_icd_procedures_raw` | 575.38 KiB | 86,423 | 86,423 | 1.11 MiB | 2.37 MiB | -- | -- | -- |
| 32 | `dim_provider` | dim | `provider_raw` | 124.35 KiB | 42,245 | 42,245 | 139.81 KiB | 163.09 KiB | -- | -- | -- |
| 33 | `fact_discharge_note` | note_text | `discharge_raw` | 1.06 GiB | 331,793 | 331,793 | 1.03 GiB | 2.06 GiB | 1.03 GiB | 1.85 GiB | 1.20 GiB |
| 34 | `fact_radiology_note` | note_text | `radiology_raw` | 745.61 MiB | 2,321,355 | 309,670 | 325.68 MiB | 649.46 MiB | 322.85 MiB | 604.54 MiB | 376.80 MiB |
| 35 | `agg_admission_daily` | agg | -- | -- | -- | 244,500 | 878.11 KiB | 909.55 KiB | -- | -- | -- |
| 36 | `agg_admission_monthly` | agg | -- | -- | -- | 10,856 | 2.86 MiB | 2.93 MiB | -- | -- | -- |
| 37 | `agg_icu_fluid_balance_daily` | agg | -- | -- | -- | 75,366 | 1.39 MiB | 1.42 MiB | -- | -- | -- |
| 38 | `fact_clinician_note_nlp_v` | ai_nlp | -- | -- | -- | n/a | n/a (format-v3) | 4.93 MiB | 4.56 MiB | 8.01 MiB | 4.79 MiB |
| 39 | `fact_discharge_note_nlp` | ai_nlp | -- | -- | -- | 10,000 | 1.59 MiB | 4.61 MiB | 1.18 MiB | 1.94 MiB | 1.23 MiB |
| 40 | `fact_radiology_note_nlp` | ai_nlp | -- | -- | -- | 2,711 | 6.18 MiB | 15.31 MiB | 4.46 MiB | 6.77 MiB | 3.42 MiB |
| 41 | `etl_control` | control | -- | -- | -- | 7 | 8.00 KiB | 123.63 KiB | -- | -- | -- |
| 42 | `etl_process_log` | control | -- | -- | -- | 677 | 53.63 KiB | 59.90 MiB | -- | -- | -- |
| 43 | `obt_admission_features` | obt | -- | -- | -- | 546,031 | 37.01 MiB | 37.58 MiB | 23.94 MiB | 30.58 MiB | 27.31 MiB |
| 44 | `obt_icu_stay_features` | obt | -- | -- | -- | 94,458 | 14.69 MiB | 15.05 MiB | -- | -- | -- |
| 45 | `obt_patient_360` | obt | -- | -- | -- | 364,627 | 7.35 MiB | 7.39 MiB | -- | -- | -- |

## Iceberg vs Parquet (ZSTD, Snappy) vs CSV -- representative table per archetype

From `docs/parquet_comparison/` (compare_parquet_iceberg.py, 2026-10-01T01:04:14.804724+00:00). Identical rows and columns in every format; Iceberg = data + metadata, excluding orphaned files. Reduction % = (comparator - subject) / comparator; negative = the first-named format is larger.

| Archetype | Table | Iceberg | Parquet (ZSTD) | Parquet (Snappy) | CSV (gzip) | Iceberg vs Parquet (ZSTD) | Iceberg vs CSV | Parquet ZSTD vs Snappy |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| Source Data Fact | `fact_admission` | 25.65 MiB | 25.60 MiB | 32.30 MiB | 32.04 MiB | -0.2% | 20.0% | 20.7% |
| Computed Structured Fact | `obt_admission_features` | 37.58 MiB | 23.94 MiB | 30.58 MiB | 27.31 MiB | -57.0% | -37.6% | 21.7% |
| Unstructured Data Fact | `fact_discharge_note` | 1.03 GiB | 1.03 GiB | 1.85 GiB | 1.20 GiB | 0.4% | 14.2% | 44.4% |
| Unstructured Data Fact | `fact_radiology_note` | 325.73 MiB | 322.85 MiB | 604.54 MiB | 376.80 MiB | -0.9% | 13.6% | 46.6% |
| Unstructured Features Fact | `fact_discharge_note_nlp` | 4.61 MiB | 1.18 MiB | 1.94 MiB | 1.23 MiB | -291.3% | -273.4% | 39.3% |
| Unstructured Features Fact | `fact_radiology_note_nlp` | 15.31 MiB | 4.46 MiB | 6.77 MiB | 3.42 MiB | -243.2% | -347.2% | 34.1% |
| Inference Fact | `fact_clinician_note_nlp_v` | 4.93 MiB | 4.56 MiB | 8.01 MiB | 4.79 MiB | -8.2% | -3.0% | 43.1% |
| **Total** | 7 tables | **1.43 GiB** | **1.40 GiB** | **2.52 GiB** | **1.63 GiB** | **-1.9%** | **12.3%** | **44.3%** |

## Fact tables -- detail (one row per fact loaded from a source CSV archive)

| Fact table | Raw source | Raw .csv.gz | Raw rows | Gold rows | Coverage | Raw B/row | Iceberg live | Iceberg B/row | Iceberg / raw per row | Change | Full-row equivalent | Iceberg physical |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `fact_chart_observation` | `chartevents_raw` | 3.26 GiB | 432,997,491 | 1,000,000 | 0.23% | 8.09 | 12.89 MiB | 13.51 | 1.671× | 67.1% | 5.45 GiB | 51.63 MiB |
| `fact_lab_result` | `labevents_raw` | 2.41 GiB | 158,374,764 | 1,000,000 | 0.63% | 16.37 | 37.24 MiB | 39.05 | 2.385× | 138.5% | 5.76 GiB | 188.21 MiB |
| `fact_medication_administration` | `emar_raw` | 773.72 MiB | 42,808,593 | 1,000,000 | 2.34% | 18.95 | 49.66 MiB | 52.07 | 2.748× | 174.8% | 2.08 GiB | 152.96 MiB |
| `fact_medication_administration_mini_detail` | `emar_detail_raw` | 713.50 MiB | 87,371,064 | 1,000,000 | 1.14% | 8.56 | 15.52 MiB | 16.28 | 1.901× | 90.1% | 1.32 GiB | 121.39 MiB |
| `fact_provider_order` | `poe_raw` | 635.71 MiB | 52,212,109 | 1,000,000 | 1.92% | 12.77 | 25.77 MiB | 27.02 | 2.117× | 111.7% | 1.31 GiB | 75.90 MiB |
| `fact_prescription` | `prescriptions_raw` | 578.21 MiB | 20,292,611 | 1,000,000 | 4.93% | 29.88 | 47.24 MiB | 49.54 | 1.658× | 65.8% | 958.70 MiB | 141.05 MiB |
| `fact_pharmacy_order` | `pharmacy_raw` | 501.35 MiB | 17,847,567 | 1,000,000 | 5.6% | 29.46 | 57.84 MiB | 60.65 | 2.059× | 105.9% | 1.01 GiB | 160.95 MiB |
| `fact_input_event` | `inputevents_raw` | 382.51 MiB | 10,953,713 | 1,000,000 | 9.13% | 36.62 | 57.31 MiB | 60.1 | 1.641× | 64.1% | 627.79 MiB | 145.16 MiB |
| `fact_ingredient_event` | `ingredientevents_raw` | 297.21 MiB | 14,253,480 | 1,000,000 | 7.02% | 21.86 | 41.68 MiB | 43.71 | 1.999× | 99.9% | 594.09 MiB | 128.64 MiB |
| `fact_microbiology_result` | `microbiologyevents_raw` | 112.19 MiB | 3,988,224 | 1,000,000 | 25.07% | 29.5 | 42.80 MiB | 44.88 | 1.522× | 52.2% | 170.71 MiB | 120.62 MiB |
| `fact_datetime_event` | `datetimeevents_raw` | 60.54 MiB | 9,979,761 | 1,000,000 | 10.02% | 6.36 | 18.96 MiB | 19.88 | 3.125× | 212.5% | 189.20 MiB | 65.25 MiB |
| `fact_provider_order_mini_detail` | `poe_detail_raw` | 52.71 MiB | 8,504,982 | 1,000,000 | 11.76% | 6.5 | 11.45 MiB | 12.0 | 1.847× | 84.7% | 97.36 MiB | 34.82 MiB |
| `fact_output_event` | `outputevents_raw` | 47.02 MiB | 5,359,395 | 1,000,000 | 18.66% | 9.2 | 23.23 MiB | 24.36 | 2.647× | 164.7% | 124.49 MiB | 66.13 MiB |
| `fact_transfer` | `transfers_raw` | 44.05 MiB | 2,413,581 | 1,000,000 | 41.43% | 19.14 | 30.81 MiB | 32.3 | 1.688× | 68.8% | 74.36 MiB | 70.10 MiB |
| `fact_outpatient_measurement` | `omr_raw` | 42.03 MiB | 7,753,027 | 1,000,000 | 12.9% | 5.68 | 8.36 MiB | 8.77 | 1.542× | 54.2% | 64.81 MiB | 26.73 MiB |
| `fact_diagnosis` | `diagnoses_icd_raw` | 32.01 MiB | 6,364,488 | 1,000,000 | 15.71% | 5.27 | 6.23 MiB | 6.53 | 1.239× | 23.9% | 39.65 MiB | 26.39 MiB |
| `fact_procedure_event` | `procedureevents_raw` | 22.98 MiB | 808,706 | 808,707 | 100.0% | 29.8 | 40.49 MiB | 52.5 | 1.762× | 76.2% | 40.49 MiB | 97.12 MiB |
| `fact_admission` | `admissions_raw` | 19.00 MiB | 546,028 | 546,029 | 100.0% | 36.5 | 25.58 MiB | 49.11 | 1.346× | 34.6% | 25.58 MiB | 52.00 MiB |
| `fact_drg_assignment` | `drgcodes_raw` | 9.29 MiB | 761,856 | 761,857 | 100.0% | 12.79 | 7.78 MiB | 10.7 | 0.837× | -16.3% | 7.78 MiB | 27.56 MiB |
| `fact_service_assignment` | `services_raw` | 8.17 MiB | 593,071 | 593,072 | 100.0% | 14.45 | 10.34 MiB | 18.28 | 1.265× | 26.5% | 10.34 MiB | 23.18 MiB |
| `fact_procedure` | `procedures_icd_raw` | 7.42 MiB | 859,655 | 859,656 | 100.0% | 9.05 | 10.80 MiB | 13.17 | 1.456× | 45.6% | 10.80 MiB | 27.36 MiB |
| `fact_icu_stay_accumulating` | `icustays_raw` | 3.19 MiB | 94,458 | 94,459 | 100.0% | 35.38 | 3.44 MiB | 38.17 | 1.079× | 7.9% | 3.44 MiB | 7.03 MiB |
| `fact_hcpcs_event` | `hcpcsevents_raw` | 2.06 MiB | 186,074 | 186,075 | 100.0% | 11.62 | 2.80 MiB | 15.8 | 1.36× | 36.0% | 2.80 MiB | 8.57 MiB |

## Dimensions -- detail

| Dimension | Raw source | Raw .csv.gz | Raw rows | Gold rows | Raw B/row | Iceberg live | Iceberg B/row | Iceberg / raw per row | Change |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `dim_patient` | `patients_raw` | 2.70 MiB | 364,627 | 364,627 | 7.78 | 1.49 MiB | 4.3 | 0.552× | -44.8% |
| `dim_diagnosis` | `d_icd_diagnoses_raw` | 855.82 KiB | 112,107 | 112,107 | 7.82 | 1.62 MiB | 15.2 | 1.944× | 94.4% |
| `dim_procedure` | `d_icd_procedures_raw` | 575.38 KiB | 86,423 | 86,423 | 6.82 | 1.11 MiB | 13.45 | 1.973× | 97.3% |
| `dim_hcpcs` | `d_hcpcs_raw` | 417.53 KiB | 89,208 | 89,208 | 4.79 | 350.49 KiB | 4.02 | 0.839× | -16.1% |
| `dim_provider` | `provider_raw` | 124.35 KiB | 42,245 | 42,245 | 3.01 | 139.81 KiB | 3.39 | 1.124× | 12.4% |
| `dim_chart_item` | `d_items_raw` | 57.36 KiB | 4,095 | 4,095 | 14.34 | 82.11 KiB | 20.53 | 1.431× | 43.1% |
| `dim_caregiver` | `caregiver_raw` | 40.59 KiB | 17,985 | 17,985 | 2.31 | 21.96 KiB | 1.25 | 0.541× | -45.9% |
| `dim_lab_item` | `d_labitems_raw` | 12.86 KiB | 1,650 | 1,650 | 7.98 | 15.37 KiB | 9.54 | 1.195× | 19.5% |

## Note text -- bytes only

Notes are rolled up from one row per note to one row per admission, so rows are not comparable; the byte comparison is as loaded (no row cap applies to these tables).

| Gold table | Raw source | Raw .csv.gz | Raw notes | Gold rows (admissions) | Iceberg live | Iceberg / raw | Change |
|---|---|--:|--:|--:|--:|--:|--:|
| `fact_discharge_note` | `discharge_raw` | 1.06 GiB | 331,793 | 331,793 | 1.03 GiB | 0.967× | -3.3% |
| `fact_radiology_note` | `radiology_raw` | 745.61 MiB | 2,321,355 | 309,670 | 325.68 MiB | 0.437× | -56.3% |

## Derived Gold tables (no raw source of their own)

| Table | Class | Iceberg live | Live rows | Iceberg physical |
|---|---|--:|--:|--:|
| `obt_admission_features` | obt | 37.01 MiB | 546,031 | 37.58 MiB |
| `obt_icu_stay_features` | obt | 14.69 MiB | 94,458 | 15.05 MiB |
| `obt_patient_360` | obt | 7.35 MiB | 364,627 | 7.39 MiB |
| `fact_radiology_note_nlp` | ai_nlp | 6.18 MiB | 2,711 | 15.31 MiB |
| `agg_admission_monthly` | agg | 2.86 MiB | 10,856 | 2.93 MiB |
| `fact_discharge_note_nlp` | ai_nlp | 1.59 MiB | 10,000 | 4.61 MiB |
| `agg_icu_fluid_balance_daily` | agg | 1.39 MiB | 75,366 | 1.42 MiB |
| `agg_admission_daily` | agg | 878.11 KiB | 244,500 | 909.55 KiB |
| `dim_date` | dim | 416.26 KiB | 47,846 | 453.43 KiB |
| `etl_process_log` | control | 53.63 KiB | 677 | 59.90 MiB |
| `etl_control` | control | 8.00 KiB | 7 | 123.63 KiB |
| `fact_clinician_note_nlp_v` | ai_nlp | n/a (format-v3) | n/a | 4.93 MiB |

## Row-count mismatches on fully loaded tables

Tables loaded in full (not capped) whose Gold row count differs from the raw row count -- worth checking (e.g. a CSV header line loaded as a data row):

| Table | Raw rows | Gold rows | Delta |
|---|--:|--:|--:|
| `fact_drg_assignment` | 761,856 | 761,857 | +1 |
| `fact_hcpcs_event` | 186,074 | 186,075 | +1 |
| `fact_procedure` | 859,655 | 859,656 | +1 |
| `fact_service_assignment` | 593,071 | 593,072 | +1 |
| `fact_procedure_event` | 808,706 | 808,707 | +1 |
| `fact_admission` | 546,028 | 546,029 | +1 |
| `fact_icu_stay_accumulating` | 94,458 | 94,459 | +1 |

## How to read this

- **Raw .csv.gz** = the source archive object in the raw bucket (row-oriented CSV, gzip). **Iceberg live** = bytes of the data files the table's current snapshot references (Parquet, ZSTD, columnar) -- excludes superseded and orphaned files. **Iceberg physical** = every object under the table's S3 prefix, including metadata and files awaiting orphan removal.
- **Per row** compares bytes per row on each side, so the 1M-row cap on large fact tables doesn't distort it. *Full-row equivalent* extrapolates linearly from the loaded rows -- `--row-limit` loads the first N rows, not a random sample.
- **Not a pure format benchmark.** Gold tables add surrogate date keys, derived measures and four audit columns, and type the columns; the codec also differs. For identical content across formats see `docs/parquet_comparison/` (compare_parquet_iceberg.py).

## Reproduce

```
python measure_storage_footprint.py --dataset fulldataset
python compare_iceberg_raw_csv.py --dataset fulldataset
```
