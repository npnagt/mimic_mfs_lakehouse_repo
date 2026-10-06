# Gold Layer Grain Summary — Fact, Aggregate, and OBT Tables

One row per table, stating its grain (what one row represents) and the column(s) that make a row unique. A CSV version of this same table is available at [fact_and_aggregate_grain_summary.csv](fact_and_aggregate_grain_summary.csv) for direct spreadsheet import.

## Fact Tables (23)

| Table | Type | Grain (one row per...) | Natural / uniqueness key | Source file |
|---|---|---|---|---|
| `fact_admission` | Fact | One hospital admission | `hadm_id` | `hosp/admissions.csv.gz` |
| `fact_diagnosis` | Factless bridge | One billed ICD diagnosis code on an admission, ranked by priority | `hadm_id`, `seq_num` | `hosp/diagnoses_icd.csv.gz` |
| `fact_drg_assignment` | Fact | One DRG code assigned to an admission | `hadm_id`, `drg_type`, `drg_code` | `hosp/drgcodes.csv.gz` |
| `fact_medication_administration` | Fact (header) | One medication administration event | `emar_id` | `hosp/emar.csv.gz` |
| `fact_medication_administration_mini_detail` | Mini-detail (child of above) | One dose/administration detail attribute row for an `emar_id` | `emar_id`, `emar_seq` | `hosp/emar_detail.csv.gz` |
| `fact_hcpcs_event` | Fact | One billed HCPCS code on an admission | `hadm_id`, `seq_num` | `hosp/hcpcsevents.csv.gz` |
| `fact_lab_result` | Fact | One laboratory test result | `labevent_id` | `hosp/labevents.csv.gz` |
| `fact_microbiology_result` | Fact | One microbiology culture/organism/sensitivity result row | `microevent_id` | `hosp/microbiologyevents.csv.gz` |
| `fact_outpatient_measurement` | Fact | One outpatient observation (height/weight/BP, etc.) | `subject_id`, `chart_date`, `seq_num` | `hosp/omr.csv.gz` |
| `fact_pharmacy_order` | Fact | One pharmacy dispensing record | `pharmacy_id` | `hosp/pharmacy.csv.gz` |
| `fact_provider_order` | Fact (header) | One provider order entry (POE) | `poe_id` | `hosp/poe.csv.gz` |
| `fact_provider_order_mini_detail` | Mini-detail (child of above) | One free-text field name/value pair for a `poe_id` | `poe_id`, `poe_seq`, `field_name` | `hosp/poe_detail.csv.gz` |
| `fact_prescription` | Fact | One prescribed medication order line | `poe_id`, `poe_seq`, `pharmacy_id` | `hosp/prescriptions.csv.gz` |
| `fact_procedure` | Factless bridge | One billed ICD procedure code on an admission, ranked by priority | `hadm_id`, `seq_num` | `hosp/procedures_icd.csv.gz` |
| `fact_service_assignment` | Fact | One clinical service change event during an admission | `hadm_id`, `transfer_time` | `hosp/services.csv.gz` |
| `fact_transfer` | Fact | One ADT (ward movement) event during an admission | `transfer_id` | `hosp/transfers.csv.gz` |
| `fact_icu_stay_accumulating` | Accumulating snapshot | One ICU stay | `stay_id` | `icu/icustays.csv.gz` |
| `fact_chart_observation` | Fact (transaction grain) | One charted vital sign/observation value | `stay_id`, `chart_time`, `item_id` | `icu/chartevents.csv.gz` |
| `fact_datetime_event` | Fact | One ICU-charted event whose value is itself a date/time | `stay_id`, `chart_time`, `item_id` | `icu/datetimeevents.csv.gz` |
| `fact_input_event` | Fact (header) | One intake (fluid/medication/nutrition) administration order | `order_id` | `icu/inputevents.csv.gz` |
| `fact_ingredient_event` | Mini-detail (child of `fact_input_event`) | One ingredient-level component of an intake order (e.g. one component of a mixed IV solution) | `order_id`, `item_id` | `icu/ingredientevents.csv.gz` |
| `fact_output_event` | Fact | One fluid output record (urine, drains, etc.) | `stay_id`, `chart_time`, `item_id` | `icu/outputevents.csv.gz` |
| `fact_procedure_event` | Fact | One ICU-charted procedure with a duration (ventilation, dialysis, line placement, etc.) | `order_id` | `icu/procedureevents.csv.gz` |

## Aggregate / OBT Tables (6)

| Table | Type | Grain (one row per...) | Uniqueness key | Rolls up from |
|---|---|---|---|---|
| `agg_admission_daily` | Daily aggregate | One `admission_type` slice of admit/discharge/death activity on one calendar day | `date_key`, `admission_type` | `fact_admission` |
| `agg_icu_fluid_balance_daily` | Daily aggregate | One ICU stay's net fluid balance on one calendar day | `date_key`, `stay_id` | `fact_input_event`, `fact_output_event` |
| `agg_admission_monthly` | Monthly aggregate | One `admission_type` slice of admit/discharge/death activity in one calendar month | `year_month`, `admission_type` | `agg_admission_daily` (**not** raw `fact_admission`) |
| `obt_admission_features` | OBT/ABT | One hospital admission, denormalized with demographics and rollups from every admission-scoped fact table | `hadm_id` | `fact_admission`, `dim_patient`, `fact_diagnosis`, `fact_procedure`, `fact_drg_assignment`, `fact_icu_stay_accumulating`, `fact_lab_result`, `fact_medication_administration`, `fact_microbiology_result`, `fact_transfer` |
| `obt_patient_360` | OBT/ABT | One patient, rolling up counts/flags across every admission they ever had | `subject_id` | `dim_patient`, `obt_admission_features` (**not** raw facts), `fact_diagnosis` (for distinct-code count only) |
| `obt_icu_stay_features` | OBT/ABT | One ICU stay, combining stay details with fluid balance and admission context | `stay_id` | `fact_icu_stay_accumulating`, `obt_admission_features`, `agg_icu_fluid_balance_daily`, `fact_chart_observation`, `fact_datetime_event`, `fact_procedure_event` |

A seventh table, `etl_control`, exists alongside these but is refresh-watermark metadata, not an analytical table — one row per `aggregate_table` name, tracking the max source `updated_ts` already processed.

## Notes on grain

- **"Factless bridge" tables** (`fact_diagnosis`, `fact_procedure`) carry no numeric measure of their own — they exist to connect an admission to a code, at the rank (`seq_num`) the coder assigned it.
- **"Mini-detail" tables** (`fact_medication_administration_mini_detail`, `fact_provider_order_mini_detail`, `fact_ingredient_event`) are narrower-grain children of a parent fact — always join back to the parent's key (`emar_id`, `poe_id`, `order_id` respectively) rather than treating them as standalone facts.
- **`fact_icu_stay_accumulating`** is the one accumulating-snapshot fact in this schema: its `los`/duration columns get overwritten in place as a stay progresses, rather than a new row being appended per update, the way every other fact table here behaves.
- **Aggregate/OBT grain is always coarser than at least one of its sources** — that's what makes the "rolls up from the smaller table, not the raw fact" refresh pattern possible (see `docs/lessons_learned_glue_iceberg_performance.md` and the `glue_visual_etl_pattern` design note for why this matters at scale).
