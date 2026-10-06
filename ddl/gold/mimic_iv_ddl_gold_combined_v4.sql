-- ==========================================================================================
-- MIMIC-IV Clinical Database Demo 2.2 -- Gold Layer DDL (Apache Iceberg via AWS Athena / AWS
-- Glue Data Catalog) -- combined HOSP + ICU modules
-- Database (schema): mimic4_db_business
--
-- This file combines the HOSP-module and ICU-module Gold DDLs into a single script, with all
-- dimension tables (dim_*) listed first, followed by all fact tables (fact_*), so dimensions
-- are always created before any fact table that references them.
--
-- COLUMN NAMING CONSISTENCY PASS: HOSP-module column names that were a single run of
-- concatenated words with no underscore (e.g. admittime, chartdate, storetime, valuenum,
-- itemid, orderid) have been renamed to underscore-separated form (admit_time, chart_date,
-- store_time, value_num, item_id, order_id) to match the convention already used by the ICU
-- module and by every existing underscored column in both modules. Columns that already
-- contained an underscore were left untouched. PARTITIONED BY (day(...)) clauses were updated
-- to reference the renamed columns where applicable.
--
-- ROLE-PLAYING DATE DIMENSION PASS (v3): every fact table that carries one or more
-- DATE/TIMESTAMP columns now also carries a matching "<role>_date_key" INT column
-- (e.g. admit_date_key, disch_date_key, death_date_key in fact_admission) intended to be
-- joined against the surrogate key dim_date.date_key. dim_date is used as a role-playing
-- dimension: the physical table is single and conformed, but a given fact table joins it
-- once per role, aliased per join (e.g. `JOIN dim_date ad ON f.admit_date_key = ad.date_key`,
-- `JOIN dim_date dd ON f.disch_date_key = dd.date_key`). The original TIMESTAMP/DATE columns
-- are intentionally kept alongside the new keys: dim_date is day-grain only, so time-of-day
-- precision (needed for LOS math, event ordering, shift analysis, etc.) still has to come
-- from the raw timestamp, not from the date dimension. PARTITIONED BY (day(...)) clauses are
-- unchanged and continue to reference the original timestamp columns -- partitioning is a
-- physical storage/query-pruning concern, independent of the dimensional-model FK. Factless
-- bridge/mini-detail tables with no date/timestamp column of their own (fact_diagnosis,
-- fact_drg_assignment, fact_medication_administration_mini_detail,
-- fact_provider_order_mini_detail) were left unchanged -- they inherit their date context
-- through hadm_id/poe_id/emar_id from the parent fact table and don't need their own date_key.
-- Gold-layer ETL must populate each *_date_key using the same surrogate-key formula as
-- dim_date.date_key (e.g. CAST(DATE_FORMAT(admit_time, 'yyyyMMdd') AS INT)), and must leave
-- it NULL wherever the source timestamp itself is NULL (e.g. death_date_key for patients who
-- did not expire during the admission).
--
-- CALCULATED / VALUE-ADDED COLUMN PASS (v4): fact tables also now carry derived measures and
-- flags computed once during Silver->Gold ETL, so every downstream query/BI tool gets a
-- consistent answer instead of each consumer re-deriving the same logic independently.
-- Two categories were added:
--   (a) Duration/interval measures in a stated unit, e.g. hospital_los_hours, ed_los_minutes,
--       time_to_death_hours (fact_admission); transfer_duration_hours (fact_transfer);
--       duration_minutes (fact_input_event, fact_ingredient_event, fact_procedure_event);
--       admin_delay_minutes (fact_medication_administration); result_turnaround_minutes
--       (fact_lab_result, fact_microbiology_result); charting_delay_minutes
--       (fact_chart_observation, fact_datetime_event, fact_output_event); time_to_icu_hours
--       (fact_icu_stay_accumulating).
--   (b) Derived flags/indicators, e.g. is_abnormal_flag (fact_lab_result, against
--       ref_range_lower/ref_range_upper; fact_chart_observation, against
--       dim_chart_item.low_normal_value/high_normal_value); is_positive_culture_flag
--       (fact_microbiology_result); is_readmission_flag + days_since_prior_discharge
--       (fact_admission, window function over prior admissions per subject_id);
--       is_icu_readmission_flag (fact_icu_stay_accumulating); age_at_admission
--       (fact_admission, from dim_patient.anchor_age/anchor_year).
-- Columns whose ETL logic needs a join beyond the fact table's own row (time_to_icu_hours,
-- is_abnormal_flag on fact_chart_observation, age_at_admission, is_readmission_flag /
-- days_since_prior_discharge, is_icu_readmission_flag) are called out explicitly in the
-- source-to-target mapping workbook -- see mimic4_calculated_and_datekey_mapping.xlsx.
-- True cross-row aggregates (e.g. net fluid balance, rolling mortality rate) were deliberately
-- NOT added as fact table columns -- they belong in a separate derived/summary fact table so
-- the transactional fact tables keep a single, unambiguous grain.
--
-- NOTE-DERIVED / NLP PASS (v5): three fact tables downstream of the clinical free-text notes
-- (mimic4_db_raw.discharge_note_raw / radiology_note_raw, loaded by etl/notes_ingest.py) --
-- fact_discharge_note_nlp, fact_radiology_note_nlp -- plus fact_clinician_note_nlp_v (a
-- VARIANT-typed, combined-note-type projection created by a Glue 6.0 job, NOT this Athena
-- DDL). See the "NOTE-DERIVED / NLP FACT TABLES" section below and the README ("Deriving
-- NLP features ...").
-- ==========================================================================================

CREATE SCHEMA IF NOT EXISTS mimic4_db_business
  COMMENT 'Business data layer / Gold layer tables -- HOSP and ICU modules combined';

-- ==========================================================================================
-- DIMENSION TABLES
-- ==========================================================================================

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_hcpcs
-- Description: Dimension table: HCPCS billing code definitions (long/short descriptions).
-- Source file: hosp/d_hcpcs.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_hcpcs (
    code STRING,
    category BIGINT,
    long_description STRING,
    short_description STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: HCPCS billing code definitions (long/short descriptions).'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_hcpcs/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_hcpcs SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_hcpcs/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_diagnosis
-- Description: Dimension table: ICD-9/ICD-10 diagnosis code definitions (title text)
--   referenced by fact_diagnosis_factless.
-- Source file: hosp/d_icd_diagnoses.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_diagnosis (
    icd_code STRING,
    icd_version INT,
    long_title STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: ICD-9/ICD-10 diagnosis code definitions (title text) referenced by fact_diagnosis_factless.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_diagnosis/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_diagnosis SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_diagnosis/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_procedure
-- Description: Dimension table: ICD-9/ICD-10 procedure code definitions (title text)
--   referenced by fact_procedure_factless.
-- Source file: hosp/d_icd_procedures.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_procedure (
    icd_code STRING,
    icd_version INT,
    long_title STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: ICD-9/ICD-10 procedure code definitions (title text) referenced by fact_procedure_factless.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_procedure/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_procedure SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_procedure/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_lab_item
-- Description: Dimension table: laboratory test (itemid) definitions -- label, fluid type,
--   and category -- referenced by fact_lab_result.
-- Source file: hosp/d_labitems.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_lab_item (
    item_id BIGINT,
    label STRING,
    fluid STRING,
    category STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: laboratory test (itemid) definitions -- label, fluid type, and category -- referenced by fact_lab_result.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_lab_item/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_lab_item SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_lab_item/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_patient
-- Description: Patient-level demographics: gender, anonymized age/year anchors, and date of
--   death (if applicable). One row per subject_id.
-- Source file: hosp/patients.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_patient (
    subject_id BIGINT,
    gender STRING,
    anchor_age INT,
    anchor_year INT,
    anchor_year_group STRING,
    dod DATE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Patient-level demographics: gender, anonymized age/year anchors, and date of death (if applicable). One row per subject_id.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_patient/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_patient SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_patient/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_provider
-- Description: Dimension table: anonymized caregiver/provider identifiers referenced
--   throughout the hosp module.
-- Source file: hosp/provider.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_provider (
    provider_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: anonymized caregiver/provider identifiers referenced throughout the hosp module.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_provider/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_provider SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_provider/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_caregiver
-- Description: Dimension table: anonymized ICU caregiver identifiers referenced throughout
--   the ICU module fact tables.
-- Source file: icu/caregiver.csv.gz
-- COLUMN MAPPING NOTE: the Glue-crawled raw (Bronze) table for this source file names its
--   single column 'choice', not 'caregiver_id'. The Gold table below intentionally keeps
--   the canonical business name 'caregiver_id' -- this is exactly the kind of raw-source
--   quirk the Bronze/Silver/Gold boundary exists to absorb, so it must not leak into Gold
--   naming. The Silver/Gold ETL populating this table must alias the column explicitly, e.g.:
--     INSERT INTO mimic4_db_business.dim_caregiver (caregiver_id)
--     SELECT choice AS caregiver_id FROM <bronze_or_silver_caregiver_table>;
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_caregiver (
    caregiver_id BIGINT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: anonymized ICU caregiver identifiers referenced throughout the ICU module fact tables.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_caregiver/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_caregiver SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_caregiver/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_chart_item
-- Description: Dimension table: ICU itemid definitions (label, category, unit of measure,
--   normal ranges) -- the conformed dimension referenced by fact_chart_observation,
--   fact_datetime_event, fact_input_event, fact_output_event, fact_procedure_event, and
--   fact_ingredient_event.
-- Source file: icu/d_items.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_chart_item (
    item_id BIGINT,
    label STRING,
    abbreviation STRING,
    links_to STRING,
    category STRING,
    unit_name STRING,
    param_type STRING,
    low_normal_value DOUBLE,
    high_normal_value DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Dimension table: ICU itemid definitions (label, category, unit of measure, normal ranges) -- the conformed dimension referenced by fact_chart_observation, fact_datetime_event, fact_input_event, fact_output_event, fact_procedure_event, and fact_ingredient_event.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_chart_item/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_chart_item SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_chart_item/'
);


-- ==========================================================================================
-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.dim_date
-- Description: Standard calendar date dimension: one row per calendar day, providing the
--   conformed date attributes (day/week/month/quarter/year, weekday flags, fiscal periods)
--   used to slice every fact table that carries a day(...) partition (fact_admission,
--   fact_lab_result, fact_chart_observation, etc.). Not itself partitioned -- small,
--   fully-loaded reference table.
-- NOTE: MIMIC-IV timestamps are date-shifted for de-identification, so calendar-holiday
--   attribution (is_holiday / holiday_name) is not meaningful against the shifted dates and
--   is included here as a standard, reusable field -- populate it only if you maintain a
--   separate, unshifted analytical calendar, or leave it null/false for this dataset.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.dim_date (
    date_key INT,
    full_date DATE,
    day_of_week INT,
    day_name STRING,
    day_name_short STRING,
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    week_start_date DATE,
    week_end_date DATE,
    month_number INT,
    month_name STRING,
    month_name_short STRING,
    first_day_of_month DATE,
    last_day_of_month DATE,
    quarter_number INT,
    quarter_name STRING,
    year INT,
    is_weekday BOOLEAN,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    fiscal_month INT,
    fiscal_quarter INT,
    fiscal_year INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Standard calendar date dimension: one row per calendar day, providing conformed date attributes (weekday/week/month/quarter/year, fiscal periods, weekend/holiday flags) used to slice all day-partitioned fact tables.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/dim_date/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.dim_date SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/dim_date/'
);

-- FACT TABLES
-- ==========================================================================================

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_admission
-- Description: Hospital admissions: one row per patient hospitalization (hadm_id), with
--   admit/discharge/death timestamps, admission type/location, insurance, and demographics
--   captured at admission.
-- Source file: hosp/admissions.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_admission (
    subject_id BIGINT,
    hadm_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain; NULL where the underlying event never occurred, e.g. death_date_key)
    admit_date_key INT,
    disch_date_key INT,
    death_date_key INT,
    ed_reg_date_key INT,
    ed_out_date_key INT,
    admit_time TIMESTAMP,
    disch_time TIMESTAMP,
    death_time TIMESTAMP,
    admission_type STRING,
    admit_provider_id STRING,
    admission_location STRING,
    discharge_location STRING,
    insurance STRING,
    language STRING,
    marital_status STRING,
    race STRING,
    ed_reg_time TIMESTAMP,
    ed_out_time TIMESTAMP,
    hospital_expire_flag INT,
    -- Calculated / value-added columns (populated at Silver->Gold ETL, not derived at query time)
    hospital_los_hours DOUBLE,
    ed_los_minutes DOUBLE,
    time_to_death_hours DOUBLE,
    age_at_admission INT,
    is_readmission_flag BOOLEAN,
    days_since_prior_discharge INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Hospital admissions: one row per patient hospitalization (hadm_id), with admit/discharge/death timestamps, admission type/location, insurance, and demographics captured at admission.'
-- bucket(4, hadm_id), not day(admit_time): MIMIC-IV timestamps are de-identification-
-- shifted across a ~100-year span, so a daily transform explodes to tens of thousands of
-- ~15-row partitions at full-dataset scale -- the Iceberg dynamic-overwrite writer then
-- opens a file per partition per task and every executor container is OOM-killed
-- ("ExecutorLostFailure ... containers exceeding thresholds" on the overwritePartitions
-- shuffle). A fixed hash bucket on the grain key keeps the partition count bounded
-- regardless of row volume; 4 buckets (not fact_diagnosis's 16) right-sizes this ~100 MB /
-- 546k-row table to ~25 MB files. Time-sliced reads use the admit_date_key column.
PARTITIONED BY (bucket(4, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_admission/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_admission SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_admission/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_diagnosis
-- Description: Billed ICD diagnosis codes assigned to each hospital admission, ranked by
--   seq_num (priority order). Factless fact: bridge between admission and diagnosis, no
--   numeric measures.
-- Source file: hosp/diagnoses_icd.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_diagnosis (
    subject_id BIGINT,
    hadm_id BIGINT,
    seq_num INT,
    icd_code STRING,
    icd_version INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Billed ICD diagnosis codes assigned to each hospital admission, ranked by seq_num (priority order). Factless fact: bridge between admission and diagnosis, no numeric measures.'
PARTITIONED BY (bucket(16, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_diagnosis/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_diagnosis SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_diagnosis/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_drg_assignment
-- Description: Diagnosis Related Group (DRG) codes assigned to each hospital admission,
--   used for billing/severity classification.
-- Source file: hosp/drgcodes.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_drg_assignment (
    subject_id BIGINT,
    hadm_id BIGINT,
    drg_type STRING,
    drg_code BIGINT,
    description STRING,
    drg_severity INT,
    drg_mortality INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Diagnosis Related Group (DRG) codes assigned to each hospital admission, used for billing/severity classification.'
PARTITIONED BY (bucket(8, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_drg_assignment/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_drg_assignment SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_drg_assignment/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_medication_administration
-- Description: Electronic Medication Administration Record: header-level record of each
--   medication administration event tied to a POE (provider order entry) and pharmacy order.
-- Source file: hosp/emar.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_medication_administration (
    subject_id BIGINT,
    hadm_id BIGINT,
    emar_id STRING,
    emar_seq INT,
    poe_id STRING,
    pharmacy_id BIGINT,
    enter_provider_id STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    chart_date_key INT,
    schedule_date_key INT,
    store_date_key INT,
    chart_time TIMESTAMP,
    medication STRING,
    event_txt STRING,
    schedule_time TIMESTAMP,
    store_time TIMESTAMP,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    admin_delay_minutes DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Electronic Medication Administration Record: header-level record of each medication administration event tied to a POE (provider order entry) and pharmacy order.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(32, emar_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_medication_administration/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_medication_administration SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_medication_administration/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_medication_administration_mini_detail
-- Description: Electronic Medication Administration Record detail: granular dose/administration
--   attributes (site, rate, barcode scan info) for each emar_id. Mini-fact: narrow attribute
--   extension of fact_medication_administration.
-- Source file: hosp/emar_detail.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_medication_administration_mini_detail (
    subject_id BIGINT,
    emar_id STRING,
    emar_seq INT,
    parent_field_ordinal DOUBLE,
    administration_type STRING,
    pharmacy_id BIGINT,
    barcode_type STRING,
    reason_for_no_barcode STRING,
    complete_dose_not_given STRING,
    dose_due DOUBLE,
    dose_due_unit STRING,
    dose_given STRING,
    dose_given_unit STRING,
    will_remainder_of_dose_be_given STRING,
    product_amount_given DOUBLE,
    product_unit STRING,
    product_code STRING,
    product_description STRING,
    product_description_other STRING,
    prior_infusion_rate DOUBLE,
    infusion_rate DOUBLE,
    infusion_rate_adjustment STRING,
    infusion_rate_adjustment_amount DOUBLE,
    infusion_rate_unit STRING,
    route STRING,
    infusion_complete STRING,
    completion_interval STRING,
    new_iv_bag_hung STRING,
    continued_infusion_in_other_location STRING,
    restart_interval STRING,
    side STRING,
    site STRING,
    non_formulary_visual_verification STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Electronic Medication Administration Record detail: granular dose/administration attributes (site, rate, barcode scan info) for each emar_id. Mini-fact: narrow attribute extension of fact_medication_administration.'
PARTITIONED BY (bucket(32, emar_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_medication_administration_mini_detail/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_medication_administration_mini_detail SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_medication_administration_mini_detail/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_hcpcs_event
-- Description: Billed HCPCS procedure/service codes recorded against a hospital admission.
-- Source file: hosp/hcpcsevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_hcpcs_event (
    subject_id BIGINT,
    hadm_id BIGINT,
    -- Role-playing FK to dim_date.date_key (day grain)
    chart_date_key INT,
    chart_date DATE,
    hcpcs_cd STRING,
    seq_num INT,
    short_description STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Billed HCPCS procedure/service codes recorded against a hospital admission.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(4, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_hcpcs_event/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_hcpcs_event SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_hcpcs_event/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_lab_result
-- Description: Laboratory test results (chemistry, hematology, etc.) collected for a patient,
--   optionally tied to a hospital admission.
-- Source file: hosp/labevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_lab_result (
    labevent_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    specimen_id BIGINT,
    item_id BIGINT,
    order_provider_id STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    chart_date_key INT,
    store_date_key INT,
    chart_time TIMESTAMP,
    store_time TIMESTAMP,
    value STRING,
    value_num DOUBLE,
    value_uom STRING,
    ref_range_lower DOUBLE,
    ref_range_upper DOUBLE,
    flag STRING,
    priority STRING,
    comments STRING,
    -- Calculated / value-added columns (populated at Silver->Gold ETL, not derived at query time)
    result_turnaround_minutes DOUBLE,
    is_abnormal_flag BOOLEAN,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Laboratory test results (chemistry, hematology, etc.) collected for a patient, optionally tied to a hospital admission.'
-- bucket(64, labevent_id): ~158M rows on the full dataset; day(chart_time) was already
-- removed (shifted-timestamp partition explosion), but a flat unpartitioned write of
-- 158M rows is unwieldy. 64 hash buckets on the grain key -> ~2.5M rows / ~300 MB files.
PARTITIONED BY (bucket(64, labevent_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_lab_result/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_lab_result SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_lab_result/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_microbiology_result
-- Description: Microbiology culture orders, organisms isolated, and antibiotic sensitivity
--   (antibiogram) results.
-- Source file: hosp/microbiologyevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_microbiology_result (
    microevent_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    micro_specimen_id BIGINT,
    order_provider_id STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    chart_date_key INT,
    store_date_key INT,
    chart_date TIMESTAMP,
    chart_time TIMESTAMP,
    spec_itemid BIGINT,
    spec_type_desc STRING,
    test_seq INT,
    store_date TIMESTAMP,
    store_time TIMESTAMP,
    test_itemid BIGINT,
    test_name STRING,
    org_itemid BIGINT,
    org_name STRING,
    isolate_num INT,
    quantity DOUBLE,
    ab_itemid BIGINT,
    ab_name STRING,
    dilution_text STRING,
    dilution_comparison STRING,
    dilution_value DOUBLE,
    interpretation STRING,
    comments STRING,
    -- Calculated / value-added columns (populated at Silver->Gold ETL, not derived at query time)
    result_turnaround_minutes DOUBLE,
    is_positive_culture_flag BOOLEAN,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Microbiology culture orders, organisms isolated, and antibiotic sensitivity (antibiogram) results.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(8, microevent_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_microbiology_result/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_microbiology_result SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_microbiology_result/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_outpatient_measurement
-- Description: Online Medical Record: outpatient-recorded observations such as height,
--   weight, and blood pressure, keyed by result_name/result_value.
-- Source file: hosp/omr.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_outpatient_measurement (
    subject_id BIGINT,
    -- Role-playing FK to dim_date.date_key (day grain)
    chart_date_key INT,
    chart_date DATE,
    seq_num INT,
    result_name STRING,
    result_value STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Online Medical Record: outpatient-recorded observations such as height, weight, and blood pressure, keyed by result_name/result_value.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(8, subject_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_outpatient_measurement/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_outpatient_measurement SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_outpatient_measurement/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_pharmacy_order
-- Description: Pharmacy dispensing records: medication orders with dosing schedule, route,
--   frequency, and dispensation details.
-- Source file: hosp/pharmacy.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_pharmacy_order (
    subject_id BIGINT,
    hadm_id BIGINT,
    pharmacy_id BIGINT,
    poe_id STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    start_date_key INT,
    stop_date_key INT,
    enter_date_key INT,
    verified_date_key INT,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    medication STRING,
    proc_type STRING,
    status STRING,
    enter_time TIMESTAMP,
    verified_time TIMESTAMP,
    route STRING,
    frequency STRING,
    disp_sched STRING,
    infusion_type STRING,
    sliding_scale STRING,
    lockout_interval DOUBLE,
    basal_rate DOUBLE,
    one_hr_max DOUBLE,
    doses_per_24_hrs DOUBLE,
    duration DOUBLE,
    duration_interval STRING,
    expiration_value DOUBLE,
    expiration_unit STRING,
    expiration_date DATE,
    dispensation STRING,
    fill_quantity DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Pharmacy dispensing records: medication orders with dosing schedule, route, frequency, and dispensation details.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(16, pharmacy_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_pharmacy_order/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_pharmacy_order SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_pharmacy_order/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_provider_order
-- Description: Provider Order Entry: header record of every order placed for a patient
--   (order type/subtype, status, ordering provider).
-- Source file: hosp/poe.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_provider_order (
    poe_id STRING,
    poe_seq INT,
    subject_id BIGINT,
    hadm_id BIGINT,
    -- Role-playing FK to dim_date.date_key (day grain)
    order_date_key INT,
    order_time TIMESTAMP,
    order_type STRING,
    order_subtype STRING,
    transaction_type STRING,
    discontinue_of_poe_id STRING,
    discontinued_by_poe_id STRING,
    order_provider_id STRING,
    order_status STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Provider Order Entry: header record of every order placed for a patient (order type/subtype, status, ordering provider).'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). Bucketed on
-- poe_id, matching fact_provider_order_mini_detail, so a header/detail join on poe_id
-- prunes the same buckets on both sides.
PARTITIONED BY (bucket(16, poe_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_provider_order/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_provider_order SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_provider_order/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_provider_order_mini_detail
-- Description: Provider Order Entry detail: free-text field name/value pairs providing
--   additional context for a poe_id. Mini-fact: narrow attribute extension of
--   fact_provider_order.
-- Source file: hosp/poe_detail.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_provider_order_mini_detail (
    poe_id STRING,
    poe_seq INT,
    subject_id BIGINT,
    field_name STRING,
    field_value STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Provider Order Entry detail: free-text field name/value pairs providing additional context for a poe_id. Mini-fact: narrow attribute extension of fact_provider_order.'
PARTITIONED BY (bucket(16, poe_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_provider_order_mini_detail/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_provider_order_mini_detail SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_provider_order_mini_detail/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_prescription
-- Description: Prescribed medication orders including drug identifiers (NDC, GSN), dose,
--   formulary code, and route, tied to pharmacy and POE records.
-- Source file: hosp/prescriptions.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_prescription (
    subject_id BIGINT,
    hadm_id BIGINT,
    pharmacy_id BIGINT,
    poe_id STRING,
    poe_seq INT,
    order_provider_id STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    start_date_key INT,
    stop_date_key INT,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    drug_type STRING,
    drug STRING,
    formulary_drug_cd STRING,
    gsn STRING,
    ndc BIGINT,
    prod_strength STRING,
    form_rx STRING,
    dose_val_rx STRING,
    dose_unit_rx STRING,
    form_val_disp DOUBLE,
    form_unit_disp STRING,
    doses_per_24_hrs DOUBLE,
    route STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Prescribed medication orders including drug identifiers (NDC, GSN), dose, formulary code, and route, tied to pharmacy and POE records.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(16, pharmacy_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_prescription/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_prescription SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_prescription/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_procedure
-- Description: Billed ICD procedure codes performed during a hospital admission, ranked by
--   seq_num. Factless fact: bridge between admission and procedure, no numeric measures.
-- Source file: hosp/procedures_icd.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_procedure (
    subject_id BIGINT,
    hadm_id BIGINT,
    seq_num INT,
    -- Role-playing FK to dim_date.date_key (day grain)
    chart_date_key INT,
    chart_date DATE,
    icd_code STRING,
    icd_version INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Billed ICD procedure codes performed during a hospital admission, ranked by seq_num. Factless fact: bridge between admission and procedure, no numeric measures.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(4, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_procedure/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_procedure SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_procedure/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_service_assignment
-- Description: Hospital service transfers: record of the clinical service (e.g., MED, SURG)
--   a patient was under during an admission.
-- Source file: hosp/services.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_service_assignment (
    subject_id BIGINT,
    hadm_id BIGINT,
    -- Role-playing FK to dim_date.date_key (day grain)
    transfer_date_key INT,
    transfer_time TIMESTAMP,
    prev_service STRING,
    curr_service STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Hospital service transfers: record of the clinical service (e.g., MED, SURG) a patient was under during an admission.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(4, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_service_assignment/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_service_assignment SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_service_assignment/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_transfer
-- Description: Patient movement between hospital units/wards (ADT events) during an
--   admission, including ICU stay boundaries.
-- Source file: hosp/transfers.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_transfer (
    subject_id BIGINT,
    hadm_id BIGINT,
    transfer_id BIGINT,
    event_type STRING,
    care_unit STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    in_date_key INT,
    out_date_key INT,
    in_time TIMESTAMP,
    out_time TIMESTAMP,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    transfer_duration_hours DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Patient movement between hospital units/wards (ADT events) during an admission, including ICU stay boundaries.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(8, transfer_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_transfer/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_transfer SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_transfer/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_icu_stay_accumulating
-- Description: One row per ICU stay (stay_id): admitting/discharging care unit, ICU in/out
--   time, and length of stay (los, in days). Accumulating-snapshot fact: child grain of
--   fact_admission, updated as the stay progresses and finalized at ICU discharge.
-- Source file: icu/icustays.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_icu_stay_accumulating (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    first_careunit STRING,
    last_careunit STRING,
    -- Role-playing FKs to dim_date.date_key (day grain)
    in_date_key INT,
    out_date_key INT,
    in_time TIMESTAMP,
    out_time TIMESTAMP,
    los DOUBLE,
    -- Calculated / value-added columns (populated at Silver->Gold ETL, not derived at query time)
    time_to_icu_hours DOUBLE,
    is_icu_readmission_flag BOOLEAN,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'One row per ICU stay (stay_id): admitting/discharging care unit, ICU in/out time, and length of stay (los, in days). Accumulating-snapshot fact: child grain of fact_admission, updated as the stay progresses and finalized at ICU discharge.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(4, stay_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_icu_stay_accumulating/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_icu_stay_accumulating SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_icu_stay_accumulating/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_chart_observation
-- Description: Charted vital signs and observations recorded at the bedside during an ICU
--   stay. Highest-volume ICU fact table; transaction grain, one row per charted value.
-- Source file: icu/chartevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_chart_observation (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    caregiver_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain)
    chart_date_key INT,
    store_date_key INT,
    chart_time TIMESTAMP,
    store_time TIMESTAMP,
    item_id BIGINT,
    value STRING,
    value_num DOUBLE,
    value_uom STRING,
    warning INT,
    -- Calculated / value-added columns (populated at Silver->Gold ETL, not derived at query time)
    charting_delay_minutes DOUBLE,
    is_abnormal_flag BOOLEAN,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Charted vital signs and observations recorded at the bedside during an ICU stay. Highest-volume ICU fact table; transaction grain, one row per charted value.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions and OOM-kills the Iceberg
-- overwritePartitions writer (see fact_admission). chartevents is ~432M rows on the full
-- dataset -- its Glue job is run with --row-limit 1000000 there (like every other >1M
-- fact) to cap cloud cost, so 8 buckets (not the ~256 the full table would want) is
-- enough. Demo loads it in full (~1.3M rows). Time-sliced reads use chart_date_key.
PARTITIONED BY (bucket(8, stay_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_chart_observation/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_chart_observation SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_chart_observation/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_datetime_event
-- Description: ICU-charted events whose value is itself a date/time (e.g., last dialysis
--   date), recorded during an ICU stay.
-- Source file: icu/datetimeevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_datetime_event (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    caregiver_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain)
    chart_date_key INT,
    store_date_key INT,
    chart_time TIMESTAMP,
    store_time TIMESTAMP,
    item_id BIGINT,
    value STRING,
    value_uom STRING,
    warning INT,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    charting_delay_minutes DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'ICU-charted events whose value is itself a date/time (e.g., last dialysis date), recorded during an ICU stay.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(8, stay_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_datetime_event/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_datetime_event SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_datetime_event/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_input_event
-- Description: Intake events: fluids, medications, and nutrition administered to a patient
--   during an ICU stay.
-- Source file: icu/inputevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_input_event (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    caregiver_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain)
    start_date_key INT,
    end_date_key INT,
    store_date_key INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    store_time TIMESTAMP,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    duration_minutes DOUBLE,
    item_id BIGINT,
    amount DOUBLE,
    amount_uom STRING,
    rate DOUBLE,
    rate_uom STRING,
    order_id BIGINT,
    link_order_id BIGINT,
    order_category_name STRING,
    secondary_order_category_name STRING,
    order_component_type_description STRING,
    order_category_description STRING,
    patient_weight DOUBLE,
    total_amount DOUBLE,
    total_amount_uom STRING,
    is_open_bag INT,
    continue_in_next_dept INT,
    status_description STRING,
    original_amount DOUBLE,
    original_rate DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Intake events: fluids, medications, and nutrition administered to a patient during an ICU stay.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(8, order_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_input_event/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_input_event SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_input_event/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_ingredient_event
-- Description: Individual ingredient-level detail (e.g., components of a mixed IV solution)
--   underlying fact_input_event records. Child grain of fact_input_event.
-- Source file: icu/ingredientevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_ingredient_event (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    caregiver_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain)
    start_date_key INT,
    end_date_key INT,
    store_date_key INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    store_time TIMESTAMP,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    duration_minutes DOUBLE,
    item_id BIGINT,
    amount DOUBLE,
    amount_uom STRING,
    rate DOUBLE,
    rate_uom STRING,
    order_id BIGINT,
    link_order_id BIGINT,
    status_description STRING,
    original_amount BIGINT,
    original_rate DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Individual ingredient-level detail (e.g., components of a mixed IV solution) underlying fact_input_event records. Child grain of fact_input_event.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(16, order_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_ingredient_event/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_ingredient_event SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_ingredient_event/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_output_event
-- Description: Output events: fluid output (e.g., urine, drains) recorded for a patient
--   during an ICU stay.
-- Source file: icu/outputevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_output_event (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    caregiver_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain)
    chart_date_key INT,
    store_date_key INT,
    chart_time TIMESTAMP,
    store_time TIMESTAMP,
    item_id BIGINT,
    value BIGINT,
    value_uom STRING,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    charting_delay_minutes DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Output events: fluid output (e.g., urine, drains) recorded for a patient during an ICU stay.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(8, stay_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_output_event/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_output_event SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_output_event/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_procedure_event
-- Description: Procedures with a duration (e.g., ventilation, dialysis, line placement)
--   performed during an ICU stay. Distinct from fact_procedure (billed ICD procedure codes,
--   HOSP module): this table captures the ICU-charted event itself, with real start/end
--   timestamps and order-level detail, not a billing code.
-- Source file: icu/procedureevents.csv.gz
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_procedure_event (
    subject_id BIGINT,
    hadm_id BIGINT,
    stay_id BIGINT,
    caregiver_id BIGINT,
    -- Role-playing FKs to dim_date.date_key (day grain)
    start_date_key INT,
    end_date_key INT,
    store_date_key INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    store_time TIMESTAMP,
    -- Calculated / value-added column (populated at Silver->Gold ETL, not derived at query time)
    duration_minutes DOUBLE,
    item_id BIGINT,
    value DOUBLE,
    value_uom STRING,
    location STRING,
    location_category STRING,
    order_id BIGINT,
    link_order_id BIGINT,
    order_category_name STRING,
    order_category_description STRING,
    patient_weight DOUBLE,
    is_open_bag INT,
    continue_in_next_dept INT,
    status_description STRING,
    original_amount BIGINT,
    original_rate INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Procedures with a duration (e.g., ventilation, dialysis, line placement) performed during an ICU stay. Distinct from fact_procedure (billed ICD procedure codes, HOSP module): this table captures the ICU-charted event itself, with real start/end timestamps and order-level detail, not a billing code.'
-- bucket(N, key) not day(...): shifted MIMIC-IV timestamps span ~100y, so a daily
-- transform explodes to tens of thousands of tiny partitions at full-dataset scale and
-- OOM-kills the Iceberg overwritePartitions writer (see fact_admission). N sized to the
-- table's row volume; time-sliced reads use the *_date_key column.
PARTITIONED BY (bucket(4, order_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_procedure_event/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_procedure_event SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_procedure_event/'
);

-- ==========================================================================================
-- NOTE-CONCAT FACTS (Unstructured Data Fact archetype, admission grain)
-- ==========================================================================================
-- mimic4_db_raw.discharge_note_raw / radiology_note_raw are grain = note_id: an admission
-- with multiple notes has multiple raw rows, which does not match every other archetype in
-- the Multimodal Fusion Schema (Source Data Fact, Computed Structured Fact, Unstructured
-- Features Fact, and Inference Fact are all one row per subject_id/hadm_id). These two
-- tables give the Unstructured Data Fact archetype a Gold instance at that SAME grain, so
-- every archetype in the schema joins on the identical key. discharge_note_raw /
-- radiology_note_raw are unchanged and remain the canonical note-id-grain source; these
-- are a rollup, not a replacement -- populated by fact_note_concat.py (one script,
-- parameterized per note type; see etl/create_fact_note_concat_job.py).
--
-- text            = every admission's note(s), concatenated in note_seq order, separated
--                   by a blank line.
-- notes_count     = count of raw notes rolled into this admission's row.
-- last_charttime, last_storetime = max(charttime)/max(storetime) across that admission's
--                   notes (cast from the raw table's STRING columns; NULL/unparseable
--                   values are ignored by max(), same as every other date-parsing path in
--                   this repo).
--
-- Full-reload write (plain overwritePartitions(), not the resumable-batched/etl_control-
-- watermarked pattern the *_nlp tables below use) -- text concatenation is cheap relative
-- to medSpaCy's per-document NLP cost, so PARTITIONED BY (bucket(4, hadm_id)) is an
-- ordinary Iceberg hidden partition transform (same as fact_admission), not a materialized
-- hadm_bucket column: this job's single write spans every bucket at once, so there is no
-- per-bucket-batch reason to need a real, queryable partition column the way
-- medspacy_nlp.py / radiology_nlp.py do.
-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_discharge_note
-- Source table: mimic4_db_raw.discharge_note_raw
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_discharge_note (
    subject_id BIGINT,
    hadm_id BIGINT,
    admit_provider_id STRING,
    admit_date_key INT,
    text STRING,
    notes_count INT,
    last_charttime TIMESTAMP,
    last_storetime TIMESTAMP,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Unstructured Data Fact at admission grain: one row per subject_id/hadm_id, with all discharge note text for that admission concatenated in note_seq order. Rolled up from mimic4_db_raw.discharge_note_raw (grain = note_id) by fact_note_concat.py so this archetype joins on the same key as every other fact table in the schema.'
PARTITIONED BY (bucket(4, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_discharge_note/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_discharge_note SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_discharge_note/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_radiology_note
-- Source table: mimic4_db_raw.radiology_note_raw
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_radiology_note (
    subject_id BIGINT,
    hadm_id BIGINT,
    admit_provider_id STRING,
    admit_date_key INT,
    text STRING,
    notes_count INT,
    last_charttime TIMESTAMP,
    last_storetime TIMESTAMP,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'Unstructured Data Fact at admission grain: one row per subject_id/hadm_id, with all radiology note text for that admission concatenated in note_seq order. Rolled up from mimic4_db_raw.radiology_note_raw (grain = note_id) by fact_note_concat.py so this archetype joins on the same key as every other fact table in the schema.'
PARTITIONED BY (bucket(8, hadm_id))
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_radiology_note/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_radiology_note SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_radiology_note/'
);

-- ==========================================================================================
-- NOTE-DERIVED / NLP FACT TABLES
-- ==========================================================================================
-- Fact tables built from the clinical free-text notes rather than the structured CSVs.
-- Source: mimic4_db_raw.discharge_note_raw / radiology_note_raw (MIMIC-IV-Note, converted
-- from gzip CSV to Parquet by etl/notes_ingest.py -- they cannot be crawled, the `text`
-- column has embedded newlines). Grain: one row per admission (subject_id, hadm_id).
--
--   fact_discharge_note_nlp    discharge notes -> medSpaCy (open-source)     (run_medspacy_nlp.py)
--   fact_radiology_note_nlp   radiology notes -> section splitter + medSpaCy (run_radiology_nlp.py)
--   fact_clinician_note_nlp_v both of the above combined, real Iceberg VARIANT cols
--                             (run_clinician_note_variant.py, Glue 6.0 -- see its comment
--                             block after fact_radiology_note_nlp)
--
-- Every JSON payload is stored in a STRING column: Athena engine v3 has no VARIANT or JSON
-- column type (CREATE TABLE ... col VARIANT / col JSON both fail to parse) and cannot read
-- an Iceberg format-version-3 table at all. Query with json_parse() / json_extract() /
-- CAST(... AS ARRAY(JSON)) + UNNEST. Switch to VARIANT if/when Athena supports format-v3.
-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_discharge_note_nlp
-- Description: NLP-derived features from the discharge notes, one row per admission
--   (subject_id, hadm_id). Populated by medspacy_nlp.py (run_medspacy_nlp.py) using medSpaCy
--   (spaCy + PyRuSH + a rule-based TargetMatcher + the ConText negation/historical/
--   hypothetical/family algorithm). Where an admission has multiple discharge notes
--   (addenda), their entities are pooled before the level rules run.
--
--   Derived level vocabularies:
--     tobacco_use, alcohol_use : NOT_MENTIONED | DENIES | FORMER | CURRENT
--     obesity_level            : NONE | OBESE | MORBID
--     tobacco_cessation_cd     : NA | NONE | COUNSELED | PHARMACOTHERAPY
--   (see derive_levels() in medspacy_nlp.py for the exact rules.)
--
--   recognized_entities holds the full medSpaCy output per note ({nlp_engine, note_count,
--   notes:[{note_id, entities:[{text,label,negated,historical,hypothetical,family,...}]}]})
--   as a JSON STRING. Athena engine v3 has no VARIANT/JSON column type and cannot read an
--   Iceberg format-v3 table; query with json_parse()/json_extract(). Switch to VARIANT
--   if/when Athena supports format-v3.
--
--   hadm_bucket = hadm_id % 64, PARTITIONED BY hadm_bucket (plain IDENTITY, not an
--   Iceberg bucket() transform): medspacy_nlp.py backfills this table in resumable
--   batches (one Iceberg overwritePartitions() write per batch, see its module docstring)
--   -- without partitioning, that overwrite replaces the WHOLE table every batch, so cost
--   grows with cumulative backfill progress instead of batch size (a real O(n^2)-ish
--   pattern, caught before it was ever exercised: this table had 0 rows when partitioning
--   was added). A real bucket() transform would avoid needing this extra column, but
--   glue_catalog's SparkCatalog here doesn't implement Iceberg's function catalog
--   (confirmed live: "AnalysisException: Catalog glue_catalog does not support
--   functions" from glue_catalog.system.bucket(...)) -- so the bucket assignment is
--   materialized as a real column instead, computed identically by the write path
--   (grouping) and stored in the row (partition value), with no Iceberg-side hash to
--   replicate. Not a date/identity partition on a real business column: there's no date
--   column that groups admissions the way agg tables group by date_key.
-- Source table: mimic4_db_raw.discharge_note_raw
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_discharge_note_nlp (
    subject_id BIGINT,
    hadm_id BIGINT,
    admit_provider_id STRING,
    admit_date_key INT,
    hadm_bucket INT,
    tobacco_use STRING,
    alcohol_use STRING,
    obesity_level STRING,
    tobacco_cessation_cd STRING,
    recognized_entities STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'NLP-derived features from discharge notes via medSpaCy (rule-based TargetMatcher + ConText): one row per admission (subject_id, hadm_id) with tobacco/alcohol/obesity levels, a tobacco cessation code, and the full medSpaCy entity JSON in recognized_entities. hadm_bucket = hadm_id % 64 is an internal partitioning column for resumable batched writes, not a business column.'
PARTITIONED BY (hadm_bucket)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_discharge_note_nlp/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_discharge_note_nlp SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_discharge_note_nlp/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_radiology_note_nlp
-- Description: NLP feature extraction from the RADIOLOGY notes (mimic4_db_raw.radiology_note_raw),
--   one row per admission (subject_id, hadm_id). Populated by radiology_nlp.py
--   (run_radiology_nlp.py) with medSpaCy: a regex section splitter (EXAMINATION / INDICATION /
--   TECHNIQUE / FINDINGS / IMPRESSION / PROCEDURE) plus a rule-based TargetMatcher + ConText
--   for negation-aware symptom / disorder recognition, a modality classifier, and a curated
--   disorder -> ICD-10-CM lookup (+ regex for ICD codes cited in the text).
--
--   Every derived column holds a JSON array/object as a STRING -- Athena engine v3 has no
--   VARIANT or JSON column type (CREATE TABLE ... col VARIANT / col JSON both fail to parse),
--   and neither Glue 4.0 nor Glue 5.0 (Spark 3.5) expose Iceberg's variant type (verified).
--   Query with json_extract() / json_parse() / CAST(... AS ARRAY(...)); switch these columns
--   to VARIANT once the platform supports it.
--     radiology_procedure_types  JSON [{exam, modality, body_region, note_id}]
--     radiology_reasons          JSON [{reason, note_id}]         -- the INDICATION "// ..." question
--     symptoms                   JSON [{text, negated, note_id}]
--     disorders                  JSON [{text, negated, note_id}]
--     icd_codes                  JSON [{code, term, source}]      -- source = cited | lookup
--     procedures                 JSON [{text, note_id}]
--     findings_summary           JSON [{note_id, text}]           -- FINDINGS section text
--     indication_summary         JSON [{note_id, text}]           -- INDICATION/HISTORY section text
--     conclusion                 JSON [{note_id, text}]           -- IMPRESSION section text
--     ner_json                   JSON {engine, note_count, notes:[{note_id, entities:[...]}]}  -- the entire medSpaCy output
--
--   hadm_bucket = hadm_id % 128, PARTITIONED BY hadm_bucket -- same resumable-batched-
--   backfill reasoning, and same plain-IDENTITY-on-a-computed-column approach (not an
--   Iceberg bucket() transform), as fact_discharge_note_nlp above (see that table's
--   comment for why); a larger bucket count here since up to ~310,000 admissions can
--   carry a radiology note, vs ~330,000 total for discharge notes but capped at 1M notes
--   rather than 1M admissions.
-- Source table: mimic4_db_raw.radiology_note_raw
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.fact_radiology_note_nlp (
    subject_id BIGINT,
    hadm_id BIGINT,
    admit_provider_id STRING,
    admit_date_key INT,
    hadm_bucket INT,
    note_count INT,
    radiology_procedure_types STRING,
    radiology_reasons STRING,
    symptoms STRING,
    disorders STRING,
    icd_codes STRING,
    procedures STRING,
    findings_summary STRING,
    indication_summary STRING,
    conclusion STRING,
    ner_json STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    created_by STRING,
    updated_by STRING
)
COMMENT 'NLP features from radiology notes via medSpaCy: one row per admission (subject_id, hadm_id) with procedure types, reasons, symptoms, disorders, ICD-10 codes, procedures, and FINDINGS/INDICATION/IMPRESSION summaries -- all JSON-in-STRING -- plus the full medSpaCy output in ner_json. hadm_bucket = hadm_id % 128 is an internal partitioning column for resumable batched writes, not a business column.'
PARTITIONED BY (hadm_bucket)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_radiology_note_nlp/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.fact_radiology_note_nlp SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/fact_radiology_note_nlp/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.fact_clinician_note_nlp_v  (VARIANT projection -- NOT created here)
-- ------------------------------------------------------------------------------------------
-- The schema's Inference Fact instance: combines fact_discharge_note_nlp and
-- fact_radiology_note_nlp into one row per admission via a FULL OUTER JOIN on hadm_id, so an
-- admission with only a discharge note, only a radiology note, or both, still gets exactly
-- one row -- has_discharge_note / has_radiology_note flag which source(s) contributed.
-- Every JSON-shaped column from either source is typed VARIANT. Created and loaded by the
-- Glue 6.0 job clinician_note_nlp_variant.py (run_clinician_note_variant.py), NOT by this
-- Athena DDL: VARIANT requires Iceberg format-version 3, which Athena engine v3 cannot read
-- at all (SELECT / DESCRIBE both fail: "Iceberg format version 3 is not supported"). Query
-- it from Spark / EMR / Athena-for-Apache-Spark with variant_get(col, '$.path', 'type'). An
-- earlier, radiology-only version of this table (fact_radiology_note_nlp_v) was retired in
-- favor of this combined one -- both note types' inference now live in a single table at
-- the same grain, rather than splitting Inference Fact across two narrower instances.
-- The Spark DDL the job runs, for reference:
--
--   CREATE TABLE IF NOT EXISTS glue_catalog.mimic4_db_business.fact_clinician_note_nlp_v (
--       subject_id BIGINT, hadm_id BIGINT, admit_provider_id STRING, admit_date_key INT,
--       has_discharge_note BOOLEAN, has_radiology_note BOOLEAN,
--       tobacco_use STRING, alcohol_use STRING, obesity_level STRING, tobacco_cessation_cd STRING,
--       recognized_entities VARIANT,
--       radiology_note_count INT,
--       radiology_procedure_types VARIANT, radiology_reasons VARIANT, symptoms VARIANT,
--       disorders VARIANT, icd_codes VARIANT, procedures VARIANT,
--       findings_summary VARIANT, indication_summary VARIANT, conclusion VARIANT,
--       ner_json VARIANT,
--       created_ts TIMESTAMP, updated_ts TIMESTAMP, created_by STRING, updated_by STRING
--   ) USING iceberg
--   LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/fact_clinician_note_nlp_v/'
--   TBLPROPERTIES ('format-version' = '3');

-- ==========================================================================================
-- AGGREGATE / SUMMARY TABLES
-- ==========================================================================================
-- These are incrementally refreshed rollups, not full-reload tables like the dims/facts
-- above. Each refresh job tracks a watermark in etl_control (max source updated_ts already
-- processed), recomputes only the calendar days touched by new/changed source rows since
-- that watermark, and writes via a partition-scoped Iceberg overwrite (overwritePartitions()
-- on a result set containing only the touched date_key values) -- so a refresh run's cost
-- scales with how many CALENDAR DAYS changed, not with total fact-table row volume. This is
-- the mechanism intended to survive a ~3000x production data volume increase, where the
-- current full-reload pattern used by every dim/fact job above would not.
--
-- The 4 note-NLP jobs use the SAME etl_control watermark (see the etl_control comment
-- below): a re-run after new notes land reprocesses only the admissions with a new note
-- and MERGEs them, instead of re-running the model over every note. --refresh-mode full
-- forces a backfill.
-- ------------------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.etl_control
-- Description: Incremental-refresh watermark per TARGET table -- the max source timestamp
--   already processed by that target's last successful refresh. One row per target, keyed
--   by aggregate_table (the column name is historical; it holds any target-table name).
--   Used by:
--     * the 6 aggregate/OBT jobs (etl/create_agg_visual_etl_jobs.py) -- watermark on the
--       source fact's updated_ts, recompute touched date_key days.
--     * the 2 note-NLP jobs (medspacy_nlp.py, radiology_nlp.py) and the combined VARIANT
--       projection (clinician_note_nlp_variant.py) -- watermark on the source note's
--       COALESCE(storetime, charttime) (or the two STRING tables' updated_ts for the
--       projection), reprocess only admissions with a newer note, MERGE by
--       (subject_id, hadm_id). Keys: fact_discharge_note_nlp, fact_radiology_note_nlp,
--       fact_clinician_note_nlp_v.
--   No seeding needed: a missing row means "first run" -> full backfill, which then writes
--   the row. --refresh-mode full forces a rebuild that still advances the watermark.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.etl_control (
    aggregate_table STRING,
    last_processed_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'Incremental-refresh watermark per target table (aggregates/OBTs + note-NLP jobs) -- max source timestamp already processed.'
PARTITIONED BY (aggregate_table)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/etl_control/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.etl_control SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/etl_control/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.etl_process_log
-- Description: Append-only job-timing log, one row per finished Glue job run (dim, fact,
--   agg/OBT, notes-ingest, or NLP) -- for analyzing pipeline timings after the fact (the
--   P5/P6/P7 propositions). Unlike etl_control (one current watermark row per target,
--   overwritten each run), every job run appends a NEW row here, so a full run history
--   accumulates across however many times the pipeline has been run.
--
--   Written by run_lakehouse_pipeline.py: after each Glue job it starts reaches a terminal
--   state (SUCCEEDED/FAILED/STOPPED/TIMEOUT/ERROR), it inserts one row via Athena, sourced
--   directly from that job run's own Glue metadata (get_job_run) -- start_ts/end_ts are
--   Glue's StartedOn/CompletedOn, execution_seconds is Glue's own ExecutionTime (actual
--   DPU execution time, excludes queue/startup), dpu_seconds is Glue's billed DPUSeconds.
--   process_name/process_type are parsed from the job name (e.g.
--   "fact-load-fact_admission-full" -> process_type "fact", process_name "fact_admission").
--   A logging failure never aborts the pipeline (see _log_process_run's docstring) --
--   query completeness is best-effort, not a correctness guarantee for the pipeline itself.
--   Pass --no-process-log to run_lakehouse_pipeline.py to disable.
--
--   full_run_id: one number shared by every row from the same `run_lakehouse_pipeline.py`
--   invocation (a full end-to-end run, or a --from <PHASE> resume of one) -- computed once
--   per invocation as MAX(full_run_id) + 1 over this table's existing rows (0/absent-table
--   -> 1). Lets a query group "every job in run N" without relying on wall-clock time
--   windows. NOT safe against two pipeline invocations started concurrently against the
--   same gold database (both would read the same MAX and collide) -- not a concern for the
--   single-operator way this pipeline is run today; would need a real sequence/lock to be.
--
--   raw_row_count / gold_row_count: COUNT(*) of the job's source raw table and destination
--   gold table respectively, taken via Athena right after the job's own row is logged.
--   NULL where not applicable: agg/OBT jobs read another GOLD table, not a raw one, so
--   raw_row_count is always NULL for process_type agg/obt; dim_date_seed has no source
--   table at all (raw_row_count NULL); notes_ingest writes to the RAW layer, not gold
--   (gold_row_count NULL; raw_row_count is discharge_note_raw + radiology_note_raw
--   combined, since the one job populates both). A count failure (e.g. the table doesn't
--   exist yet) leaves that column NULL rather than raising -- same best-effort principle
--   as the rest of this table.
--
--   Schema note: if mimic4_db_business.etl_process_log already exists from before these
--   three columns were added, CREATE TABLE IF NOT EXISTS below will NOT add them -- run
--   once, by hand:
--     ALTER TABLE mimic4_db_business.etl_process_log ADD COLUMNS (
--       full_run_id BIGINT, raw_row_count BIGINT, gold_row_count BIGINT)
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.etl_process_log (
    full_run_id BIGINT,
    run_id STRING,
    job_name STRING,
    process_name STRING,
    process_type STRING,
    phase STRING,
    dataset STRING,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    execution_seconds DOUBLE,
    dpu_seconds DOUBLE,
    raw_row_count BIGINT,
    gold_row_count BIGINT,
    status STRING,
    error_message STRING,
    logged_ts TIMESTAMP
)
COMMENT 'Append-only ETL job timing log (one row per finished Glue job run: full_run_id, start/end/execution/DPU seconds, raw/gold row counts, status) for post-hoc pipeline timing analysis.'
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/etl_process_log/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.etl_process_log SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/etl_process_log/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.agg_admission_daily
-- Description: Daily admission/discharge/mortality counts per admission_type, rolled up
--   from fact_admission. One row per (date_key, admission_type) -- a given calendar day
--   can be an admit-day for one admission_type slice and a discharge-day for another, so
--   counts are not blended across the three date roles.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.agg_admission_daily (
    date_key INT,
    date_bucket INT,
    admission_type STRING,
    admit_count INT,
    discharge_count INT,
    death_count INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'Daily admission/discharge/mortality counts per admission_type, incrementally rolled up from fact_admission. date_bucket = date_key % 64 (materialized, not the Iceberg bucket() transform -- glue_catalog SparkCatalog here does not implement the Iceberg function catalog, so system.bucket() cannot be called from Spark to compute bucket membership for the incremental refresh carry-forward step; see etl/create_agg_visual_etl_jobs.py). Query-time date_key lookups no longer get partition pruning on their own -- add date_bucket = date_key % 64 to a WHERE clause to get it back, the same trade-off already made for fact_admission bucket(4, hadm_id). NOTE: do not put an apostrophe (an escaped quote) in this comment -- Athenas Iceberg CREATE TABLE parser silently breaks (mismatched input on the next clause keyword) when a COMMENT string contains one, regardless of what follows it.'
PARTITIONED BY (date_bucket)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/agg_admission_daily/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.agg_admission_daily SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/agg_admission_daily/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.agg_icu_fluid_balance_daily
-- Description: Daily net ICU fluid balance per stay_id -- total_intake_ml (from
--   fact_input_event.amount, day-bucketed by start_date_key) minus total_output_ml (from
--   fact_output_event.value, day-bucketed by chart_date_key). fact_ingredient_event is
--   deliberately NOT included: it's a child grain of fact_input_event (ingredient-level
--   decomposition of the same administered volume, e.g. components of a mixed IV
--   solution), so summing it alongside fact_input_event would double-count intake.
--   amount_uom on fact_input_event is NOT consistently a volume unit -- the same table
--   also carries medication/electrolyte doses (mg, mcg, units, mEq, mmol, grams) --
--   so only rows where amount_uom is 'ml' (case-insensitive) are summed.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.agg_icu_fluid_balance_daily (
    date_key INT,
    date_bucket INT,
    stay_id BIGINT,
    total_intake_ml DOUBLE,
    total_output_ml DOUBLE,
    net_balance_ml DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'Daily net ICU fluid balance per stay_id: total_intake_ml (fact_input_event) minus total_output_ml (fact_output_event), incrementally rolled up. date_bucket = date_key % 64 (materialized, not the Iceberg bucket() transform -- see the comment on agg_admission_daily for why). Query-time date_key lookups no longer get partition pruning on their own -- add date_bucket = date_key % 64 to a WHERE clause to get it back.'
PARTITIONED BY (date_bucket)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/agg_icu_fluid_balance_daily/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.agg_icu_fluid_balance_daily SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/agg_icu_fluid_balance_daily/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.agg_admission_monthly
-- Description: Monthly admission/discharge/mortality counts per admission_type. Rolled up
--   from agg_admission_daily, NOT from raw fact_admission -- re-aggregating a handful of
--   daily rows per month is cheap regardless of how large fact_admission grows, which is
--   the whole point of building a daily aggregate first: monthly (and any coarser grain)
--   rolls up from it instead of rescanning full fact-table history.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.agg_admission_monthly (
    year_month INT,
    admission_type STRING,
    admit_count INT,
    discharge_count INT,
    death_count INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'Monthly admission/discharge/mortality counts per admission_type, rolled up from agg_admission_daily.'
PARTITIONED BY (year_month)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/agg_admission_monthly/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.agg_admission_monthly SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/agg_admission_monthly/'
);

-- ==========================================================================================
-- ONE BIG TABLE (OBT) / ANALYTICS BASE TABLE (ABT)
-- ==========================================================================================
-- Denormalized, wide feature tables at a specific analytical grain -- pre-joined across many
-- fact/dim tables so a BI tool or ML training job needs no joins at query time. Refreshed
-- with the same incremental watermark/partition-scoped-overwrite mechanics as the aggregate
-- tables above (see etl_control), with one added subtlety: an OBT's row grain (e.g. hadm_id)
-- is finer than its partition (e.g. admit_year_month), so a refresh must recompute EVERY row
-- sharing a touched partition, not just the rows directly touched by a source-table change --
-- otherwise overwritePartitions() would silently drop the untouched rows sharing that
-- partition. See etl/create_agg_visual_etl_jobs.py's OBT_ADMISSION_FEATURES_CODE.
-- ------------------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.obt_admission_features
-- Description: One row per hadm_id -- denormalized admission-level feature table for
--   readmission/mortality-prediction style analysis. Combines fact_admission (demographics
--   via dim_patient join, admission details, calculated LOS/age/readmission columns) with
--   rollup counts/flags from every other admission-scoped fact table: diagnosis/procedure
--   counts and the primary (seq_num=1) diagnosis code, max DRG severity/mortality, ICU stay
--   count and total ICU LOS, lab volume and abnormal-result rate, medication administration
--   volume, positive microbiology culture count, and ward transfer count.
-- Source tables: fact_admission, dim_patient, fact_diagnosis, fact_procedure,
--   fact_drg_assignment, fact_icu_stay_accumulating, fact_lab_result,
--   fact_medication_administration, fact_microbiology_result, fact_transfer.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.obt_admission_features (
    hadm_id BIGINT,
    subject_id BIGINT,
    admit_provider_id STRING,
    admit_year_month INT,
    admit_date_key INT,
    admit_time TIMESTAMP,
    disch_time TIMESTAMP,
    death_time TIMESTAMP,
    admission_type STRING,
    admission_location STRING,
    discharge_location STRING,
    insurance STRING,
    language STRING,
    marital_status STRING,
    race STRING,
    hospital_expire_flag INT,
    hospital_los_hours DOUBLE,
    ed_los_minutes DOUBLE,
    time_to_death_hours DOUBLE,
    age_at_admission INT,
    is_readmission_flag BOOLEAN,
    days_since_prior_discharge INT,
    gender STRING,
    anchor_year_group STRING,
    dod DATE,
    diagnosis_count INT,
    primary_icd_code STRING,
    procedure_count INT,
    max_drg_severity INT,
    max_drg_mortality INT,
    icu_stay_count INT,
    total_icu_los_days DOUBLE,
    had_icu_stay BOOLEAN,
    lab_count INT,
    abnormal_lab_count INT,
    abnormal_lab_rate DOUBLE,
    med_admin_count INT,
    positive_culture_count INT,
    transfer_count INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'One row per hadm_id -- denormalized admission-level feature table (demographics, admission details, and rollup counts/flags from every contributing fact table) for readmission/mortality-prediction style analysis.'
PARTITIONED BY (admit_year_month)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/obt_admission_features/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.obt_admission_features SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/obt_admission_features/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.obt_patient_360
-- Description: One row per subject_id -- patient-level 360 view rolling up admission counts
--   and flags across every admission for that patient. Rolls up from obt_admission_features
--   (not raw fact tables) for every admission-scoped metric that's a simple sum/count/max,
--   the same "coarser rolls up from finer" principle agg_admission_monthly already uses --
--   except distinct_diagnosis_count, which needs fact_diagnosis directly, since a distinct
--   code count across admissions cannot be derived from obt_admission_features's per-
--   admission counts alone.
-- Partitioned by anchor_year_group (not subject_id): identity-partitioning by subject_id
--   would create one partition per patient (unbounded growth at production scale), and
--   Iceberg's bucket() hash transform is not usable for a partition-scoped incremental
--   refresh in this environment (its system.bucket() function isn't reachable from Spark
--   here -- see etl/create_fact_visual_etl_jobs.py's PARTITION_SORT history).
--   anchor_year_group is MIMIC-IV's fixed de-identification cohort grouping (a handful of
--   values, e.g. '2011 - 2013', regardless of how many patients exist), so it partitions
--   cleanly without that problem.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.obt_patient_360 (
    subject_id BIGINT,
    gender STRING,
    anchor_age INT,
    anchor_year INT,
    anchor_year_group STRING,
    dod DATE,
    total_admission_count INT,
    first_admit_time TIMESTAMP,
    most_recent_admit_time TIMESTAMP,
    most_recent_disch_time TIMESTAMP,
    ever_expired_in_hospital BOOLEAN,
    total_readmission_count INT,
    total_diagnosis_count INT,
    distinct_diagnosis_count INT,
    total_procedure_count INT,
    total_icu_stay_count INT,
    total_icu_los_days DOUBLE,
    total_lab_count INT,
    total_abnormal_lab_count INT,
    overall_abnormal_lab_rate DOUBLE,
    total_med_admin_count INT,
    total_positive_culture_count INT,
    total_transfer_count INT,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'One row per subject_id -- patient-level 360 view rolling up admission counts/flags across all admissions for that patient, from obt_admission_features.'
PARTITIONED BY (anchor_year_group)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/obt_patient_360/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.obt_patient_360 SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/obt_patient_360/'
);

-- ------------------------------------------------------------------------------------------
-- Table: mimic4_db_business.obt_icu_stay_features
-- Description: One row per stay_id -- ICU-stay-level feature table for LOS/severity models.
--   Combines fact_icu_stay_accumulating (care unit, LOS, calculated columns) with:
--     - age_at_admission, gender, hospital_expire_flag joined from obt_admission_features
--       via hadm_id (not re-derived -- one ICU stay belongs to exactly one admission, so
--       this is a plain many-to-one left join, no fan-out risk).
--     - total_intake_ml/total_output_ml summed from agg_icu_fluid_balance_daily (not raw
--       fact_input_event/fact_output_event) -- same "coarser rolls up from finer" principle
--       as agg_admission_monthly and obt_patient_360.
--     - chart observation volume/abnormal-rate, datetime-event volume, and procedure-event
--       volume/duration rolled up directly from their fact tables (fact_chart_observation,
--       fact_datetime_event, fact_procedure_event) -- these have no coarser precomputed
--       rollup to source from yet.
-- Partitioned by in_year_month (derived from in_date_key), not stay_id: same reasoning as
--   obt_admission_features's admit_year_month and obt_patient_360's anchor_year_group --
--   identity-partitioning by the grain column itself would create unbounded partition
--   growth at production scale, and Iceberg's bucket() transform isn't usable here.
-- ------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mimic4_db_business.obt_icu_stay_features (
    stay_id BIGINT,
    subject_id BIGINT,
    hadm_id BIGINT,
    first_careunit STRING,
    last_careunit STRING,
    in_year_month INT,
    in_date_key INT,
    in_time TIMESTAMP,
    out_time TIMESTAMP,
    los DOUBLE,
    time_to_icu_hours DOUBLE,
    is_icu_readmission_flag BOOLEAN,
    age_at_admission INT,
    gender STRING,
    hospital_expire_flag INT,
    total_intake_ml DOUBLE,
    total_output_ml DOUBLE,
    net_fluid_balance_ml DOUBLE,
    chart_observation_count INT,
    abnormal_chart_count INT,
    abnormal_chart_rate DOUBLE,
    datetime_event_count INT,
    procedure_event_count INT,
    total_procedure_duration_minutes DOUBLE,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP
)
COMMENT 'One row per stay_id -- ICU-stay-level feature table (care unit, LOS, fluid balance, vitals/procedure volume, admission context) for LOS/severity models.'
PARTITIONED BY (in_year_month)
LOCATION 's3://mimic4-lakehouse-v3-2/mimic_bus/obt_icu_stay_features/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);

ALTER TABLE mimic4_db_business.obt_icu_stay_features SET TBLPROPERTIES (
  'write.data.path'='s3://mimic4-lakehouse-v3-2/mimic_bus/obt_icu_stay_features/'
);