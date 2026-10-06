-- ============================================================================================
-- Plain-CSV comparison DDL -- one table per Multimodal Fusion Schema archetype
-- ============================================================================================
-- Purpose: extends the Iceberg-vs-Parquet storage research question with a fourth table
-- format/codec combination: "How does the storage taken by Iceberg (ZSTD) / Parquet (ZSTD
-- and Snappy) compare to gzip-compressed plain CSV for the Multimodal Fusion Schema?" Same
-- seven archetype-representative tables, same grain (hadm_id, subject_id,
-- admit_provider_id, admit_date_key) as their Iceberg (mimic_iv_ddl_gold_combined_v4.sql)
-- and Parquet (mimic_iv_ddl_parquet_comparison.sql / mimic_iv_ddl_parquet_snappy_comparison.sql)
-- counterparts.
--
--   fact_admission              Source Data Fact
--   obt_admission_features      Computed Structured Fact
--   fact_discharge_note         Unstructured Data Fact
--   fact_radiology_note         Unstructured Data Fact
--   fact_discharge_note_nlp     Unstructured Features Fact
--   fact_radiology_note_nlp     Unstructured Features Fact
--   fact_clinician_note_nlp_v   Inference Fact (combines discharge + radiology inference;
--                               retired the earlier radiology-only fact_radiology_note_nlp_v)
--
-- Table names are suffixed _csv, same Gold database (mimic4_db_business) as the Iceberg
-- and Parquet siblings.
--
-- Supports the paper's RQ1 (see mimic_iv_ddl_parquet_comparison.sql's header for the full
-- research question, and the repo README's "Research paper: RQ1 and RQ2").
--
-- DESIGN DECISIONS (read before running measure_parquet_comparison.py against these tables):
--
-- 1. EVERY COLUMN IS DECLARED STRING. Athena's OpenCSVSerDe (the only Hive/Athena SerDe
--    that gives RFC4180-correct comma/quote escaping -- the plain LazySimpleSerDe used for
--    this repo's other CSV-shaped raw tables does not quote-escape at all, which is exactly
--    the limitation that forced MIMIC-IV-Note's discharge/radiology notes to be ingested as
--    Parquet instead of CSV in the first place, see notes_ingest.py) recognizes only STRING
--    columns: Athena rejects any other declared type against OpenCSVSerDe. This means the
--    CSV leg of the comparison is intentionally not just a format switch -- every BIGINT,
--    INT, DOUBLE, TIMESTAMP and BOOLEAN column is also a string-encoded value here, so its
--    byte footprint is not directly comparable to the same column's Iceberg/Parquet native
--    binary encoding on a per-column basis. Report table-level totals; a per-column
--    breakdown would conflate the format difference with the encoding difference.
--
-- 2. EMBEDDED NEWLINES ARE ESCAPED BEFORE WRITE, NOT LEFT RAW. Hive/Presto/Athena's
--    TextInputFormat splits CSV records on physical newline bytes BEFORE the SerDe ever
--    parses a line -- RFC4180 quote-escaping (which OpenCSVSerDe otherwise handles
--    correctly for embedded commas/quotes) does not help here, because the line boundary
--    is decided upstream of the SerDe. fact_discharge_note_csv / fact_radiology_note_csv's
--    `text` column (concatenated note text, containing real embedded newlines) would
--    silently corrupt row boundaries if written as-is. csv_comparison_load.py replaces
--    every `\n`/`\r` in `text` with the literal two-character escape `\n`/`\r` (the same
--    thing json.dumps() already does for the JSON-shaped STRING columns on the NLP tables,
--    which is why those columns need no special handling) before writing, so every row is
--    exactly one physical line. This is a necessary transform for CSV to be queryable at
--    all, not an optional cleanup -- it is the CSV-specific analogue of why this project
--    uses Parquet for notes in the canonical pipeline (see notes_ingest_pipeline.md).
--
-- 3. UNPARTITIONED, same rationale as the Parquet comparison DDL's design decision 1: no
--    Hive-style directory partitioning, so the comparison measures table-format overhead
--    alone.
--
-- Apply with the same tooling as the other two DDL files (apply_gold_ddl in
-- src/mimic_lakehouse/aws_workflow.py), which substitutes the literal bucket placeholder
-- below for the active --dataset's actual Gold bucket.
-- ============================================================================================


-- ------------------------------------------------------------------------------------------
-- Source Data Fact -> fact_admission_csv
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_admission_csv` (
  `subject_id` string,
  `hadm_id` string,
  `admit_date_key` string,
  `disch_date_key` string,
  `death_date_key` string,
  `ed_reg_date_key` string,
  `ed_out_date_key` string,
  `admit_time` string,
  `disch_time` string,
  `death_time` string,
  `admission_type` string,
  `admit_provider_id` string,
  `admission_location` string,
  `discharge_location` string,
  `insurance` string,
  `language` string,
  `marital_status` string,
  `race` string,
  `ed_reg_time` string,
  `ed_out_time` string,
  `hospital_expire_flag` string,
  `hospital_los_hours` string,
  `ed_los_minutes` string,
  `time_to_death_hours` string,
  `age_at_admission` string,
  `is_readmission_flag` string,
  `days_since_prior_discharge` string,
  `created_ts` string,
  `updated_ts` string,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/fact_admission_csv/';


-- ------------------------------------------------------------------------------------------
-- Computed Structured Fact -> obt_admission_features_csv
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`obt_admission_features_csv` (
  `hadm_id` string,
  `subject_id` string,
  `admit_provider_id` string,
  `admit_year_month` string,
  `admit_date_key` string,
  `admit_time` string,
  `disch_time` string,
  `death_time` string,
  `admission_type` string,
  `admission_location` string,
  `discharge_location` string,
  `insurance` string,
  `language` string,
  `marital_status` string,
  `race` string,
  `hospital_expire_flag` string,
  `hospital_los_hours` string,
  `ed_los_minutes` string,
  `time_to_death_hours` string,
  `age_at_admission` string,
  `is_readmission_flag` string,
  `days_since_prior_discharge` string,
  `gender` string,
  `anchor_year_group` string,
  `dod` string,
  `diagnosis_count` string,
  `primary_icd_code` string,
  `procedure_count` string,
  `max_drg_severity` string,
  `max_drg_mortality` string,
  `icu_stay_count` string,
  `total_icu_los_days` string,
  `had_icu_stay` string,
  `lab_count` string,
  `abnormal_lab_count` string,
  `abnormal_lab_rate` string,
  `med_admin_count` string,
  `positive_culture_count` string,
  `transfer_count` string,
  `created_ts` string,
  `updated_ts` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/obt_admission_features_csv/';


-- ------------------------------------------------------------------------------------------
-- Unstructured Data Fact -> fact_discharge_note_csv
-- `text` has embedded newlines escaped before write -- see design decision 2 above.
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_discharge_note_csv` (
  `subject_id` string,
  `hadm_id` string,
  `admit_provider_id` string,
  `admit_date_key` string,
  `text` string,
  `notes_count` string,
  `last_charttime` string,
  `last_storetime` string,
  `created_ts` string,
  `updated_ts` string,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/fact_discharge_note_csv/';


-- ------------------------------------------------------------------------------------------
-- Unstructured Data Fact -> fact_radiology_note_csv
-- `text` has embedded newlines escaped before write -- see design decision 2 above.
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_radiology_note_csv` (
  `subject_id` string,
  `hadm_id` string,
  `admit_provider_id` string,
  `admit_date_key` string,
  `text` string,
  `notes_count` string,
  `last_charttime` string,
  `last_storetime` string,
  `created_ts` string,
  `updated_ts` string,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/fact_radiology_note_csv/';


-- ------------------------------------------------------------------------------------------
-- Unstructured Features Fact -> fact_discharge_note_nlp_csv
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_discharge_note_nlp_csv` (
  `subject_id` string,
  `hadm_id` string,
  `admit_provider_id` string,
  `admit_date_key` string,
  `hadm_bucket` string,
  `tobacco_use` string,
  `alcohol_use` string,
  `obesity_level` string,
  `tobacco_cessation_cd` string,
  `recognized_entities` string,
  `created_ts` string,
  `updated_ts` string,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/fact_discharge_note_nlp_csv/';


-- ------------------------------------------------------------------------------------------
-- Unstructured Features Fact -> fact_radiology_note_nlp_csv
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_radiology_note_nlp_csv` (
  `subject_id` string,
  `hadm_id` string,
  `admit_provider_id` string,
  `admit_date_key` string,
  `hadm_bucket` string,
  `note_count` string,
  `radiology_procedure_types` string,
  `radiology_reasons` string,
  `symptoms` string,
  `disorders` string,
  `icd_codes` string,
  `procedures` string,
  `findings_summary` string,
  `indication_summary` string,
  `conclusion` string,
  `ner_json` string,
  `created_ts` string,
  `updated_ts` string,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/fact_radiology_note_nlp_csv/';


-- ------------------------------------------------------------------------------------------
-- Inference Fact -> fact_clinician_note_nlp_v_csv
-- Same row shape as fact_clinician_note_nlp_v (VARIANT columns flattened to string) -- see
-- ddl/gold/mimic_iv_ddl_gold_combined_v4.sql's fact_clinician_note_nlp_v reference block.
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_clinician_note_nlp_v_csv` (
  `subject_id` string,
  `hadm_id` string,
  `admit_provider_id` string,
  `admit_date_key` string,
  `has_discharge_note` string,
  `has_radiology_note` string,
  `tobacco_use` string,
  `alcohol_use` string,
  `obesity_level` string,
  `tobacco_cessation_cd` string,
  `recognized_entities` string,
  `radiology_note_count` string,
  `radiology_procedure_types` string,
  `radiology_reasons` string,
  `symptoms` string,
  `disorders` string,
  `icd_codes` string,
  `procedures` string,
  `findings_summary` string,
  `indication_summary` string,
  `conclusion` string,
  `ner_json` string,
  `created_ts` string,
  `updated_ts` string,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://mimic4-lakehouse-v3-2/csv_compare/fact_clinician_note_nlp_v_csv/';
