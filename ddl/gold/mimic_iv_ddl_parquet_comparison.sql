-- ============================================================================================
-- Plain-Parquet comparison DDL -- one table per Multimodal Fusion Schema archetype
-- ============================================================================================
-- Purpose: supports the added research question "How does the storage taken by Iceberg
-- compare to the storage taken by plain Parquet table format for the Multimodal Fusion
-- Schema?" Each table below is a plain Hive/Parquet EXTERNAL TABLE (no Iceberg metadata,
-- no snapshots, no manifests) holding the IDENTICAL column list as its Iceberg counterpart
-- in ddl/gold/mimic_iv_ddl_gold_combined_v4.sql, so a physical-byte comparison between the
-- two isolates the table-format variable rather than conflating it with a schema difference.
--
-- One representative table per archetype, matching the mapping already established for the
-- paper (paper6_multimodal_fusion_schema.tex, Table II):
--   fact_admission              Source Data Fact
--   obt_admission_features      Computed Structured Fact
--   fact_discharge_note         Unstructured Data Fact
--   fact_radiology_note         Unstructured Data Fact
--   fact_discharge_note_nlp     Unstructured Features Fact
--   fact_radiology_note_nlp     Unstructured Features Fact
--   fact_clinician_note_nlp_v   Inference Fact (combines discharge + radiology inference
--                               into one VARIANT-typed table; retired the earlier
--                               radiology-only fact_radiology_note_nlp_v)
--
-- All seven now share the SAME grain as fact_admission (hadm_id, subject_id,
-- admit_provider_id, admit_date_key) -- admit_provider_id/admit_date_key were added to
-- every one of these *_parquet tables to mirror the identical grain-consistency columns
-- added to their Iceberg counterparts (ddl/gold/mimic_iv_ddl_gold_combined_v4.sql).
--
-- Supports the paper's RQ1: "To what extent, if any, does the Apache Iceberg open table
-- format yield measurable storage efficiency relative to a raw (CSV) table format and the
-- Apache Parquet table format, when all three are populated with the same multimodal fact
-- archetypes at an identical grain?" See the repo README's "Research paper: RQ1 and RQ2".
--
-- Table names are suffixed _parquet to distinguish them from the Iceberg originals while
-- sharing the same Gold database (mimic4_db_business), so both formats can be queried and
-- measured side by side without a second database or crawler.
--
-- DESIGN DECISIONS (read before running measure_storage_footprint.py against these tables):
--
-- 1. UNPARTITIONED BY DESIGN. The Iceberg originals use bucket(N, key) hash partitioning
--    (fact_admission, etc.) or a materialized bucket column (fact_discharge_note_nlp,
--    fact_radiology_note_nlp) to avoid the Iceberg overwritePartitions() OOM failure
--    documented in the Iceberg DDL. bucket() is an Iceberg-specific partition transform
--    with no Hive/plain-Parquet equivalent, and Hive-style directory partitioning is itself
--    a second variable (partition-file-count overhead applies to Iceberg and Parquet
--    differently). Every table below is therefore left UNPARTITIONED, so the comparison
--    measures table-format overhead (Iceberg manifests/manifest-lists/snapshot metadata
--    vs. none) alone. If a follow-up study specifically wants to test partitioning x
--    format interaction, add PARTITIONED BY / repartition each side identically first.
--
-- 2. fact_clinician_note_nlp_v has no Parquet-representable equivalent for its defining
--    feature. The Iceberg original types every JSON-shaped column as VARIANT (Iceberg
--    format-version 3, Spark-only -- see the Iceberg DDL's comment block). Plain Parquet /
--    Hive has no VARIANT type at all, so fact_clinician_note_nlp_v_parquet below types
--    those columns STRING instead. This means the Iceberg-vs-Parquet byte comparison for
--    the Inference Fact archetype is Iceberg-VARIANT vs Parquet-STRING, not a like-for-like
--    encoding of the same type -- report it as such: it measures the storage cost of
--    gaining native semi-structured typing, not just a format switch.
--
-- 3. fact_discharge_note_parquet / fact_radiology_note_parquet are real copies (new S3
--    locations under parquet_compare/, populated by parquet_comparison_load.py), NOT a
--    reuse of the existing raw note locations -- unlike the discharge_note_raw /
--    radiology_note_raw tables this replaced. Now that fact_discharge_note /
--    fact_radiology_note give the Unstructured Data Fact archetype a real Iceberg instance
--    at admission grain, the like-for-like Parquet comparison point is that admission-grain
--    rollup, not the note-id-grain raw table (a different grain would confound the format
--    comparison with a row-count/row-width difference).
--
-- Apply with the same tooling as the Iceberg DDL (apply_gold_ddl in
-- src/mimic_lakehouse/aws_workflow.py), which substitutes the literal bucket placeholder
-- below for the active --dataset's actual Gold bucket.
-- ============================================================================================


-- ------------------------------------------------------------------------------------------
-- Source Data Fact -> fact_admission_parquet
-- Identical columns to mimic4_db_business.fact_admission (ddl/gold/mimic_iv_ddl_gold_combined_v4.sql).
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_admission_parquet` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admit_date_key` int,
  `disch_date_key` int,
  `death_date_key` int,
  `ed_reg_date_key` int,
  `ed_out_date_key` int,
  `admit_time` timestamp,
  `disch_time` timestamp,
  `death_time` timestamp,
  `admission_type` string,
  `admit_provider_id` string,
  `admission_location` string,
  `discharge_location` string,
  `insurance` string,
  `language` string,
  `marital_status` string,
  `race` string,
  `ed_reg_time` timestamp,
  `ed_out_time` timestamp,
  `hospital_expire_flag` int,
  `hospital_los_hours` double,
  `ed_los_minutes` double,
  `time_to_death_hours` double,
  `age_at_admission` int,
  `is_readmission_flag` boolean,
  `days_since_prior_discharge` int,
  `created_ts` timestamp,
  `updated_ts` timestamp,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/fact_admission_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- ------------------------------------------------------------------------------------------
-- Computed Structured Fact -> obt_admission_features_parquet
-- Identical columns to mimic4_db_business.obt_admission_features.
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`obt_admission_features_parquet` (
  `hadm_id` bigint,
  `subject_id` bigint,
  `admit_provider_id` string,
  `admit_year_month` int,
  `admit_date_key` int,
  `admit_time` timestamp,
  `disch_time` timestamp,
  `death_time` timestamp,
  `admission_type` string,
  `admission_location` string,
  `discharge_location` string,
  `insurance` string,
  `language` string,
  `marital_status` string,
  `race` string,
  `hospital_expire_flag` int,
  `hospital_los_hours` double,
  `ed_los_minutes` double,
  `time_to_death_hours` double,
  `age_at_admission` int,
  `is_readmission_flag` boolean,
  `days_since_prior_discharge` int,
  `gender` string,
  `anchor_year_group` string,
  `dod` date,
  `diagnosis_count` int,
  `primary_icd_code` string,
  `procedure_count` int,
  `max_drg_severity` int,
  `max_drg_mortality` int,
  `icu_stay_count` int,
  `total_icu_los_days` double,
  `had_icu_stay` boolean,
  `lab_count` int,
  `abnormal_lab_count` int,
  `abnormal_lab_rate` double,
  `med_admin_count` int,
  `positive_culture_count` int,
  `transfer_count` int,
  `created_ts` timestamp,
  `updated_ts` timestamp
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/obt_admission_features_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- ------------------------------------------------------------------------------------------
-- Unstructured Data Fact -> fact_discharge_note_parquet
-- Identical columns to mimic4_db_business.fact_discharge_note (admission grain -- see
-- design decision 3 above for why this replaced discharge_note_raw_parquet).
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_discharge_note_parquet` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admit_provider_id` string,
  `admit_date_key` int,
  `text` string,
  `notes_count` int,
  `last_charttime` timestamp,
  `last_storetime` timestamp,
  `created_ts` timestamp,
  `updated_ts` timestamp,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/fact_discharge_note_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- ------------------------------------------------------------------------------------------
-- Unstructured Data Fact -> fact_radiology_note_parquet
-- Identical columns to mimic4_db_business.fact_radiology_note (admission grain -- see
-- design decision 3 above for why this replaced radiology_note_raw_parquet).
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_radiology_note_parquet` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admit_provider_id` string,
  `admit_date_key` int,
  `text` string,
  `notes_count` int,
  `last_charttime` timestamp,
  `last_storetime` timestamp,
  `created_ts` timestamp,
  `updated_ts` timestamp,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/fact_radiology_note_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- ------------------------------------------------------------------------------------------
-- Unstructured Features Fact -> fact_discharge_note_nlp_parquet
-- Identical columns to mimic4_db_business.fact_discharge_note_nlp, including hadm_bucket
-- (kept as an ordinary column for row-width parity; not used as a partition key here --
-- see design decision 1).
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_discharge_note_nlp_parquet` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admit_provider_id` string,
  `admit_date_key` int,
  `hadm_bucket` int,
  `tobacco_use` string,
  `alcohol_use` string,
  `obesity_level` string,
  `tobacco_cessation_cd` string,
  `recognized_entities` string,
  `created_ts` timestamp,
  `updated_ts` timestamp,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/fact_discharge_note_nlp_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- ------------------------------------------------------------------------------------------
-- Unstructured Features Fact -> fact_radiology_note_nlp_parquet
-- Identical columns to mimic4_db_business.fact_radiology_note_nlp (all JSON payloads
-- remain STRING, same as the Iceberg original -- this table was never VARIANT-typed).
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_radiology_note_nlp_parquet` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admit_provider_id` string,
  `admit_date_key` int,
  `hadm_bucket` int,
  `note_count` int,
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
  `created_ts` timestamp,
  `updated_ts` timestamp,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/fact_radiology_note_nlp_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- ------------------------------------------------------------------------------------------
-- Inference Fact -> fact_clinician_note_nlp_v_parquet
-- Same row shape as fact_clinician_note_nlp_v's Spark/Iceberg DDL (see the commented block
-- in ddl/gold/mimic_iv_ddl_gold_combined_v4.sql), with every VARIANT column typed STRING
-- instead -- see design decision 2 above (same STRING-vs-VARIANT caveat).
-- ------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_business`.`fact_clinician_note_nlp_v_parquet` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admit_provider_id` string,
  `admit_date_key` int,
  `has_discharge_note` boolean,
  `has_radiology_note` boolean,
  `tobacco_use` string,
  `alcohol_use` string,
  `obesity_level` string,
  `tobacco_cessation_cd` string,
  `recognized_entities` string,
  `radiology_note_count` int,
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
  `created_ts` timestamp,
  `updated_ts` timestamp,
  `created_by` string,
  `updated_by` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-lakehouse-v3-2/parquet_compare/fact_clinician_note_nlp_v_parquet/'
TBLPROPERTIES ('parquet.compression'='ZSTD');
