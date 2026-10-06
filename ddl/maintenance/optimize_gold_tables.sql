-- ============================================================================
-- Iceberg compaction for every table in mimic4_db_business
-- ============================================================================
-- OPTIMIZE ... REWRITE DATA USING BIN_PACK rewrites the current snapshot's data
-- files into fewer, larger files. It does NOT delete the old files -- run VACUUM
-- afterwards (see the bottom of this file) to expire snapshots and reclaim the
-- physical S3 bytes.
--
-- Athena only runs ONE statement per query. Run each line individually in the
-- console, or use `python compact_gold_tables.py` which executes them in order
-- with timing, error handling, and a JSON log.
--
-- PARTITIONED TABLES NEED SEVERAL RUNS. Athena OPTIMIZE processes at most 100
-- partitions per invocation and returns ICEBERG_OPTIMIZE_MORE_RUNS_NEEDED when
-- more remain -- re-run the same statement until it succeeds. At this project's
-- fragmentation level the day()/bucket()-partitioned event facts
-- (fact_chart_observation, fact_medication_administration, fact_input_event,
-- fact_prescription, fact_pharmacy_order, fact_ingredient_event,
-- fact_datetime_event, fact_output_event, fact_microbiology_result,
-- fact_procedure_event, fact_procedure, fact_transfer, fact_outpatient_measurement)
-- take multiple rounds. `compact_gold_tables.py` loops automatically
-- (--max-optimize-rounds, default 30).
--
-- Database name: mimic4_db_business
--
-- fact_clinician_note_nlp_v is Iceberg format-version 3 -- Athena engine v3
-- cannot OPTIMIZE it; compact it from a Glue 6.0 / Spark job instead, e.g.
-- `python run_compact_variant_table.py --tables fact_clinician_note_nlp_v --run-now`
-- (or CALL glue_catalog.system.rewrite_data_files('mimic4_db_business.fact_clinician_note_nlp_v') directly).
-- ============================================================================

OPTIMIZE mimic4_db_business.agg_admission_daily REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.agg_admission_monthly REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.agg_icu_fluid_balance_daily REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_caregiver REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_chart_item REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_date REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_diagnosis REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_hcpcs REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_lab_item REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_patient REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_procedure REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.dim_provider REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.etl_control REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.etl_process_log REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_admission REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_chart_observation REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_datetime_event REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_diagnosis REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_drg_assignment REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_hcpcs_event REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_icu_stay_accumulating REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_ingredient_event REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_input_event REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_lab_result REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_medication_administration REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_medication_administration_mini_detail REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_microbiology_result REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_discharge_note REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_discharge_note_nlp REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_outpatient_measurement REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_output_event REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_pharmacy_order REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_prescription REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_procedure REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_procedure_event REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_provider_order REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_provider_order_mini_detail REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_radiology_note REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_radiology_note_nlp REWRITE DATA USING BIN_PACK;
-- fact_clinician_note_nlp_v: Iceberg format-version 3 -- OPTIMIZE from Spark/Glue 6.0, not Athena.
OPTIMIZE mimic4_db_business.fact_service_assignment REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.fact_transfer REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.obt_admission_features REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.obt_icu_stay_features REWRITE DATA USING BIN_PACK;
OPTIMIZE mimic4_db_business.obt_patient_360 REWRITE DATA USING BIN_PACK;

-- ----------------------------------------------------------------------------
-- Reclaim physical S3 bytes: expire snapshots + remove orphan files.
-- VACUUM honors the table properties vacuum_max_snapshot_age_seconds (default
-- 432000 = 5 days) and vacuum_min_snapshots_to_keep (default 1). A VACUUM run
-- immediately after OPTIMIZE will NOT remove the just-superseded files because
-- their snapshot is < 5 days old. To reclaim them now, lower the threshold
-- first (per table), then VACUUM, then restore it:
--
--   ALTER TABLE mimic4_db_business.<table> SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '300');
--   VACUUM mimic4_db_business.<table>;
--   ALTER TABLE mimic4_db_business.<table> SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds' = '432000');
--
-- `compact_gold_tables.py --vacuum --vacuum-max-age 300` does this for every table.
--
-- KNOWN LIMITATION (verified 2026-09-08): Athena VACUUM reliably EXPIRES old
-- snapshots (afterwards "<table>$snapshots" shows a single row) but does NOT
-- physically delete the data files those expired snapshots referenced -- they
-- become true orphans that back no snapshot. After a full OPTIMIZE+VACUUM pass
-- the Gold LIVE snapshot dropped 693 MiB -> 81.5 MiB but PHYSICAL S3 stayed
-- ~1.47 GiB. To reclaim the physical bytes, run a Spark orphan-file sweep from a
-- Glue 6.0 job:
--   CALL glue_catalog.system.remove_orphan_files(
--        table => 'mimic4_db_business.<table>', older_than => TIMESTAMP '<now>')
-- (Glue 6.0 Spark is also how fact_clinician_note_nlp_v, format-v3, is compacted --
--  see compact_variant_table.py, which runs rewrite_data_files + expire_snapshots +
--  remove_orphan_files in one job, since Athena can perform none of the three on it.)
-- ----------------------------------------------------------------------------
