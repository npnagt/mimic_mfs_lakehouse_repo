#!/usr/bin/env python3
"""
create_fact_visual_etl_jobs.py

Creates (or updates) an AWS Glue Studio VISUAL ETL job (JobMode='VISUAL' with
CodeGenConfigurationNodes) for each raw-sourced Gold fact/bridge table -- one job per
table, all generated from this single script. Each job appears in the Glue Studio
console as an editable drag-and-drop DAG:

    <raw table> (Data Catalog)
        --> ApplyMapping (rename/cast to Gold schema)
        --> Add derived columns & write to Gold (Custom Transform -- PySpark DataFrame
            API, no SQL; this node both derives audit/date_key/calculated columns AND
            performs the Iceberg write itself -- see the note below)

Mirrors etl/create_dim_visual_etl_jobs.py's pattern and every hard-won fix from it, plus
one more specific to fact tables:
  - The Custom Transform's `Code` is the function BODY ONLY (no `def` line) -- Glue
    Studio wraps it in its own `def <ClassName>(glueContext, dfc) -> DynamicFrameCollection:`
    using `ClassName`; including a `def` line here shadows the outer one and the
    job silently returns None.
  - `--conf` registers `glue_catalog` as an Iceberg SparkCatalog and sets
    `spark.sql.iceberg.handle-timestamp-without-timezone=true` (several Gold columns are
    plain TIMESTAMP, i.e. timestamp-without-timezone in Iceberg's spec).
  - Like the (now-updated) dimension jobs, this DAG has no S3IcebergCatalogTarget/
    SelectFromCollection at all -- Glue's write_data_frame.from_catalog wrapper turned
    out to silently call Iceberg's .append() regardless of additional_options={"overwrite":
    "true"} (confirmed the hard way: every dimension job that was ever re-run had
    silently appended a second copy of its data). It also cannot be made to write
    Iceberg-partitioned data reliably even when overwrite genuinely is intended (its
    `additional_options` don't forward arbitrary Iceberg write options, and the
    partition-transform SQL functions needed to manually cluster the data aren't
    reachable through it either) -- most Gold fact tables are Iceberg-partitioned
    (day(...) or bucket(...) per the DDL). The Custom Transform instead performs the
    write itself via Iceberg's native `df.writeTo(...).option("fanout-enabled", "true")
    .overwritePartitions()`, where that option is genuinely respected -- see the comment
    above build_transform_code for the full story of what was tried and ruled out first.

Mapping source of truth: mimic_iv_raw_to_gold_mapping_v2.xlsx (not distributed)
(Column Mapping sheet), cross-checked column-by-column against each raw table's LIVE
crawled schema (aws glue get-table) and the Gold DDL
(ddl/gold/mimic_iv_ddl_gold_combined_v4.sql). That cross-check caught two real bugs in
the deployed DDL itself (fact_medication_administration_mini_detail and
fact_pharmacy_order each had several text/date columns mistyped as DOUBLE/BIGINT,
confirmed against real raw CSV values) -- both were fixed in the DDL and the two tables
recreated before this script was written; see git history on
ddl/gold/mimic_iv_ddl_gold_combined_v4.sql for the corrected column list.

Every raw timestamp/date column crawls as `string` (MIMIC-IV's CSV text format), so the
`timestamp`/`date` Gold columns are produced by a plain ApplyMapping string cast, not a
separate step. `*_date_key` columns are then derived from that ALREADY-CAST sibling Gold
column (not the raw column) via the same surrogate-key formula as dim_date:
CAST(DATE_FORMAT(<col>, 'yyyyMMdd') AS INT), NULL-safe.

Scope: all 23 fact/bridge tables. 19 need only ApplyMapping + audit/date_key/calculated
columns (Phase 1). The other 4 (Phase 2) also need a cross-table lookup and/or a window
function for one of their calculated columns:
  - fact_lab_result: turned out NOT to need a join at all -- labevents_raw already
    carries ref_range_lower/ref_range_upper on every row (unlike chartevents_raw, which
    has no per-row normal range). See the comment on its FACTS entry.
  - fact_chart_observation: joins dim_chart_item (low_normal_value/high_normal_value)
    for is_abnormal_flag.
  - fact_admission: joins dim_patient (anchor_age/anchor_year) for age_at_admission;
    uses a window function (partition by subject_id, order by admit_time) for
    is_readmission_flag/days_since_prior_discharge.
  - fact_icu_stay_accumulating: joins fact_admission itself (via hadm_id) for
    time_to_icu_hours -- this means its job must be (re-)run AFTER fact_admission has
    current data, not in parallel with it. Also uses a window function (partition by
    subject_id, order by in_time) for is_icu_readmission_flag.
None of these joins use a Glue Join node -- see build_transform_code's "join" handling
and the comment there for why (Iceberg tables can't be read via the DynamicFrame-based
S3CatalogSource a Join node would need).

Prerequisites:
  1. The raw CSVs have already been crawled into Glue tables in --raw-database.
  2. The Gold Iceberg tables already exist in --gold-database (created by
     ddl/gold/mimic_iv_ddl_gold_combined_v4.sql via Athena) -- this script does not
     create tables, only the ETL jobs that load them.
  3. An IAM role Glue can assume, with AWSGlueServiceRole plus S3 read on the raw
     bucket and S3 read/write on the Gold bucket (use --create-role to have this
     script set one up; the same role is shared across every fact job).

Usage:
  python3 create_fact_visual_etl_jobs.py \\
      --create-role \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket \\
      --raw-s3-bucket mimic4-datalake-v3-2 \\
      --gold-s3-bucket mimic4-lakehouse-v3-2 \\
      --run-now

  # Only (re)create a subset of jobs:
  python3 create_fact_visual_etl_jobs.py --role-arn ... --scripts-bucket ... \\
      --only fact_transfer,fact_procedure
"""

import argparse
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mimic_lakehouse import config  # noqa: E402

# ---------------------------------------------------------------------------------------
# Per-table configuration.
#
#   mapping     -- list of (from_path, from_type, to_key, to_type) for ApplyMapping.
#                  from_type/to_type use Glue DynamicFrame type names (string, int, long,
#                  double, date, timestamp, boolean) -- NOT the Athena/Iceberg DDL type
#                  names (STRING vs string, BIGINT vs long, etc.). from_type is the LIVE
#                  crawled raw type, verified via aws glue get-table, not assumed from
#                  the raw CSV's conceptual meaning.
#   date_keys   -- list of (date_key_column, source_gold_column) -- source_gold_column is
#                  a column already produced by `mapping` above (the just-cast Gold
#                  timestamp/date column for that date role), not a raw column.
#   calculated  -- list of literal PySpark withColumn statements (strings) appended to the
#                  Custom Transform body, one per calculated/value-added column.
# ---------------------------------------------------------------------------------------
FACTS = [
    {
        "name": "fact_diagnosis",
        "raw_table": "diagnoses_icd_raw",
        "gold_table": "fact_diagnosis",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("seq_num", "long", "seq_num", "int"),
            ("icd_code", "string", "icd_code", "string"),
            ("icd_version", "long", "icd_version", "int"),
        ],
        "date_keys": [],
        "calculated": [],
    },
    {
        "name": "fact_drg_assignment",
        "raw_table": "drgcodes_raw",
        "gold_table": "fact_drg_assignment",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("drg_type", "string", "drg_type", "string"),
            ("drg_code", "long", "drg_code", "long"),
            ("description", "string", "description", "string"),
            ("drg_severity", "long", "drg_severity", "int"),
            ("drg_mortality", "long", "drg_mortality", "int"),
        ],
        "date_keys": [],
        "calculated": [],
    },
    {
        "name": "fact_medication_administration",
        "raw_table": "emar_raw",
        "gold_table": "fact_medication_administration",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("emar_id", "string", "emar_id", "string"),
            ("emar_seq", "long", "emar_seq", "int"),
            ("poe_id", "string", "poe_id", "string"),
            ("pharmacy_id", "long", "pharmacy_id", "long"),
            ("enter_provider_id", "string", "enter_provider_id", "string"),
            ("charttime", "string", "chart_time", "timestamp"),
            ("medication", "string", "medication", "string"),
            ("event_txt", "string", "event_txt", "string"),
            ("scheduletime", "string", "schedule_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_time"),
            ("schedule_date_key", "schedule_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            # admin_delay_minutes: order-to-admin gap (scheduled -> actually charted/administered)
            'df = df.withColumn("admin_delay_minutes", F.when(F.col("schedule_time").isNull() | F.col("chart_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("chart_time") - F.unix_timestamp("schedule_time")) / 60.0))',
        ],
    },
    {
        "name": "fact_medication_administration_mini_detail",
        "raw_table": "emar_detail_raw",
        "gold_table": "fact_medication_administration_mini_detail",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("emar_id", "string", "emar_id", "string"),
            ("emar_seq", "long", "emar_seq", "int"),
            ("parent_field_ordinal", "double", "parent_field_ordinal", "double"),
            ("administration_type", "string", "administration_type", "string"),
            ("pharmacy_id", "long", "pharmacy_id", "long"),
            ("barcode_type", "string", "barcode_type", "string"),
            ("reason_for_no_barcode", "string", "reason_for_no_barcode", "string"),
            ("complete_dose_not_given", "string", "complete_dose_not_given", "string"),
            ("dose_due", "string", "dose_due", "double"),
            ("dose_due_unit", "string", "dose_due_unit", "string"),
            ("dose_given", "string", "dose_given", "string"),
            ("dose_given_unit", "string", "dose_given_unit", "string"),
            ("will_remainder_of_dose_be_given", "string", "will_remainder_of_dose_be_given", "string"),
            ("product_amount_given", "double", "product_amount_given", "double"),
            ("product_unit", "string", "product_unit", "string"),
            # product_code is alphanumeric (e.g. "MVI") -- DDL originally said BIGINT, fixed to STRING.
            ("product_code", "string", "product_code", "string"),
            ("product_description", "string", "product_description", "string"),
            ("product_description_other", "string", "product_description_other", "string"),
            ("prior_infusion_rate", "double", "prior_infusion_rate", "double"),
            ("infusion_rate", "double", "infusion_rate", "double"),
            ("infusion_rate_adjustment", "string", "infusion_rate_adjustment", "string"),
            ("infusion_rate_adjustment_amount", "string", "infusion_rate_adjustment_amount", "double"),
            ("infusion_rate_unit", "string", "infusion_rate_unit", "string"),
            # route/side/site/non_formulary_visual_verification/continued_infusion_in_other_location
            # are all real text values (e.g. route="PO", side="Left"/"Right"/"Center",
            # site="abdomen", verification/location flags="Y") -- DDL originally said DOUBLE,
            # fixed to STRING (verified against real raw CSV values, not assumed).
            ("route", "string", "route", "string"),
            ("infusion_complete", "string", "infusion_complete", "string"),
            ("completion_interval", "string", "completion_interval", "string"),
            ("new_iv_bag_hung", "string", "new_iv_bag_hung", "string"),
            ("continued_infusion_in_other_location", "string", "continued_infusion_in_other_location", "string"),
            ("restart_interval", "string", "restart_interval", "string"),
            ("side", "string", "side", "string"),
            ("site", "string", "site", "string"),
            ("non_formulary_visual_verification", "string", "non_formulary_visual_verification", "string"),
        ],
        "date_keys": [],
        "calculated": [],
    },
    {
        "name": "fact_hcpcs_event",
        "raw_table": "hcpcsevents_raw",
        "gold_table": "fact_hcpcs_event",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("chartdate", "string", "chart_date", "date"),
            ("hcpcs_cd", "string", "hcpcs_cd", "string"),
            ("seq_num", "long", "seq_num", "int"),
            ("short_description", "string", "short_description", "string"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_date"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_microbiology_result",
        "raw_table": "microbiologyevents_raw",
        "gold_table": "fact_microbiology_result",
        "mapping": [
            ("microevent_id", "long", "microevent_id", "long"),
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("micro_specimen_id", "long", "micro_specimen_id", "long"),
            ("order_provider_id", "string", "order_provider_id", "string"),
            ("chartdate", "string", "chart_date", "timestamp"),
            ("charttime", "string", "chart_time", "timestamp"),
            ("spec_itemid", "long", "spec_itemid", "long"),
            ("spec_type_desc", "string", "spec_type_desc", "string"),
            ("test_seq", "long", "test_seq", "int"),
            ("storedate", "string", "store_date", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("test_itemid", "long", "test_itemid", "long"),
            ("test_name", "string", "test_name", "string"),
            ("org_itemid", "long", "org_itemid", "long"),
            ("org_name", "string", "org_name", "string"),
            ("isolate_num", "long", "isolate_num", "int"),
            ("quantity", "string", "quantity", "double"),
            ("ab_itemid", "long", "ab_itemid", "long"),
            ("ab_name", "string", "ab_name", "string"),
            ("dilution_text", "string", "dilution_text", "string"),
            ("dilution_comparison", "string", "dilution_comparison", "string"),
            ("dilution_value", "double", "dilution_value", "double"),
            ("interpretation", "string", "interpretation", "string"),
            ("comments", "string", "comments", "string"),
        ],
        # chart_date/store_date (not chart_time/store_time) are used for the date_key:
        # chartdate/storedate are date-only and always populated in MIMIC-IV, while
        # charttime/storetime are optional finer-grained timestamps -- deriving the key
        # from the *_time sibling would manufacture NULLs whenever it's absent.
        "date_keys": [
            ("chart_date_key", "chart_date"),
            ("store_date_key", "store_date"),
        ],
        "calculated": [
            'df = df.withColumn("result_turnaround_minutes", F.when(F.col("chart_time").isNull() | F.col("store_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("store_time") - F.unix_timestamp("chart_time")) / 60.0))',
            # A non-blank org_name means an organism was isolated (positive culture);
            # blank org_name is MIMIC-IV's convention for no-growth/negative results.
            # Blank CSV fields land as empty string, not NULL, even after ApplyMapping's
            # string->string pass-through -- checking isNotNull() alone misses this and
            # marks every row positive (confirmed against real data: org_name='' for
            # negative cultures, not null), so this also excludes empty/whitespace-only.
            'df = df.withColumn("is_positive_culture_flag", F.col("org_name").isNotNull() & (F.trim(F.col("org_name")) != ""))',
        ],
    },
    {
        "name": "fact_outpatient_measurement",
        "raw_table": "omr_raw",
        "gold_table": "fact_outpatient_measurement",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("chartdate", "string", "chart_date", "date"),
            ("seq_num", "long", "seq_num", "int"),
            ("result_name", "string", "result_name", "string"),
            ("result_value", "string", "result_value", "string"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_date"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_pharmacy_order",
        "raw_table": "pharmacy_raw",
        "gold_table": "fact_pharmacy_order",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("pharmacy_id", "long", "pharmacy_id", "long"),
            ("poe_id", "string", "poe_id", "string"),
            ("starttime", "string", "start_time", "timestamp"),
            ("stoptime", "string", "stop_time", "timestamp"),
            ("medication", "string", "medication", "string"),
            ("proc_type", "string", "proc_type", "string"),
            ("status", "string", "status", "string"),
            ("entertime", "string", "enter_time", "timestamp"),
            ("verifiedtime", "string", "verified_time", "timestamp"),
            ("route", "string", "route", "string"),
            ("frequency", "string", "frequency", "string"),
            ("disp_sched", "string", "disp_sched", "string"),
            # infusion_type is a category code (e.g. "R"/"B"/"C"/"N") -- DDL originally
            # said DOUBLE, fixed to STRING (verified against real raw CSV values).
            ("infusion_type", "string", "infusion_type", "string"),
            ("sliding_scale", "string", "sliding_scale", "string"),
            ("lockout_interval", "string", "lockout_interval", "double"),
            ("basal_rate", "string", "basal_rate", "double"),
            ("one_hr_max", "string", "one_hr_max", "double"),
            ("doses_per_24_hrs", "long", "doses_per_24_hrs", "double"),
            ("duration", "long", "duration", "double"),
            ("duration_interval", "string", "duration_interval", "string"),
            ("expiration_value", "long", "expiration_value", "double"),
            ("expiration_unit", "string", "expiration_unit", "string"),
            # expirationdate is a date -- DDL originally said DOUBLE, fixed to DATE.
            ("expirationdate", "string", "expiration_date", "date"),
            ("dispensation", "string", "dispensation", "string"),
            ("fill_quantity", "string", "fill_quantity", "double"),
        ],
        "date_keys": [
            ("start_date_key", "start_time"),
            ("stop_date_key", "stop_time"),
            ("enter_date_key", "enter_time"),
            ("verified_date_key", "verified_time"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_provider_order",
        "raw_table": "poe_raw",
        "gold_table": "fact_provider_order",
        "mapping": [
            ("poe_id", "string", "poe_id", "string"),
            ("poe_seq", "long", "poe_seq", "int"),
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("ordertime", "string", "order_time", "timestamp"),
            ("order_type", "string", "order_type", "string"),
            ("order_subtype", "string", "order_subtype", "string"),
            ("transaction_type", "string", "transaction_type", "string"),
            ("discontinue_of_poe_id", "string", "discontinue_of_poe_id", "string"),
            ("discontinued_by_poe_id", "string", "discontinued_by_poe_id", "string"),
            ("order_provider_id", "string", "order_provider_id", "string"),
            ("order_status", "string", "order_status", "string"),
        ],
        "date_keys": [
            ("order_date_key", "order_time"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_provider_order_mini_detail",
        "raw_table": "poe_detail_raw",
        "gold_table": "fact_provider_order_mini_detail",
        "mapping": [
            ("poe_id", "string", "poe_id", "string"),
            ("poe_seq", "long", "poe_seq", "int"),
            ("subject_id", "long", "subject_id", "long"),
            ("field_name", "string", "field_name", "string"),
            ("field_value", "string", "field_value", "string"),
        ],
        "date_keys": [],
        "calculated": [],
    },
    {
        "name": "fact_prescription",
        "raw_table": "prescriptions_raw",
        "gold_table": "fact_prescription",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("pharmacy_id", "long", "pharmacy_id", "long"),
            ("poe_id", "string", "poe_id", "string"),
            ("poe_seq", "long", "poe_seq", "int"),
            ("order_provider_id", "string", "order_provider_id", "string"),
            ("starttime", "string", "start_time", "timestamp"),
            ("stoptime", "string", "stop_time", "timestamp"),
            ("drug_type", "string", "drug_type", "string"),
            ("drug", "string", "drug", "string"),
            ("formulary_drug_cd", "string", "formulary_drug_cd", "string"),
            ("gsn", "string", "gsn", "string"),
            ("ndc", "long", "ndc", "long"),
            ("prod_strength", "string", "prod_strength", "string"),
            ("form_rx", "string", "form_rx", "string"),
            ("dose_val_rx", "string", "dose_val_rx", "string"),
            ("dose_unit_rx", "string", "dose_unit_rx", "string"),
            ("form_val_disp", "long", "form_val_disp", "double"),
            ("form_unit_disp", "string", "form_unit_disp", "string"),
            ("doses_per_24_hrs", "long", "doses_per_24_hrs", "double"),
            ("route", "string", "route", "string"),
        ],
        "date_keys": [
            ("start_date_key", "start_time"),
            ("stop_date_key", "stop_time"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_procedure",
        "raw_table": "procedures_icd_raw",
        "gold_table": "fact_procedure",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("seq_num", "long", "seq_num", "int"),
            ("chartdate", "string", "chart_date", "date"),
            ("icd_code", "string", "icd_code", "string"),
            ("icd_version", "long", "icd_version", "int"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_date"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_service_assignment",
        "raw_table": "services_raw",
        "gold_table": "fact_service_assignment",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("transfertime", "string", "transfer_time", "timestamp"),
            ("prev_service", "string", "prev_service", "string"),
            ("curr_service", "string", "curr_service", "string"),
        ],
        "date_keys": [
            ("transfer_date_key", "transfer_time"),
        ],
        "calculated": [],
    },
    {
        "name": "fact_transfer",
        "raw_table": "transfers_raw",
        "gold_table": "fact_transfer",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("transfer_id", "long", "transfer_id", "long"),
            ("eventtype", "string", "event_type", "string"),
            ("careunit", "string", "care_unit", "string"),
            ("intime", "string", "in_time", "timestamp"),
            ("outtime", "string", "out_time", "timestamp"),
        ],
        "date_keys": [
            ("in_date_key", "in_time"),
            ("out_date_key", "out_time"),
        ],
        "calculated": [
            # out_time can be NULL for a patient's current/ongoing location.
            'df = df.withColumn("transfer_duration_hours", F.when(F.col("in_time").isNull() | F.col("out_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("out_time") - F.unix_timestamp("in_time")) / 3600.0))',
        ],
    },
    {
        "name": "fact_datetime_event",
        "raw_table": "datetimeevents_raw",
        "gold_table": "fact_datetime_event",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("caregiver_id", "long", "caregiver_id", "long"),
            ("charttime", "string", "chart_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("itemid", "long", "item_id", "long"),
            ("value", "string", "value", "string"),
            ("valueuom", "string", "value_uom", "string"),
            ("warning", "long", "warning", "int"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            'df = df.withColumn("charting_delay_minutes", F.when(F.col("chart_time").isNull() | F.col("store_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("store_time") - F.unix_timestamp("chart_time")) / 60.0))',
        ],
    },
    {
        "name": "fact_input_event",
        "raw_table": "inputevents_raw",
        "gold_table": "fact_input_event",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("caregiver_id", "long", "caregiver_id", "long"),
            ("starttime", "string", "start_time", "timestamp"),
            ("endtime", "string", "end_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("itemid", "long", "item_id", "long"),
            ("amount", "double", "amount", "double"),
            ("amountuom", "string", "amount_uom", "string"),
            ("rate", "double", "rate", "double"),
            ("rateuom", "string", "rate_uom", "string"),
            ("orderid", "long", "order_id", "long"),
            ("linkorderid", "long", "link_order_id", "long"),
            ("ordercategoryname", "string", "order_category_name", "string"),
            ("secondaryordercategoryname", "string", "secondary_order_category_name", "string"),
            ("ordercomponenttypedescription", "string", "order_component_type_description", "string"),
            ("ordercategorydescription", "string", "order_category_description", "string"),
            ("patientweight", "double", "patient_weight", "double"),
            ("totalamount", "long", "total_amount", "double"),
            ("totalamountuom", "string", "total_amount_uom", "string"),
            ("isopenbag", "long", "is_open_bag", "int"),
            ("continueinnextdept", "long", "continue_in_next_dept", "int"),
            ("statusdescription", "string", "status_description", "string"),
            ("originalamount", "double", "original_amount", "double"),
            ("originalrate", "double", "original_rate", "double"),
        ],
        "date_keys": [
            ("start_date_key", "start_time"),
            ("end_date_key", "end_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            'df = df.withColumn("duration_minutes", F.when(F.col("start_time").isNull() | F.col("end_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 60.0))',
        ],
    },
    {
        "name": "fact_ingredient_event",
        "raw_table": "ingredientevents_raw",
        "gold_table": "fact_ingredient_event",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("caregiver_id", "long", "caregiver_id", "long"),
            ("starttime", "string", "start_time", "timestamp"),
            ("endtime", "string", "end_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("itemid", "long", "item_id", "long"),
            ("amount", "double", "amount", "double"),
            ("amountuom", "string", "amount_uom", "string"),
            ("rate", "double", "rate", "double"),
            ("rateuom", "string", "rate_uom", "string"),
            ("orderid", "long", "order_id", "long"),
            ("linkorderid", "long", "link_order_id", "long"),
            ("statusdescription", "string", "status_description", "string"),
            ("originalamount", "long", "original_amount", "long"),
            ("originalrate", "double", "original_rate", "double"),
        ],
        "date_keys": [
            ("start_date_key", "start_time"),
            ("end_date_key", "end_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            'df = df.withColumn("duration_minutes", F.when(F.col("start_time").isNull() | F.col("end_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 60.0))',
        ],
    },
    {
        "name": "fact_output_event",
        "raw_table": "outputevents_raw",
        "gold_table": "fact_output_event",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("caregiver_id", "long", "caregiver_id", "long"),
            ("charttime", "string", "chart_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("itemid", "long", "item_id", "long"),
            ("value", "long", "value", "long"),
            ("valueuom", "string", "value_uom", "string"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            'df = df.withColumn("charting_delay_minutes", F.when(F.col("chart_time").isNull() | F.col("store_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("store_time") - F.unix_timestamp("chart_time")) / 60.0))',
        ],
    },
    {
        "name": "fact_procedure_event",
        "raw_table": "procedureevents_raw",
        "gold_table": "fact_procedure_event",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("caregiver_id", "long", "caregiver_id", "long"),
            ("starttime", "string", "start_time", "timestamp"),
            ("endtime", "string", "end_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("itemid", "long", "item_id", "long"),
            ("value", "double", "value", "double"),
            ("valueuom", "string", "value_uom", "string"),
            ("location", "string", "location", "string"),
            ("locationcategory", "string", "location_category", "string"),
            ("orderid", "long", "order_id", "long"),
            ("linkorderid", "long", "link_order_id", "long"),
            ("ordercategoryname", "string", "order_category_name", "string"),
            ("ordercategorydescription", "string", "order_category_description", "string"),
            ("patientweight", "double", "patient_weight", "double"),
            ("isopenbag", "long", "is_open_bag", "int"),
            ("continueinnextdept", "long", "continue_in_next_dept", "int"),
            ("statusdescription", "string", "status_description", "string"),
            ("originalamount", "double", "original_amount", "long"),
            ("originalrate", "long", "original_rate", "int"),
        ],
        "date_keys": [
            ("start_date_key", "start_time"),
            ("end_date_key", "end_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            'df = df.withColumn("duration_minutes", F.when(F.col("start_time").isNull() | F.col("end_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 60.0))',
        ],
    },
    # -------------------------------------------------------------------------------
    # Phase 2: previously deferred because their calculated columns need a cross-table
    # Join and/or a window function -- both now proven working (see the "join" /
    # "drop_after" support in build_dag/build_transform_code above).
    # -------------------------------------------------------------------------------
    {
        # fact_lab_result.is_abnormal_flag turned out NOT to need a join at all --
        # labevents_raw already carries ref_range_lower/ref_range_upper on every row
        # (unlike chartevents_raw, which has no per-row normal range, only an itemid to
        # look up in dim_chart_item). The mapping workbook's note describing this column
        # ("requires join to dim_lab_item/dim_chart_item") was reused/copy-pasted from
        # fact_chart_observation's identical-sounding column and doesn't actually apply
        # here -- verified against the deployed DDL (ref_range_lower/upper are fact_lab_
        # result's own columns) before taking the note at face value.
        "name": "fact_lab_result",
        "raw_table": "labevents_raw",
        "gold_table": "fact_lab_result",
        "mapping": [
            ("labevent_id", "long", "labevent_id", "long"),
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("specimen_id", "long", "specimen_id", "long"),
            ("itemid", "long", "item_id", "long"),
            ("order_provider_id", "string", "order_provider_id", "string"),
            ("charttime", "string", "chart_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("value", "string", "value", "string"),
            ("valuenum", "double", "value_num", "double"),
            ("valueuom", "string", "value_uom", "string"),
            ("ref_range_lower", "double", "ref_range_lower", "double"),
            ("ref_range_upper", "double", "ref_range_upper", "double"),
            ("flag", "string", "flag", "string"),
            ("priority", "string", "priority", "string"),
            ("comments", "string", "comments", "string"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_time"),
            ("store_date_key", "store_time"),
        ],
        "calculated": [
            'df = df.withColumn("result_turnaround_minutes", F.when(F.col("chart_time").isNull() | F.col("store_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("store_time") - F.unix_timestamp("chart_time")) / 60.0))',
            'df = df.withColumn("is_abnormal_flag", F.when(F.col("value_num").isNull() | F.col("ref_range_lower").isNull() | F.col("ref_range_upper").isNull(), F.lit(None)).otherwise((F.col("value_num") < F.col("ref_range_lower")) | (F.col("value_num") > F.col("ref_range_upper"))))',
        ],
    },
    {
        # is_abnormal_flag here genuinely needs dim_chart_item's low_normal_value/
        # high_normal_value -- chartevents_raw carries only itemid, no per-row range.
        "name": "fact_chart_observation",
        "raw_table": "chartevents_raw",
        "gold_table": "fact_chart_observation",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("caregiver_id", "long", "caregiver_id", "long"),
            ("charttime", "string", "chart_time", "timestamp"),
            ("storetime", "string", "store_time", "timestamp"),
            ("itemid", "long", "item_id", "long"),
            ("value", "string", "value", "string"),
            ("valuenum", "double", "value_num", "double"),
            ("valueuom", "string", "value_uom", "string"),
            ("warning", "long", "warning", "int"),
        ],
        "date_keys": [
            ("chart_date_key", "chart_time"),
            ("store_date_key", "store_time"),
        ],
        "join": {
            "table": "dim_chart_item",
            # Only the columns actually needed, renaming the join key so it can't
            # collide with the fact side's own item_id after the join.
            "mapping": [
                ("item_id", "long", "chart_item_id", "long"),
                ("low_normal_value", "double", "low_normal_value", "double"),
                ("high_normal_value", "double", "high_normal_value", "double"),
            ],
            "fact_key": "item_id",
            "join_key": "chart_item_id",
            "join_type": "left",
        },
        "drop_after": ["chart_item_id", "low_normal_value", "high_normal_value"],
        "calculated": [
            'df = df.withColumn("charting_delay_minutes", F.when(F.col("chart_time").isNull() | F.col("store_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("store_time") - F.unix_timestamp("chart_time")) / 60.0))',
            'df = df.withColumn("is_abnormal_flag", F.when(F.col("value_num").isNull() | F.col("low_normal_value").isNull() | F.col("high_normal_value").isNull(), F.lit(None)).otherwise((F.col("value_num") < F.col("low_normal_value")) | (F.col("value_num") > F.col("high_normal_value"))))',
        ],
    },
    {
        # age_at_admission needs dim_patient.anchor_age/anchor_year (MIMIC-IV's standard
        # age-at-event formula: anchor_age + (event_year - anchor_year)).
        # is_readmission_flag / days_since_prior_discharge use a window function over
        # prior admissions per subject_id, not a join -- "readmission" here just means
        # "this patient has at least one earlier admission"; days_since_prior_discharge
        # carries the actual gap so downstream consumers can apply their own threshold
        # (e.g. WHERE days_since_prior_discharge <= 30), since no specific threshold is
        # specified anywhere in the source-to-target mapping.
        "name": "fact_admission",
        "raw_table": "admissions_raw",
        "gold_table": "fact_admission",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("admittime", "string", "admit_time", "timestamp"),
            ("dischtime", "string", "disch_time", "timestamp"),
            ("deathtime", "string", "death_time", "timestamp"),
            ("admission_type", "string", "admission_type", "string"),
            ("admit_provider_id", "string", "admit_provider_id", "string"),
            ("admission_location", "string", "admission_location", "string"),
            ("discharge_location", "string", "discharge_location", "string"),
            ("insurance", "string", "insurance", "string"),
            ("language", "string", "language", "string"),
            ("marital_status", "string", "marital_status", "string"),
            ("race", "string", "race", "string"),
            ("edregtime", "string", "ed_reg_time", "timestamp"),
            ("edouttime", "string", "ed_out_time", "timestamp"),
            ("hospital_expire_flag", "long", "hospital_expire_flag", "int"),
        ],
        "date_keys": [
            ("admit_date_key", "admit_time"),
            ("disch_date_key", "disch_time"),
            ("death_date_key", "death_time"),
            ("ed_reg_date_key", "ed_reg_time"),
            ("ed_out_date_key", "ed_out_time"),
        ],
        "join": {
            "table": "dim_patient",
            "mapping": [
                ("subject_id", "long", "patient_subject_id", "long"),
                ("anchor_age", "int", "anchor_age", "int"),
                ("anchor_year", "int", "anchor_year", "int"),
            ],
            "fact_key": "subject_id",
            "join_key": "patient_subject_id",
            "join_type": "left",
        },
        "drop_after": ["patient_subject_id", "anchor_age", "anchor_year", "prior_disch_time"],
        "calculated": [
            'df = df.withColumn("hospital_los_hours", F.when(F.col("admit_time").isNull() | F.col("disch_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("disch_time") - F.unix_timestamp("admit_time")) / 3600.0))',
            'df = df.withColumn("ed_los_minutes", F.when(F.col("ed_reg_time").isNull() | F.col("ed_out_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("ed_out_time") - F.unix_timestamp("ed_reg_time")) / 60.0))',
            'df = df.withColumn("time_to_death_hours", F.when(F.col("admit_time").isNull() | F.col("death_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("death_time") - F.unix_timestamp("admit_time")) / 3600.0))',
            'df = df.withColumn("age_at_admission", F.when(F.col("admit_time").isNull() | F.col("anchor_age").isNull() | F.col("anchor_year").isNull(), F.lit(None)).otherwise(F.col("anchor_age") + (F.year(F.col("admit_time")) - F.col("anchor_year"))))',
            '_admit_window = Window.partitionBy("subject_id").orderBy("admit_time")',
            'df = df.withColumn("prior_disch_time", F.lag("disch_time").over(_admit_window))',
            'df = df.withColumn("is_readmission_flag", F.col("prior_disch_time").isNotNull())',
            'df = df.withColumn("days_since_prior_discharge", F.when(F.col("prior_disch_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("admit_time") - F.unix_timestamp("prior_disch_time")) / 86400.0).cast("int"))',
        ],
    },
    {
        # time_to_icu_hours needs fact_admission.admit_time (via hadm_id) -- this job
        # must therefore be run AFTER fact_admission has been loaded, not in parallel
        # with it; it reads fact_admission through the Glue Catalog, not the raw layer.
        # is_icu_readmission_flag uses a window function over prior ICU stays per
        # subject_id (same "has an earlier one" convention as fact_admission, above).
        "name": "fact_icu_stay_accumulating",
        "raw_table": "icustays_raw",
        "gold_table": "fact_icu_stay_accumulating",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("hadm_id", "long", "hadm_id", "long"),
            ("stay_id", "long", "stay_id", "long"),
            ("first_careunit", "string", "first_careunit", "string"),
            ("last_careunit", "string", "last_careunit", "string"),
            ("intime", "string", "in_time", "timestamp"),
            ("outtime", "string", "out_time", "timestamp"),
            ("los", "double", "los", "double"),
        ],
        "date_keys": [
            ("in_date_key", "in_time"),
            ("out_date_key", "out_time"),
        ],
        "join": {
            "table": "fact_admission",
            "mapping": [
                ("hadm_id", "long", "admission_hadm_id", "long"),
                ("admit_time", "timestamp", "admission_admit_time", "timestamp"),
            ],
            "fact_key": "hadm_id",
            "join_key": "admission_hadm_id",
            "join_type": "left",
        },
        "drop_after": ["admission_hadm_id", "admission_admit_time"],
        "calculated": [
            'df = df.withColumn("time_to_icu_hours", F.when(F.col("admission_admit_time").isNull() | F.col("in_time").isNull(), F.lit(None)).otherwise((F.unix_timestamp("in_time") - F.unix_timestamp("admission_admit_time")) / 3600.0))',
            '_icu_window = Window.partitionBy("subject_id").orderBy("in_time")',
            'df = df.withColumn("is_icu_readmission_flag", F.row_number().over(_icu_window) > 1)',
        ],
    },
]

AUDIT_TRANSFORM_CLASS_NAME = "AddDerivedColumns"

# Every Gold fact table is Iceberg-partitioned (see PARTITIONED BY in the DDL). Iceberg's
# default ClusteredDataWriter requires incoming rows to already be grouped by partition
# value; Spark's parallel write does not guarantee that ordering, so without enabling
# Iceberg's fanout writer the write fails intermittently with:
#   IllegalStateException: Incoming records violate the writer assumption that records
#   are clustered by spec and by partition within each spec.
# (small tables can luck into a single task and succeed, larger ones reliably don't).
#
# Getting Iceberg to actually honor "fanout-enabled" took three attempts:
#   1. additional_options={"fanout-enabled": "true"} on the S3IcebergCatalogTarget node
#      (write_data_frame.from_catalog) -- silently ignored; Glue's own DataSink wrapper
#      doesn't forward unrecognized keys to the underlying Iceberg writer.
#   2. Setting write.spark.fanout.enabled as a table property via Athena ALTER TABLE --
#      rejected outright ("Unsupported table property key").
#   3. Sorting by Iceberg's own system.bucket()/system.days() partition-transform
#      functions via F.expr() -- these are per-catalog function namespaces
#      (glue_catalog.system.days(...), not a bare system.days(...)), but glue_catalog
#      here is a plain org.apache.iceberg.spark.SparkCatalog, which doesn't implement
#      Spark's function-catalog interface at all ("Catalog glue_catalog does not
#      support functions").
# What actually works: bypass Glue's S3IcebergCatalogTarget/write_data_frame.from_catalog
# entirely and write via Iceberg's native Spark DataFrameWriterV2 API directly from this
# Custom Transform (df.writeTo(...).option("fanout-enabled", "true").overwritePartitions()),
# where the option is genuinely respected. This node is therefore the DAG's terminal
# node -- there is no downstream SelectFromCollection/S3IcebergCatalogTarget.
def build_transform_code(date_keys, calculated_lines, gold_database, gold_table, drop_after=None, join=None,
                         row_limit=None, mapping=None, raw_database=None, raw_table=None):
    #  Glue Studio's CustomCode node generates its own
    #     def {AUDIT_TRANSFORM_CLASS_NAME}(glueContext, dfc) -> DynamicFrameCollection:
    #  wrapper around whatever is returned here (using ClassName as the function name) --
    #  this must be just the body, not another `def` line, or the inner def shadows the
    #  outer one and the outer function returns None with no error.
    lines = [
        "from pyspark.sql import functions as F",
        "from pyspark.sql.window import Window",
        "",
    ]
    if row_limit:
        # --row-limit reads only the first N rows of the RAW source, instead of the whole
        # table. This must read via plain Spark SQL (spark.table(...) against the Glue Data
        # Catalog's default -- not Iceberg -- catalog, the same one a Hive/Athena query would
        # use), NOT via the S3CatalogSource + ApplyMapping Glue-DynamicFrame nodes build_dag()
        # uses when there's no cap. Measured against real full-dataset runs: a DynamicFrame
        # (S3CatalogSource -> ApplyMapping) with .limit(N) applied afterwards in this
        # CustomCode still fully read and cast every raw row before discarding the rest --
        # e.g. fact_medication_administration_mini_detail (emar_detail.csv.gz, 87M rows,
        # 748MB, single non-splittable gzip file) took ~57 minutes with --row-limit 1000000,
        # identical to what it'd cost with no cap at all, because DynamicFrames don't carry
        # Spark's LocalLimit down into the file scan. A plain spark.table(...).limit(N) is an
        # ordinary lazy DataFrame the whole way down, so Catalyst pushes the limit into the
        # (single-partition) file scan and the reader stops decompressing once it has N rows,
        # rather than reading to the end of the file.
        #
        # This also means the mapping (rename+cast to the Gold schema) that build_dag()
        # otherwise does with a separate ApplyMapping node has to happen here instead, on
        # the now-tiny limited DataFrame -- see the `mapping` block below.
        lines.append(f'df = spark.table("{raw_database}.{raw_table}")')
        # spark.table() does NOT honour the crawler's skip.header.line.count=1 table property
        # on these gzip CSV tables (Athena and the DynamicFrame path above both do), so the
        # CSV header line arrives as a data row -- every --row-limit fact table had exactly
        # one extra row whose string columns held their own column names and whose typed
        # columns were NULL (found 2026-09-30: fact_admission 546,029 Gold rows vs 546,028
        # raw). Drop it before .limit(N): a row where EVERY string column equals its own name
        # can only be the header. Filtering ahead of the limit keeps it a lazy filter+limit,
        # so the limit is still pushed into the file scan.
        lines.append('_str_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]')
        lines.append("if _str_cols:")
        lines.append("    _is_header = F.lit(True)")
        lines.append("    for _c in _str_cols:")
        lines.append("        _is_header = _is_header & F.col(_c).eqNullSafe(F.lit(_c))")
        lines.append("    df = df.filter(~_is_header)")
        lines.append(f"df = df.limit({int(row_limit)})")
        # .limit(N) collapses the DataFrame to a SINGLE partition (Spark's GlobalLimit
        # funnels the limited result through one task) -- without an explicit repartition
        # right after it, every downstream join/derived-column computation/Iceberg write
        # runs on ONE core, no matter how many workers the job has. Confirmed against a
        # real run: fact_chart_observation (--row-limit 1000000, 10x G.2X = 36 cores
        # available) took 2+ hours, with the driver log showing a single silent task the
        # entire time between the initial ~9s setup and completion -- repartitioning back
        # out restores real parallelism for the (still substantial) row_limit-sized rest
        # of the job.
        num_partitions = max(8, min(200, int(row_limit) // 5000))
        lines.append(f"df = df.repartition({num_partitions})")
        if mapping:
            lines.append("")
            select_cols = ", ".join(
                f'F.col("{from_path}").cast("{to_type}").alias("{to_key}")'
                for (from_path, _from_type, to_key, to_type) in mapping
            )
            lines.append(f"df = df.select({select_cols})")
    else:
        lines.append("input_key = list(dfc.keys())[0]")
        lines.append("df = dfc.select(input_key).toDF()")
    if join:
        # A Glue S3CatalogSource node reads via glueContext.create_dynamic_frame.from_
        # catalog, which does not support Iceberg tables at all ("IllegalArgumentException:
        # getDynamicFrame is not supported for iceberg table. Please use getDataFrame
        # instead."). Every table joined against here (a dimension, or fact_admission for
        # fact_icu_stay_accumulating) is Iceberg, so the join is done with plain Spark
        # instead of a Join node: spark.table(...) is a DataFrame-returning API call (like
        # spark.read), not a SQL statement, so this stays within "no SQL-based transform".
        select_cols = ", ".join(
            f'F.col("{from_col}").alias("{to_col}")' if from_col != to_col else f'F.col("{from_col}")'
            for (from_col, _, to_col, _) in join["mapping"]
        )
        lines.append("")
        lines.append(f'_join_df = spark.table("glue_catalog.{gold_database}.{join["table"]}").select({select_cols})')
        lines.append(
            f'df = df.join(_join_df, df["{join["fact_key"]}"] == _join_df["{join["join_key"]}"], "{join.get("join_type", "left")}")'
        )
    if date_keys:
        lines.append("")
        for date_key_col, source_col in date_keys:
            lines.append(
                f'df = df.withColumn("{date_key_col}", '
                f'F.when(F.col("{source_col}").isNull(), F.lit(None))'
                f'.otherwise(F.date_format(F.col("{source_col}"), "yyyyMMdd").cast("int")))'
            )
    if calculated_lines:
        lines.append("")
        lines.extend(calculated_lines)
    if drop_after:
        lines.append("")
        cols = ", ".join(f'"{c}"' for c in drop_after)
        lines.append(f"df = df.drop({cols})")
    lines.append("")
    lines.extend([
        "df = (",
        '    df.withColumn("created_ts", F.current_timestamp())',
        '      .withColumn("updated_ts", F.current_timestamp())',
        '      .withColumn("created_by", F.lit("glue_visual_etl"))',
        '      .withColumn("updated_by", F.lit("glue_visual_etl"))',
        ")",
        "",
        # Native Iceberg DataFrameWriterV2 write, bypassing Glue's DataSink wrapper --
        # fanout-enabled here is genuinely respected, unlike additional_options on an
        # S3IcebergCatalogTarget node. overwritePartitions() (not overwrite(condition))
        # -- passing a Column condition to .overwrite() hits a py4j argument-conversion
        # bug in this Glue runtime (TypeError: Column is not iterable). Since every run
        # reloads the complete raw source rather than an incremental subset,
        # overwritePartitions()'s "replace only the partitions present in this write"
        # semantics are equivalent to a full overwrite here.
        f'df.writeTo("glue_catalog.{gold_database}.{gold_table}").option("fanout-enabled", "true").overwritePartitions()',
        "",
        f'dyf = DynamicFrame.fromDF(df, glueContext, "{AUDIT_TRANSFORM_CLASS_NAME}")',
        f'return DynamicFrameCollection({{"{AUDIT_TRANSFORM_CLASS_NAME}": dyf}}, glueContext)',
    ])
    return "\n".join(lines) + "\n"


def ensure_glue_etl_role(iam, role_name, scripts_bucket, raw_bucket, gold_bucket) -> str:
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        response = iam.get_role(RoleName=role_name)
        config.tprint(f"      IAM role '{role_name}' already exists -- reusing it.")
        role_arn = response["Role"]["Arn"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
        config.tprint(f"      Creating IAM role '{role_name}' for Glue Visual ETL jobs...")
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Shared role used by all AWS Glue Visual ETL jobs that load Gold fact tables",
            Tags=config.project_tags_list(),
        )
        role_arn = response["Role"]["Arn"]
        time.sleep(10)

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    )

    statements = [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{scripts_bucket}",
                f"arn:aws:s3:::{scripts_bucket}/*",
                f"arn:aws:s3:::{raw_bucket}",
                f"arn:aws:s3:::{raw_bucket}/*",
            ],
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"],
            "Resource": [
                f"arn:aws:s3:::{gold_bucket}",
                f"arn:aws:s3:::{gold_bucket}/*",
            ],
        },
    ]

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueFactLoadS3Access",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )

    return role_arn


def build_dag(fact, raw_database, gold_database, row_limit=None):
    # --row-limit: skip the ApplyMapping (Glue DynamicFrame) node entirely and let the
    # CustomCode node read the raw table itself via plain Spark SQL (spark.table(...)) and
    # apply .limit(N) immediately -- see build_transform_code's comment for why the
    # DynamicFrame path can't actually short-circuit the read. The S3CatalogSource node is
    # kept only because Glue's API rejects a CustomCode node with an empty Inputs list
    # (ParamValidationError: "valid min length: 1") -- CustomCode's generated code never
    # touches `dfc` in this branch (it reads via spark.table(...) instead), so this upstream
    # DynamicFrame is created (a cheap Glue Catalog metadata lookup) but never materialized
    # into an actual Spark job, unlike ApplyMapping which forces a real read+cast over
    # every raw row.
    if row_limit:
        source_node = {
            "S3CatalogSource": {
                "Name": f"{fact['raw_table']} (raw, unused -- see build_dag's row_limit comment)",
                "Database": raw_database,
                "Table": fact["raw_table"],
            }
        }
        transform_node = {
            "CustomCode": {
                "Name": "Read (capped) & add derived columns & write to Gold",
                "Inputs": ["node-source"],
                "ClassName": AUDIT_TRANSFORM_CLASS_NAME,
                "Code": build_transform_code(
                    fact["date_keys"], fact["calculated"], gold_database, fact["gold_table"],
                    drop_after=fact.get("drop_after"), join=fact.get("join"), row_limit=row_limit,
                    mapping=fact["mapping"], raw_database=raw_database, raw_table=fact["raw_table"],
                ),
            }
        }
        return {"node-source": source_node, "node-transform": transform_node}

    source_node = {
        "S3CatalogSource": {
            "Name": f"{fact['raw_table']} (raw)",
            "Database": raw_database,
            "Table": fact["raw_table"],
        }
    }

    mapping_node = {
        "ApplyMapping": {
            "Name": "Rename & cast to Gold schema",
            "Inputs": ["node-source"],
            "Mapping": [
                {
                    "ToKey": to_key,
                    "FromPath": [from_path],
                    "FromType": from_type,
                    "ToType": to_type,
                    "Dropped": False,
                }
                for (from_path, from_type, to_key, to_type) in fact["mapping"]
            ],
        }
    }

    # Some Phase-2 facts need a lookup against another already-loaded Gold table (a
    # dimension for a normal-range lookup, or -- for fact_icu_stay_accumulating -- the
    # fact_admission table itself) to compute one of their calculated columns. This is
    # NOT done with a Glue Join node reading the second table via an S3CatalogSource --
    # that reads through glueContext.create_dynamic_frame.from_catalog, which doesn't
    # support Iceberg tables at all ("getDynamicFrame is not supported for iceberg
    # table"), and every Gold table here is Iceberg. Instead build_transform_code does
    # the join itself with plain Spark (spark.table(...) + DataFrame.join()), passed
    # through via the "join" parameter below.
    transform_node = {
        "CustomCode": {
            "Name": "Add derived columns & write to Gold",
            "Inputs": ["node-mapping"],
            "ClassName": AUDIT_TRANSFORM_CLASS_NAME,
            "Code": build_transform_code(
                fact["date_keys"], fact["calculated"], gold_database, fact["gold_table"],
                drop_after=fact.get("drop_after"), join=fact.get("join"), row_limit=row_limit,
            ),
        }
    }

    return {
        "node-source": source_node,
        "node-mapping": mapping_node,
        "node-transform": transform_node,
    }


def create_or_update_job(glue, job_name, dag, role_arn, scripts_bucket, gold_s3_bucket,
                          glue_version, worker_type, num_workers):
    iceberg_conf = " ".join([
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
        f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{gold_s3_bucket}/",
        "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
    ])

    common_kwargs = dict(
        Description=f"Visual ETL: raw -> Gold for {job_name} (auto-generated)",
        Role=role_arn,
        GlueVersion=glue_version,
        WorkerType=worker_type,
        NumberOfWorkers=num_workers,
        Command={
            "Name": "glueetl",
            "ScriptLocation": f"s3://{scripts_bucket}/scripts/{job_name}",
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--job-language": "python",
            "--conf": iceberg_conf,
            "--TempDir": f"s3://{scripts_bucket}/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            # Wires Spark's own SQL catalog (spark.table(...) / spark.sql(...)) to the Glue
            # Data Catalog as its Hive metastore. Without this, only Glue's own DynamicFrame
            # APIs (create_dynamic_frame.from_catalog) can see raw_database.raw_table --
            # plain spark.table("raw_db.raw_table") in a --row-limit job's CustomCode
            # (build_transform_code) raises "Table or view not found" without it.
            "--enable-glue-datacatalog": "true",
        },
        CodeGenConfigurationNodes=dag,
        JobMode="VISUAL",
    )

    try:
        glue.get_job(JobName=job_name)
        config.tprint(f"      Job '{job_name}' already exists -- updating its DAG/config.")
        glue.update_job(JobName=job_name, JobUpdate=common_kwargs)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating job '{job_name}'...")
        glue.create_job(Name=job_name, Tags=config.project_tags(), **common_kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config.add_dataset_arg(parser)
    parser.add_argument("--role-arn", default=None, help="IAM role ARN for Glue to assume when running these jobs")
    parser.add_argument("--create-role", action="store_true", help="Create or reuse a single shared IAM role for all fact jobs instead of passing --role-arn")
    parser.add_argument("--role-name", default="MimicGlueFactLoadRole", help="IAM role base name (suffixed per --dataset; default: MimicGlueFactLoadRole)")
    parser.add_argument("--raw-database", default=None, help="Glue raw database (default: from --dataset)")
    parser.add_argument("--raw-s3-bucket", default=None, help="Raw S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database (default: from --dataset)")
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket / Iceberg warehouse (default: from --dataset)")
    parser.add_argument("--scripts-bucket", default=None, help=f"Glue script/temp bucket (default: {config.SCRIPTS_BUCKET})")
    parser.add_argument("--job-prefix", default="fact-load-", help="Prefix for generated job names (default: fact-load-)")
    parser.add_argument("--glue-version", default="4.0", help="Glue version (must be 4.0+ for native Iceberg support; default: 4.0)")
    parser.add_argument("--worker-type", default="G.1X", help="Worker type (default: G.1X)")
    parser.add_argument("--num-workers", type=int, default=2, help="Number of workers (default: 2)")
    parser.add_argument("--only", default=None, help="Comma-separated subset of fact table names to generate (default: all)")
    parser.add_argument("--row-limit", type=int, default=None, help="Cap each generated job to a df.limit(N) sample of its source (e.g. --only fact_chart_observation --row-limit 100000 to avoid loading all ~432M rows on the full dataset)")
    parser.add_argument("--run-now", action="store_true", help="Also start a run of each job immediately after creating/updating it")
    parser.add_argument("--region", default=None, help="AWS region override")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config.resolve_job_defaults(args, role_base=args.role_name)

    if args.create_role and args.role_arn:
        parser.error("Cannot specify both --create-role and --role-arn; choose one.")
    if not args.role_arn and not args.create_role:
        parser.error("--role-arn or --create-role is required.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    glue = session.client("glue")

    if args.create_role:
        iam = session.client("iam")
        args.role_arn = ensure_glue_etl_role(
            iam, args.role_name, args.scripts_bucket, args.raw_s3_bucket, args.gold_s3_bucket,
        )
        config.tprint(f"Using Glue ETL role ARN: {args.role_arn}")

    selected = FACTS
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        selected = [f for f in FACTS if f["name"] in wanted]
        missing = wanted - {f["name"] for f in selected}
        if missing:
            config.tprint(f"WARNING: unknown fact table name(s) ignored: {missing}", file=sys.stderr)

    if not selected:
        config.tprint("Nothing to do -- no fact tables selected.", file=sys.stderr)
        sys.exit(1)

    created_jobs = []
    for fact in selected:
        job_name = f"{args.job_prefix}{fact['name']}{args.profile.suffix}"
        config.tprint(f"[{fact['name']}] raw:{args.raw_database}.{fact['raw_table']} -> gold:{args.gold_database}.{fact['gold_table']}")
        dag = build_dag(fact, args.raw_database, args.gold_database, row_limit=args.row_limit)
        if args.row_limit:
            config.tprint(f"    (--row-limit {args.row_limit}: job will load only a {args.row_limit}-row sample)")
        create_or_update_job(
            glue, job_name, dag, args.role_arn, args.scripts_bucket, args.gold_s3_bucket,
            args.glue_version, args.worker_type, args.num_workers,
        )
        created_jobs.append(job_name)

    if args.run_now:
        config.tprint("\nStarting job runs...")
        for job_name in created_jobs:
            resp = glue.start_job_run(JobName=job_name)
            config.tprint(f"      {job_name}: run id {resp['JobRunId']}")

    config.tprint("\nDone. Jobs created/updated:")
    for job_name in created_jobs:
        config.tprint(f"  - {job_name}  (view/edit the DAG in AWS Glue Studio -> Jobs -> {job_name})")

    if any(f["name"] == "fact_icu_stay_accumulating" for f in selected):
        config.tprint(
            "\nNOTE: fact_icu_stay_accumulating joins against fact_admission (via hadm_id) "
            "for time_to_icu_hours -- make sure fact_admission has already been loaded with "
            "current data before (re-)running fact_icu_stay_accumulating, not run in parallel "
            "with it."
        )


if __name__ == "__main__":
    main()
