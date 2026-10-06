-- Raw-layer (mimic4_db_raw) external-table DDL. Mostly regenerable from the Glue
-- catalog with: python ddl/extract_raw_ddl.py --database mimic4_db_raw
--
-- A few tables are NOT crawler-produced and are hand-maintained here:
--   demo_subject_raw      single-column CSV the crawler mis-classifies (like caregiver_raw/
--                         provider_raw) -- aws_workflow.overwrite_demo_subject_table() also
--                         rewrites it after each crawl.
--   discharge_note_raw    MIMIC-IV-Note discharge summaries, converted gzip-CSV -> Parquet
--   radiology_note_raw    MIMIC-IV-Note radiology reports,    by etl/notes_ingest.py
--                         (the note `text` column has embedded newlines, so the CSVs
--                         cannot be crawled; the crawler is told to skip those prefixes).

CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`admissions_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `admittime` string,
  `dischtime` string,
  `deathtime` string,
  `admission_type` string,
  `admit_provider_id` string,
  `admission_location` string,
  `discharge_location` string,
  `insurance` string,
  `language` string,
  `marital_status` string,
  `race` string,
  `edregtime` string,
  `edouttime` string,
  `hospital_expire_flag` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/admissions_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/admissions_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`caregiver_raw` (
  `caregiver_id` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/caregiver_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/caregiver_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`chartevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `caregiver_id` bigint,
  `charttime` string,
  `storetime` string,
  `itemid` bigint,
  `value` string,
  `valuenum` double,
  `valueuom` string,
  `warning` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/chartevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/chartevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`d_hcpcs_raw` (
  `code` string,
  `category` string,
  `long_description` string,
  `short_description` string
)
LOCATION 's3://mimic4-datalake-v3-2/d_hcpcs_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/d_hcpcs_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`d_icd_diagnoses_raw` (
  `icd_code` string,
  `icd_version` bigint,
  `long_title` string
)
LOCATION 's3://mimic4-datalake-v3-2/d_icd_diagnoses_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('quoteChar'='"', 'separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/d_icd_diagnoses_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`d_icd_procedures_raw` (
  `icd_code` bigint,
  `icd_version` bigint,
  `long_title` string
)
LOCATION 's3://mimic4-datalake-v3-2/d_icd_procedures_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/d_icd_procedures_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`d_items_raw` (
  `itemid` bigint,
  `label` string,
  `abbreviation` string,
  `linksto` string,
  `category` string,
  `unitname` string,
  `param_type` string,
  `lownormalvalue` string,
  `highnormalvalue` string
)
LOCATION 's3://mimic4-datalake-v3-2/d_items_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/d_items_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`d_labitems_raw` (
  `itemid` bigint,
  `label` string,
  `fluid` string,
  `category` string
)
LOCATION 's3://mimic4-datalake-v3-2/d_labitems_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/d_labitems_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`datetimeevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `caregiver_id` bigint,
  `charttime` string,
  `storetime` string,
  `itemid` bigint,
  `value` string,
  `valueuom` string,
  `warning` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/datetimeevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/datetimeevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`demo_subject_raw` (
  `subject_id` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/demo_subject_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/demo_subject_raw/'
TBLPROPERTIES ('classification'='csv', 'skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`diagnoses_icd_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `seq_num` bigint,
  `icd_code` string,
  `icd_version` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/diagnoses_icd_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/diagnoses_icd_raw/';


-- discharge_note_raw / radiology_note_raw are NOT crawled from CSV: the MIMIC-IV-Note
-- `text` column has embedded newlines inside quoted fields, which no Hive/Athena CSV
-- SerDe can parse. etl/notes_ingest.py reads the raw CSVs with a multiLine-aware
-- Spark reader and writes these Parquet tables (see run_notes_ingest.py).
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`discharge_note_raw` (
  `note_id` string,
  `subject_id` bigint,
  `hadm_id` bigint,
  `note_type` string,
  `note_seq` bigint,
  `charttime` string,
  `storetime` string,
  `text` string
)
STORED AS PARQUET
LOCATION 's3://mimic4-datalake-v3-2/discharge_note_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`drgcodes_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `drg_type` string,
  `drg_code` bigint,
  `description` string,
  `drg_severity` bigint,
  `drg_mortality` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/drgcodes_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/drgcodes_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`emar_detail_raw` (
  `subject_id` bigint,
  `emar_id` string,
  `emar_seq` bigint,
  `parent_field_ordinal` double,
  `administration_type` string,
  `pharmacy_id` bigint,
  `barcode_type` string,
  `reason_for_no_barcode` string,
  `complete_dose_not_given` string,
  `dose_due` string,
  `dose_due_unit` string,
  `dose_given` string,
  `dose_given_unit` string,
  `will_remainder_of_dose_be_given` string,
  `product_amount_given` double,
  `product_unit` string,
  `product_code` string,
  `product_description` string,
  `product_description_other` string,
  `prior_infusion_rate` double,
  `infusion_rate` double,
  `infusion_rate_adjustment` string,
  `infusion_rate_adjustment_amount` string,
  `infusion_rate_unit` string,
  `route` string,
  `infusion_complete` string,
  `completion_interval` string,
  `new_iv_bag_hung` string,
  `continued_infusion_in_other_location` string,
  `restart_interval` string,
  `side` string,
  `site` string,
  `non_formulary_visual_verification` string
)
LOCATION 's3://mimic4-datalake-v3-2/emar_detail_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/emar_detail_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`emar_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `emar_id` string,
  `emar_seq` bigint,
  `poe_id` string,
  `pharmacy_id` bigint,
  `enter_provider_id` string,
  `charttime` string,
  `medication` string,
  `event_txt` string,
  `scheduletime` string,
  `storetime` string
)
LOCATION 's3://mimic4-datalake-v3-2/emar_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/emar_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`hcpcsevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `chartdate` string,
  `hcpcs_cd` string,
  `seq_num` bigint,
  `short_description` string
)
LOCATION 's3://mimic4-datalake-v3-2/hcpcsevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/hcpcsevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`icustays_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `first_careunit` string,
  `last_careunit` string,
  `intime` string,
  `outtime` string,
  `los` double
)
LOCATION 's3://mimic4-datalake-v3-2/icustays_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/icustays_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`ingredientevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `caregiver_id` bigint,
  `starttime` string,
  `endtime` string,
  `storetime` string,
  `itemid` bigint,
  `amount` double,
  `amountuom` string,
  `rate` double,
  `rateuom` string,
  `orderid` bigint,
  `linkorderid` bigint,
  `statusdescription` string,
  `originalamount` bigint,
  `originalrate` double
)
LOCATION 's3://mimic4-datalake-v3-2/ingredientevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/ingredientevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`inputevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `caregiver_id` bigint,
  `starttime` string,
  `endtime` string,
  `storetime` string,
  `itemid` bigint,
  `amount` double,
  `amountuom` string,
  `rate` double,
  `rateuom` string,
  `orderid` bigint,
  `linkorderid` bigint,
  `ordercategoryname` string,
  `secondaryordercategoryname` string,
  `ordercomponenttypedescription` string,
  `ordercategorydescription` string,
  `patientweight` double,
  `totalamount` bigint,
  `totalamountuom` string,
  `isopenbag` bigint,
  `continueinnextdept` bigint,
  `statusdescription` string,
  `originalamount` double,
  `originalrate` double
)
LOCATION 's3://mimic4-datalake-v3-2/inputevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/inputevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`labevents_raw` (
  `labevent_id` bigint,
  `subject_id` bigint,
  `hadm_id` bigint,
  `specimen_id` bigint,
  `itemid` bigint,
  `order_provider_id` string,
  `charttime` string,
  `storetime` string,
  `value` string,
  `valuenum` double,
  `valueuom` string,
  `ref_range_lower` double,
  `ref_range_upper` double,
  `flag` string,
  `priority` string,
  `comments` string
)
LOCATION 's3://mimic4-datalake-v3-2/labevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/labevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`microbiologyevents_raw` (
  `microevent_id` bigint,
  `subject_id` bigint,
  `hadm_id` bigint,
  `micro_specimen_id` bigint,
  `order_provider_id` string,
  `chartdate` string,
  `charttime` string,
  `spec_itemid` bigint,
  `spec_type_desc` string,
  `test_seq` bigint,
  `storedate` string,
  `storetime` string,
  `test_itemid` bigint,
  `test_name` string,
  `org_itemid` bigint,
  `org_name` string,
  `isolate_num` bigint,
  `quantity` string,
  `ab_itemid` bigint,
  `ab_name` string,
  `dilution_text` string,
  `dilution_comparison` string,
  `dilution_value` double,
  `interpretation` string,
  `comments` string
)
LOCATION 's3://mimic4-datalake-v3-2/microbiologyevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/microbiologyevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`omr_raw` (
  `subject_id` bigint,
  `chartdate` string,
  `seq_num` bigint,
  `result_name` string,
  `result_value` string
)
LOCATION 's3://mimic4-datalake-v3-2/omr_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/omr_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`outputevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `caregiver_id` bigint,
  `charttime` string,
  `storetime` string,
  `itemid` bigint,
  `value` bigint,
  `valueuom` string
)
LOCATION 's3://mimic4-datalake-v3-2/outputevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/outputevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`patients_raw` (
  `subject_id` bigint,
  `gender` string,
  `anchor_age` bigint,
  `anchor_year` bigint,
  `anchor_year_group` string,
  `dod` string
)
LOCATION 's3://mimic4-datalake-v3-2/patients_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/patients_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`pharmacy_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `pharmacy_id` bigint,
  `poe_id` string,
  `starttime` string,
  `stoptime` string,
  `medication` string,
  `proc_type` string,
  `status` string,
  `entertime` string,
  `verifiedtime` string,
  `route` string,
  `frequency` string,
  `disp_sched` string,
  `infusion_type` string,
  `sliding_scale` string,
  `lockout_interval` string,
  `basal_rate` string,
  `one_hr_max` string,
  `doses_per_24_hrs` bigint,
  `duration` bigint,
  `duration_interval` string,
  `expiration_value` bigint,
  `expiration_unit` string,
  `expirationdate` string,
  `dispensation` string,
  `fill_quantity` string
)
LOCATION 's3://mimic4-datalake-v3-2/pharmacy_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/pharmacy_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`poe_detail_raw` (
  `poe_id` string,
  `poe_seq` bigint,
  `subject_id` bigint,
  `field_name` string,
  `field_value` string
)
LOCATION 's3://mimic4-datalake-v3-2/poe_detail_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/poe_detail_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`poe_raw` (
  `poe_id` string,
  `poe_seq` bigint,
  `subject_id` bigint,
  `hadm_id` bigint,
  `ordertime` string,
  `order_type` string,
  `order_subtype` string,
  `transaction_type` string,
  `discontinue_of_poe_id` string,
  `discontinued_by_poe_id` string,
  `order_provider_id` string,
  `order_status` string
)
LOCATION 's3://mimic4-datalake-v3-2/poe_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/poe_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`prescriptions_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `pharmacy_id` bigint,
  `poe_id` string,
  `poe_seq` bigint,
  `order_provider_id` string,
  `starttime` string,
  `stoptime` string,
  `drug_type` string,
  `drug` string,
  `formulary_drug_cd` string,
  `gsn` string,
  `ndc` bigint,
  `prod_strength` string,
  `form_rx` string,
  `dose_val_rx` string,
  `dose_unit_rx` string,
  `form_val_disp` bigint,
  `form_unit_disp` string,
  `doses_per_24_hrs` bigint,
  `route` string
)
LOCATION 's3://mimic4-datalake-v3-2/prescriptions_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/prescriptions_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`procedureevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `caregiver_id` bigint,
  `starttime` string,
  `endtime` string,
  `storetime` string,
  `itemid` bigint,
  `value` double,
  `valueuom` string,
  `location` string,
  `locationcategory` string,
  `orderid` bigint,
  `linkorderid` bigint,
  `ordercategoryname` string,
  `ordercategorydescription` string,
  `patientweight` double,
  `isopenbag` bigint,
  `continueinnextdept` bigint,
  `statusdescription` string,
  `originalamount` double,
  `originalrate` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/procedureevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/procedureevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`procedures_icd_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `seq_num` bigint,
  `chartdate` string,
  `icd_code` string,
  `icd_version` bigint
)
LOCATION 's3://mimic4-datalake-v3-2/procedures_icd_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/procedures_icd_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`provider_raw` (
  `provider_id` string
)
LOCATION 's3://mimic4-datalake-v3-2/provider_raw'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/provider_raw';


-- See the note above discharge_note_raw: produced by etl/notes_ingest.py, not the crawler.
CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`radiology_note_raw` (
  `note_id` string,
  `subject_id` bigint,
  `hadm_id` bigint,
  `note_type` string,
  `note_seq` bigint,
  `charttime` string,
  `storetime` string,
  `text` string
)
STORED AS PARQUET
LOCATION 's3://mimic4-datalake-v3-2/radiology_note_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`services_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `transfertime` string,
  `prev_service` string,
  `curr_service` string
)
LOCATION 's3://mimic4-datalake-v3-2/services_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/services_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw`.`transfers_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `transfer_id` bigint,
  `eventtype` string,
  `careunit` string,
  `intime` string,
  `outtime` string
)
LOCATION 's3://mimic4-datalake-v3-2/transfers_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2/transfers_raw/';

