CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`admissions_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/admissions_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/admissions_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`caregiver_raw` (
  `caregiver_id` bigint
)
LOCATION 's3://mimic4-datalake-v3-2-full/caregiver_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/caregiver_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`chartevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/chartevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/chartevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`d_hcpcs_raw` (
  `code` string,
  `category` bigint,
  `long_description` string,
  `short_description` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/d_hcpcs_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/d_hcpcs_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`d_icd_diagnoses_raw` (
  `icd_code` bigint,
  `icd_version` bigint,
  `long_title` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/d_icd_diagnoses_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/d_icd_diagnoses_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`d_icd_procedures_raw` (
  `icd_code` string,
  `icd_version` bigint,
  `long_title` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/d_icd_procedures_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/d_icd_procedures_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`d_items_raw` (
  `itemid` bigint,
  `label` string,
  `abbreviation` string,
  `linksto` string,
  `category` string,
  `unitname` string,
  `param_type` string,
  `lownormalvalue` bigint,
  `highnormalvalue` double
)
LOCATION 's3://mimic4-datalake-v3-2-full/d_items_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/d_items_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`d_labitems_raw` (
  `itemid` bigint,
  `label` string,
  `fluid` string,
  `category` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/d_labitems_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/d_labitems_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`datetimeevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/datetimeevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/datetimeevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`demo_subject_raw` (
  `subject_id` bigint
)
LOCATION 's3://mimic4-datalake-v3-2-full/demo_subject_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/demo_subject_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`diagnoses_icd_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `seq_num` bigint,
  `icd_code` string,
  `icd_version` bigint
)
LOCATION 's3://mimic4-datalake-v3-2-full/diagnoses_icd_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/diagnoses_icd_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`discharge_note_raw` (
  `note_id` string,
  `subject_id` bigint,
  `hadm_id` bigint,
  `note_type` string,
  `note_seq` bigint,
  `charttime` string,
  `storetime` string,
  `text` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/discharge_note_raw'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1', 'path'='s3://mimic4-datalake-v3-2-full/discharge_note_raw')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/discharge_note_raw';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`drgcodes_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `drg_type` string,
  `drg_code` bigint,
  `description` string,
  `drg_severity` bigint,
  `drg_mortality` bigint
)
LOCATION 's3://mimic4-datalake-v3-2-full/drgcodes_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/drgcodes_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`emar_detail_raw` (
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
  `dose_given` double,
  `dose_given_unit` string,
  `will_remainder_of_dose_be_given` string,
  `product_amount_given` double,
  `product_unit` string,
  `product_code` string,
  `product_description` string,
  `product_description_other` string,
  `prior_infusion_rate` bigint,
  `infusion_rate` bigint,
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
LOCATION 's3://mimic4-datalake-v3-2-full/emar_detail_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/emar_detail_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`emar_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/emar_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/emar_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`hcpcsevents_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `chartdate` string,
  `hcpcs_cd` string,
  `seq_num` bigint,
  `short_description` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/hcpcsevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/hcpcsevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`icustays_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `stay_id` bigint,
  `first_careunit` string,
  `last_careunit` string,
  `intime` string,
  `outtime` string,
  `los` double
)
LOCATION 's3://mimic4-datalake-v3-2-full/icustays_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/icustays_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`ingredientevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/ingredientevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/ingredientevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`inputevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/inputevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/inputevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`labevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/labevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/labevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`microbiologyevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/microbiologyevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/microbiologyevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`omr_raw` (
  `subject_id` bigint,
  `chartdate` string,
  `seq_num` bigint,
  `result_name` string,
  `result_value` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/omr_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/omr_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`outputevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/outputevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/outputevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`patients_raw` (
  `subject_id` bigint,
  `gender` string,
  `anchor_age` bigint,
  `anchor_year` bigint,
  `anchor_year_group` string,
  `dod` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/patients_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/patients_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`pharmacy_raw` (
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
  `lockout_interval` bigint,
  `basal_rate` bigint,
  `one_hr_max` double,
  `doses_per_24_hrs` bigint,
  `duration` bigint,
  `duration_interval` string,
  `expiration_value` bigint,
  `expiration_unit` string,
  `expirationdate` string,
  `dispensation` string,
  `fill_quantity` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/pharmacy_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/pharmacy_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`poe_detail_raw` (
  `poe_id` string,
  `poe_seq` bigint,
  `subject_id` bigint,
  `field_name` string,
  `field_value` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/poe_detail_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/poe_detail_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`poe_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/poe_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/poe_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`prescriptions_raw` (
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
  `gsn` bigint,
  `ndc` bigint,
  `prod_strength` string,
  `form_rx` string,
  `dose_val_rx` string,
  `dose_unit_rx` string,
  `form_val_disp` string,
  `form_unit_disp` string,
  `doses_per_24_hrs` bigint,
  `route` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/prescriptions_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/prescriptions_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`procedureevents_raw` (
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
LOCATION 's3://mimic4-datalake-v3-2-full/procedureevents_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/procedureevents_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`procedures_icd_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `seq_num` bigint,
  `chartdate` string,
  `icd_code` string,
  `icd_version` bigint
)
LOCATION 's3://mimic4-datalake-v3-2-full/procedures_icd_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/procedures_icd_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`provider_raw` (
  `provider_id` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/provider_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('serialization.format'=',', 'field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/provider_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`radiology_note_raw` (
  `note_id` string,
  `subject_id` bigint,
  `hadm_id` bigint,
  `note_type` string,
  `note_seq` bigint,
  `charttime` string,
  `storetime` string,
  `text` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/radiology_note_raw'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1', 'path'='s3://mimic4-datalake-v3-2-full/radiology_note_raw')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/radiology_note_raw';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`services_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `transfertime` string,
  `prev_service` string,
  `curr_service` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/services_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/services_raw/';


CREATE EXTERNAL TABLE IF NOT EXISTS `mimic4_db_raw_full`.`transfers_raw` (
  `subject_id` bigint,
  `hadm_id` bigint,
  `transfer_id` bigint,
  `eventtype` string,
  `careunit` string,
  `intime` string,
  `outtime` string
)
LOCATION 's3://mimic4-datalake-v3-2-full/transfers_raw/'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://mimic4-datalake-v3-2-full/transfers_raw/';

