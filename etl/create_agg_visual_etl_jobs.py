#!/usr/bin/env python3
"""
create_agg_visual_etl_jobs.py

Creates (or updates) an AWS Glue Studio VISUAL ETL job (JobMode='VISUAL' with
CodeGenConfigurationNodes) for each Gold aggregate/summary table AND each OBT/ABT
(One Big Table / Analytics Base Table) -- one job per entry in AGGREGATES, all generated
from this single script. Mechanically an OBT is just an aggregate whose grain (e.g.
hadm_id) is finer than its partition (e.g. admit_year_month) -- see
OBT_ADMISSION_FEATURES_CODE's comment for the one extra step that requires (expanding
"touched" to every row sharing a touched partition, not just the directly-touched ones).

Unlike the dim/fact jobs, these are INCREMENTALLY refreshed, not full-reloaded every run:
  1. Read the aggregate's watermark from mimic4_db_business.etl_control (the max source
     updated_ts already processed by this aggregate's last successful run; a very old
     default timestamp on first run, so the first run is a full backfill).
  2. Find every calendar day (date_key) touched by source rows with
     updated_ts > watermark -- this catches new rows AND corrections to existing rows,
     not just inserts.
  3. Fully recompute those touched days from the CURRENT full state of the source table
     (not a delta accumulation -- corrections/deletes in source data make incremental
     addition unsafe; recompute-and-replace per touched day is the safe, idempotent
     pattern).
  4. Write via
       df.repartition(_write_partitions, F.col(<partition column>)).sortWithinPartitions(<partition column>)
       .writeTo(...).overwritePartitions()
     -- month/year-grain tables (agg_admission_monthly, the OBTs) are identity-partitioned
     (year_month / admit_year_month / anchor_year_group / in_year_month) and the result
     only contains touched partition values, so this is a genuine partition-scoped
     overwrite: every other month's existing row(s) are left untouched. Their cardinality
     is naturally bounded (~1,200 distinct year-months even across MIMIC-IV's ~130-year
     shifted date range), so plain identity partitioning never produces more than a
     few thousand files.
       The two DAY-grain tables (agg_admission_daily, agg_icu_fluid_balance_daily) are
     different: a *day*-identity partition explodes to tens of thousands of distinct
     values across that same ~130-year range, and after enough incremental runs (each
     writing 1 tiny file into whatever few days it touched) accumulate tens of thousands
     of ~1-10-row files -- confirmed against a real run: agg_admission_daily reached
     36,961 files for 244,500 rows (~6.6 rows/file), and Iceberg's bin-pack OPTIMIZE
     can't fix it after the fact (compaction only merges files *within* one partition
     value, and most individual days only ever had one tiny file to begin with -- the
     fragmentation is structural, not a backlog). So these two use a MATERIALIZED
     date_bucket = date_key % 64 column instead (see AGG_ADMISSION_DAILY_CODE's comment
     for why not Iceberg's native bucket() transform), bounding the table to at most 64
     files long-term regardless of how many distinct days exist or how many incremental
     runs accumulate. The correctness cost: overwritePartitions() now replaces a whole
     date_bucket, which mixes in every OTHER day hashing into it, so the refresh must
     CARRY FORWARD every existing row in an affected bucket that isn't one of the
     touched dates (see the CODE templates) -- omit that and untouched days sharing a
     bucket with a touched day would be silently deleted. This also means query-time
     date_key lookups lose automatic partition pruning; add date_bucket = date_key % 64
     to a WHERE clause to get it back (the same trade-off already made for
     fact_admission's bucket(4, hadm_id) -- see create_fact_visual_etl_jobs.py).
       Both repartition() and sortWithinPartitions() are required on every one of these
     jobs' writes -- found out the hard way, in two steps, against real full-dataset
     runs, back when the day-grain tables were still plain date_key-identity-partitioned.
     On the full dataset, a first-run backfill touches essentially every admission's
     date, spread across that ~130-year range -- tens of thousands of distinct identity-
     partition values at the time. With the job's default spark.sql.shuffle.partitions
     (4 here), each of those 4 tasks ended up holding rows for thousands of distinct
     partition values, and Iceberg's fanout writer (fanout-enabled=true) must hold a file
     writer open *simultaneously* for every distinct value a task has seen so far -- that
     many concurrently open writers/buffers is what actually OOMed the executor.
     sortWithinPartitions(<col>) ALONE was tried first and is NOT sufficient: it only
     reorders rows *within* whatever few, still-huge tasks already exist, so a task still
     eventually touches thousands of distinct values (just no longer interleaved) --
     confirmed against a real run: agg_icu_fluid_balance_daily's backfill failed with the
     identical "ExecutorLostFailure ... Remote RPC client disassociated. Likely due to
     containers exceeding thresholds" both before AND after adding sortWithinPartitions
     alone. repartition(_write_partitions, F.col(<col>)) is the fix that actually bounds
     the problem: an upfront shuffle that spreads the distinct partition values across
     `_write_partitions` tasks instead of 4, so each task sees only a fraction of them.
     Now that the day-grain tables partition on the 64-valued date_bucket instead of raw
     date_key, this OOM class of failure can't recur there regardless of backfill size
     (64 is a small, safe number of concurrently open writers on its own) -- repartition/
     sortWithinPartitions are kept anyway for the month/year-grain tables, where
     `_write_partitions` still scales with how many partition values are actually
     touched (roughly one task per ~50 touched values, clamped to [4, 200]).
  5. Advance the watermark in etl_control the same way (also identity-partitioned, by
     aggregate_table, so this too is a scoped overwrite of just this aggregate's row).

This design is what's meant to survive a large production data-volume increase where the
dim/fact jobs' full-reload pattern would not: a refresh's cost scales with how many
CALENDAR DAYS changed, not with total fact-table row volume.

Each job's DAG:

    <small raw table> (Data Catalog, unused placeholder -- see below)
        --> Refresh <aggregate> (Custom Transform -- PySpark DataFrame API, no SQL; reads
            the real inputs itself via spark.table(...), computes the incremental refresh,
            and performs the Iceberg writes)

WHY THE UNUSED PLACEHOLDER SOURCE: a Glue Visual ETL job's DAG needs at least one node
with no upstream inputs to start from, but every actual input this job needs
(fact_admission, etl_control) is a Gold Iceberg table, and a Glue S3CatalogSource node
reads via glueContext.create_dynamic_frame.from_catalog, which does not support Iceberg
tables at all (see etl/create_fact_visual_etl_jobs.py's build_transform_code docstring for
the exact error). So the DAG's root node points at a small, unrelated RAW (non-Iceberg,
crawled) table purely to give the CustomCode node something to hang off of; its output is
never referenced. The Custom Transform instead reads every real input directly via
spark.table("glue_catalog.<db>.<table>") -- an ordinary DataFrame-returning API call, not
a SQL statement, so this stays within "no SQL-based transform" the same way the fact jobs'
cross-table joins do.

Prerequisites:
  1. mimic4_db_business.etl_control and the target aggregate table(s) already exist
     (created by ddl/gold/mimic_iv_ddl_gold_combined_v4.sql via Athena).
  2. Each aggregate's source table(s) (e.g. fact_admission) are already loaded.
  3. An IAM role Glue can assume, with AWSGlueServiceRole plus S3 read on the raw bucket
     (for the placeholder source) and S3 read/write on the Gold bucket (use --create-role
     to have this script set one up; the same role is shared across every aggregate job).

Usage:
  python3 create_agg_visual_etl_jobs.py \\
      --create-role \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket \\
      --raw-s3-bucket mimic4-datalake-v3-2 \\
      --gold-s3-bucket mimic4-lakehouse-v3-2 \\
      --run-now
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
# Per-aggregate configuration.
#
#   placeholder_raw_table -- a small, unrelated raw table used only to give the DAG a
#                             root node (see the module docstring); its data is discarded.
#   code_body             -- the Custom Transform's full function body (no `def` line --
#                             Glue Studio supplies that wrapper itself, same convention as
#                             every other Visual ETL job in this project).
# ---------------------------------------------------------------------------------------

AGG_ADMISSION_DAILY_CODE = '''\
from pyspark.sql import functions as F

# Placeholder input intentionally unused -- see the module docstring for why this DAG
# needs a non-Iceberg root node at all. The real inputs are read directly below.
_ = dfc.select(list(dfc.keys())[0]).toDF()

AGGREGATE_NAME = "agg_admission_daily"
FIRST_RUN_WATERMARK = "1900-01-01T00:00:00"
# Must match agg_admission_daily's PARTITIONED BY (date_bucket) in the Gold DDL. A
# materialized date_key % NUM_BUCKETS column, not Iceberg's bucket() transform:
# glue_catalog's SparkCatalog here doesn't implement Iceberg's function catalog
# (confirmed live via "AnalysisException: Catalog glue_catalog does not support
# functions" -- the same limitation already worked around for the NLP fact tables), so
# system.bucket(N, date_key) can't be called from Spark to compute which existing rows
# share a bucket with a touched date -- needed below for the carry-forward step.
NUM_BUCKETS = 64

control_df = spark.table("glue_catalog.mimic4_db_business.etl_control").filter(F.col("aggregate_table") == AGGREGATE_NAME)
watermark_row = control_df.select(F.max("last_processed_ts").alias("wm")).collect()[0]
last_processed_ts = watermark_row["wm"] if watermark_row["wm"] is not None else FIRST_RUN_WATERMARK

fact_df = spark.table("glue_catalog.mimic4_db_business.fact_admission")

changed = fact_df.filter(F.col("updated_ts") > F.lit(last_processed_ts).cast("timestamp"))

touched_date_keys = (
    changed.select(F.col("admit_date_key").alias("dk"))
    .union(changed.select(F.col("disch_date_key").alias("dk")))
    .union(changed.select(F.col("death_date_key").alias("dk")))
    .where(F.col("dk").isNotNull())
    .distinct()
)
touched_list = [row["dk"] for row in touched_date_keys.collect()]

if not touched_list:
    print(f"{AGGREGATE_NAME}: no source rows changed since watermark {last_processed_ts} -- nothing to refresh.")
    dyf = DynamicFrame.fromDF(spark.createDataFrame([], "date_key int"), glueContext, "RefreshAggAdmissionDaily")
    return DynamicFrameCollection({"RefreshAggAdmissionDaily": dyf}, glueContext)

touched_buckets = sorted({dk % NUM_BUCKETS for dk in touched_list})
print(f"{AGGREGATE_NAME}: refreshing {len(touched_list)} touched date_key day(s) ({len(touched_buckets)} bucket(s)) since watermark {last_processed_ts}.")

affected = fact_df.filter(
    F.col("admit_date_key").isin(touched_list)
    | F.col("disch_date_key").isin(touched_list)
    | F.col("death_date_key").isin(touched_list)
)

admits = (
    affected.where(F.col("admit_date_key").isin(touched_list))
    .groupBy(F.col("admit_date_key").alias("date_key"), "admission_type")
    .agg(F.count("*").alias("admit_count"))
)
discharges = (
    affected.where(F.col("disch_date_key").isin(touched_list))
    .groupBy(F.col("disch_date_key").alias("date_key"), "admission_type")
    .agg(F.count("*").alias("discharge_count"))
)
deaths = (
    affected.where(F.col("death_date_key").isin(touched_list))
    .groupBy(F.col("death_date_key").alias("date_key"), "admission_type")
    .agg(F.count("*").alias("death_count"))
)

recomputed = (
    admits.join(discharges, ["date_key", "admission_type"], "full_outer")
          .join(deaths, ["date_key", "admission_type"], "full_outer")
          .withColumn("admit_count", F.coalesce(F.col("admit_count"), F.lit(0)).cast("int"))
          .withColumn("discharge_count", F.coalesce(F.col("discharge_count"), F.lit(0)).cast("int"))
          .withColumn("death_count", F.coalesce(F.col("death_count"), F.lit(0)).cast("int"))
          .withColumn("created_ts", F.current_timestamp())
          .withColumn("updated_ts", F.current_timestamp())
          .withColumn("date_bucket", (F.col("date_key") % F.lit(NUM_BUCKETS)).cast("int"))
)

# agg_admission_daily is PARTITIONED BY (date_bucket), not date_key, so
# overwritePartitions() below replaces the WHOLE bucket -- every OTHER date hashing into
# it, not just the touched ones. Carry forward every existing row in an affected bucket
# that ISN'T one of the touched dates, or those untouched days would be silently deleted.
existing = spark.table("glue_catalog.mimic4_db_business.agg_admission_daily")
carry_forward = existing.where(
    F.col("date_bucket").isin(touched_buckets) & (~F.col("date_key").isin(touched_list))
)

df = recomputed.unionByName(carry_forward)

# Partition-scoped overwrite: df only contains rows whose date_bucket is one of
# touched_buckets, so this replaces exactly those buckets and leaves every other
# existing bucket (and thus every day not sharing a touched bucket) untouched.
_write_partitions = max(4, len(touched_buckets))
df = df.repartition(_write_partitions, F.col("date_bucket")).sortWithinPartitions("date_bucket")
df.writeTo("glue_catalog.mimic4_db_business.agg_admission_daily").option("fanout-enabled", "true").overwritePartitions()

new_watermark = fact_df.select(F.max("updated_ts").alias("wm")).collect()[0]["wm"]
control_out = (
    spark.createDataFrame([(AGGREGATE_NAME, new_watermark)], ["aggregate_table", "last_processed_ts"])
    .withColumn("updated_ts", F.current_timestamp())
)
control_out.writeTo("glue_catalog.mimic4_db_business.etl_control").option("fanout-enabled", "true").overwritePartitions()

dyf = DynamicFrame.fromDF(df, glueContext, "RefreshAggAdmissionDaily")
return DynamicFrameCollection({"RefreshAggAdmissionDaily": dyf}, glueContext)
'''

# fact_ingredient_event is deliberately NOT summed alongside fact_input_event -- it's a
# child grain of fact_input_event (ingredient-level decomposition of the SAME
# administered volume, e.g. components of a mixed IV solution), so including both would
# double-count intake. amount_uom on fact_input_event is also not consistently a volume
# unit -- the same table carries medication/electrolyte doses (mg, mcg, units, mEq,
# mmol, grams) through the same column -- so only rows where amount_uom is "ml"
# (case-insensitive; the raw data uses lowercase) are summed. See the DDL comment on
# agg_icu_fluid_balance_daily for the full reasoning; both were confirmed against real
# loaded data (aws glue/Athena), not assumed from MIMIC's documentation alone.
AGG_ICU_FLUID_BALANCE_DAILY_CODE = '''\
from pyspark.sql import functions as F

# Placeholder input intentionally unused -- see the module docstring for why this DAG
# needs a non-Iceberg root node at all. The real inputs are read directly below.
_ = dfc.select(list(dfc.keys())[0]).toDF()

AGGREGATE_NAME = "agg_icu_fluid_balance_daily"
FIRST_RUN_WATERMARK = "1900-01-01T00:00:00"
# Must match agg_icu_fluid_balance_daily's PARTITIONED BY (date_bucket) in the Gold DDL
# -- see AGG_ADMISSION_DAILY_CODE's comment for why a materialized date_key % NUM_BUCKETS
# column instead of Iceberg's bucket() transform.
NUM_BUCKETS = 64

control_df = spark.table("glue_catalog.mimic4_db_business.etl_control").filter(F.col("aggregate_table") == AGGREGATE_NAME)
watermark_row = control_df.select(F.max("last_processed_ts").alias("wm")).collect()[0]
last_processed_ts = watermark_row["wm"] if watermark_row["wm"] is not None else FIRST_RUN_WATERMARK

input_df = spark.table("glue_catalog.mimic4_db_business.fact_input_event")
output_df = spark.table("glue_catalog.mimic4_db_business.fact_output_event")

watermark_ts = F.lit(last_processed_ts).cast("timestamp")
changed_input = input_df.filter(F.col("updated_ts") > watermark_ts)
changed_output = output_df.filter(F.col("updated_ts") > watermark_ts)

touched_date_keys = (
    changed_input.select(F.col("start_date_key").alias("dk"))
    .union(changed_output.select(F.col("chart_date_key").alias("dk")))
    .where(F.col("dk").isNotNull())
    .distinct()
)
touched_list = [row["dk"] for row in touched_date_keys.collect()]

if not touched_list:
    print(f"{AGGREGATE_NAME}: no source rows changed since watermark {last_processed_ts} -- nothing to refresh.")
    dyf = DynamicFrame.fromDF(spark.createDataFrame([], "date_key int"), glueContext, "RefreshAggIcuFluidBalanceDaily")
    return DynamicFrameCollection({"RefreshAggIcuFluidBalanceDaily": dyf}, glueContext)

touched_buckets = sorted({dk % NUM_BUCKETS for dk in touched_list})
print(f"{AGGREGATE_NAME}: refreshing {len(touched_list)} touched date_key day(s) ({len(touched_buckets)} bucket(s)) since watermark {last_processed_ts}.")

intake = (
    input_df.where(F.col("start_date_key").isin(touched_list) & (F.lower(F.col("amount_uom")) == "ml"))
    .groupBy(F.col("start_date_key").alias("date_key"), "stay_id")
    .agg(F.sum("amount").alias("total_intake_ml"))
)
output = (
    output_df.where(F.col("chart_date_key").isin(touched_list) & (F.lower(F.col("value_uom")) == "ml"))
    .groupBy(F.col("chart_date_key").alias("date_key"), "stay_id")
    .agg(F.sum(F.col("value").cast("double")).alias("total_output_ml"))
)

recomputed = (
    intake.join(output, ["date_key", "stay_id"], "full_outer")
          .withColumn("total_intake_ml", F.coalesce(F.col("total_intake_ml"), F.lit(0.0)))
          .withColumn("total_output_ml", F.coalesce(F.col("total_output_ml"), F.lit(0.0)))
          .withColumn("net_balance_ml", F.col("total_intake_ml") - F.col("total_output_ml"))
          .withColumn("created_ts", F.current_timestamp())
          .withColumn("updated_ts", F.current_timestamp())
          .withColumn("date_bucket", (F.col("date_key") % F.lit(NUM_BUCKETS)).cast("int"))
)

# agg_icu_fluid_balance_daily is PARTITIONED BY (date_bucket), not date_key -- see
# AGG_ADMISSION_DAILY_CODE's comment for why the carry-forward below is required for
# correctness once overwritePartitions() is scoped to a shared bucket instead of a
# single day.
existing = spark.table("glue_catalog.mimic4_db_business.agg_icu_fluid_balance_daily")
carry_forward = existing.where(
    F.col("date_bucket").isin(touched_buckets) & (~F.col("date_key").isin(touched_list))
)

df = recomputed.unionByName(carry_forward)

# Partition-scoped overwrite: df only contains rows whose date_bucket is one of
# touched_buckets, so this replaces exactly those buckets and leaves every other
# existing bucket (and thus every day not sharing a touched bucket) untouched.
_write_partitions = max(4, len(touched_buckets))
df = df.repartition(_write_partitions, F.col("date_bucket")).sortWithinPartitions("date_bucket")
df.writeTo("glue_catalog.mimic4_db_business.agg_icu_fluid_balance_daily").option("fanout-enabled", "true").overwritePartitions()

new_watermark = (
    input_df.select(F.max("updated_ts").alias("wm"))
    .union(output_df.select(F.max("updated_ts").alias("wm")))
    .agg(F.max("wm").alias("wm"))
    .collect()[0]["wm"]
)
control_out = (
    spark.createDataFrame([(AGGREGATE_NAME, new_watermark)], ["aggregate_table", "last_processed_ts"])
    .withColumn("updated_ts", F.current_timestamp())
)
control_out.writeTo("glue_catalog.mimic4_db_business.etl_control").option("fanout-enabled", "true").overwritePartitions()

dyf = DynamicFrame.fromDF(df, glueContext, "RefreshAggIcuFluidBalanceDaily")
return DynamicFrameCollection({"RefreshAggIcuFluidBalanceDaily": dyf}, glueContext)
'''

# Rolls up FROM agg_admission_daily, not raw fact_admission -- the watermark and touched-
# partition set are both computed against the (small) daily table, so this refresh's cost
# is bounded by how many *months* changed, re-aggregating only that daily table's own rows
# for those months, never rescanning fact_admission itself.
AGG_ADMISSION_MONTHLY_CODE = '''\
from pyspark.sql import functions as F

# Placeholder input intentionally unused -- see the module docstring for why this DAG
# needs a non-Iceberg root node at all. The real inputs are read directly below.
_ = dfc.select(list(dfc.keys())[0]).toDF()

AGGREGATE_NAME = "agg_admission_monthly"
FIRST_RUN_WATERMARK = "1900-01-01T00:00:00"

control_df = spark.table("glue_catalog.mimic4_db_business.etl_control").filter(F.col("aggregate_table") == AGGREGATE_NAME)
watermark_row = control_df.select(F.max("last_processed_ts").alias("wm")).collect()[0]
last_processed_ts = watermark_row["wm"] if watermark_row["wm"] is not None else FIRST_RUN_WATERMARK

# date_key is yyyyMMdd -- integer-dividing by 100 truncates to yyyyMM without needing a
# dim_date join.
daily_df = spark.table("glue_catalog.mimic4_db_business.agg_admission_daily").withColumn(
    "year_month", (F.col("date_key") / F.lit(100)).cast("int")
)

changed = daily_df.filter(F.col("updated_ts") > F.lit(last_processed_ts).cast("timestamp"))
touched_list = [row["year_month"] for row in changed.select("year_month").distinct().collect()]

if not touched_list:
    print(f"{AGGREGATE_NAME}: no source rows changed since watermark {last_processed_ts} -- nothing to refresh.")
    dyf = DynamicFrame.fromDF(spark.createDataFrame([], "year_month int"), glueContext, "RefreshAggAdmissionMonthly")
    return DynamicFrameCollection({"RefreshAggAdmissionMonthly": dyf}, glueContext)

print(f"{AGGREGATE_NAME}: refreshing {len(touched_list)} touched year_month(s) since watermark {last_processed_ts}.")

affected = daily_df.where(F.col("year_month").isin(touched_list))

df = (
    affected.groupBy("year_month", "admission_type")
    .agg(
        F.sum("admit_count").cast("int").alias("admit_count"),
        F.sum("discharge_count").cast("int").alias("discharge_count"),
        F.sum("death_count").cast("int").alias("death_count"),
    )
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
)

# Partition-scoped overwrite: df only contains the touched year_month values, so this
# replaces exactly those partitions and leaves every other existing month untouched.
_write_partitions = max(4, min(200, len(touched_list) // 50))
df = df.repartition(_write_partitions, F.col("year_month")).sortWithinPartitions("year_month")
df.writeTo("glue_catalog.mimic4_db_business.agg_admission_monthly").option("fanout-enabled", "true").overwritePartitions()

new_watermark = daily_df.select(F.max("updated_ts").alias("wm")).collect()[0]["wm"]
control_out = (
    spark.createDataFrame([(AGGREGATE_NAME, new_watermark)], ["aggregate_table", "last_processed_ts"])
    .withColumn("updated_ts", F.current_timestamp())
)
control_out.writeTo("glue_catalog.mimic4_db_business.etl_control").option("fanout-enabled", "true").overwritePartitions()

dyf = DynamicFrame.fromDF(df, glueContext, "RefreshAggAdmissionMonthly")
return DynamicFrameCollection({"RefreshAggAdmissionMonthly": dyf}, glueContext)
'''

# obt_admission_features: one row per hadm_id, denormalized across 10 source tables. Unlike
# the pure aggregates above, this table's grain (hadm_id) is FINER than its partition
# (admit_year_month) -- multiple admissions share one partition. That means a partition-
# scoped overwrite must recompute EVERY hadm_id sharing a touched month, not just the ones
# directly touched by a source-table change, or overwritePartitions() would silently drop
# the untouched admissions sharing that month. So refresh happens in two passes: (1) find
# directly-touched hadm_ids from each source table's updated_ts (dim_patient has no hadm_id,
# so its changed rows are mapped via a join back to fact_admission on subject_id), (2)
# expand to the full set of admit_year_month values those hadm_ids fall in, then rebuild
# every admission in those months from scratch. The set of touched admissions is kept as a
# DataFrame and used only in joins (never collected to the driver as a list) precisely
# because -- unlike a small, calendar-bounded list of dates/months -- it can be arbitrarily
# large at production data volume; only the small, calendar-bounded touched_months list is
# ever collected.
OBT_ADMISSION_FEATURES_CODE = '''\
from pyspark.sql import functions as F

# Placeholder input intentionally unused -- see the module docstring for why this DAG
# needs a non-Iceberg root node at all. The real inputs are read directly below.
_ = dfc.select(list(dfc.keys())[0]).toDF()

AGGREGATE_NAME = "obt_admission_features"
FIRST_RUN_WATERMARK = "1900-01-01T00:00:00"

control_df = spark.table("glue_catalog.mimic4_db_business.etl_control").filter(F.col("aggregate_table") == AGGREGATE_NAME)
watermark_row = control_df.select(F.max("last_processed_ts").alias("wm")).collect()[0]
last_processed_ts = watermark_row["wm"] if watermark_row["wm"] is not None else FIRST_RUN_WATERMARK
watermark_ts = F.lit(last_processed_ts).cast("timestamp")

admission_df = spark.table("glue_catalog.mimic4_db_business.fact_admission")
patient_df = spark.table("glue_catalog.mimic4_db_business.dim_patient")
diagnosis_df = spark.table("glue_catalog.mimic4_db_business.fact_diagnosis")
procedure_df = spark.table("glue_catalog.mimic4_db_business.fact_procedure")
drg_df = spark.table("glue_catalog.mimic4_db_business.fact_drg_assignment")
icu_df = spark.table("glue_catalog.mimic4_db_business.fact_icu_stay_accumulating")
lab_df = spark.table("glue_catalog.mimic4_db_business.fact_lab_result")
med_df = spark.table("glue_catalog.mimic4_db_business.fact_medication_administration")
micro_df = spark.table("glue_catalog.mimic4_db_business.fact_microbiology_result")
transfer_df = spark.table("glue_catalog.mimic4_db_business.fact_transfer")

hadm_keyed_sources = [admission_df, diagnosis_df, procedure_df, drg_df, icu_df, lab_df, med_df, micro_df, transfer_df]

directly_touched = None
for frame in hadm_keyed_sources:
    changed = frame.filter(F.col("updated_ts") > watermark_ts).select("hadm_id")
    directly_touched = changed if directly_touched is None else directly_touched.union(changed)

changed_patients = patient_df.filter(F.col("updated_ts") > watermark_ts).select("subject_id")
directly_touched = directly_touched.union(admission_df.join(changed_patients, "subject_id").select("hadm_id"))
directly_touched = directly_touched.where(F.col("hadm_id").isNotNull()).distinct()

if directly_touched.limit(1).count() == 0:
    print(f"{AGGREGATE_NAME}: no source rows changed since watermark {last_processed_ts} -- nothing to refresh.")
    dyf = DynamicFrame.fromDF(spark.createDataFrame([], "hadm_id long"), glueContext, "RefreshObtAdmissionFeatures")
    return DynamicFrameCollection({"RefreshObtAdmissionFeatures": dyf}, glueContext)

touched_months = [
    row["admit_year_month"] for row in
    admission_df.join(directly_touched, "hadm_id")
    .withColumn("admit_year_month", (F.col("admit_date_key") / F.lit(100)).cast("int"))
    .select("admit_year_month").distinct().collect()
]

print(f"{AGGREGATE_NAME}: refreshing all admissions across {len(touched_months)} touched month(s) since watermark {last_processed_ts}.")

base = (
    admission_df
    .withColumn("admit_year_month", (F.col("admit_date_key") / F.lit(100)).cast("int"))
    .where(F.col("admit_year_month").isin(touched_months))
)
touched_admissions = base.select("hadm_id")

diagnosis_agg = (
    diagnosis_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id").agg(F.count("*").cast("int").alias("diagnosis_count"))
)
primary_diagnosis = (
    diagnosis_df.join(touched_admissions, "hadm_id")
    .where(F.col("seq_num") == 1)
    .select("hadm_id", F.col("icd_code").alias("primary_icd_code"))
)
procedure_agg = (
    procedure_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id").agg(F.count("*").cast("int").alias("procedure_count"))
)
drg_agg = (
    drg_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id")
    .agg(F.max("drg_severity").alias("max_drg_severity"), F.max("drg_mortality").alias("max_drg_mortality"))
)
icu_agg = (
    icu_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id")
    .agg(F.count("*").cast("int").alias("icu_stay_count"), F.sum("los").alias("total_icu_los_days"))
)
lab_agg = (
    lab_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id")
    .agg(
        F.count("*").cast("int").alias("lab_count"),
        F.sum(F.col("is_abnormal_flag").cast("int")).alias("abnormal_lab_count"),
    )
)
med_agg = (
    med_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id").agg(F.count("*").cast("int").alias("med_admin_count"))
)
micro_agg = (
    micro_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id").agg(F.sum(F.col("is_positive_culture_flag").cast("int")).alias("positive_culture_count"))
)
transfer_agg = (
    transfer_df.join(touched_admissions, "hadm_id")
    .groupBy("hadm_id").agg(F.count("*").cast("int").alias("transfer_count"))
)

df = (
    base
    .join(patient_df.select("subject_id", "gender", "anchor_year_group", "dod"), "subject_id", "left")
    .join(diagnosis_agg, "hadm_id", "left")
    .join(primary_diagnosis, "hadm_id", "left")
    .join(procedure_agg, "hadm_id", "left")
    .join(drg_agg, "hadm_id", "left")
    .join(icu_agg, "hadm_id", "left")
    .join(lab_agg, "hadm_id", "left")
    .join(med_agg, "hadm_id", "left")
    .join(micro_agg, "hadm_id", "left")
    .join(transfer_agg, "hadm_id", "left")
    .withColumn("diagnosis_count", F.coalesce(F.col("diagnosis_count"), F.lit(0)))
    .withColumn("procedure_count", F.coalesce(F.col("procedure_count"), F.lit(0)))
    .withColumn("icu_stay_count", F.coalesce(F.col("icu_stay_count"), F.lit(0)))
    .withColumn("had_icu_stay", F.col("icu_stay_count") > 0)
    .withColumn("total_icu_los_days", F.coalesce(F.col("total_icu_los_days"), F.lit(0.0)))
    .withColumn("lab_count", F.coalesce(F.col("lab_count"), F.lit(0)))
    .withColumn("abnormal_lab_count", F.coalesce(F.col("abnormal_lab_count"), F.lit(0)))
    .withColumn(
        "abnormal_lab_rate",
        F.when(F.col("lab_count") > 0, F.col("abnormal_lab_count") / F.col("lab_count")).otherwise(F.lit(None).cast("double")),
    )
    .withColumn("med_admin_count", F.coalesce(F.col("med_admin_count"), F.lit(0)))
    .withColumn("positive_culture_count", F.coalesce(F.col("positive_culture_count"), F.lit(0)))
    .withColumn("transfer_count", F.coalesce(F.col("transfer_count"), F.lit(0)))
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
    .select(
        "hadm_id", "subject_id", "admit_provider_id", "admit_year_month", "admit_date_key",
        "admit_time", "disch_time", "death_time",
        "admission_type", "admission_location", "discharge_location",
        "insurance", "language", "marital_status", "race",
        "hospital_expire_flag", "hospital_los_hours", "ed_los_minutes", "time_to_death_hours",
        "age_at_admission", "is_readmission_flag", "days_since_prior_discharge",
        "gender", "anchor_year_group", "dod",
        "diagnosis_count", "primary_icd_code", "procedure_count",
        "max_drg_severity", "max_drg_mortality",
        "icu_stay_count", "total_icu_los_days", "had_icu_stay",
        "lab_count", "abnormal_lab_count", "abnormal_lab_rate",
        "med_admin_count", "positive_culture_count", "transfer_count",
        "created_ts", "updated_ts",
    )
)

# Partition-scoped overwrite: df contains every admission in each touched month (not just
# directly-touched ones -- see the comment above this code block), so this replaces exactly
# those month partitions in full and leaves every other existing month untouched.
_write_partitions = max(4, min(200, len(touched_months) // 50))
df = df.repartition(_write_partitions, F.col("admit_year_month")).sortWithinPartitions("admit_year_month")
df.writeTo("glue_catalog.mimic4_db_business.obt_admission_features").option("fanout-enabled", "true").overwritePartitions()

new_watermark = None
for frame in hadm_keyed_sources + [patient_df]:
    frame_max = frame.select(F.max("updated_ts").alias("wm")).collect()[0]["wm"]
    if frame_max is not None and (new_watermark is None or frame_max > new_watermark):
        new_watermark = frame_max

control_out = (
    spark.createDataFrame([(AGGREGATE_NAME, new_watermark)], ["aggregate_table", "last_processed_ts"])
    .withColumn("updated_ts", F.current_timestamp())
)
control_out.writeTo("glue_catalog.mimic4_db_business.etl_control").option("fanout-enabled", "true").overwritePartitions()

dyf = DynamicFrame.fromDF(df, glueContext, "RefreshObtAdmissionFeatures")
return DynamicFrameCollection({"RefreshObtAdmissionFeatures": dyf}, glueContext)
'''

# obt_patient_360: one row per subject_id. Rolls up from obt_admission_features (not raw
# facts) for every admission-scoped metric that's a simple sum/count/max -- same "coarser
# rolls up from finer" principle as agg_admission_monthly -- except distinct_diagnosis_count,
# which needs fact_diagnosis directly, since a distinct code count across admissions can't
# be derived from obt_admission_features's per-admission counts alone.
#
# Partitioned by anchor_year_group, not subject_id: identity-partitioning by subject_id
# would create one partition per patient (unbounded growth at production scale), and
# Iceberg's bucket() hash transform isn't usable here for a partition-scoped incremental
# refresh (its system.bucket() function isn't reachable from Spark in this environment --
# see the PARTITION_SORT history in etl/create_fact_visual_etl_jobs.py). anchor_year_group
# is MIMIC-IV's fixed de-identification cohort grouping (a handful of values regardless of
# patient count), so it partitions cleanly without that problem. Same grain-finer-than-
# partition subtlety as obt_admission_features applies: touched must expand to every
# subject_id sharing a touched anchor_year_group, not just the directly-touched ones.
OBT_PATIENT_360_CODE = '''\
from pyspark.sql import functions as F

# Placeholder input intentionally unused -- see the module docstring for why this DAG
# needs a non-Iceberg root node at all. The real inputs are read directly below.
_ = dfc.select(list(dfc.keys())[0]).toDF()

AGGREGATE_NAME = "obt_patient_360"
FIRST_RUN_WATERMARK = "1900-01-01T00:00:00"

control_df = spark.table("glue_catalog.mimic4_db_business.etl_control").filter(F.col("aggregate_table") == AGGREGATE_NAME)
watermark_row = control_df.select(F.max("last_processed_ts").alias("wm")).collect()[0]
last_processed_ts = watermark_row["wm"] if watermark_row["wm"] is not None else FIRST_RUN_WATERMARK
watermark_ts = F.lit(last_processed_ts).cast("timestamp")

patient_df = spark.table("glue_catalog.mimic4_db_business.dim_patient")
admission_features_df = spark.table("glue_catalog.mimic4_db_business.obt_admission_features")
diagnosis_df = spark.table("glue_catalog.mimic4_db_business.fact_diagnosis")

changed_patients = patient_df.filter(F.col("updated_ts") > watermark_ts).select("subject_id")
changed_admissions = admission_features_df.filter(F.col("updated_ts") > watermark_ts).select("subject_id")
changed_diagnoses = diagnosis_df.filter(F.col("updated_ts") > watermark_ts).select("subject_id")

directly_touched = (
    changed_patients.union(changed_admissions).union(changed_diagnoses)
    .where(F.col("subject_id").isNotNull()).distinct()
)

if directly_touched.limit(1).count() == 0:
    print(f"{AGGREGATE_NAME}: no source rows changed since watermark {last_processed_ts} -- nothing to refresh.")
    dyf = DynamicFrame.fromDF(spark.createDataFrame([], "subject_id long"), glueContext, "RefreshObtPatient360")
    return DynamicFrameCollection({"RefreshObtPatient360": dyf}, glueContext)

touched_groups = [
    row["anchor_year_group"] for row in
    patient_df.join(directly_touched, "subject_id")
    .select("anchor_year_group").distinct().collect()
]

print(f"{AGGREGATE_NAME}: refreshing all patients across {len(touched_groups)} touched anchor_year_group(s) since watermark {last_processed_ts}.")

base = patient_df.where(F.col("anchor_year_group").isin(touched_groups))
touched_patients = base.select("subject_id")

admission_rollup = (
    admission_features_df.join(touched_patients, "subject_id")
    .groupBy("subject_id")
    .agg(
        F.count("*").cast("int").alias("total_admission_count"),
        F.min("admit_time").alias("first_admit_time"),
        F.max("admit_time").alias("most_recent_admit_time"),
        F.max("disch_time").alias("most_recent_disch_time"),
        (F.max("hospital_expire_flag") == 1).alias("ever_expired_in_hospital"),
        F.sum(F.col("is_readmission_flag").cast("int")).alias("total_readmission_count"),
        F.sum("diagnosis_count").cast("int").alias("total_diagnosis_count"),
        F.sum("procedure_count").cast("int").alias("total_procedure_count"),
        F.sum("icu_stay_count").cast("int").alias("total_icu_stay_count"),
        F.sum("total_icu_los_days").alias("total_icu_los_days"),
        F.sum("lab_count").cast("int").alias("total_lab_count"),
        F.sum("abnormal_lab_count").cast("int").alias("total_abnormal_lab_count"),
        F.sum("med_admin_count").cast("int").alias("total_med_admin_count"),
        F.sum("positive_culture_count").cast("int").alias("total_positive_culture_count"),
        F.sum("transfer_count").cast("int").alias("total_transfer_count"),
    )
)

diagnosis_distinct = (
    diagnosis_df.join(touched_patients, "subject_id")
    .groupBy("subject_id")
    .agg(F.countDistinct("icd_code").cast("int").alias("distinct_diagnosis_count"))
)

df = (
    base
    .join(admission_rollup, "subject_id", "left")
    .join(diagnosis_distinct, "subject_id", "left")
    .withColumn("total_admission_count", F.coalesce(F.col("total_admission_count"), F.lit(0)))
    .withColumn("ever_expired_in_hospital", F.coalesce(F.col("ever_expired_in_hospital"), F.lit(False)))
    .withColumn("total_readmission_count", F.coalesce(F.col("total_readmission_count"), F.lit(0)))
    .withColumn("total_diagnosis_count", F.coalesce(F.col("total_diagnosis_count"), F.lit(0)))
    .withColumn("distinct_diagnosis_count", F.coalesce(F.col("distinct_diagnosis_count"), F.lit(0)))
    .withColumn("total_procedure_count", F.coalesce(F.col("total_procedure_count"), F.lit(0)))
    .withColumn("total_icu_stay_count", F.coalesce(F.col("total_icu_stay_count"), F.lit(0)))
    .withColumn("total_icu_los_days", F.coalesce(F.col("total_icu_los_days"), F.lit(0.0)))
    .withColumn("total_lab_count", F.coalesce(F.col("total_lab_count"), F.lit(0)))
    .withColumn("total_abnormal_lab_count", F.coalesce(F.col("total_abnormal_lab_count"), F.lit(0)))
    .withColumn(
        "overall_abnormal_lab_rate",
        F.when(F.col("total_lab_count") > 0, F.col("total_abnormal_lab_count") / F.col("total_lab_count")).otherwise(F.lit(None).cast("double")),
    )
    .withColumn("total_med_admin_count", F.coalesce(F.col("total_med_admin_count"), F.lit(0)))
    .withColumn("total_positive_culture_count", F.coalesce(F.col("total_positive_culture_count"), F.lit(0)))
    .withColumn("total_transfer_count", F.coalesce(F.col("total_transfer_count"), F.lit(0)))
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
    .select(
        "subject_id", "gender", "anchor_age", "anchor_year", "anchor_year_group", "dod",
        "total_admission_count", "first_admit_time", "most_recent_admit_time", "most_recent_disch_time",
        "ever_expired_in_hospital", "total_readmission_count",
        "total_diagnosis_count", "distinct_diagnosis_count", "total_procedure_count",
        "total_icu_stay_count", "total_icu_los_days",
        "total_lab_count", "total_abnormal_lab_count", "overall_abnormal_lab_rate",
        "total_med_admin_count", "total_positive_culture_count", "total_transfer_count",
        "created_ts", "updated_ts",
    )
)

# Partition-scoped overwrite: df contains every patient in each touched anchor_year_group
# (not just directly-touched ones -- see the comment above this code block), so this
# replaces exactly those group partitions in full and leaves every other group untouched.
_write_partitions = max(4, min(200, len(touched_groups) // 50))
df = df.repartition(_write_partitions, F.col("anchor_year_group")).sortWithinPartitions("anchor_year_group")
df.writeTo("glue_catalog.mimic4_db_business.obt_patient_360").option("fanout-enabled", "true").overwritePartitions()

new_watermark = None
for frame in [patient_df, admission_features_df, diagnosis_df]:
    frame_max = frame.select(F.max("updated_ts").alias("wm")).collect()[0]["wm"]
    if frame_max is not None and (new_watermark is None or frame_max > new_watermark):
        new_watermark = frame_max

control_out = (
    spark.createDataFrame([(AGGREGATE_NAME, new_watermark)], ["aggregate_table", "last_processed_ts"])
    .withColumn("updated_ts", F.current_timestamp())
)
control_out.writeTo("glue_catalog.mimic4_db_business.etl_control").option("fanout-enabled", "true").overwritePartitions()

dyf = DynamicFrame.fromDF(df, glueContext, "RefreshObtPatient360")
return DynamicFrameCollection({"RefreshObtPatient360": dyf}, glueContext)
'''

# obt_icu_stay_features: one row per stay_id. Rolls up total_intake_ml/total_output_ml from
# agg_icu_fluid_balance_daily (not raw fact_input_event/fact_output_event) and
# age_at_admission/gender/hospital_expire_flag from obt_admission_features via hadm_id (a
# plain many-to-one join -- one ICU stay belongs to exactly one admission, so no fan-out
# risk) -- same "coarser rolls up from finer" principle as agg_admission_monthly and
# obt_patient_360. Chart-observation/datetime-event/procedure-event volumes are rolled up
# directly from their fact tables (no coarser precomputed rollup exists for those yet).
#
# Partitioned by in_year_month (derived from in_date_key), not stay_id: same reasoning as
# obt_admission_features's admit_year_month and obt_patient_360's anchor_year_group --
# identity-partitioning by the grain column itself would create unbounded partition growth
# at production scale, and Iceberg's bucket() transform isn't usable here (its
# system.bucket() function isn't reachable from Spark in this environment). Same grain-
# finer-than-partition subtlety applies: touched must expand to every stay_id sharing a
# touched in_year_month, not just the directly-touched ones.
OBT_ICU_STAY_FEATURES_CODE = '''\
from pyspark.sql import functions as F

# Placeholder input intentionally unused -- see the module docstring for why this DAG
# needs a non-Iceberg root node at all. The real inputs are read directly below.
_ = dfc.select(list(dfc.keys())[0]).toDF()

AGGREGATE_NAME = "obt_icu_stay_features"
FIRST_RUN_WATERMARK = "1900-01-01T00:00:00"

control_df = spark.table("glue_catalog.mimic4_db_business.etl_control").filter(F.col("aggregate_table") == AGGREGATE_NAME)
watermark_row = control_df.select(F.max("last_processed_ts").alias("wm")).collect()[0]
last_processed_ts = watermark_row["wm"] if watermark_row["wm"] is not None else FIRST_RUN_WATERMARK
watermark_ts = F.lit(last_processed_ts).cast("timestamp")

icu_df = spark.table("glue_catalog.mimic4_db_business.fact_icu_stay_accumulating")
admission_features_df = spark.table("glue_catalog.mimic4_db_business.obt_admission_features")
fluid_df = spark.table("glue_catalog.mimic4_db_business.agg_icu_fluid_balance_daily")
chart_df = spark.table("glue_catalog.mimic4_db_business.fact_chart_observation")
datetime_df = spark.table("glue_catalog.mimic4_db_business.fact_datetime_event")
procedure_event_df = spark.table("glue_catalog.mimic4_db_business.fact_procedure_event")

stay_keyed_sources = [icu_df, fluid_df, chart_df, datetime_df, procedure_event_df]

directly_touched = None
for frame in stay_keyed_sources:
    changed = frame.filter(F.col("updated_ts") > watermark_ts).select("stay_id")
    directly_touched = changed if directly_touched is None else directly_touched.union(changed)

changed_admissions = admission_features_df.filter(F.col("updated_ts") > watermark_ts).select("hadm_id")
directly_touched = directly_touched.union(icu_df.join(changed_admissions, "hadm_id").select("stay_id"))
directly_touched = directly_touched.where(F.col("stay_id").isNotNull()).distinct()

if directly_touched.limit(1).count() == 0:
    print(f"{AGGREGATE_NAME}: no source rows changed since watermark {last_processed_ts} -- nothing to refresh.")
    dyf = DynamicFrame.fromDF(spark.createDataFrame([], "stay_id long"), glueContext, "RefreshObtIcuStayFeatures")
    return DynamicFrameCollection({"RefreshObtIcuStayFeatures": dyf}, glueContext)

touched_months = [
    row["in_year_month"] for row in
    icu_df.join(directly_touched, "stay_id")
    .withColumn("in_year_month", (F.col("in_date_key") / F.lit(100)).cast("int"))
    .select("in_year_month").distinct().collect()
]

print(f"{AGGREGATE_NAME}: refreshing all ICU stays across {len(touched_months)} touched month(s) since watermark {last_processed_ts}.")

base = (
    icu_df
    .withColumn("in_year_month", (F.col("in_date_key") / F.lit(100)).cast("int"))
    .where(F.col("in_year_month").isin(touched_months))
)
touched_stays = base.select("stay_id")

fluid_rollup = (
    fluid_df.join(touched_stays, "stay_id")
    .groupBy("stay_id")
    .agg(F.sum("total_intake_ml").alias("total_intake_ml"), F.sum("total_output_ml").alias("total_output_ml"))
)
chart_rollup = (
    chart_df.join(touched_stays, "stay_id")
    .groupBy("stay_id")
    .agg(
        F.count("*").cast("int").alias("chart_observation_count"),
        F.sum(F.col("is_abnormal_flag").cast("int")).alias("abnormal_chart_count"),
    )
)
datetime_rollup = (
    datetime_df.join(touched_stays, "stay_id")
    .groupBy("stay_id").agg(F.count("*").cast("int").alias("datetime_event_count"))
)
procedure_event_rollup = (
    procedure_event_df.join(touched_stays, "stay_id")
    .groupBy("stay_id")
    .agg(
        F.count("*").cast("int").alias("procedure_event_count"),
        F.sum("duration_minutes").alias("total_procedure_duration_minutes"),
    )
)
admission_context = admission_features_df.select("hadm_id", "age_at_admission", "gender", "hospital_expire_flag")

df = (
    base
    .join(admission_context, "hadm_id", "left")
    .join(fluid_rollup, "stay_id", "left")
    .join(chart_rollup, "stay_id", "left")
    .join(datetime_rollup, "stay_id", "left")
    .join(procedure_event_rollup, "stay_id", "left")
    .withColumn("total_intake_ml", F.coalesce(F.col("total_intake_ml"), F.lit(0.0)))
    .withColumn("total_output_ml", F.coalesce(F.col("total_output_ml"), F.lit(0.0)))
    .withColumn("net_fluid_balance_ml", F.col("total_intake_ml") - F.col("total_output_ml"))
    .withColumn("chart_observation_count", F.coalesce(F.col("chart_observation_count"), F.lit(0)))
    .withColumn("abnormal_chart_count", F.coalesce(F.col("abnormal_chart_count"), F.lit(0)))
    .withColumn(
        "abnormal_chart_rate",
        F.when(F.col("chart_observation_count") > 0, F.col("abnormal_chart_count") / F.col("chart_observation_count")).otherwise(F.lit(None).cast("double")),
    )
    .withColumn("datetime_event_count", F.coalesce(F.col("datetime_event_count"), F.lit(0)))
    .withColumn("procedure_event_count", F.coalesce(F.col("procedure_event_count"), F.lit(0)))
    .withColumn("total_procedure_duration_minutes", F.coalesce(F.col("total_procedure_duration_minutes"), F.lit(0.0)))
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
    .select(
        "stay_id", "subject_id", "hadm_id", "first_careunit", "last_careunit",
        "in_year_month", "in_date_key", "in_time", "out_time",
        "los", "time_to_icu_hours", "is_icu_readmission_flag",
        "age_at_admission", "gender", "hospital_expire_flag",
        "total_intake_ml", "total_output_ml", "net_fluid_balance_ml",
        "chart_observation_count", "abnormal_chart_count", "abnormal_chart_rate",
        "datetime_event_count", "procedure_event_count", "total_procedure_duration_minutes",
        "created_ts", "updated_ts",
    )
)

# Partition-scoped overwrite: df contains every ICU stay in each touched month (not just
# directly-touched ones -- see the comment above this code block), so this replaces exactly
# those month partitions in full and leaves every other existing month untouched.
_write_partitions = max(4, min(200, len(touched_months) // 50))
df = df.repartition(_write_partitions, F.col("in_year_month")).sortWithinPartitions("in_year_month")
df.writeTo("glue_catalog.mimic4_db_business.obt_icu_stay_features").option("fanout-enabled", "true").overwritePartitions()

new_watermark = None
for frame in stay_keyed_sources + [admission_features_df]:
    frame_max = frame.select(F.max("updated_ts").alias("wm")).collect()[0]["wm"]
    if frame_max is not None and (new_watermark is None or frame_max > new_watermark):
        new_watermark = frame_max

control_out = (
    spark.createDataFrame([(AGGREGATE_NAME, new_watermark)], ["aggregate_table", "last_processed_ts"])
    .withColumn("updated_ts", F.current_timestamp())
)
control_out.writeTo("glue_catalog.mimic4_db_business.etl_control").option("fanout-enabled", "true").overwritePartitions()

dyf = DynamicFrame.fromDF(df, glueContext, "RefreshObtIcuStayFeatures")
return DynamicFrameCollection({"RefreshObtIcuStayFeatures": dyf}, glueContext)
'''

AGGREGATES = [
    {
        "name": "agg_admission_daily",
        "gold_table": "agg_admission_daily",
        "placeholder_raw_table": "d_labitems_raw",
        "class_name": "RefreshAggAdmissionDaily",
        "code_body": AGG_ADMISSION_DAILY_CODE,
    },
    {
        "name": "agg_icu_fluid_balance_daily",
        "gold_table": "agg_icu_fluid_balance_daily",
        "placeholder_raw_table": "d_labitems_raw",
        "class_name": "RefreshAggIcuFluidBalanceDaily",
        "code_body": AGG_ICU_FLUID_BALANCE_DAILY_CODE,
    },
    {
        # Depends on agg_admission_daily -- must be (re-)run after it, not in parallel,
        # same ordering constraint as fact_icu_stay_accumulating depending on fact_admission.
        "name": "agg_admission_monthly",
        "gold_table": "agg_admission_monthly",
        "placeholder_raw_table": "d_labitems_raw",
        "class_name": "RefreshAggAdmissionMonthly",
        "code_body": AGG_ADMISSION_MONTHLY_CODE,
    },
    {
        # First OBT/ABT (see project memory: obt_patient_360 and obt_icu_stay_features
        # are planned next, but only after this one is built and verified). Depends on
        # fact_admission, dim_patient, and 8 other fact tables all being loaded already.
        "name": "obt_admission_features",
        "gold_table": "obt_admission_features",
        "placeholder_raw_table": "d_labitems_raw",
        "class_name": "RefreshObtAdmissionFeatures",
        "code_body": OBT_ADMISSION_FEATURES_CODE,
    },
    {
        # Second OBT. Depends on obt_admission_features -- must be (re-)run after it, not
        # in parallel, same ordering constraint as agg_admission_monthly depending on
        # agg_admission_daily.
        "name": "obt_patient_360",
        "gold_table": "obt_patient_360",
        "placeholder_raw_table": "d_labitems_raw",
        "class_name": "RefreshObtPatient360",
        "code_body": OBT_PATIENT_360_CODE,
    },
    {
        # Third and (per current project memory) final planned OBT. Depends on both
        # obt_admission_features and agg_icu_fluid_balance_daily -- must be (re-)run
        # after both have current data, not in parallel with either.
        "name": "obt_icu_stay_features",
        "gold_table": "obt_icu_stay_features",
        "placeholder_raw_table": "d_labitems_raw",
        "class_name": "RefreshObtIcuStayFeatures",
        "code_body": OBT_ICU_STAY_FEATURES_CODE,
    },
]


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
            Description="Shared role used by all AWS Glue Visual ETL jobs that refresh Gold aggregate tables",
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
        PolicyName="MimicGlueAggRefreshS3Access",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )

    return role_arn


def build_dag(agg, raw_database, gold_database):
    source_node = {
        "S3CatalogSource": {
            "Name": f"{agg['placeholder_raw_table']} (raw, unused placeholder)",
            "Database": raw_database,
            "Table": agg["placeholder_raw_table"],
        }
    }

    # The code bodies are written against the default catalog DB name; retarget them
    # at the dataset's gold database (glue_catalog.<gold_database>.<table>).
    code = agg["code_body"].replace(
        "glue_catalog.mimic4_db_business.", f"glue_catalog.{gold_database}."
    )
    transform_node = {
        "CustomCode": {
            "Name": f"Refresh {agg['name']}",
            "Inputs": ["node-source"],
            "ClassName": agg["class_name"],
            "Code": code,
        }
    }

    return {
        "node-source": source_node,
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
        # obt_admission_features (9-source join) failed with "Could not execute
        # broadcast in 300 secs" -- Spark's auto-broadcast optimizer picked a join side
        # that was small in RESULT size but too slow to COMPUTE (scanning/filtering a
        # larger upstream table first) for 2 G.1X workers to materialize in time.
        # Disabling auto-broadcast forces predictable sort-merge joins everywhere in
        # this script instead of relying on Spark's broadcast-size guess, which is more
        # likely to misfire as more source tables get added to future aggregates/OBTs.
        "--conf spark.sql.autoBroadcastJoinThreshold=-1",
    ])

    common_kwargs = dict(
        Description=f"Incremental Visual ETL refresh for {job_name} (auto-generated)",
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
    parser.add_argument("--create-role", action="store_true", help="Create or reuse a single shared IAM role for all aggregate jobs instead of passing --role-arn")
    parser.add_argument("--role-name", default="MimicGlueAggRefreshRole", help="IAM role base name (suffixed per --dataset; default: MimicGlueAggRefreshRole)")
    parser.add_argument("--raw-database", default=None, help="Glue raw database, for the placeholder DAG root (default: from --dataset)")
    parser.add_argument("--raw-s3-bucket", default=None, help="Raw S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database the aggregate code reads/writes (default: from --dataset)")
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket / Iceberg warehouse (default: from --dataset)")
    parser.add_argument("--scripts-bucket", default=None, help=f"Glue script/temp bucket (default: {config.SCRIPTS_BUCKET})")
    parser.add_argument("--job-prefix", default="agg-refresh-", help="Prefix for generated job names (default: agg-refresh-)")
    parser.add_argument("--glue-version", default="4.0", help="Glue version (must be 4.0+ for native Iceberg support; default: 4.0)")
    parser.add_argument("--worker-type", default="G.1X", help="Worker type (default: G.1X)")
    parser.add_argument("--num-workers", type=int, default=2, help="Number of workers (default: 2)")
    parser.add_argument("--only", default=None, help="Comma-separated subset of aggregate names to generate (default: all)")
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

    selected = AGGREGATES
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        selected = [a for a in AGGREGATES if a["name"] in wanted]
        missing = wanted - {a["name"] for a in selected}
        if missing:
            config.tprint(f"WARNING: unknown aggregate name(s) ignored: {missing}", file=sys.stderr)

    if not selected:
        config.tprint("Nothing to do -- no aggregates selected.", file=sys.stderr)
        sys.exit(1)

    created_jobs = []
    for agg in selected:
        job_name = f"{args.job_prefix}{agg['name']}{args.profile.suffix}"
        config.tprint(f"[{agg['name']}] -> gold:{args.gold_database}.{agg['gold_table']} (incremental)")
        dag = build_dag(agg, args.raw_database, args.gold_database)
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


if __name__ == "__main__":
    main()
