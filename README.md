# Multimodal Fusion Schema — reference implementation on MIMIC-IV

Code and measurement results for the paper *The Multimodal Fusion Schema: Extending the
Star Schema with an Inference Fact Pattern for AI-Native Analytics* (N. P. Nagtode, Golden
Gate University).

The repository builds an Apache Iceberg lakehouse on AWS over MIMIC-IV v3.1 and
MIMIC-IV-Note v2.2: raw CSVs → S3 → a Glue crawler catalogs a bronze layer → Glue jobs load
an Iceberg gold layer → NLP jobs derive fact tables from the clinical notes. Everything is
queryable from Athena (and, for the format-version-3 Inference Fact, from Spark) over one
Glue Data Catalog.

**No MIMIC data is included.** MIMIC-IV and MIMIC-IV-Note are credentialed-access datasets
on [PhysioNet](https://physionet.org/content/mimiciv/); obtain them under the PhysioNet data
use agreement and place them under `data/raw/` (see
[Choosing the dataset](#choosing-the-dataset---dataset)). The result files under `docs/`
contain only aggregate counts, sizes and timings.

## Paper → code map

| Paper element | Where it is built or measured |
|---|---|
| **Source Data Facts** (23) and conformed dimensions (9) | [ddl/gold/mimic_iv_ddl_gold_combined_v4.sql](ddl/gold/mimic_iv_ddl_gold_combined_v4.sql); [etl/create_fact_visual_etl_jobs.py](etl/create_fact_visual_etl_jobs.py), [etl/create_dim_visual_etl_jobs.py](etl/create_dim_visual_etl_jobs.py), [dim_date_seed.py](dim_date_seed.py) |
| **Computed Structured Facts** (aggregates and OBTs) | [etl/create_agg_visual_etl_jobs.py](etl/create_agg_visual_etl_jobs.py) |
| **Unstructured Data Facts** (`fact_discharge_note`, `fact_radiology_note`) | [notes_ingest.py](notes_ingest.py) (multi-line CSV → Parquet), [fact_note_concat.py](fact_note_concat.py) (one row per admission) |
| **Unstructured Features Facts** (`fact_discharge_note_nlp`, `fact_radiology_note_nlp`) | [medspacy_nlp.py](medspacy_nlp.py), [radiology_nlp.py](radiology_nlp.py) |
| **Inference Fact** (`fact_clinician_note_nlp_v`, Iceberg format-v3 `VARIANT`) | [clinician_note_nlp_variant.py](clinician_note_nlp_variant.py), [compact_variant_table.py](compact_variant_table.py) |
| Grain and key of every fact table | [docs/fact_and_aggregate_grain_summary.md](docs/fact_and_aggregate_grain_summary.md) |
| **Dataset** section (source records vs rows loaded) | [docs/iceberg_vs_raw/full/latest.md](docs/iceberg_vs_raw/full/latest.md) ([compare_iceberg_raw_csv.py](compare_iceberg_raw_csv.py)) |
| **RQ1** — thirty-workflow query coverage (100% vs 86.67%) | [docs/query_coverage/rq2_workflow_queries.sql](docs/query_coverage/rq2_workflow_queries.sql), [validate_rq2_queries.py](validate_rq2_queries.py); results in [docs/query_coverage/full/](docs/query_coverage/full/) |
| In-lake vs exported cross-modal join (2.49× / 2.55×) | [measure_native_vs_external_join.py](measure_native_vs_external_join.py); results in [docs/p5/full/latest.md](docs/p5/full/latest.md) |
| **RQ2** — storage by table format and codec (Iceberg, Parquet ZSTD/Snappy, gzip CSV) | comparison DDL in [ddl/gold/](ddl/gold/); copy jobs [parquet_comparison_load.py](parquet_comparison_load.py), [parquet_snappy_comparison_load.py](parquet_snappy_comparison_load.py), [csv_comparison_load.py](csv_comparison_load.py); [measure_parquet_comparison.py](measure_parquet_comparison.py) → [compare_parquet_iceberg.py](compare_parquet_iceberg.py); results in [docs/parquet_comparison/full/latest_comparison.md](docs/parquet_comparison/full/latest_comparison.md) |
| Storage maintenance before measuring (OPTIMIZE / VACUUM / orphan removal) | [compact_gold_tables.py](compact_gold_tables.py), [remove_orphan_files.py](remove_orphan_files.py), [measure_storage_footprint.py](measure_storage_footprint.py) |

**Research-question numbering.** The paper numbers query coverage as RQ1 and storage as
RQ2. File names and some docs in this repository use the opposite, earlier numbering
(`validate_rq2_queries.py` and `rq2_workflow_queries.sql` are the paper's **RQ1**). The
names are unchanged so results stay traceable to the runs that produced them.

**Results provenance.** The `docs/**/latest*` files are the measurements reported in the
paper, from the full-dataset run of 2026-09-30/10-01 (first 1M rows per large fact table,
first 10,000 notes per type for NLP).

Some code comments mention propositions (P3, P5, P7 …) and a sibling data-warehouse
repository. These come from the dissertation the paper is part of and are not needed to
reproduce the paper's results.

## Reproducing the paper's results

```bash
# 1. Build the lakehouse (full dataset, 1M cap per fact table, NLP on the first 10,000 notes per type)
python run_lakehouse_pipeline.py --dataset fulldataset --fresh-reload --with-nlp \
    --row-limit 1000000 --nlp-row-limit 10000 --with-compaction

# 2. RQ1: validate (EXPLAIN) and run the thirty workflow queries
python -m pip install -e .[reports]
python validate_rq2_queries.py --dataset fulldataset --execute

# 3. In-lake vs exported join benchmark
python -m pip install -e .[benchmark]
python measure_native_vs_external_join.py --dataset fulldataset

# 4. RQ2: measure every format/codec copy, then compute the controlled comparison
python measure_storage_footprint.py --dataset fulldataset
python measure_parquet_comparison.py --dataset fulldataset
python compare_parquet_iceberg.py --dataset fulldataset
python compare_iceberg_raw_csv.py --dataset fulldataset
```

Run `python <script> --help` for every option. A full-dataset build runs dozens of Glue
jobs and incurs AWS charges; `--dataset demodataset` (the default) builds the same schema
over the free MIMIC-IV demo for a cheap end-to-end check.

## Project layout

- `data/raw/` — local MIMIC source files (git-ignored, not distributed). `mimic-iv-clinical-database-demo-2.2/` for `--dataset demodataset` (the default); `mimic-iv-3.1-fulldataset/` (~19 GB) for `--dataset fulldataset`.
- [run_lakehouse_pipeline.py](run_lakehouse_pipeline.py) — one-command end-to-end driver: setup → create every Glue job → run them in dependency order. `--dataset`, `--with-nlp`, `--row-limit N` (fact-table cap), `--nlp-row-limit N` (NLP note cap), `--from <PHASE>`, `--dry-run`. Logs every job to `etl_process_log`.
- [setup_mimic4_s3_glue.py](setup_mimic4_s3_glue.py), [src/mimic_lakehouse/](src/mimic_lakehouse/) — buckets, upload, crawler, Gold DDL; `config.py` resolves per-dataset resource names (see [.env.example](.env.example)).
- [ddl/](ddl/) — Gold-layer DDL, the format-comparison DDL, and table maintenance SQL.
- [etl/](etl/) — creators for every Glue job (dimensions, facts, aggregates/OBTs, notes ingest, NLP, Inference Fact, comparison copies, orphan-file removal). The matching `run_*.py` files in the root are thin drivers for them.
- Root Glue scripts — the Spark code those jobs run (`notes_ingest.py`, `fact_note_concat.py`, `medspacy_nlp.py`, `radiology_nlp.py`, `clinician_note_nlp_variant.py`, `compact_variant_table.py`, `*_comparison_load.py`, `remove_orphan_files.py`, `dim_date_seed.py`).
- Measurement scripts — `validate_rq2_queries.py`, `measure_native_vs_external_join.py`, `measure_storage_footprint.py`, `measure_parquet_comparison.py`, `compare_parquet_iceberg.py`, `compare_iceberg_raw_csv.py`, `list_table_record_counts.py`.
- [docs/](docs/) — results reported in the paper, the query catalogue, the grain summary, and [lessons learned](docs/lessons_learned_glue_iceberg_performance.md) on Glue/Iceberg performance.
- [cleanup_mimic4_environment.py](cleanup_mimic4_environment.py) — removes every AWS resource the pipeline creates. See [Full environment cleanup](#full-environment-cleanup).
- [tests/](tests/) — unit tests (`pytest`).

## Setup

```bash
python -m pip install -e .
```

## AWS CLI setup (first time)

If you have not used AWS from this machine before, configure the AWS CLI and enter your credentials once:

```bash
aws configure
```

You will be prompted for:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name — use the same one throughout (this project was built in `us-east-2`; also settable as `AWS_REGION` in `.env`)
- Default output format (for example `json`)

You can also verify that your credentials are working:

```bash
aws sts get-caller-identity
```

If the command succeeds, your AWS client is configured correctly and the workflow can use it.

## Choosing the dataset (`--dataset`)

Every load / ETL / measurement / cleanup driver takes `--dataset {demodataset,fulldataset}`
(default `demodataset`, overridable with `DATASET=` in `.env`). The choice resolves — via
`DatasetProfile` in [src/mimic_lakehouse/config.py](src/mimic_lakehouse/config.py) — to a
fully isolated set of resource names, so the two datasets can be built and measured side by
side without colliding:

| | `demodataset` | `fulldataset` |
|---|---|---|
| local raw CSVs | `data/raw/mimic-iv-clinical-database-demo-2.2/` | `data/raw/mimic-iv-3.1-fulldataset/` (~19 GB) |
| raw / landing S3 bucket | `mimic4-datalake-v3-2-demo` | `mimic4-datalake-v3-2-full` |
| Iceberg gold S3 bucket | `mimic4-lakehouse-v3-2-demo` | `mimic4-lakehouse-v3-2-full` |
| raw Glue catalog DB (bronze) | `mimic4_db_raw_demo` | `mimic4_db_raw_full` |
| gold Glue catalog DB (business) | `mimic4_db_business_demo` | `mimic4_db_business_full` |
| NLP bucket | `mimic4-nlp-v3-2-demo` | `mimic4-nlp-v3-2-full` |
| Glue crawler | `mimic4-data-v3-2-crawler-demo` | `mimic4-data-v3-2-crawler-full` |
| Glue jobs | `…-demo` (e.g. `dim-load-dim_patient-demo`) | `…-full` |
| IAM roles | `MimicGlue*Role-demo` | `MimicGlue*Role-full` |
| measurement outputs | `docs/<area>/demo/…` | `docs/<area>/full/…` |

Shared between datasets: the Glue scripts bucket (`mimic4-glue-scripts-v3-2-bucket`) and the
format-only CSV classifier (`mimic4_csv_classifier`). The raw S3 upload is idempotent — a
file already present at the same size is skipped — so re-running a load is cheap.

Every explicit flag in the examples below (`--bucket`, `--database`, `--gold-bucket`, …) is
optional: pass `--dataset` alone and the value comes from the profile; pass an explicit flag
to override just that one. To load the full dataset end to end, add `--dataset fulldataset`
to each command in the runbook.

### AWS resource tagging

Every AWS resource this repo provisions — S3 buckets, IAM roles, Glue jobs, the crawler —
is tagged **`Project=LAKEHSE`** on creation (override with `PROJECT_TAG` in `.env`).
Activate the `Project` key under **Billing → Cost allocation tags** to see this project's
spend in Cost Explorer. Glue
*databases*, *classifiers* and *catalog tables* are not taggable in AWS, so those carry no
tag; everything with a per-resource cost does.

## Run the whole pipeline (one command)

[run_lakehouse_pipeline.py](run_lakehouse_pipeline.py) does every step below in order — setup (buckets, upload,
crawler, Gold DDL), creates all the Glue job definitions, then runs them in dependency
order, **waiting** for each Glue job to finish before starting the ones that depend on it
(`fact_icu_stay_accumulating` after `fact_admission`; `agg_admission_monthly` after
`agg_admission_daily`; the OBTs after their sources).

```bash
python run_lakehouse_pipeline.py                          # demo dataset, structured tables only
python run_lakehouse_pipeline.py --dataset fulldataset     # full dataset, every fact loaded in full
python run_lakehouse_pipeline.py --dataset fulldataset --with-nlp --row-limit 1000000 --nlp-row-limit 50000
                                                          # full dataset, 1M cap on every fact table,
                                                          # 50,000-note cap on medSpaCy/radiology NLP
python run_lakehouse_pipeline.py --with-nlp                # also notes-ingest + medSpaCy/radiology NLP (10k-note cap)
python run_lakehouse_pipeline.py --dry-run                 # print the phase plan, touch nothing
python run_lakehouse_pipeline.py --skip-setup --skip-create --from D   # resume at phase D after a failure
```

**`--row-limit N`** caps every fact table to a `df.limit(N)` sample of its raw source
(tables already smaller than `N` are untouched) — `--row-limit 1000000` is the standard
1M cap for the full dataset, keeping `chartevents` / `labevents` / `emar` / `*events` at a
million rows instead of hundreds of millions. It does **not** affect NLP.

**`--nlp-row-limit N`** (default `10000`) separately caps the medSpaCy / radiology NLP
note set (`--note-limit` on those job creators), kept independent of `--row-limit`:
medSpaCy tags notes one at a time (~1-1.5s/note observed on 2×G.1X), so a fact-table-scale
cap like 1,000,000 notes would take far longer than a single Glue job Timeout. Pass
`--nlp-row-limit 0` to process every note (`--all-notes`). Repeat both flags on any
resumed run that re-creates the job definitions (i.e. without `--skip-create`).

**Forcing a full (non-incremental) reload.** The dim/fact jobs always fully reload every
run — no flag needed. The agg/OBT jobs and (with `--with-nlp`) the NLP jobs are
**incremental by default** via the `<gold_database>.etl_control` watermark, and neither
those job creators nor `run_lakehouse_pipeline.py` expose a `--refresh-mode` flag to force
them past it. The one clean way to force a true full reload of everything is to clear
`etl_control` first, so every watermark-based job sees "no watermark" and runs its
full-backfill path — the same code path a first-ever run takes:

```bash
# 1. Clear the watermark table so agg/OBT (and NLP, if included) treat this as a fresh backfill
aws athena start-query-execution \
  --query-string "DELETE FROM mimic4_db_business_full.etl_control" \
  --query-execution-context Database=mimic4_db_business_full \
  --result-configuration OutputLocation=s3://mimic4-glue-scripts-v3-2-bucket/athena-results/ \
  --region us-east-2

# 2. Run the full pipeline
python run_lakehouse_pipeline.py --dataset fulldataset --row-limit 1000000
```

Add `--with-nlp --nlp-row-limit 50000` (or whatever cap) to the same command to also fully
reprocess the NLP tables — with `etl_control` cleared, they run their `auto` full-backfill
path too, so `--refresh-mode full` isn't needed separately on top of this.

**Worker sizing + concurrency (full dataset).** The dim/fact/agg jobs default to 2 × G.1X,
which OOMs on the multi-100M-row raw tables. `--num-workers` / `--worker-type` /
`--glue-version` set the default for every job; `--big-worker-type` (default `G.2X`) ×
`--big-num-workers` (default 10) apply to the `--big-jobs` list — the fact/OBT jobs that
read `chartevents` / `labevents` / `emar` / `poe` / `prescriptions` / `*events`. Within a
phase the pipeline launches jobs in **DPU-budgeted waves** capped at `--max-dpu` (default
90; the account's hard concurrent-capacity limit is 100 DPU — G.1X = 1 DPU/worker, G.2X =
2). `--only fact_lab_result,fact_chart_observation` restricts a resumed run to specific
jobs (targeted catch-up after a partial failure).

```bash
python run_lakehouse_pipeline.py --dataset fulldataset --max-dpu 90
# resume just two jobs after a failure, then continue:
python run_lakehouse_pipeline.py --dataset fulldataset --from C --only fact_lab_result,fact_chart_observation
python run_lakehouse_pipeline.py --dataset fulldataset --from D
```

**Job timing log.** Every time a Glue job the driver started reaches a terminal state
(`SUCCEEDED`/`FAILED`/`STOPPED`/`TIMEOUT`/`ERROR`), it appends one row to
`<gold_database>.etl_process_log` — `full_run_id`, `run_id`, `job_name`,
`process_name`/`process_type` (parsed from the job name, e.g.
`fact-load-fact_admission-full` → `fact` / `fact_admission`), `phase`, `dataset`,
`start_ts`/`end_ts` (Glue's own `StartedOn`/`CompletedOn`), `execution_seconds` (Glue's
`ExecutionTime`, excludes queue/startup), `dpu_seconds`, `raw_row_count`/`gold_row_count`
(a `COUNT(*)` of the job's source raw table and destination gold table, taken right after
it finishes; `NULL` where not applicable — e.g. aggregates read another gold table, not a
raw one), `status`, and `error_message`. `full_run_id` is one number shared by every row
from the same driver invocation (`MAX(full_run_id) + 1` over the table's existing rows,
so it survives a `--from <PHASE>` resume being a separate invocation) — group by it to see
one end-to-end run's jobs together. Unlike `etl_control` (one current watermark row per
target, overwritten every run), this table is append-only, so a full run history
accumulates — query it in Athena to compare timings across runs (e.g. before/after a
partitioning fix, or demo vs. full scale):

```sql
SELECT process_type, process_name, status, execution_seconds, dpu_seconds,
       raw_row_count, gold_row_count, start_ts
FROM mimic4_db_business_full.etl_process_log
WHERE full_run_id = (SELECT max(full_run_id) FROM mimic4_db_business_full.etl_process_log)
ORDER BY start_ts;
```

Pass `--no-process-log` to disable it. A logging failure (e.g. the table doesn't exist yet
on an older deployment) only prints a warning — it never aborts the pipeline.


Everything is idempotent; on a job failure the script stops and prints the exact
`--from <PHASE>` command to resume. The manual step-by-step below is still available for
running or re-running any single piece.

## Manual step-by-step

`run_lakehouse_pipeline.py` runs all of the below in order. Run the individual steps by
hand when you want to (re-)run one piece, inspect a job's DAG in Glue Studio, or debug.
Every command takes `--dataset` (default `demodataset`); add `--dataset fulldataset` to
each one for the full dataset. `--scripts-bucket`, `--raw-s3-bucket`, `--gold-s3-bucket`,
`--database`, `--crawler-name`, and the role names all default from the `--dataset`
profile — pass one explicitly only to override it.

### 1. Setup: buckets, upload, crawler, Gold DDL

```bash
python setup_mimic4_s3_glue.py --create-role
```

Creates the raw / gold / scripts buckets (`Project=LAKEHSE`-tagged), uploads the local
CSVs (idempotent — same-size files are skipped), creates the crawler IAM role
(`MimicGlueCrawlerRole-demo`) + the `mimic4_csv_classifier`, creates + runs the crawler
(`mimic4-data-v3-2-crawler-demo` → `mimic4_db_raw_demo`), patches a few mis-classified raw
tables, then applies the gold-zone DDL via Athena (`mimic4_db_business_demo`, empty
Iceberg tables).

Flags:
- `--iam-role-arn <arn>` — use an existing crawler role instead of `--create-role`
- `--skip-load` — skip the raw half (bucket, upload, role, crawler); still runs the gold DDL. Use it to re-apply only the Gold DDL against data already loaded.
- `--skip-gold-ddl` — stop after the crawler; don't create the gold tables

The crawler IAM role is granted `AWSGlueServiceRole`, S3 read on the scripts + raw
buckets, and S3 read/write on the gold bucket.

### 2. dim_date (calendar spine)

`dim_date` has no raw CSV source — it's generated by a standalone Glue **script** job
(`dim_date_seed.py`), created and run by:

```bash
python etl/create_dim_date_seed_job.py --create-role --run-now
```

This creates the `dim-date-seed-job-demo` Glue job (role `MimicGlueDimDateSeedRole-demo`)
and starts a run that writes `glue_catalog.mimic4_db_business_demo.dim_date`.
(`run_dim_date_seed.py` only *prints* a standalone PySpark snippet — it does not deploy
anything; use `create_dim_date_seed_job.py` for the real job.)

### 3. Raw-sourced Gold dimensions

Unlike `dim_date`, most Gold dimensions are sourced from a raw crawled table, so each is loaded by its own AWS Glue Studio Visual ETL job (`<raw table>` -> ApplyMapping -> Custom Transform (audit columns) -> `<gold table>`), all generated from one script. The audit columns (`created_ts`, `updated_ts`, `created_by`, `updated_by`) are populated with a Custom Transform node using the PySpark DataFrame API, not a SQL-based transform node.

Create/update and run every dimension job with:

```bash
python run_dim_visual_etl.py --create-role --run-now
```

This loads `etl/create_dim_visual_etl_jobs.py` and creates one job per entry in its `DIMENSIONS` list (`dim_hcpcs`, `dim_diagnosis`, `dim_procedure`, `dim_lab_item`, `dim_patient`, `dim_provider`, `dim_caregiver`, `dim_chart_item`), named `dim-load-<dimension>-<demo|full>`. All jobs share a single IAM role (`MimicGlueDimLoadRole-<demo|full>`). Use `--only dim_diagnosis,dim_procedure` to (re)create a subset. Each job's DAG can be viewed and edited in the AWS Glue Studio console.

`dim_date` is excluded (see above) -- it's a calendar spine with no raw CSV source. `provider_raw` and `caregiver_raw` are single-column CSVs that Glue's built-in CSV classifier doesn't reliably identify (crawled with zero/wrong columns); `aws_workflow.py`'s setup workflow overwrites both with an explicit schema (`overwrite_provider_table()` / `overwrite_caregiver_table()`) after the crawler runs, so `dim_provider` and `dim_caregiver` load correctly.

Each job's Custom Transform writes with Iceberg's native `df.writeTo(...).overwritePartitions()`, not Glue's `write_data_frame.from_catalog(additional_options={"overwrite": "true"})` -- that call turned out to silently call Iceberg's `.append()` regardless of the `overwrite` option, so any dimension job that was ever re-run quietly appended a second copy of its data instead of replacing it. If you loaded dimensions before this fix, re-run the affected jobs once to deduplicate (`SELECT count(*)` vs `SELECT count(DISTINCT <key>)` per table will show 2x if still affected).

### 4. Raw-sourced Gold facts

Fact/bridge tables are loaded the same way as the dimensions -- one Visual ETL job per table (`<raw table>` -> ApplyMapping -> Custom Transform -> `<gold table>`) -- but the Custom Transform does more work: alongside the audit columns, it derives every `*_date_key` role-playing column using the same surrogate-key formula as `dim_date` (`CAST(DATE_FORMAT(<col>, 'yyyyMMdd') AS INT)`, applied to the already-cast sibling Gold timestamp/date column, not the raw column), and populates the "Calculated / value-added" measures documented in the Gold DDL (e.g. `duration_minutes`, `admin_delay_minutes`, `transfer_duration_hours`) via `pyspark.sql.functions`, never a SQL string. Four tables (`fact_lab_result`, `fact_chart_observation`, `fact_admission`, `fact_icu_stay_accumulating`) also need a lookup against another Gold table for one calculated column -- done as a plain Spark `spark.table(...)` read + `DataFrame.join()` inside the Custom Transform, not a Glue Join node (a Join node's `S3CatalogSource` reads via `glueContext.create_dynamic_frame.from_catalog`, which doesn't support Iceberg tables at all).

Create/update and run every fact job with:

```bash
python run_fact_visual_etl.py --create-role --run-now
```

This loads `etl/create_fact_visual_etl_jobs.py` and creates one job per entry in its `FACTS` list (all 23 fact/bridge tables), named `fact-load-<table>-<demo|full>`. All jobs share a single IAM role (`MimicGlueFactLoadRole-<demo|full>`). Use `--only fact_transfer,fact_procedure` to (re)create a subset.

**`fact_icu_stay_accumulating` joins `fact_admission` itself** (via `hadm_id`, for `time_to_icu_hours`) -- its job must be (re-)run *after* `fact_admission` has current data, not in parallel with it. `run_lakehouse_pipeline.py` handles this ordering automatically (phase D after phase C); running by hand, do `--only fact_icu_stay_accumulating` as a separate step once `fact-load-fact_admission-*` has SUCCEEDED.

Several fact tables (`fact_provider_order`, `fact_lab_result`) had their `PARTITIONED BY (day(...))` clause removed from the DDL and their tables recreated -- with only a few dozen rows per calendar day, Iceberg's partitioned writer opens far more small files than 2 G.1X workers can shuffle reliably, causing intermittent `IllegalStateException`/`MetadataFetchFailedException` failures. If a fact job fails the same way, check its row-count-to-distinct-day ratio before assuming it's a code bug.

The column-level raw-to-Gold mapping was cross-checked against each raw table's *live* crawled schema, not assumed from the Gold DDL or the mapping workbook alone -- this caught real bugs in the deployed DDL itself: `fact_medication_administration_mini_detail` and `fact_pharmacy_order` each had several text/date columns mistyped as `DOUBLE`/`BIGINT` (confirmed against real raw CSV values, e.g. `route`, `side`, `site`, `product_code`, `expiration_date`), corrected in `ddl/gold/mimic_iv_ddl_gold_combined_v4.sql` with both tables recreated before their ETL jobs were built. It also caught a genuine calculated-column bug (`fact_microbiology_result.is_positive_culture_flag`: blank CSV fields land as empty string, not `NULL`, so checking `isNotNull()` alone marked every row positive) and a raw-crawler type bug: `d_icd_procedures_raw.icd_code` was crawled as `bigint`, but ICD-10-PCS procedure codes are alphanumeric (e.g. `001U3J6`), so `dim_procedure.icd_code` was `NULL` for the ~95% of rows that are ICD-10. Fixed the same way as `provider_raw`/`caregiver_raw`: `overwrite_d_icd_procedures_table()` in `aws_workflow.py` patches the raw table's column type in place (unlike the other two, this preserves the crawler's existing CSV/SerDe settings rather than reconstructing them, since `long_title` has embedded commas inside quoted values that are already parsed correctly).

### 5. Gold aggregate / summary tables (+ OBTs)

Aggregate tables (e.g. `agg_admission_daily`) are rollups computed from already-loaded Gold fact tables, refreshed **incrementally** rather than full-reloaded like every dim/fact job above. Each job:

1. Reads a watermark from `<gold db>.etl_control` (the max source `updated_ts` already processed by this aggregate's last successful run -- a very old default on first run, so the first run is a full backfill).
2. Finds every calendar day (`date_key`) touched by source rows with `updated_ts` newer than that watermark -- catching corrections to existing rows, not just new ones.
3. Fully recomputes only those touched days from the source table's current state (recompute-and-replace per day, not incremental addition -- safe against corrections/deletes in the source).
4. Writes with the same `df.writeTo(...).overwritePartitions()` call every dim/fact job already uses -- but since the result only contains the touched `date_key` values and the table is identity-partitioned by `date_key`, this is a genuine partition-scoped overwrite: every other day's existing row is left untouched.
5. Advances the watermark in `etl_control` (also identity-partitioned, by `aggregate_table`, so this too is a scoped overwrite of just one row).

This is the mechanism intended to survive a large production data-volume increase where the dim/fact jobs' full-reload pattern would not: a refresh's cost scales with how many **calendar days** changed, not with total fact-table row volume.

Create/update and run every aggregate job with:

```bash
python run_agg_visual_etl.py --create-role --run-now
```

This loads `etl/create_agg_visual_etl_jobs.py` and creates one job per entry in its `AGGREGATES` list, named `agg-refresh-<name>-<demo|full>`. Several have ordering dependencies (`agg_admission_monthly` after `agg_admission_daily`; the OBTs after their sources) — `run_lakehouse_pipeline.py` sequences these across phases E→F→G; running by hand, use `--only` to do them in order. The jobs:

- **`agg_admission_daily`** -- daily admit/discharge/death counts per `admission_type`, rolled up from `fact_admission`.
- **`agg_icu_fluid_balance_daily`** -- daily net ICU fluid balance per `stay_id` (`total_intake_ml` from `fact_input_event.amount` minus `total_output_ml` from `fact_output_event.value`, both day-bucketed). `fact_ingredient_event` is deliberately excluded -- it's a child grain of `fact_input_event` (ingredient-level decomposition of the *same* administered volume), so summing both would double-count intake. `amount_uom` on `fact_input_event` is also not consistently a volume unit -- the same column carries medication/electrolyte doses (`mg`, `mcg`, `units`, `mEq`, `mmol`, `grams`) alongside `ml` -- confirmed against the actual loaded data, not assumed from documentation, so only rows where the unit is `ml` (case-insensitive) are summed.
- **`agg_admission_monthly`** -- monthly admit/discharge/death counts per `admission_type`, rolled up from **`agg_admission_daily`**, not raw `fact_admission`. `year_month` is derived from `date_key` (`yyyyMMdd`) by integer-dividing by 100 -- no `dim_date` join needed. This is the "monthly rolls up from daily" principle in practice: its watermark and touched-partition set are computed against the small daily table, so its refresh cost is bounded by how many months changed, never rescanning `fact_admission` itself. Because of this dependency, **`agg_admission_monthly` must be (re-)run after `agg_admission_daily` has current data**, not in parallel with it -- same ordering constraint as `fact_icu_stay_accumulating` depending on `fact_admission`.
- **`obt_admission_features`** -- the first OBT/ABT (One Big Table / Analytics Base Table): one row per `hadm_id`, denormalizing `fact_admission` (joined to `dim_patient` for demographics) with rollup counts/flags from 8 other fact tables -- diagnosis/procedure counts and the primary (`seq_num=1`) diagnosis code, max DRG severity/mortality, ICU stay count and total ICU LOS, lab volume and abnormal-result rate, medication administration volume, positive microbiology culture count, and ward transfer count. A feature table for readmission/mortality-prediction style analysis. Unlike the pure aggregates, this table's row grain (`hadm_id`) is *finer* than its partition (`admit_year_month`), so its refresh recomputes every admission sharing a touched month, not just the directly-touched ones, or a partition-scoped overwrite would silently drop the untouched admissions sharing that month.
- **`obt_patient_360`** -- the second OBT: one row per `subject_id`, rolling up admission-scoped counts/flags across *every* admission for that patient (total admission/readmission/ICU-stay counts, total ICU LOS, lab/med/culture/transfer volumes, first/most-recent admit and discharge times, ever-expired-in-hospital flag). Rolls up from **`obt_admission_features`**, not raw facts, for every metric that's a simple sum/count/max -- the same "coarser rolls up from finer" principle as `agg_admission_monthly` -- except `distinct_diagnosis_count`, which needs `fact_diagnosis` directly (a distinct-code count across admissions can't be derived from per-admission counts alone; verified against a direct `COUNT(DISTINCT icd_code)` for a sample patient, exact match). Partitioned by `anchor_year_group` (MIMIC-IV's fixed, small de-identification cohort grouping), not `subject_id` -- identity-partitioning by `subject_id` would create one partition per patient (unbounded growth at production scale), and Iceberg's `bucket()` hash transform isn't usable for a partition-scoped incremental refresh in this environment (its `system.bucket()` function isn't reachable from Spark here). Because it depends on `obt_admission_features`, **`obt_patient_360` must be (re-)run after `obt_admission_features` has current data**, not in parallel with it.
- **`obt_icu_stay_features`** -- the third OBT: one row per `stay_id`, combining `fact_icu_stay_accumulating` (care unit, LOS, calculated columns) with `total_intake_ml`/`total_output_ml` rolled up from **`agg_icu_fluid_balance_daily`** (not raw `fact_input_event`/`fact_output_event`) and `age_at_admission`/`gender`/`hospital_expire_flag` joined from **`obt_admission_features`** via `hadm_id` (a plain many-to-one join -- one ICU stay belongs to exactly one admission, no fan-out risk). Chart-observation volume/abnormal-rate, datetime-event volume, and procedure-event volume/duration are rolled up directly from their fact tables, since no coarser precomputed rollup exists for those yet. Partitioned by `in_year_month` (derived from `in_date_key`), same reasoning as the other two OBTs' partition choices. Because it depends on both `obt_admission_features` and `agg_icu_fluid_balance_daily`, **it must be (re-)run after both have current data**. Its first backfill took ~23 minutes (vs. ~2-4 minutes for the other OBTs) since it joins `fact_chart_observation` -- at 1.3M+ rows, the largest table in the whole schema -- but the incremental no-op path (no chart-table scan needed) ran in ~2 minutes, confirming the incremental design's payoff.

This completes the three OBTs originally planned (see the memory note on OBT build order) -- all verified with the same rigor: exact-match totals against a direct aggregation of source tables on backfill, and a confirmed no-op on immediate re-run.

Each job's DAG is unusual: its root node is a small, unrelated **raw** table (`d_labitems_raw`) whose output is never used. This is a workaround, not a design choice -- a Glue Visual ETL job's DAG needs at least one node with no upstream inputs to start from, but every one of these jobs' actual inputs (Gold tables like `fact_admission`, `dim_patient`, `agg_admission_daily`, `etl_control`, etc.) are **Iceberg**, and a Glue `S3CatalogSource` node reads via `glueContext.create_dynamic_frame.from_catalog`, which doesn't support Iceberg tables at all. So the Custom Transform ignores the placeholder input entirely and reads every real input itself via `spark.table("glue_catalog.<db>.<table>")` -- an ordinary DataFrame API call, not a SQL statement.

`obt_admission_features`'s first backfill run (9 source tables joined/aggregated) failed with `Could not execute broadcast in 300 secs` -- Spark's auto-broadcast optimizer picked a join side that was small in result size but too slow to *compute* (scanning a larger upstream table first) for 2 G.1X workers to materialize in time. Fixed by adding `--conf spark.sql.autoBroadcastJoinThreshold=-1` to every aggregate/OBT job, forcing predictable sort-merge joins instead of relying on Spark's broadcast-size guess -- not an index or partitioning issue (Iceberg tables here have no secondary indexes at all).

Each table was verified the same way: totals match a direct aggregation of its source table(s) exactly on the initial backfill (`agg_admission_daily`: 275/275/15 admit/discharge/death; `agg_icu_fluid_balance_daily`: 1,621,564.66 mL intake / 1,319,257.0 mL output, matching a direct `SUM` over the filtered raw facts to the decimal; `agg_admission_monthly`: same 275/275/15 rolled up from the daily table; `obt_admission_features`: row count matches `fact_admission` exactly, and every rollup total matches a direct join/aggregation against its source fact -- including confirming that `fact_lab_result`/`fact_medication_administration`/`fact_microbiology_result`/`fact_transfer` undercounts vs. their raw totals are correct behavior, not a join bug, since those tables carry rows not tied to any admission in `fact_admission`, e.g. 28,420 of 107,727 lab rows have a `NULL hadm_id`; `obt_patient_360`: row count matches `dim_patient` exactly (100), every rollup total matches `obt_admission_features`'s totals exactly, and `distinct_diagnosis_count` matches a direct `COUNT(DISTINCT icd_code)` for a sample patient; `obt_icu_stay_features`: row count matches `fact_icu_stay_accumulating` exactly (140), and every rollup total -- fluid intake/output, chart observation/abnormal counts, datetime-event count, procedure-event count/duration -- matches a direct aggregation of its source table exactly), and re-running immediately afterward (no source changes) logs `no source rows changed since watermark ... -- nothing to refresh` and leaves each table's row count unchanged -- confirming the watermark/skip logic actually works, not just the full-backfill path.

## NLP pipelines (clinical free-text notes) — overview

Everything above is built from the structured MIMIC-IV CSVs. The pipelines below add fact tables derived from the **free-text notes** (MIMIC-IV-Note): tobacco/alcohol/obesity features from discharge summaries, and procedure/finding/ICD extraction from radiology reports.

The extraction is **entirely local, rule-based medSpaCy** (spaCy + PyRuSH + TargetMatcher +
the ConText negation algorithm) — no managed service, no per-character billing. They are
**optional and opt-in**: none run as part of `setup_mimic4_s3_glue.py` (use
`python run_lakehouse_pipeline.py --with-nlp`, or the drivers below). Each `run_*.py` driver
creates its IAM role(s), its Glue job, and the S3 folders it needs, then (`--run-now`)
starts the job. All take `--dataset` and share the `mimic4-nlp-v3-2-<demo|full>` bucket;
result-prefix names come from `.env` (see [.env.example](.env.example)).

### Run order

| # | Command | Produces | Engine / runtime | Notes |
|---|---|---|---|---|
| 1 | `python run_notes_ingest.py --create-role --run-now` | `discharge_note_raw`, `radiology_note_raw` | Spark, Glue 4.0 | prerequisite for all of the below; demo filters to the demo-subject list, full keeps every note (`--all-subjects` overrides) |
| 2a | `python run_medspacy_nlp.py --create-role --run-now` | `fact_discharge_note_nlp` | medSpaCy, Glue 4.0 | discharge-note features; 100,000-note cap by default |
| 2b | `python run_radiology_nlp.py --create-role --run-now` | `fact_radiology_note_nlp` | section splitter + medSpaCy, Glue 4.0 | radiology-note features; 100,000-note cap by default |
| 3 | `python run_clinician_note_variant.py --create-role --run-now` | `fact_clinician_note_nlp_v` (Inference Fact) | Spark, **Glue 6.0** | after 2a and 2b; VARIANT-typed, Spark-readable only |

The `mimic4_db_raw` / `mimic4_db_business` / `s3://mimic4-nlp-v3-2/…` names in the walkthroughs
below show the **demo** values — read them as `…_demo` / `mimic4-nlp-v3-2-demo` (or `…_full` /
`mimic4-nlp-v3-2-full` under `--dataset fulldataset`).

Steps 2a and 2b are independent. Each NLP job defaults to a **100,000-note cap** (`--note-limit N`) -- lowered from the 1M fact-table cap because medSpaCy's per-document cost makes a million-note run take far longer than a single Glue job Timeout on the default 2×G.1X; pass `--all-notes` for the full corpus. Re-runs are **incremental** by default (`--refresh-mode auto`) via the `etl_control` watermark — only admissions with a note newer than the last run are re-processed and `MERGE`d; `--refresh-mode full` forces a full re-process. The Glue jobs / roles / NLP bucket are all torn down by `python cleanup_mimic4_environment.py --dataset <mode> --yes`.

## Loading the clinical notes (MIMIC-IV-Note)

Drop `discharge.csv.gz` / `radiology.csv.gz` (from the separate **MIMIC-IV-Note** download) into the dataset's `notes/` dir (`data/raw/mimic-iv-clinical-database-demo-2.2/notes/` for demo, `data/raw/mimic-iv-3.1-fulldataset/notes/` for full). `setup_mimic4_s3_glue.py` uploads them to `s3://<raw-bucket>/discharge_raw/` and `radiology_raw/`, but the **crawler skips every note prefix** and `drop_stale_note_crawler_tables()` removes any table a prior crawl made: the note `text` column has newlines inside quoted fields, which no Hive/Athena CSV SerDe can parse (`TextInputFormat` splits on `\n`, so one note is shredded across hundreds of rows and the typed columns throw `BAD_DATA`).

Instead, convert them with:

```bash
python run_notes_ingest.py --create-role --run-now
```

This creates/runs `notes-ingest-job-<demo|full>`, which reads each gz with a `multiLine=true` Spark CSV reader, filters to the demo-subject list (demo dataset only; full keeps every note, and `--all-subjects` forces that for demo too), writes Parquet to `<name>_note_raw/`, and registers `<raw db>.discharge_note_raw` / `radiology_note_raw` itself. Verified (demo): `discharge_note_raw` 243 rows / 100 subjects, `radiology_note_raw` 1,799 rows / 99 subjects, every row's `note_id` distinct.

## Deriving NLP features from the discharge notes (medSpaCy)

`run_medspacy_nlp.py` runs a Glue job that extracts tobacco/alcohol/obesity levels and a
tobacco-cessation code from the discharge notes with [**medSpaCy**](https://github.com/medspacy/medspacy)
(spaCy + PyRuSH sentence segmentation + a rule-based `TargetMatcher` + the **ConText**
algorithm for negation / historical / hypothetical / family) and loads
`mimic4_db_business.fact_discharge_note_nlp`. Entirely local — no managed service.

```bash
python run_medspacy_nlp.py --create-role --run-now
```

The generator (`etl/create_medspacy_nlp_job.py`) idempotently ensures the `mimic4-nlp-v3-2` bucket and a `medspacy-results` folder (name from `.env`), creates a single Glue job role (`MimicGlueMedspacyNlpRole` — Glue + S3 + Data Catalog only), and creates the `medspacy-nlp-job` Glue job. medSpaCy is installed at job start via `--additional-python-modules medspacy==1.1.5` (adjust the pin with `--medspacy-modules`), which needs internet egress from the Glue job — the default no-VPC connection has it.

It processes the note set **one `hadm_id` bucket at a time** (64 buckets by default, matching `fact_discharge_note_nlp`'s `PARTITIONED BY (hadm_bucket)`), so a Glue Timeout only loses the in-flight bucket's work and a resumed run picks up where it left off — no `.txt` files leave the lake:

1. Reads `mimic4_db_raw.discharge_note_raw` (`--note-limit N`, default 100000; `--all-notes` for every note), groups by `hadm_id % num_buckets`, and for each bucket in turn `repartition`s and `mapPartitions` a medSpaCy pipeline that is loaded **once per partition** (`medspacy.load()` — a blank `en` model; if PyRuSH is unavailable in the runtime it falls back to spaCy's `sentencizer`).
2. For every note, `TargetMatcher` tags spans from an explicit phrase list (`TARGET_PATTERNS` in `medspacy_nlp.py`) into five labels — `TOBACCO`, `ALCOHOL`, `OBESITY`, `CESSATION_COUNSEL`, `CESSATION_PHARM` — and ConText attaches `negated` / `historical` / `hypothetical` / `family` to each span. The normalized span list (with the containing sentence) is emitted per note, and, unless `--no-note-json` is given, also written to `s3://mimic4-nlp-v3-2/medspacy-results/${subject_id}_${hadm_id}_${note_id}.json`.
3. On the driver, spans are pooled per admission and the levels are derived, then that bucket's rows are written with `overwritePartitions()` — Iceberg's dynamic partition overwrite replaces only that `hadm_bucket`, leaving every other bucket's already-written admissions untouched (`created_by = glue_nlp_medspacy`). On a resumed backfill, admissions already present in the target table are anti-joined out before batching so re-runs make forward progress instead of restarting from zero.

**Incremental re-runs (P7).** Both note-NLP jobs use the same `mimic4_db_business.etl_control` watermark pattern as the aggregate jobs. `--refresh-mode auto` (default): the first run backfills every note and records a watermark (`max COALESCE(storetime, charttime)`); a later run selects only the admissions with a discharge note newer than the watermark, re-runs medSpaCy on **just those admissions'** notes, `MERGE`s the recomputed rows, and advances the watermark. If nothing is newer, the job exits immediately without touching the table. `--refresh-mode full` forces a complete re-process (and still advances the watermark). A `--note-limit N` sample run never moves the watermark.

Derivation rules (`derive_levels` in `medspacy_nlp.py`):

| Column | Values | Rule |
|---|---|---|
| `tobacco_use`, `alcohol_use` | `NOT_MENTIONED` / `DENIES` / `FORMER` / `CURRENT` | Ignore `hypothetical` / `family` spans. No span left → `NOT_MENTIONED`; every remaining span `negated` → `DENIES`; any non-negated & non-historical → `CURRENT`; otherwise (non-negated but all historical) → `FORMER`. |
| `obesity_level` | `NONE` / `OBESE` / `MORBID` | Non-negated, non-hypothetical, non-family `OBESITY` span → `MORBID` if the span text or its sentence (or the note) says morbid / severe / class III / `BMI ≥ 40`, else `OBESE`; no such span → `NONE`. |
| `tobacco_cessation_cd` | `NA` / `NONE` / `COUNSELED` / `PHARMACOTHERAPY` | Non-negated `CESSATION_PHARM` span (varenicline / bupropion / NRT …) → `PHARMACOTHERAPY`; else non-negated `CESSATION_COUNSEL` span → `COUNSELED`; else `NONE` if `tobacco_use` is `CURRENT`/`FORMER`; else `NA`. |

`recognized_entities` holds the **entire medSpaCy output** for the admission's note(s) as a JSON `STRING` — `{"nlp_engine":"medspacy","note_count":N,"notes":[{"note_id":…,"entities":[{text,label,negated,historical,hypothetical,family,uncertain,sentence,…}]}]}`. Athena engine v3 has no VARIANT/JSON column type and can't read an Iceberg format-v3 table — query with `json_parse` / `json_extract`.

**Cost / scope:** free — no per-character billing. Defaults to a **100,000-note cap** (`--note-limit N`) -- lowered from the 1M fact-table cap since medSpaCy's per-document cost (~1-1.5s/note observed) makes a million-note run take far longer than a single Glue job Timeout on the default 2×G.1X; pass `--all-notes` for every discharge note. Early verification on a small sample: ConText correctly negated *"She denies recent drug use or alcohol use"*, marked *"Past history of smoking"* historical → `FORMER`, and excluded family-history matches.

## Deriving features from the radiology notes

`run_radiology_nlp.py` is the radiology counterpart of the discharge-note NLP pipeline. Source is `mimic4_db_raw.radiology_note_raw` (produced by `run_notes_ingest.py`); output is `mimic4_db_business.fact_radiology_note_nlp`, one row per admission `(subject_id, hadm_id)`.

```bash
python run_radiology_nlp.py --create-role --run-now
```

Radiology reports are rigidly sectioned, so `radiology_nlp.py` uses a **regex section splitter** (`EXAMINATION` / `INDICATION` / `TECHNIQUE` / `COMPARISON` / `FINDINGS` / `IMPRESSION` / `PROCEDURE` …) for most of the work, plus **medSpaCy** (`TargetMatcher` + `ConText`) for negation-aware symptom / disorder recognition over the relevant sections, a modality classifier, and a curated disorder → ICD-10-CM lookup. Same bucketed, resumable batching as the discharge-note job (128 `hadm_bucket` buckets by default, `mapPartitions` per bucket), default 100,000-note cap (`--note-limit N`, `--all-notes` for every radiology note). Per-note JSON is written to `s3://mimic4-nlp-v3-2/radiology-nlp-results/` unless `--no-note-json`.

Derived columns — **every one is a JSON array/object stored in a `STRING`** (see below), keyed to the requested fields:

| Column | Contents |
|---|---|
| `radiology_procedure_types` | `[{exam, modality, body_region, note_id}]` — `modality` ∈ CT / MR / US / XR / MAMMO / FLUORO / NM / PET / ANGIO / OTHER |
| `radiology_reasons` | `[{reason, note_id}]` — the clinical question from `INDICATION` (text after `//`, "eval for …", "r/o …") |
| `symptoms` / `disorders` | `[{text, negated, note_id}]` — matched terms with the ConText negation flag |
| `icd_codes` | `[{code, term, source}]` — `source = cited` (ICD-10 pattern found in the text) or `lookup` (mapped from a non-negated disorder) |
| `procedures` | `[{text, note_id}]` — the `PROCEDURE` section text and matched procedure terms (paracentesis, biopsy, drainage, line placement, …) |
| `findings_summary` / `indication_summary` / `conclusion` | `[{note_id, text}]` — the `FINDINGS` / `INDICATION` / `IMPRESSION` section text per note |
| `ner_json` | `{engine, note_count, notes:[{note_id, entities:[…]}]}` — the **entire medSpaCy output** for the admission |

Early verification on a small sample (8 admissions): modality split XR 9 / US 5 / CT 5 / MR 4; ConText correctly negated *"no focal consolidation, pleural effusion or pneumothorax"* while keeping *"compatible with cirrhosis"* positive; 16 ICD-10 codes mapped (`K74.60` cirrhosis, `R18.8` ascites, `K80.20` cholelithiasis, …).

**JSON storage:** `fact_radiology_note_nlp` holds JSON text in `STRING` columns, queried with Athena's JSON functions — e.g. `CAST(json_parse(disorders) AS ARRAY(JSON))` + `UNNEST`. This is the **canonical, Athena-SQL-queryable** table (`format-version` 2), consistent with the rest of the lakehouse.

### `fact_clinician_note_nlp_v` — the Inference Fact (real Iceberg VARIANT, Glue 6.0, Spark-only)

`run_clinician_note_variant.py` combines `fact_discharge_note_nlp` and `fact_radiology_note_nlp` into `fact_clinician_note_nlp_v`: a full outer join on the admission, one row per admission with `has_discharge_note` / `has_radiology_note` flags, and every JSON column typed as a real Iceberg **`VARIANT`**. It does no NLP — it reads the two STRING tables and `parse_json()`s the columns — so run it **after** both NLP jobs (phase J of `run_lakehouse_pipeline.py --with-nlp`):

```bash
python run_clinician_note_variant.py --create-role --run-now
```

This runs on **Glue 6.0** (Spark 4.1.1, Iceberg 1.11) — the only Glue runtime that has a `VARIANT` type. Tested findings:

- A `VARIANT` column **requires Iceberg `format-version = 3`** (v2 and the default both reject it), and Glue 4.0 / Glue 5.0 (Spark 3.5) have no `VariantType` at all.
- **Athena SQL (engine v3) cannot read a format-v3 table** — `SELECT` and even `DESCRIBE` fail with `Iceberg format version 3 is not supported`. So `fact_clinician_note_nlp_v` is created by the Glue job itself (not the Athena-run gold DDL) and is queryable only from **Spark / EMR / Athena-for-Apache-Spark**, for example:

  ```python
  variant_get(ner_json, '$.engine', 'string')                       # -> 'medspacy'
  to_json(variant_get(radiology_procedure_types, '$[0]'))           # -> {"modality":"MR","exam":"MRI of the brain...",...}
  SELECT variant_get(d.value,'$.text','string')
  FROM fact_clinician_note_nlp_v, LATERAL variant_explode(disorders) d   # -> cirrhosis, consolidation, pleural effusion, ...
  ```

Keep the two STRING-typed Unstructured Features Facts as the Athena-queryable tables; use `_v` where a Spark consumer wants the native VARIANT type. [compact_variant_table.py](compact_variant_table.py) rewrites its data files and expires snapshots (`--with-compaction`), since Athena's `OPTIMIZE`/`VACUUM` cannot touch a format-v3 table.

## Full environment cleanup

`cleanup_mimic4_environment.py` drops the Glue databases + tables, Glue jobs, crawler, and
IAM roles for one `--dataset`, and empties its S3 buckets. Defaults to a **dry run** —
pass `--yes` to apply.

```bash
python cleanup_mimic4_environment.py --dataset demodataset --yes    # dry-run without --yes
python cleanup_mimic4_environment.py --dataset fulldataset --yes
```

`--dataset` scopes the teardown to that dataset's `-demo` / `-full` resources, so the other
dataset is untouched. Extra flags:

| flag | effect |
|---|---|
| `--delete-buckets` | also delete the emptied bucket shells (not just their contents) |
| `--delete-log-groups` | delete the `/aws-glue/*` CloudWatch log groups (they recreate on next run) |
| `--empty-scripts-bucket` / `--classifier-names mimic4_csv_classifier` | include the shared scripts bucket / CSV classifier once no dataset needs them |
| `--resource-suffix ""` | target a deployment created **before** the `--dataset` naming change (unsuffixed `mimic4_db_raw`, `dim-load-*`, `MimicGlue*Role` …); pass the legacy names via `--raw-database`/`--crawler-names`/`--nlp-bucket`/etc. |
| `--extra-crawlers` / `--extra-classifiers` / `--extra-buckets` | sweep up stray/legacy resources by name |

## Glue CSV classifier

`setup_mimic4_s3_glue.py` creates a Glue classifier (`mimic4_csv_classifier`, a plain
comma-delimited CSV classifier) and attaches it to the crawler automatically —
`ensure_csv_classifier()` in `aws_workflow.py`. Nothing to set up by hand. It's a
format-only classifier and is shared between datasets, so `cleanup_mimic4_environment.py`
leaves it in place unless you pass `--classifier-names mimic4_csv_classifier`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE); see also [NOTICE](NOTICE). The
license covers this code and the aggregate results under `docs/` only. MIMIC-IV and
MIMIC-IV-Note are not included and remain under the PhysioNet Credentialed Health Data Use
Agreement.
