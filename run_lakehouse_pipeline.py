#!/usr/bin/env python3
"""One-shot end-to-end driver for the MIMIC-IV lakehouse pipeline.

Runs every step of the runbook in order, for one --dataset, waiting for each Glue job
to finish before starting the ones that depend on it:

  0. setup_mimic4_s3_glue.py   -- buckets, upload CSVs, crawler -> mimic4_db_raw_<mode>,
                                  Gold DDL -> mimic4_db_business_<mode>
  1. create every Glue job definition (dim_date seed, dims, facts, aggregates,
     and -- with --with-nlp -- notes ingest + medSpaCy/radiology NLP)
  2. run the jobs in dependency order:
       A  dim-date-seed-job
       B  dim-load-*                         (parallel)
       C  fact-load-*  (except icu_stay)     (parallel)
       D  fact-load-fact_icu_stay_accumulating   (needs fact_admission)
       E  agg_admission_daily, agg_icu_fluid_balance_daily      (parallel)
       F  agg_admission_monthly, obt_admission_features         (parallel, need E)
       G  obt_patient_360, obt_icu_stay_features                (parallel, need F)
     with --with-nlp, also (medSpaCy / rule-based only -- 10,000-note cap by
     default, see --nlp-row-limit; see run_medspacy_nlp.py / run_radiology_nlp.py):
       H  notes-ingest-job
       I  medspacy-nlp-job, radiology-nlp-job, fact-discharge-note-job,
          fact-radiology-note-job                               (parallel, need H)
       J  clinician-note-variant-job                            (needs I)
     with --with-format-compare (needs --with-nlp to have populated the NLP fact tables,
     in this run or an earlier one), also the Iceberg-vs-Parquet-vs-CSV storage study
     (see ddl/gold/mimic_iv_ddl_parquet_comparison.sql / mimic_iv_ddl_parquet_snappy_comparison.sql
     / mimic_iv_ddl_csv_comparison.sql):
       K  parquet-comparison-load-job, parquet-snappy-comparison-load-job,
          csv-comparison-load-job                                (parallel, need J)

     with --with-compaction, as the LAST step after every phase above:
       OPTIMIZE ... REWRITE DATA USING BIN_PACK + VACUUM on every Iceberg Gold table
          (compact_gold_tables.py --vacuum, via Athena), then with --with-nlp:
       L  compact-variant-table-job                              (format-v3 tables Athena
                                                                  can't OPTIMIZE; Glue 6.0)
       Orphan-file removal is not included: Iceberg refuses it for files under 24h old, so
       run run_remove_orphan_files.py a day later to reclaim those S3 bytes.

Every step is idempotent -- re-run the whole thing, or resume after a failure with
--from <PHASE> (a letter A-K) once you've fixed the cause.

Usage:

    # every run except --dry-run saves its full console output (this driver, every child
    # script, and any traceback) to logs/run_lakehouse_end_to_end_<local-timestamp>.log
    # automatically -- no `| tee` needed
    python run_lakehouse_pipeline.py --dataset fulldataset --fresh-reload --with-nlp --row-limit 1000000 --nlp-row-limit 10000 --with-compaction # full run

    python run_lakehouse_pipeline.py                                   # demo dataset, structured only
    python run_lakehouse_pipeline.py --dataset fulldataset             # full dataset, every fact loaded in full
    python run_lakehouse_pipeline.py --dataset fulldataset --with-nlp --row-limit 1000000
                                                                      # full dataset, 1M cap on every fact table
    python run_lakehouse_pipeline.py --with-nlp                        # + the notes/NLP jobs (10k-note cap)
    python run_lakehouse_pipeline.py --with-nlp --nlp-row-limit 50000  # NLP capped independently of --row-limit
    python run_lakehouse_pipeline.py --from D                          # resume at phase D (jobs already created)
    python run_lakehouse_pipeline.py --skip-setup --skip-create --from E
    python run_lakehouse_pipeline.py --fresh-reload                    # drop + recreate every Gold table first
    python run_lakehouse_pipeline.py --dataset fulldataset --with-nlp --with-compaction
                                                                      # ... then compact the Gold layer last
    python run_lakehouse_pipeline.py --dry-run                         # print the plan, touch nothing

--fresh-reload drops every existing Gold-zone table (dims, facts, aggregates, OBTs, NLP
facts, etl_control) and recreates them empty from the Gold DDL before the run -- for a
completely clean reload. etl_process_log is kept across reloads: it's the append-only
cross-run job-timing history the P6/P7 analyses compare across full_run_ids. Runs in step 0
(setup_mimic4_s3_glue.py), so it cannot be combined with --skip-setup or --from.

--row-limit N caps every fact table to a df.limit(N) sample of its raw source (tables
already smaller than N are unchanged). --nlp-row-limit N (default 10000) separately caps
the medSpaCy/radiology NLP note set (--note-limit) -- kept independent of --row-limit
because medSpaCy's per-document cost makes a fact-table-scale cap far too slow for NLP;
pass --nlp-row-limit 0 to process every note.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "etl"))

from mimic_lakehouse import config  # noqa: E402

# Canonical job-name lists, imported from the job creators so this stays in sync.
import create_agg_visual_etl_jobs as _agg  # noqa: E402
import create_dim_visual_etl_jobs as _dim  # noqa: E402
import create_fact_visual_etl_jobs as _fact  # noqa: E402

_DIMS = [d["name"] for d in _dim.DIMENSIONS]
_FACTS = [f["name"] for f in _fact.FACTS]
_AGGS = [a["name"] for a in _agg.AGGREGATES]
_ICU_FACT = "fact_icu_stay_accumulating"

# name -> source raw table, for etl_process_log's raw_row_count (dim/fact jobs only --
# agg/OBT jobs read another gold table, not a raw one; see _table_refs).
_DIM_RAW_TABLE = {d["name"]: d["raw_table"] for d in _dim.DIMENSIONS}
_FACT_RAW_TABLE = {f["name"]: f["raw_table"] for f in _fact.FACTS}

# process_name -> (raw note table, gold NLP fact table) for the note-NLP jobs.
_NLP_TABLES = {
    "medspacy_nlp": ("discharge_note_raw", "fact_discharge_note_nlp"),
    "radiology_nlp": ("radiology_note_raw", "fact_radiology_note_nlp"),
    # two sources (fact_discharge_note_nlp + fact_radiology_note_nlp), not one raw table --
    # raw_row_count stays None for this process_name, same treatment notes_ingest's own
    # multi-table case gets in _row_counts().
    "clinician_note_nlp_variant": (None, "fact_clinician_note_nlp_v"),
    # note-id-grain -> subject_id/hadm_id-grain rollups (Unstructured Data Fact archetype,
    # same grain as every other archetype -- see fact_note_concat.py). Reuses the "nlp"
    # process_type purely for its raw/gold row-count-logging shape in _row_counts(); these
    # jobs do no NLP.
    "fact_discharge_note": ("discharge_note_raw", "fact_discharge_note"),
    "fact_radiology_note": ("radiology_note_raw", "fact_radiology_note"),
}

# fact / agg jobs that read or join the multi-100M-row raw tables (chartevents,
# labevents, emar, poe, prescriptions, *events). At full-dataset scale the default
# 2 x G.1X will OOM / fail the shuffle -- these get --big-worker-type x --big-num-workers.
_BIG_JOBS_DEFAULT = (
    "fact_chart_observation", "fact_lab_result",
    "fact_medication_administration", "fact_medication_administration_mini_detail",
    "fact_provider_order", "fact_provider_order_mini_detail", "fact_prescription",
    "fact_input_event", "fact_output_event", "fact_datetime_event",
    "fact_procedure_event", "fact_ingredient_event",
    "obt_admission_features", "obt_patient_360", "obt_icu_stay_features",
)

# The 3 non-big aggregate jobs (everything in _AGGS except the 3 OBTs above, which are
# already in _BIG_JOBS_DEFAULT). Their source tables are small (fact_admission etc., not
# the multi-100M-row raw facts), but their incremental-refresh logic runs several groupBy/
# full_outer-join/large-isin() shuffle stages -- on a first/full backfill (no watermark
# yet) every row is "touched", so this is a full-table shuffle, not a trivial delta.
# --num-workers 2 (the creators' shared small default) gives exactly ONE executor for a
# Glue Spark ETL job (NumberOfWorkers includes the driver), which serializes those shuffle
# stages: measured on real full-dataset runs, agg_admission_daily took ~25-28 minutes and
# agg_icu_fluid_balance_daily ~14-17 minutes on 2 x G.1X, both slower than most of the
# _BIG_JOBS_DEFAULT fact loads that read hundreds of millions of raw rows on 10 x G.2X.
# These don't need G.2X-level per-worker memory, just more than one executor -- see
# --agg-worker-type/--agg-num-workers.
_MEDIUM_AGG_JOBS_DEFAULT = (
    "agg_admission_daily", "agg_icu_fluid_balance_daily", "agg_admission_monthly",
)

# --with-compaction: Iceberg format-version 3 (VARIANT) tables this pipeline writes (phase J)
# -- Athena can't OPTIMIZE them, so they're compacted by a Glue 6.0 Spark job instead.
_VARIANT_TABLES = ("fact_clinician_note_nlp_v",)
# VACUUM expires snapshots older than this (seconds) so the files superseded by this run's
# OPTIMIZE are released now; safe at the end of a run with no concurrent writers.
_VACUUM_MAX_AGE_S = 300

_TERMINAL_OK = {"SUCCEEDED"}
_TERMINAL_BAD = {"FAILED", "TIMEOUT", "ERROR", "STOPPED"}

# DPU per worker by worker type -- Glue bills concurrent capacity in these units and the
# account has a hard "max concurrent DPU" cap (100 on this account); the pipeline keeps
# each phase's in-flight total under --max-dpu.
_DPU_PER_WORKER = {"G.1X": 1, "G.2X": 2, "G.4X": 4, "G.8X": 16, "G.025X": 0.25, "Standard": 1}


def _job_dpu(name: str, big: set[str], wt, nw, bwt, bnw) -> int:
    """Best-effort DPU for a fact/agg/dim job given the pipeline's worker settings."""
    is_big = any(b and b in name for b in big)
    w_type = (bwt if is_big else wt) or "G.1X"
    n = (bnw if is_big else nw) or 2
    return max(1, round(_DPU_PER_WORKER.get(w_type, 1) * n))


def _print(msg: str = "") -> None:
    """print(), prefixed with a wall-clock timestamp and always flushed -- this pipeline's
    own stdout is teed into a log file (see _start_run_log), where Python's default block
    buffering can otherwise sit on output for many minutes with nothing visible on screen or
    in the log, making a slow-but-healthy step look hung. A leading '\\n' in msg (used
    throughout for blank-line spacing) prints as a real blank line ahead of the timestamp
    rather than after it."""
    if msg.startswith("\n"):
        print()
        msg = msg[1:]
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


class _Tee:
    """Write-through copy of a console stream into the run's log file."""

    def __init__(self, stream, log_file):
        self._stream, self._log = stream, log_file

    def write(self, text):
        self._stream.write(text)
        self._log.write(text)
        self._log.flush()
        return len(text)

    def flush(self):
        self._stream.flush()
        self._log.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


def _start_run_log() -> Path:
    """Tee everything this driver prints -- its own output, every child script's output
    (see _sh) and any traceback on stderr -- to logs/run_lakehouse_end_to_end_<local-
    timestamp>.log, so the run's full record exists without piping through `tee`."""
    logs_dir = REPO_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / f"run_lakehouse_end_to_end_{time.strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(path, "w", encoding="utf-8")
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    _print(f"run log: {path}")
    _print(f"$ {' '.join([Path(sys.executable).name, *sys.argv])}")
    return path


def _sh(*args: str) -> None:
    """Run a child python script from the repo root; raise (stop the pipeline) on failure.
    The child's stdout+stderr are streamed line by line through this process's stdout, so
    they reach both the console and the run log."""
    cmd = [sys.executable, *args]
    _print(f"\n$ {' '.join(cmd)}")
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(cmd, cwd=REPO_ROOT, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                            errors="replace", bufsize=1)
    for line in proc.stdout:
        sys.stdout.write(line)
    sys.stdout.flush()
    if proc.wait() != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def _worker_flags(num_workers, worker_type, glue_version) -> list[str]:
    f: list[str] = []
    if num_workers:
        f += ["--num-workers", str(num_workers)]
    if worker_type:
        f += ["--worker-type", worker_type]
    if glue_version:
        f += ["--glue-version", glue_version]
    return f


def _create_split(script: str, ds: list[str], names: list[str], big: set[str],
                  small_flags: list[str], big_flags: list[str],
                  extra: list[str] | None = None) -> None:
    """Create the jobs for `script` (fact or agg creator) in up to two --only calls so the
    big ones get bigger workers than the rest. `extra` is appended to every call (e.g.
    --row-limit for the fact creator)."""
    extra = extra or []
    small = [n for n in names if n not in big]
    big_here = [n for n in names if n in big]
    if not big_here:
        _sh(script, *ds, "--create-role", *small_flags, *extra)
        return
    if small:
        _sh(script, *ds, "--create-role", "--only", ",".join(small), *small_flags, *extra)
    _sh(script, *ds, "--create-role", "--only", ",".join(big_here), *big_flags, *extra)


def _phases(mode: str, with_nlp: bool, with_format_compare: bool = False) -> list[tuple[str, str, list[str]]]:
    """(letter, human label, [glue job names]) in run order, suffixed for the dataset."""
    s = f"-{mode}"
    p: list[tuple[str, str, list[str]]] = [
        ("A", "dim_date seed", [f"dim-date-seed-job{s}"]),
        ("B", "dimensions", [f"dim-load-{n}{s}" for n in _DIMS]),
        ("C", "facts (bulk)", [f"fact-load-{n}{s}" for n in _FACTS if n != _ICU_FACT]),
        ("D", "fact_icu_stay_accumulating", [f"fact-load-{_ICU_FACT}{s}"]),
        ("E", "aggregates: daily", [f"agg-refresh-agg_admission_daily{s}",
                                    f"agg-refresh-agg_icu_fluid_balance_daily{s}"]),
        ("F", "aggregates: monthly + admission OBT", [f"agg-refresh-agg_admission_monthly{s}",
                                                      f"agg-refresh-obt_admission_features{s}"]),
        ("G", "OBTs: patient_360 + icu_stay", [f"agg-refresh-obt_patient_360{s}",
                                               f"agg-refresh-obt_icu_stay_features{s}"]),
    ]
    if with_nlp:
        p += [
            ("H", "notes ingest", [f"notes-ingest-job{s}"]),
            ("I", "NLP: medSpaCy + radiology + note-concat rollups",
             [f"medspacy-nlp-job{s}", f"radiology-nlp-job{s}",
              f"fact-discharge-note-job{s}", f"fact-radiology-note-job{s}"]),
            ("J", "NLP: combined clinician-note VARIANT", [f"clinician-note-variant-job{s}"]),
        ]
    if with_format_compare:
        p += [
            ("K", "Iceberg vs Parquet(ZSTD) vs Parquet(Snappy) vs CSV storage comparison load",
             [f"parquet-comparison-load-job{s}", f"parquet-snappy-comparison-load-job{s}",
              f"csv-comparison-load-job{s}"]),
        ]
    return p


def _start(glue, job: str) -> str:
    """start_job_run, returning the run id; attach to an in-flight run if one exists."""
    try:
        return glue.start_job_run(JobName=job)["JobRunId"]
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "ConcurrentRunsExceededException":
            rid = glue.get_job_runs(JobName=job, MaxResults=1)["JobRuns"][0]["Id"]
            _print(f"  {job} already running -- attaching to {rid}")
            return rid
        if code == "EntityNotFoundException":
            raise SystemExit(f"Glue job '{job}' does not exist. Run without --skip-create first.")
        raise


def _classify_job(job: str, mode: str) -> tuple[str, str]:
    """Best-effort (process_type, process_name) from a Glue job name, e.g.
    'fact-load-fact_admission-full' -> ('fact', 'fact_admission')."""
    suffix = f"-{mode}"
    base = job[: -len(suffix)] if job.endswith(suffix) else job
    if base == "dim-date-seed-job":
        return "dim_date_seed", "dim_date"
    if base.startswith("dim-load-"):
        return "dim", base[len("dim-load-"):]
    if base.startswith("fact-load-"):
        return "fact", base[len("fact-load-"):]
    if base.startswith("agg-refresh-"):
        name = base[len("agg-refresh-"):]
        return ("obt" if name.startswith("obt_") else "agg"), name
    if base == "notes-ingest-job":
        return "notes_ingest", "notes_ingest"
    if base == "medspacy-nlp-job":
        return "nlp", "medspacy_nlp"
    if base == "radiology-nlp-job":
        return "nlp", "radiology_nlp"
    if base == "clinician-note-variant-job":
        return "nlp", "clinician_note_nlp_variant"
    if base == "fact-discharge-note-job":
        return "nlp", "fact_discharge_note"
    if base == "fact-radiology-note-job":
        return "nlp", "fact_radiology_note"
    if base == "parquet-comparison-load-job":
        return "compare", "parquet_comparison"
    if base == "parquet-snappy-comparison-load-job":
        return "compare", "parquet_snappy_comparison"
    if base == "csv-comparison-load-job":
        return "compare", "csv_comparison"
    if base == "compact-variant-table-job":
        return "maintenance", "compact_variant_table"
    return "other", base


def _sql_str(v) -> str:
    return "NULL" if v is None else "'" + str(v).replace("'", "''")[:2000] + "'"


def _sql_ts(dt) -> str:
    if dt is None:
        return "NULL"
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"


def _athena_exec(athena, sql: str, output_location: str) -> str:
    """Run one Athena statement to completion; returns its terminal state."""
    qid = athena.start_query_execution(
        QueryString=sql, ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=config.ATHENA_WORKGROUP,
    )["QueryExecutionId"]
    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state
        time.sleep(1)


def _athena_scalar(athena, sql: str, output_location: str):
    """Run a query expected to return exactly one row/column; the value as a string, or
    None if the query fails or returns no rows. Never raises."""
    try:
        qid = athena.start_query_execution(
            QueryString=sql, ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=config.ATHENA_WORKGROUP,
        )["QueryExecutionId"]
        while True:
            state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if state != "SUCCEEDED":
            return None
        rows = athena.get_query_results(QueryExecutionId=qid, MaxResults=2)["ResultSet"]["Rows"]
        if len(rows) < 2:  # row 0 is the header row
            return None
        return rows[1]["Data"][0].get("VarCharValue")
    except Exception:  # noqa: BLE001 -- a count is best-effort, never worth aborting the pipeline for
        return None


def _next_full_run_id(athena, output_location: str, gold_database: str) -> int:
    """MAX(full_run_id) + 1 over etl_process_log's existing rows (1 if the table is empty,
    doesn't exist yet, or the query fails). One number shared by every job this pipeline
    invocation logs; see the etl_process_log DDL comment for why this isn't safe against
    two concurrent invocations."""
    val = _athena_scalar(
        athena, f"SELECT max(full_run_id) FROM {gold_database}.etl_process_log", output_location)
    try:
        return int(val) + 1 if val is not None else 1
    except (TypeError, ValueError):
        return 1


def _count_rows(athena, output_location: str, database: str, table: str):
    """SELECT count(*) for one table -- None (not 0) on any failure, so a table that
    doesn't exist or a transient Athena error doesn't get logged as a misleading zero."""
    val = _athena_scalar(athena, f"SELECT count(*) FROM {database}.{table}", output_location)
    return int(val) if val is not None else None


def _row_counts(athena, output_location: str, process_type: str, process_name: str,
               raw_database: str, gold_database: str) -> tuple[int | None, int | None]:
    """(raw_row_count, gold_row_count) for one finished job -- None where not applicable
    for that process_type (see the etl_process_log DDL comment for the exact rules)."""
    if process_type == "dim_date_seed":
        return None, _count_rows(athena, output_location, gold_database, "dim_date")
    if process_type == "dim":
        raw_table = _DIM_RAW_TABLE.get(process_name)
        raw = _count_rows(athena, output_location, raw_database, raw_table) if raw_table else None
        return raw, _count_rows(athena, output_location, gold_database, process_name)
    if process_type == "fact":
        raw_table = _FACT_RAW_TABLE.get(process_name)
        raw = _count_rows(athena, output_location, raw_database, raw_table) if raw_table else None
        return raw, _count_rows(athena, output_location, gold_database, process_name)
    if process_type in ("agg", "obt"):
        return None, _count_rows(athena, output_location, gold_database, process_name)
    if process_type == "notes_ingest":
        # One job populates both raw note tables -- combined() so a caller doesn't have to
        # pick just one; gold_row_count doesn't apply, this job writes the raw layer only.
        d = _count_rows(athena, output_location, raw_database, "discharge_note_raw")
        r = _count_rows(athena, output_location, raw_database, "radiology_note_raw")
        combined = (d or 0) + (r or 0) if (d is not None or r is not None) else None
        return combined, None
    if process_type == "nlp":
        raw_table, gold_table = _NLP_TABLES.get(process_name, (None, None))
        raw = _count_rows(athena, output_location, raw_database, raw_table) if raw_table else None
        gold = _count_rows(athena, output_location, gold_database, gold_table) if gold_table else None
        return raw, gold
    return None, None


def _log_process_run(athena, output_location: str, gold_database: str, raw_database: str, *,
                     full_run_id: int, mode: str, phase: str, job_name: str, run) -> None:
    """Append one row to <gold_database>.etl_process_log for a finished Glue job run.

    `run` is a Glue JobRun dict (from get_job_run) -- StartedOn/CompletedOn/
    ExecutionTime/DPUSeconds/JobRunState/ErrorMessage. Logging never raises: a failure
    to log a row (or to count rows) must not be mistaken for (or mask) a pipeline failure.
    """
    process_type, process_name = _classify_job(job_name, mode)
    execution_seconds = (run.get("ExecutionTime") or 0)  # Glue reports this in seconds already
    raw_count, gold_count = _row_counts(athena, output_location, process_type, process_name,
                                        raw_database, gold_database)
    sql = (
        f"INSERT INTO {gold_database}.etl_process_log "
        "(full_run_id, run_id, job_name, process_name, process_type, phase, dataset, "
        "start_ts, end_ts, execution_seconds, dpu_seconds, raw_row_count, gold_row_count, "
        "status, error_message, logged_ts) VALUES ("
        f"{full_run_id}, {_sql_str(run.get('Id'))}, {_sql_str(job_name)}, {_sql_str(process_name)}, "
        f"{_sql_str(process_type)}, {_sql_str(phase)}, {_sql_str(mode)}, "
        f"{_sql_ts(run.get('StartedOn'))}, {_sql_ts(run.get('CompletedOn'))}, "
        f"{execution_seconds}, {run.get('DPUSeconds', 'NULL')}, "
        f"{raw_count if raw_count is not None else 'NULL'}, "
        f"{gold_count if gold_count is not None else 'NULL'}, "
        f"{_sql_str(run.get('JobRunState'))}, {_sql_str(run.get('ErrorMessage'))}, "
        "current_timestamp)"
    )
    try:
        state = _athena_exec(athena, sql, output_location)
        if state != "SUCCEEDED":
            _print(f"  [process-log] WARNING: could not log {job_name} (Athena query {state})")
    except Exception as exc:  # noqa: BLE001 -- a logging failure must never abort the pipeline
        _print(f"  [process-log] WARNING: could not log {job_name}: {exc}")


def _run_phase(glue, letter: str, label: str, jobs: list[str], poll: int,
               dpu: dict[str, int], max_dpu: int, *,
               athena=None, athena_output: str | None = None,
               gold_database: str | None = None, raw_database: str | None = None,
               mode: str | None = None, full_run_id: int | None = None) -> None:
    _print(f"\n=== Phase {letter}: {label}  ({len(jobs)} job(s), <= {max_dpu} DPU in flight) ===")
    queue = list(jobs)
    running: dict[str, str] = {}   # job -> run id
    used = 0

    def _fill():
        nonlocal used
        i = 0
        while i < len(queue):
            job = queue[i]
            need = dpu.get(job, 2)
            if running and used + need > max_dpu:
                i += 1  # try a smaller job later in the queue
                continue
            running[job] = _start(glue, job)
            used += need
            queue.pop(i)
            _print(f"  started {job}  ({need} DPU; {used}/{max_dpu} in flight)")

    _fill()
    while running or queue:
        time.sleep(poll)
        for job, run_id in list(running.items()):
            run = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]
            st = run["JobRunState"]
            if st in _TERMINAL_OK:
                _print(f"  OK   {job}")
                used -= dpu.get(job, 2)
                running.pop(job)
                if athena is not None:
                    _log_process_run(athena, athena_output, gold_database, raw_database,
                                     full_run_id=full_run_id, mode=mode,
                                     phase=letter, job_name=job, run=run)
            elif st in _TERMINAL_BAD:
                if athena is not None:
                    _log_process_run(athena, athena_output, gold_database, raw_database,
                                     full_run_id=full_run_id, mode=mode,
                                     phase=letter, job_name=job, run=run)
                err = run.get("ErrorMessage", "")
                raise SystemExit(f"Phase {letter} FAILED: {job} -> {st}. {err}\n"
                                 f"Fix the cause, then resume with:  python run_lakehouse_pipeline.py "
                                 f"--skip-setup --skip-create --from {letter}")
        _fill()
    _print(f"=== Phase {letter} complete ===")


def main(run_log: bool = False) -> int:
    """run_log=True (the script entry point) tees all output to logs/ -- off when main() is
    called in-process (tests), so that doesn't create log files or replace sys.stdout."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    config.add_dataset_arg(ap)
    ap.add_argument("--with-nlp", action="store_true",
                    help="Also create + run the notes-ingest and medSpaCy/radiology NLP jobs "
                         "(needs discharge.csv.gz / radiology.csv.gz under the dataset's notes/ dir).")
    ap.add_argument("--with-format-compare", action="store_true",
                    help="Also apply ddl/gold/mimic_iv_ddl_parquet_comparison.sql + "
                         "mimic_iv_ddl_csv_comparison.sql and run the Iceberg-vs-Parquet-vs-CSV storage "
                         "comparison load jobs (phase K). Needs fact_discharge_note(_nlp)/"
                         "fact_radiology_note(_nlp/_v) already populated -- pass --with-nlp in the same "
                         "run, or run this after an earlier --with-nlp run has populated them.")
    ap.add_argument("--with-compaction", action="store_true",
                    help="Last step, after every job phase: compact the Gold layer -- Athena OPTIMIZE "
                         "... REWRITE DATA USING BIN_PACK + VACUUM on every Iceberg table "
                         "(compact_gold_tables.py --vacuum), and with --with-nlp also the format-v3 "
                         "VARIANT table(s) Athena can't OPTIMIZE, via a Glue 6.0 Spark job (phase L). "
                         "Mutates the tables and scans them via Athena (billed). Orphan-file removal "
                         "is NOT included -- Iceberg refuses it within 24h; run run_remove_orphan_files.py "
                         "a day later.")
    ap.add_argument("--fresh-reload", action="store_true",
                    help="Drop and recreate every Gold-zone table (dims/facts/aggs/OBTs/NLP + etl_control) "
                         "before this run, for a completely clean reload. etl_process_log is kept -- it's "
                         "the cross-run timing history the P6/P7 analyses compare across runs. Implemented "
                         "in step 0 (setup_mimic4_s3_glue.py), so cannot be combined with --skip-setup.")
    ap.add_argument("--skip-setup", action="store_true", help="Skip step 0 (setup_mimic4_s3_glue.py).")
    ap.add_argument("--skip-create", action="store_true", help="Skip step 1 (creating Glue job definitions).")
    ap.add_argument("--from", dest="from_phase", metavar="PHASE", default=None,
                    help="Resume the run at this phase letter (A-K); implies --skip-setup --skip-create.")
    ap.add_argument("--only", default=None,
                    help="Comma list of job base names (e.g. fact_lab_result) to run; every phase is "
                         "filtered to just these and empty phases are skipped. For a targeted catch-up "
                         "after a partial failure.")
    ap.add_argument("--create-only", action="store_true", help="Do steps 0-1 only; don't run any job.")
    ap.add_argument("--row-limit", type=int, default=None,
                    help="Cap every fact table to a df.limit(N) sample of its raw source (tables already "
                         "under N are unaffected). Use 1000000 for the standard 1M cap on the full dataset. "
                         "Does not affect NLP -- see --nlp-row-limit.")
    ap.add_argument("--nlp-row-limit", type=int, default=10000,
                    help="Cap the medSpaCy/radiology NLP note set (--note-limit), independently of "
                         "--row-limit -- medSpaCy's per-document cost makes a fact-table-scale cap far too "
                         "slow for a single Glue job Timeout. Default: 10000. Pass 0 to process every note.")
    ap.add_argument("--num-workers", type=int, default=None, help="Workers for every dim/fact/agg job (default: the creators' 2).")
    ap.add_argument("--worker-type", default=None, help="Worker type for every dim/fact/agg job (default: G.1X).")
    ap.add_argument("--glue-version", default=None, help="Glue version for every dim/fact/agg job (default: 4.0).")
    ap.add_argument("--big-jobs", default=",".join(_BIG_JOBS_DEFAULT),
                    help="Comma list of fact/agg names that get the bigger --big-worker-type/--big-num-workers "
                         "(they read the huge raw tables). Pass \"\" to disable the split.")
    ap.add_argument("--big-worker-type", default="G.2X", help="Worker type for the --big-jobs (default: G.2X).")
    ap.add_argument("--big-num-workers", type=int, default=10, help="Workers for the --big-jobs (default: 10).")
    ap.add_argument("--agg-jobs", default=",".join(_MEDIUM_AGG_JOBS_DEFAULT),
                    help="Comma list of aggregate names that get --agg-worker-type/--agg-num-workers instead "
                         "of the shared small default (small source tables, but shuffle-heavy incremental-"
                         "refresh logic that a single-executor small job serializes). Pass \"\" to disable.")
    ap.add_argument("--agg-worker-type", default="G.1X", help="Worker type for the --agg-jobs (default: G.1X).")
    ap.add_argument("--agg-num-workers", type=int, default=4,
                    help="Workers for the --agg-jobs (default: 4 -- 3 executors, up from the small default's "
                         "1, since NumberOfWorkers includes the Spark driver).")
    ap.add_argument("--max-dpu", type=int, default=90,
                    help="Cap on total Glue DPU in flight within a phase (default: 90; the account's "
                         "hard concurrent-capacity limit is 100). Jobs are launched in DPU-budgeted waves.")
    ap.add_argument("--poll-interval", type=int, default=20, help="Seconds between Glue job-run status checks.")
    ap.add_argument("--no-process-log", action="store_true",
                    help="Don't append a row to <gold_database>.etl_process_log for each finished job "
                         "(start/end timestamps, execution/DPU seconds, status -- for analyzing timings "
                         "after the fact). On by default; a logging failure never aborts the pipeline.")
    ap.add_argument("--region", default=None, help="AWS region (default: AWS_REGION / your CLI config).")
    ap.add_argument("--dry-run", action="store_true", help="Print the plan and exit.")
    args = ap.parse_args()

    if args.fresh_reload and (args.skip_setup or args.from_phase):
        ap.error("--fresh-reload runs in step 0 (setup_mimic4_s3_glue.py) and cannot be combined "
                 "with --skip-setup or --from.")
    if run_log and not args.dry_run:
        _start_run_log()

    profile = config.dataset_profile(args.dataset)
    mode = profile.mode
    region = args.region or config.get("AWS_REGION")
    phases = _phases(mode, args.with_nlp, args.with_format_compare)

    if args.from_phase:
        args.skip_setup = args.skip_create = True
        start = args.from_phase.strip().upper()
        letters = [p[0] for p in phases]
        if start not in letters:
            raise SystemExit(f"--from {start}: unknown phase. Choices: {', '.join(letters)}")
        phases = phases[letters.index(start):]

    if args.only:
        # match the whole dash-delimited job-name token, so "fact_procedure" does not
        # also pull in "fact_procedure_event"
        want = [f"-{n.strip()}-" for n in args.only.split(",") if n.strip()]
        phases = [(letter, label, [j for j in jobs if any(w in f"-{j}-" for w in want)])
                  for letter, label, jobs in phases]
        phases = [p for p in phases if p[2]]
        if not phases:
            raise SystemExit(f"--only {args.only}: no matching jobs in the selected phases.")

    _print(f"Dataset      : {profile.dataset}  (resources *-{mode})")
    _print(f"Raw / Gold DB: {profile.raw_database} / {profile.gold_database}")
    _print(f"Region       : {region or '(from AWS config)'}")
    _print("Plan:")
    if not args.skip_setup:
        _print("  0. setup_mimic4_s3_glue.py --create-role  (buckets, upload, crawler, Gold DDL)"
               + (" + --fresh-reload (drop & recreate every Gold table except etl_process_log)"
                  if args.fresh_reload else ""))
    if not args.skip_create:
        _big = {n for n in args.big_jobs.split(",") if n.strip()}
        _agg_med = {n for n in args.agg_jobs.split(",") if n.strip()}
        _dflt = args.worker_type or "G.1X", args.num_workers or 2
        _print(f"  1. create Glue job definitions (dim_date, dims, facts, aggregates"
               + (", notes, NLP" if args.with_nlp else "")
               + (", parquet/CSV format-compare" if args.with_format_compare else "") + ")")
        _print(f"     workers: default {_dflt[0]} x{_dflt[1]}"
               + (f"; big ({len(_big)} jobs) {args.big_worker_type} x{args.big_num_workers}" if _big else "")
               + (f"; medium agg ({len(_agg_med)} jobs) {args.agg_worker_type} x{args.agg_num_workers}"
                  if _agg_med else ""))
        if args.row_limit:
            _print(f"     row cap: {args.row_limit:,} per fact table")
        if args.with_nlp:
            _print(f"     NLP note cap: {args.nlp_row_limit:,}" if args.nlp_row_limit
                   else "     NLP note cap: none (--all-notes)")
    variant_job = f"compact-variant-table-job{profile.suffix}"
    if not args.create_only:
        for letter, label, jobs in phases:
            _print(f"  {letter}. {label}: {', '.join(jobs)}")
        if args.with_compaction:
            _print("  compaction: compact_gold_tables.py --vacuum --vacuum-max-age "
                   f"{_VACUUM_MAX_AGE_S}  (OPTIMIZE + VACUUM every Iceberg table)")
            if args.with_nlp:
                _print(f"  L. compaction: format-v3 tables ({', '.join(_VARIANT_TABLES)}): {variant_job}")
    if args.dry_run:
        return 0

    ds = ["--dataset", args.dataset]

    # ---- step 0: infra + raw + Gold DDL ------------------------------------------------
    if not args.skip_setup:
        setup_extra = ["--fresh-reload"] if args.fresh_reload else []
        _sh("setup_mimic4_s3_glue.py", *ds, "--create-role", *setup_extra)

    # ---- step 1: create job definitions (no --run-now; run_phase drives execution) ----
    if not args.skip_create:
        small_flags = _worker_flags(args.num_workers, args.worker_type, args.glue_version)
        big_flags = _worker_flags(args.big_num_workers, args.big_worker_type, args.glue_version)
        agg_flags = _worker_flags(args.agg_num_workers, args.agg_worker_type, args.glue_version)
        big = {n for n in args.big_jobs.split(",") if n.strip()}
        agg_medium = {n for n in args.agg_jobs.split(",") if n.strip()}
        if big:
            _print(f"\n[workers] big jobs -> {args.big_worker_type} x{args.big_num_workers}: "
                   f"{', '.join(sorted(big))}")
        if agg_medium:
            _print(f"[workers] medium aggregate jobs -> {args.agg_worker_type} x{args.agg_num_workers}: "
                   f"{', '.join(sorted(agg_medium))}")
        fact_extra = ["--row-limit", str(args.row_limit)] if args.row_limit else None
        nlp_extra = ["--note-limit", str(args.nlp_row_limit)] if args.nlp_row_limit else ["--all-notes"]
        _sh("etl/create_dim_date_seed_job.py", *ds, "--create-role")
        _sh("etl/create_dim_visual_etl_jobs.py", *ds, "--create-role", *small_flags)
        _create_split("etl/create_fact_visual_etl_jobs.py", ds, _FACTS, big, small_flags, big_flags, fact_extra)
        # 3-way split for aggregates, not the shared 2-way _create_split: big (the OBTs,
        # multi-100M-row joins) / medium (small source tables but shuffle-heavy incremental
        # refresh -- see _MEDIUM_AGG_JOBS_DEFAULT) / small (anything left over, currently none).
        _agg_big = [n for n in _AGGS if n in big]
        _agg_medium = [n for n in _AGGS if n in agg_medium and n not in big]
        _agg_small = [n for n in _AGGS if n not in big and n not in agg_medium]
        if _agg_small:
            _sh("etl/create_agg_visual_etl_jobs.py", *ds, "--create-role", "--only", ",".join(_agg_small), *small_flags)
        if _agg_medium:
            _sh("etl/create_agg_visual_etl_jobs.py", *ds, "--create-role", "--only", ",".join(_agg_medium), *agg_flags)
        if _agg_big:
            _sh("etl/create_agg_visual_etl_jobs.py", *ds, "--create-role", "--only", ",".join(_agg_big), *big_flags)
        if args.with_nlp:
            _sh("etl/create_notes_ingest_job.py", *ds, "--create-role")
            _sh("etl/create_medspacy_nlp_job.py", *ds, "--create-role", *nlp_extra)
            _sh("etl/create_radiology_nlp_job.py", *ds, "--create-role", *nlp_extra)
            _sh("etl/create_clinician_note_variant_job.py", *ds, "--create-role")
            _sh("etl/create_fact_note_concat_job.py", *ds, "--create-role", "--note-type", "both")
        if args.with_format_compare:
            # --create-role is required by aws_workflow's argparse validation even with
            # --skip-load (it's unconditional, not just-for-crawler) -- unused in practice
            # since --skip-load skips the whole crawler-role code path.
            _sh("setup_mimic4_s3_glue.py", *ds, "--skip-load", "--create-role",
                "--gold-ddl-file", "ddl/gold/mimic_iv_ddl_parquet_comparison.sql")
            _sh("setup_mimic4_s3_glue.py", *ds, "--skip-load", "--create-role",
                "--gold-ddl-file", "ddl/gold/mimic_iv_ddl_parquet_snappy_comparison.sql")
            _sh("setup_mimic4_s3_glue.py", *ds, "--skip-load", "--create-role",
                "--gold-ddl-file", "ddl/gold/mimic_iv_ddl_csv_comparison.sql")
            _sh("etl/create_parquet_comparison_job.py", *ds, "--create-role",
                "--job-name", f"parquet-comparison-load-job{profile.suffix}")
            _sh("etl/create_parquet_snappy_comparison_job.py", *ds, "--create-role",
                "--job-name", f"parquet-snappy-comparison-load-job{profile.suffix}")
            _sh("etl/create_csv_comparison_job.py", *ds, "--create-role",
                "--job-name", f"csv-comparison-load-job{profile.suffix}")

    if args.create_only:
        _print("\n--create-only: job definitions ready, nothing run.")
        return 0

    # ---- step 2: run the jobs in dependency order ------------------------------------
    glue = boto3.client("glue", region_name=region) if region else boto3.client("glue")
    athena = None
    athena_output = None
    full_run_id = None
    if not args.no_process_log:
        athena = boto3.client("athena", region_name=region) if region else boto3.client("athena")
        athena_output = f"s3://{profile.gold_bucket}/athena-results/"
        full_run_id = _next_full_run_id(athena, athena_output, profile.gold_database)
        _print(f"[process-log] logging job timings to {profile.gold_database}.etl_process_log "
               f"(full_run_id {full_run_id})")
    big = {n for n in args.big_jobs.split(",") if n.strip()}
    all_jobs = [j for _l, _lbl, js in phases for j in js]
    dpu = {j: _job_dpu(j, big, args.worker_type, args.num_workers,
                       args.big_worker_type, args.big_num_workers) for j in all_jobs}
    t0 = time.time()
    for letter, label, jobs in phases:
        _run_phase(glue, letter, label, jobs, args.poll_interval, dpu, args.max_dpu,
                  athena=athena, athena_output=athena_output,
                  gold_database=profile.gold_database, raw_database=profile.raw_database,
                  mode=mode, full_run_id=full_run_id)

    # ---- step 3 (optional): compact the Gold layer ------------------------------------
    if args.with_compaction:
        _print("\n=== Compaction: OPTIMIZE + VACUUM every Iceberg Gold table ===")
        _sh("compact_gold_tables.py", *ds, "--vacuum", "--vacuum-max-age", str(_VACUUM_MAX_AGE_S))
        if args.with_nlp:
            # Athena engine v3 cannot OPTIMIZE Iceberg format-version 3 tables
            # (compact_gold_tables.py skips them) -- compact those from Glue 6.0 / Spark.
            _sh("etl/create_compact_variant_table_job.py", *ds, "--create-role",
                "--job-name", variant_job, "--tables", ",".join(_VARIANT_TABLES))
            _run_phase(glue, "L", "compaction: format-v3 tables", [variant_job],
                       args.poll_interval, {variant_job: _job_dpu(variant_job, set(), None, None, None, None)},
                       args.max_dpu, athena=athena, athena_output=athena_output,
                       gold_database=profile.gold_database, raw_database=profile.raw_database,
                       mode=mode, full_run_id=full_run_id)
        _print("Compaction done. Orphaned data files are reclaimed separately, 24h+ later: "
               f"python run_remove_orphan_files.py {' '.join(ds)} --create-role --run-now --tables <...>")

    _print(f"\nPipeline complete for {profile.dataset} in {int(time.time() - t0)}s. "
           f"Gold tables are in {profile.gold_database}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(run_log=True))
