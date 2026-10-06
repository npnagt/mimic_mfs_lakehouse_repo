"""Phase planning for run_lakehouse_pipeline.py -- the end-to-end orchestrator's dependency order."""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "etl"))

import run_lakehouse_pipeline


def _flat(phases):
    return [job for _letter, _label, jobs in phases for job in jobs]


def test_phases_are_suffixed_for_the_dataset():
    demo = _flat(run_lakehouse_pipeline._phases("demo", with_nlp=False))
    full = _flat(run_lakehouse_pipeline._phases("full", with_nlp=False))
    assert all(j.endswith("-demo") for j in demo)
    assert all(j.endswith("-full") for j in full)
    assert "dim-load-dim_patient-demo" in demo
    assert "agg-refresh-obt_patient_360-full" in full


def test_structured_run_covers_every_dim_fact_agg_job():
    jobs = set(_flat(run_lakehouse_pipeline._phases("demo", with_nlp=False)))
    assert len(jobs) == 1 + len(run_lakehouse_pipeline._DIMS) + len(run_lakehouse_pipeline._FACTS) + 6
    assert "dim-date-seed-job-demo" in jobs
    for n in run_lakehouse_pipeline._FACTS:
        assert f"fact-load-{n}-demo" in jobs


def test_icu_fact_runs_after_the_fact_bulk_phase():
    phases = run_lakehouse_pipeline._phases("demo", with_nlp=False)
    letters = {letter: jobs for letter, _l, jobs in phases}
    assert "fact-load-fact_icu_stay_accumulating-demo" not in letters["C"]
    assert letters["D"] == ["fact-load-fact_icu_stay_accumulating-demo"]
    order = [letter for letter, _l, _j in phases]
    assert order.index("C") < order.index("D")


def test_aggregate_dependency_order():
    phases = run_lakehouse_pipeline._phases("demo", with_nlp=False)
    by = {letter: jobs for letter, _l, jobs in phases}
    # daily before monthly; admission OBT before patient_360 / icu_stay OBTs
    assert "agg-refresh-agg_admission_daily-demo" in by["E"]
    assert "agg-refresh-agg_admission_monthly-demo" in by["F"]
    assert "agg-refresh-obt_admission_features-demo" in by["F"]
    assert "agg-refresh-obt_patient_360-demo" in by["G"]
    assert "agg-refresh-obt_icu_stay_features-demo" in by["G"]


def test_nlp_phases_are_opt_in_and_ordered():
    without = run_lakehouse_pipeline._phases("demo", with_nlp=False)
    with_nlp = run_lakehouse_pipeline._phases("demo", with_nlp=True)
    assert len(with_nlp) == len(without) + 3
    j = _flat(with_nlp)
    assert j.index("notes-ingest-job-demo") < j.index("medspacy-nlp-job-demo")
    assert j.index("radiology-nlp-job-demo") < j.index("clinician-note-variant-job-demo")


def test_unknown_resume_phase_rejected(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["run_lakehouse_pipeline.py", "--from", "Z", "--dry-run"])
    with pytest.raises(SystemExit):
        run_lakehouse_pipeline.main()


def test_worker_flags():
    assert run_lakehouse_pipeline._worker_flags(None, None, None) == []
    assert run_lakehouse_pipeline._worker_flags(10, "G.2X", "4.0") == [
        "--num-workers", "10", "--worker-type", "G.2X", "--glue-version", "4.0"
    ]


def test_create_split_sends_big_and_small_separately(monkeypatch):
    calls = []
    monkeypatch.setattr(run_lakehouse_pipeline, "_sh", lambda *a: calls.append(list(a)))
    run_lakehouse_pipeline._create_split(
        "etl/create_fact_visual_etl_jobs.py", ["--dataset", "fulldataset"],
        ["fact_a", "fact_chart_observation", "fact_b"],
        {"fact_chart_observation"},
        small_flags=["--num-workers", "2"],
        big_flags=["--num-workers", "10", "--worker-type", "G.2X"],
    )
    assert len(calls) == 2
    small_call, big_call = calls
    assert "--only" in small_call and "fact_a,fact_b" in small_call
    assert small_call[-2:] == ["--num-workers", "2"]
    assert "fact_chart_observation" in big_call and "G.2X" in big_call


def test_create_split_single_call_when_no_big_members(monkeypatch):
    calls = []
    monkeypatch.setattr(run_lakehouse_pipeline, "_sh", lambda *a: calls.append(list(a)))
    run_lakehouse_pipeline._create_split(
        "etl/create_agg_visual_etl_jobs.py", ["--dataset", "demodataset"],
        ["agg_admission_daily", "agg_admission_monthly"], set(),
        small_flags=[], big_flags=["--num-workers", "10"],
    )
    assert len(calls) == 1
    assert "--only" not in calls[0]


def test_create_split_appends_extra_to_every_call(monkeypatch):
    calls = []
    monkeypatch.setattr(run_lakehouse_pipeline, "_sh", lambda *a: calls.append(list(a)))
    run_lakehouse_pipeline._create_split(
        "etl/create_fact_visual_etl_jobs.py", ["--dataset", "fulldataset"],
        ["fact_a", "fact_chart_observation"], {"fact_chart_observation"},
        small_flags=[], big_flags=["--worker-type", "G.2X"],
        extra=["--row-limit", "1000000"],
    )
    assert len(calls) == 2
    assert all(c[-2:] == ["--row-limit", "1000000"] for c in calls)


def test_row_limit_threads_to_fact_creators_not_nlp(monkeypatch, capsys):
    # --row-limit caps the fact tables only; the NLP note cap is the separate
    # --nlp-row-limit (default 10,000).
    calls = []
    monkeypatch.setattr(run_lakehouse_pipeline, "_sh", lambda *a: calls.append(list(a)))
    monkeypatch.setattr(sys, "argv", [
        "run_lakehouse_pipeline.py", "--dataset", "fulldataset", "--with-nlp",
        "--row-limit", "1000000", "--create-only",
    ])
    run_lakehouse_pipeline.main()
    joined = [" ".join(c) for c in calls]
    fact = next(c for c in joined if "create_fact_visual_etl_jobs.py" in c)
    assert "--row-limit 1000000" in fact
    medspacy = next(c for c in joined if "create_medspacy_nlp_job.py" in c)
    assert "--note-limit 10000" in medspacy and "--note-limit 1000000" not in medspacy
    radiology = next(c for c in joined if "create_radiology_nlp_job.py" in c)
    assert "--note-limit 10000" in radiology and "--note-limit 1000000" not in radiology
    # aggregate creator has no --row-limit
    agg = next(c for c in joined if "create_agg_visual_etl_jobs.py" in c)
    assert "--row-limit" not in agg


def test_default_big_jobs_are_real_fact_or_agg_names():
    known = set(run_lakehouse_pipeline._FACTS) | set(run_lakehouse_pipeline._AGGS)
    assert set(run_lakehouse_pipeline._BIG_JOBS_DEFAULT) <= known


def test_job_dpu_big_vs_small():
    big = {"fact_chart_observation", "obt_icu_stay_features"}
    # small: default G.1X x2 = 2 DPU
    assert run_lakehouse_pipeline._job_dpu("fact-load-fact_admission-full", big, None, None, "G.2X", 10) == 2
    # big: G.2X x10 = 2 DPU/worker x 10 = 20
    assert run_lakehouse_pipeline._job_dpu("fact-load-fact_chart_observation-full", big, None, None, "G.2X", 10) == 20
    assert run_lakehouse_pipeline._job_dpu("agg-refresh-obt_icu_stay_features-full", big, None, None, "G.1X", 8) == 8


def test_only_filter_narrows_every_phase(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", [
        "run_lakehouse_pipeline.py", "--dataset", "fulldataset",
        "--from", "C", "--only", "fact_lab_result,fact_chart_observation", "--dry-run",
    ])
    run_lakehouse_pipeline.main()
    out = capsys.readouterr().out
    assert "fact-load-fact_lab_result-full" in out
    assert "fact-load-fact_chart_observation-full" in out
    assert "fact-load-fact_admission-full" not in out   # filtered out
    assert "\n  D." not in out and "\n  E." not in out   # D-G have no matching jobs -> dropped


def test_only_with_no_matches_errors(monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "run_lakehouse_pipeline.py", "--from", "E", "--only", "fact_lab_result", "--dry-run",
    ])
    with pytest.raises(SystemExit):
        run_lakehouse_pipeline.main()


# --- process-log helpers (etl_process_log timing table) ------------------------------------

@pytest.mark.parametrize("job,mode,expected", [
    ("dim-date-seed-job-full", "full", ("dim_date_seed", "dim_date")),
    ("dim-load-dim_patient-demo", "demo", ("dim", "dim_patient")),
    ("fact-load-fact_admission-full", "full", ("fact", "fact_admission")),
    ("agg-refresh-agg_admission_daily-full", "full", ("agg", "agg_admission_daily")),
    ("agg-refresh-obt_patient_360-full", "full", ("obt", "obt_patient_360")),
    ("notes-ingest-job-demo", "demo", ("notes_ingest", "notes_ingest")),
    ("medspacy-nlp-job-full", "full", ("nlp", "medspacy_nlp")),
    ("radiology-nlp-job-full", "full", ("nlp", "radiology_nlp")),
    ("clinician-note-variant-job-full", "full", ("nlp", "clinician_note_nlp_variant")),
    ("something-unrecognized", "full", ("other", "something-unrecognized")),
])
def test_classify_job(job, mode, expected):
    assert run_lakehouse_pipeline._classify_job(job, mode) == expected


def test_sql_str_escapes_quotes_and_handles_none():
    assert run_lakehouse_pipeline._sql_str(None) == "NULL"
    assert run_lakehouse_pipeline._sql_str("O'Brien") == "'O''Brien'"
    assert run_lakehouse_pipeline._sql_str("plain") == "'plain'"


def test_sql_ts_formats_naive_and_aware_datetimes():
    import datetime
    naive = datetime.datetime(2026, 1, 2, 3, 4, 5, 678000)
    assert run_lakehouse_pipeline._sql_ts(naive) == "TIMESTAMP '2026-01-02 03:04:05.678'"
    aware = datetime.datetime(2026, 1, 2, 3, 4, 5, 678000, tzinfo=datetime.timezone.utc)
    assert run_lakehouse_pipeline._sql_ts(aware) == "TIMESTAMP '2026-01-02 03:04:05.678'"
    assert run_lakehouse_pipeline._sql_ts(None) == "NULL"


def test_log_process_run_inserts_expected_columns(monkeypatch):
    import datetime

    captured = {}

    def fake_exec(athena, sql, output_location):
        captured["sql"] = sql
        return "SUCCEEDED"

    monkeypatch.setattr(run_lakehouse_pipeline, "_athena_exec", fake_exec)
    monkeypatch.setattr(run_lakehouse_pipeline, "_row_counts", lambda *a, **kw: (546028, 546028))
    run = {
        "Id": "jr_abc123",
        "StartedOn": datetime.datetime(2026, 1, 1, 0, 0, 0),
        "CompletedOn": datetime.datetime(2026, 1, 1, 0, 5, 0),
        "ExecutionTime": 250,
        "DPUSeconds": 500.0,
        "JobRunState": "SUCCEEDED",
        "ErrorMessage": None,
    }
    run_lakehouse_pipeline._log_process_run(
        object(), "s3://bucket/athena-results/", "mimic4_db_business_full", "mimic4_db_raw_full",
        full_run_id=7, mode="full", phase="C", job_name="fact-load-fact_admission-full", run=run,
    )
    sql = captured["sql"]
    assert "INSERT INTO mimic4_db_business_full.etl_process_log" in sql
    assert "'jr_abc123'" in sql
    assert "'fact_admission'" in sql
    assert "'fact'" in sql
    assert "250" in sql
    assert sql.startswith("INSERT INTO mimic4_db_business_full.etl_process_log "
                          "(full_run_id, run_id, job_name")
    assert "VALUES (7, " in sql
    assert "546028, 546028" in sql


def test_log_process_run_never_raises_on_athena_failure(monkeypatch, capsys):
    def boom(*a, **kw):
        raise RuntimeError("no such table")

    monkeypatch.setattr(run_lakehouse_pipeline, "_athena_exec", boom)
    monkeypatch.setattr(run_lakehouse_pipeline, "_row_counts", lambda *a, **kw: (None, None))
    run_lakehouse_pipeline._log_process_run(
        object(), "s3://bucket/athena-results/", "mimic4_db_business_full", "mimic4_db_raw_full",
        full_run_id=1, mode="full", phase="A", job_name="dim-date-seed-job-full",
        run={"JobRunState": "SUCCEEDED"},
    )
    assert "WARNING" in capsys.readouterr().out


def test_athena_scalar_returns_value_on_success(monkeypatch):
    class FakeAthena:
        def start_query_execution(self, **kw):
            return {"QueryExecutionId": "q1"}

        def get_query_execution(self, **kw):
            return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

        def get_query_results(self, **kw):
            return {"ResultSet": {"Rows": [
                {"Data": [{"VarCharValue": "_col0"}]},
                {"Data": [{"VarCharValue": "42"}]},
            ]}}

    val = run_lakehouse_pipeline._athena_scalar(FakeAthena(), "SELECT count(*) FROM t", "s3://x/")
    assert val == "42"


def test_athena_scalar_returns_none_on_failed_query(monkeypatch):
    class FakeAthena:
        def start_query_execution(self, **kw):
            return {"QueryExecutionId": "q1"}

        def get_query_execution(self, **kw):
            return {"QueryExecution": {"Status": {"State": "FAILED"}}}

    assert run_lakehouse_pipeline._athena_scalar(FakeAthena(), "bad sql", "s3://x/") is None


def test_athena_scalar_never_raises(monkeypatch):
    class BoomAthena:
        def start_query_execution(self, **kw):
            raise RuntimeError("network blip")

    assert run_lakehouse_pipeline._athena_scalar(BoomAthena(), "SELECT 1", "s3://x/") is None


def test_count_rows_casts_to_int_or_none(monkeypatch):
    monkeypatch.setattr(run_lakehouse_pipeline, "_athena_scalar", lambda *a, **kw: "12345")
    assert run_lakehouse_pipeline._count_rows(object(), "s3://x/", "db", "t") == 12345

    monkeypatch.setattr(run_lakehouse_pipeline, "_athena_scalar", lambda *a, **kw: None)
    assert run_lakehouse_pipeline._count_rows(object(), "s3://x/", "db", "t") is None


def test_next_full_run_id_increments_and_defaults_to_one(monkeypatch):
    monkeypatch.setattr(run_lakehouse_pipeline, "_athena_scalar", lambda *a, **kw: "7")
    assert run_lakehouse_pipeline._next_full_run_id(object(), "s3://x/", "db") == 8

    monkeypatch.setattr(run_lakehouse_pipeline, "_athena_scalar", lambda *a, **kw: None)
    assert run_lakehouse_pipeline._next_full_run_id(object(), "s3://x/", "db") == 1


@pytest.mark.parametrize("process_type,process_name,expect_raw_table,expect_gold_table", [
    ("dim_date_seed", "dim_date", None, "dim_date"),
    ("dim", "dim_patient", "patients_raw", "dim_patient"),
    ("fact", "fact_admission", "admissions_raw", "fact_admission"),
    ("agg", "agg_admission_daily", None, "agg_admission_daily"),
    ("obt", "obt_patient_360", None, "obt_patient_360"),
    ("nlp", "medspacy_nlp", "discharge_note_raw", "fact_discharge_note_nlp"),
    ("nlp", "radiology_nlp", "radiology_note_raw", "fact_radiology_note_nlp"),
])
def test_row_counts_queries_the_expected_tables(monkeypatch, process_type, process_name,
                                                expect_raw_table, expect_gold_table):
    calls = []

    def fake_count(athena, output_location, database, table):
        calls.append((database, table))
        return 1

    monkeypatch.setattr(run_lakehouse_pipeline, "_count_rows", fake_count)
    run_lakehouse_pipeline._row_counts(
        object(), "s3://x/", process_type, process_name, "mimic4_db_raw_full", "mimic4_db_business_full")
    tables_queried = [t for _db, t in calls]
    if expect_raw_table:
        assert expect_raw_table in tables_queried
    if expect_gold_table:
        assert expect_gold_table in tables_queried


def test_row_counts_notes_ingest_sums_both_note_tables(monkeypatch):
    def fake_count(athena, output_location, database, table):
        return {"discharge_note_raw": 331794, "radiology_note_raw": 2321355}[table]

    monkeypatch.setattr(run_lakehouse_pipeline, "_count_rows", fake_count)
    raw, gold = run_lakehouse_pipeline._row_counts(
        object(), "s3://x/", "notes_ingest", "notes_ingest", "mimic4_db_raw_full", "mimic4_db_business_full")
    assert raw == 331794 + 2321355
    assert gold is None
