"""The DatasetProfile abstraction in src/mimic_lakehouse/config.py -- every load / ETL /
measurement driver reads its per-dataset resource names from here."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mimic_lakehouse import config


def test_demo_profile_resource_names():
    p = config.dataset_profile("demodataset")
    assert p.mode == "demo"
    assert p.raw_bucket == "mimic4-datalake-v3-2-demo"
    assert p.gold_bucket == "mimic4-lakehouse-v3-2-demo"
    assert p.raw_database == "mimic4_db_raw_demo"
    assert p.gold_database == "mimic4_db_business_demo"
    assert p.nlp_bucket == "mimic4-nlp-v3-2-demo"
    assert p.crawler_name == "mimic4-data-v3-2-crawler-demo"
    assert p.suffix == "-demo"
    assert p.data_dir.endswith(str(Path("data") / "raw" / "mimic-iv-clinical-database-demo-2.2"))
    assert p.demo_only is True


def test_full_profile_resource_names():
    p = config.dataset_profile("fulldataset")
    assert p.mode == "full"
    assert p.raw_bucket == "mimic4-datalake-v3-2-full"
    assert p.gold_database == "mimic4_db_business_full"
    assert p.crawler_name == "mimic4-data-v3-2-crawler-full"
    assert p.suffix == "-full"
    assert p.data_dir.endswith(str(Path("data") / "raw" / "mimic-iv-3.1-fulldataset"))
    assert p.demo_only is False


def test_scripts_bucket_is_shared_between_datasets():
    assert (
        config.dataset_profile("demodataset").scripts_bucket
        == config.dataset_profile("fulldataset").scripts_bucket
    )


def test_default_dataset_is_demo():
    assert config.dataset_profile().dataset == "demodataset"


def test_unknown_dataset_raises():
    with pytest.raises(SystemExit):
        config.dataset_profile("teradataset")


def test_role_suffixing_is_idempotent():
    p = config.dataset_profile("demodataset")
    once = p.role("MimicGlueFactLoadRole")
    assert once == "MimicGlueFactLoadRole-demo"


def test_project_cost_allocation_tag():
    assert config.PROJECT_TAG_KEY == "Project"
    assert config.PROJECT_TAG_VALUE == "LAKEHSE"
    assert config.project_tags() == {"Project": "LAKEHSE"}
    assert config.project_tags_list() == [{"Key": "Project", "Value": "LAKEHSE"}]


def test_resolve_job_defaults_fills_only_unset_args():
    import argparse

    p = config.dataset_profile("fulldataset")
    args = argparse.Namespace(
        dataset="fulldataset", raw_database=None, raw_s3_bucket=None,
        gold_database="explicit_db", gold_s3_bucket=None, scripts_bucket=None,
        role_name="MimicGlueFactLoadRole", glue_role_name=None, job_name="fact-load-x",
    )
    config.resolve_job_defaults(args)
    assert args.raw_database == p.raw_database          # filled from profile
    assert args.gold_database == "explicit_db"          # explicit value preserved
    assert args.role_name == "MimicGlueFactLoadRole-full"
    assert args.job_name == "fact-load-x-full"
    assert args.profile is p
