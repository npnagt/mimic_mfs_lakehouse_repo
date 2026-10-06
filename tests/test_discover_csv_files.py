from pathlib import Path

from botocore.exceptions import ClientError

from mimic_lakehouse.aws_workflow import (
    CRAWLER_EXCLUDED_NOTE_PREFIXES,
    build_crawler_request,
    build_parser,
    discover_csv_files,
    ensure_csv_classifier,
    overwrite_caregiver_table,
    overwrite_d_icd_procedures_table,
    overwrite_provider_table,
    resolve_dataset_defaults,
)


def test_discover_csv_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "demo_subject_id.csv").write_text("id\n1\n", encoding="utf-8")
    (data_dir / "notes.txt").write_text("ignore me", encoding="utf-8")
    hosp_dir = data_dir / "hosp"
    hosp_dir.mkdir()
    (hosp_dir / "admissions.csv.gz").write_bytes(b"id\n2\n")
    icu_dir = data_dir / "icu"
    icu_dir.mkdir()
    (icu_dir / "transfers.csv.gz").write_bytes(b"id\n3\n")
    other_dir = data_dir / "other"
    other_dir.mkdir()
    (other_dir / "ignored.csv").write_text("id\n4\n", encoding="utf-8")

    files = discover_csv_files(str(data_dir), recursive=True)

    assert files == [
        str(hosp_dir / "admissions.csv.gz"),
        str(icu_dir / "transfers.csv.gz"),
    ]


def test_build_crawler_request_has_expected_targets() -> None:
    request = build_crawler_request(
        role_arn="arn:aws:iam::123456789012:role/TestRole",
        database_name="test_db",
        s3_target="s3://example-bucket/",
        table_prefix="raw_",
        classifier_name="mimic4_csv_classifier",
    )

    assert request["Targets"] == {
        "S3Targets": [
            {
                "Path": "s3://example-bucket/",
                # notes are owned end-to-end by etl/notes_ingest.py, so the crawler
                # skips both the un-parseable note CSVs and the Parquet it writes
                "Exclusions": [f"{prefix}/**" for prefix in CRAWLER_EXCLUDED_NOTE_PREFIXES],
            }
        ]
    }
    assert request["TablePrefix"] == "raw_"
    assert request["Classifiers"] == ["mimic4_csv_classifier"]


def test_default_dataset_resolves_to_demo_under_data_raw() -> None:
    args = build_parser().parse_args([])
    resolve_dataset_defaults(args)

    assert args.dataset == "demodataset"
    assert Path(args.data_dir) == (
        Path(__file__).resolve().parents[1] / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
    )
    assert args.database == "mimic4_db_raw_demo"
    assert args.gold_bucket == "mimic4-lakehouse-v3-2-demo"


def test_fulldataset_resolves_to_full_resource_names() -> None:
    args = build_parser().parse_args(["--dataset", "fulldataset"])
    resolve_dataset_defaults(args)

    assert Path(args.data_dir) == (
        Path(__file__).resolve().parents[1] / "data" / "raw" / "mimic-iv-3.1-fulldataset"
    )
    assert args.bucket == "mimic4-datalake-v3-2-full"
    assert args.database == "mimic4_db_raw_full"
    assert args.crawler_name == "mimic4-data-v3-2-crawler-full"


def test_parser_accepts_skip_load_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(["--skip-load"])

    assert args.skip_load is True


def test_ensure_csv_classifier_uses_supported_glue_payload() -> None:
    class StubGlueClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        def get_classifier(self, Name: str) -> dict:
            raise ClientError(
                {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
                "GetClassifier",
            )

        def create_classifier(self, **kwargs: dict) -> dict:
            self.calls.append(("create_classifier", kwargs))
            return {}

    glue = StubGlueClient()
    ensure_csv_classifier(glue, classifier_name="mimic4_csv_classifier")

    assert glue.calls == [
        (
            "create_classifier",
            {
                "CsvClassifier": {
                    "Name": "mimic4_csv_classifier",
                    "Delimiter": ",",
                    "ContainsHeader": "UNKNOWN",
                }
            },
        )
    ]


def test_overwrite_caregiver_table_replaces_existing_table() -> None:
    class StubGlueClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []
            self.existing = True

        def get_table(self, **kwargs: dict) -> dict:
            if not self.existing:
                raise ClientError(
                    {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
                    "GetTable",
                )
            return {"Table": {"Name": "caregiver_raw"}}

        def delete_table(self, **kwargs: dict) -> None:
            self.calls.append(("delete_table", kwargs))

        def create_table(self, **kwargs: dict) -> dict:
            self.calls.append(("create_table", kwargs))
            return {}

    glue = StubGlueClient()
    overwrite_caregiver_table(glue, database_name="test_db", bucket_name="mimic4-datalake-v3-2")

    assert glue.calls[0][0] == "delete_table"
    assert glue.calls[1][0] == "create_table"
    assert glue.calls[1][1]["DatabaseName"] == "test_db"
    assert glue.calls[1][1]["TableInput"]["Name"] == "caregiver_raw"
    assert glue.calls[1][1]["TableInput"]["StorageDescriptor"]["Location"] == "s3://mimic4-datalake-v3-2/caregiver_raw/"
    assert glue.calls[1][1]["TableInput"]["StorageDescriptor"]["Columns"][0]["Name"] == "caregiver_id"
    assert glue.calls[1][1]["TableInput"]["StorageDescriptor"]["Columns"][0]["Type"] == "bigint"


def test_overwrite_provider_table_replaces_existing_table() -> None:
    class StubGlueClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []
            self.existing = True

        def get_table(self, **kwargs: dict) -> dict:
            if not self.existing:
                raise ClientError(
                    {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
                    "GetTable",
                )
            return {"Table": {"Name": "provider_raw"}}

        def delete_table(self, **kwargs: dict) -> None:
            self.calls.append(("delete_table", kwargs))

        def create_table(self, **kwargs: dict) -> dict:
            self.calls.append(("create_table", kwargs))
            return {}

    glue = StubGlueClient()
    overwrite_provider_table(glue, database_name="test_db", bucket_name="mimic4-datalake-v3-2")

    assert glue.calls[0][0] == "delete_table"
    assert glue.calls[1][0] == "create_table"
    assert glue.calls[1][1]["DatabaseName"] == "test_db"
    assert glue.calls[1][1]["TableInput"]["Name"] == "provider_raw"
    assert glue.calls[1][1]["TableInput"]["StorageDescriptor"]["Location"] == "s3://mimic4-datalake-v3-2/provider_raw/"
    assert glue.calls[1][1]["TableInput"]["StorageDescriptor"]["Columns"][0]["Name"] == "provider_id"


def test_overwrite_d_icd_procedures_table_patches_icd_code_type() -> None:
    class StubGlueClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        def get_table(self, **kwargs: dict) -> dict:
            return {
                "Table": {
                    "Name": "d_icd_procedures_raw",
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "icd_code", "Type": "bigint"},
                            {"Name": "icd_version", "Type": "bigint"},
                            {"Name": "long_title", "Type": "string"},
                        ],
                        "Location": "s3://mimic4-datalake-v3-2/d_icd_procedures_raw/",
                        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                    },
                    "PartitionKeys": [],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
                }
            }

        def update_table(self, **kwargs: dict) -> None:
            self.calls.append(("update_table", kwargs))

    glue = StubGlueClient()
    overwrite_d_icd_procedures_table(glue, database_name="test_db")

    assert glue.calls[0][0] == "update_table"
    table_input = glue.calls[0][1]["TableInput"]
    assert glue.calls[0][1]["DatabaseName"] == "test_db"
    columns_by_name = {c["Name"]: c["Type"] for c in table_input["StorageDescriptor"]["Columns"]}
    assert columns_by_name["icd_code"] == "string"
    assert columns_by_name["icd_version"] == "bigint"
    # SerdeInfo/Parameters (skip.header.line.count, etc.) must be preserved untouched --
    # this is a targeted patch, not a from-scratch table definition.
    assert table_input["Parameters"]["skip.header.line.count"] == "1"
    assert table_input["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"] == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"


def test_overwrite_d_icd_procedures_table_skips_when_already_string() -> None:
    class StubGlueClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        def get_table(self, **kwargs: dict) -> dict:
            return {
                "Table": {
                    "Name": "d_icd_procedures_raw",
                    "StorageDescriptor": {"Columns": [{"Name": "icd_code", "Type": "string"}]},
                    "Parameters": {},
                }
            }

        def update_table(self, **kwargs: dict) -> None:
            self.calls.append(("update_table", kwargs))

    glue = StubGlueClient()
    overwrite_d_icd_procedures_table(glue, database_name="test_db")

    assert glue.calls == []
