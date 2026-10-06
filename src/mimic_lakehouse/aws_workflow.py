#!/usr/bin/env python3
"""AWS S3 + Glue workflow for ingesting local CSV data into the catalog."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional, Sequence

import boto3
from botocore.exceptions import ClientError

from mimic_lakehouse import config

# MIMIC-IV-Note CSVs (discharge.csv.gz / radiology.csv.gz) have a free-text `text`
# column with embedded newlines inside quoted fields. No Hive/Athena CSV SerDe can
# parse that (TextInputFormat splits on \n, shredding one note across many rows and
# throwing BAD_DATA on the typed columns). etl/notes_ingest.py reads them with a
# multiLine Spark reader, writes the *_note_raw Parquet, and catalogs those tables
# itself -- see run_notes_ingest.py. The crawler must skip BOTH the un-parseable
# CSV inputs and the Parquet outputs (it mis-catalogs Spark-written Parquet as
# hash-suffixed duplicate tables).
RAW_NOTE_CSV_PREFIXES = ("discharge_raw", "radiology_raw", "discharge_detail_raw", "radiology_detail_raw")
NOTE_PARQUET_PREFIXES = (
    "discharge_note_raw", "radiology_note_raw",
    "discharge_detail_note_raw", "radiology_detail_note_raw",
)
CRAWLER_EXCLUDED_NOTE_PREFIXES = RAW_NOTE_CSV_PREFIXES + NOTE_PARQUET_PREFIXES

# --fresh-reload drops every Gold table it finds in the DDL file and recreates them empty --
# except these, which hold state that's meant to survive a reload: etl_process_log is the
# append-only cross-run timing history the P6/P7 analyses compare across full_run_ids.
_KEEP_ON_FRESH_RELOAD = frozenset({"etl_process_log"})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    repo_root = Path(__file__).resolve().parents[2]
    config.add_dataset_arg(parser)
    parser.add_argument("--bucket", default=None, help="Raw/landing S3 bucket (default: from --dataset)")
    parser.add_argument("--data-dir", default=None, help="Local directory containing CSV files (default: from --dataset)")
    parser.add_argument("--recursive", action="store_true", help="Recurse into subfolders of --data-dir looking for CSVs")
    parser.add_argument("--database", default=None, help="Glue raw database name (default: from --dataset)")
    parser.add_argument("--crawler-name", default=None, help="Glue crawler name (default: from --dataset)")
    parser.add_argument("--table-prefix", default="", help="Optional prefix applied to table names created by the crawler")
    parser.add_argument("--iam-role-arn", default=None, help="Existing IAM role ARN for the Glue crawler to use")
    parser.add_argument("--create-role", action="store_true", help="Create (or reuse) an IAM role for the crawler instead of passing --iam-role-arn")
    parser.add_argument("--role-name", default=None, help="Crawler IAM role base name (default: MimicGlueCrawlerRole, suffixed per --dataset)")
    parser.add_argument("--scripts-bucket", default=config.SCRIPTS_BUCKET, help=f"S3 bucket for Glue script assets and temp files (default: {config.SCRIPTS_BUCKET})")
    parser.add_argument("--poll-interval", type=int, default=15, help="Seconds between crawler status checks (default: 15)")
    parser.add_argument("--region", default=None, help="AWS region override (defaults to your CLI/session config)")
    parser.add_argument("--skip-load", action="store_true", help="Skip S3 upload and Glue crawler execution; useful when existing data already exists and you want to avoid extra AWS usage")

    default_gold_ddl_file = repo_root / "ddl" / "gold" / "mimic_iv_ddl_gold_combined_v4.sql"
    parser.add_argument("--gold-ddl-file", default=str(default_gold_ddl_file), help=f"SQL file with Gold-zone CREATE SCHEMA/TABLE DDL to run via Athena (default: {default_gold_ddl_file})")
    parser.add_argument("--gold-bucket", default=None, help="S3 bucket backing the Gold-zone Iceberg tables (default: from --dataset)")
    parser.add_argument("--athena-workgroup", default=config.ATHENA_WORKGROUP, help="Athena workgroup to run the Gold DDL statements in (default: ATHENA_WORKGROUP, else primary)")
    parser.add_argument("--athena-output-location", default=None, help="S3 location for Athena query results (default: s3://<gold-bucket>/athena-results/)")
    parser.add_argument("--athena-poll-interval", type=int, default=2, help="Seconds between Athena query status checks (default: 2)")
    parser.add_argument("--skip-gold-ddl", action="store_true", help="Skip creating the Gold-zone schema/tables via Athena")
    parser.add_argument("--fresh-reload", action="store_true",
                        help="Drop every existing Gold-zone table (dims/facts/aggs/OBTs/NLP + etl_control) "
                             "before recreating them from --gold-ddl-file, for a completely clean reload. "
                             f"etl_process_log is kept across reloads (see {', '.join(sorted(_KEEP_ON_FRESH_RELOAD))} "
                             "-- it's the cross-run timing history the P6/P7 analyses depend on).")
    return parser


def resolve_dataset_defaults(args: argparse.Namespace) -> config.DatasetProfile:
    """Fill any dataset-derived arg still left as None from the --dataset profile.
    Returns the resolved profile (also stored as args.profile)."""
    profile = config.dataset_profile(getattr(args, "dataset", None))
    args.bucket = args.bucket or profile.raw_bucket
    args.gold_bucket = args.gold_bucket or profile.gold_bucket
    args.database = args.database or profile.raw_database
    args.crawler_name = args.crawler_name or profile.crawler_name
    args.data_dir = args.data_dir or profile.data_dir
    base_role = args.role_name or "MimicGlueCrawlerRole"
    args.role_name = base_role if base_role.endswith(profile.role_suffix) else base_role + profile.role_suffix
    args.profile = profile
    return profile


def discover_csv_files(data_dir: str, recursive: bool) -> list[str]:
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"Local data directory '{data_dir}' does not exist.")

    csv_paths: list[str] = []
    allowed_roots = {"hosp", "icu", "subject", "notes"}
    excluded_files: set[str] = set()

    if recursive:
        for root, dirs, files in os.walk(data_dir):
            rel_root = os.path.relpath(root, data_dir)
            if rel_root == ".":
                dirs[:] = [d for d in dirs if d.lower() in allowed_roots]
                continue

            first_segment = rel_root.split(os.sep)[0].lower()
            if first_segment not in allowed_roots:
                dirs[:] = []
                continue

            for filename in files:
                if filename.lower() in excluded_files:
                    continue
                if filename.lower().endswith((".csv", ".csv.gz", ".gz")):
                    csv_paths.append(os.path.join(root, filename))
    else:
        for folder_name in allowed_roots:
            folder_path = os.path.join(data_dir, folder_name)
            if not os.path.isdir(folder_path):
                continue
            for filename in os.listdir(folder_path):
                if filename.lower() in excluded_files:
                    continue
                full_path = os.path.join(folder_path, filename)
                if os.path.isfile(full_path) and filename.lower().endswith((".csv", ".csv.gz", ".gz")):
                    csv_paths.append(full_path)

    return sorted(csv_paths)


def get_region(session: boto3.Session) -> str:
    region = session.region_name
    if not region:
        config.tprint(
            "ERROR: No AWS region configured. Set it via `aws configure` or the AWS_DEFAULT_REGION environment variable.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return region


def _tag_bucket(s3, bucket_name: str) -> None:
    """Apply the Project cost-allocation tag to a bucket we just created."""
    try:
        s3.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": config.project_tags_list()},
        )
    except ClientError as exc:
        config.tprint(f"      WARNING: could not tag bucket '{bucket_name}': {exc}", file=sys.stderr)


def create_bucket(s3, bucket_name: str, region: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
        config.tprint(f"[1/3] Bucket '{bucket_name}' already exists -- skipping creation.")
        return
    except ClientError as exc:
        error_code = int(exc.response["Error"]["Code"]) if exc.response["Error"]["Code"].isdigit() else None
        if error_code not in (404,) and exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            if exc.response["Error"]["Code"] == "403":
                config.tprint(
                    f"ERROR: Bucket '{bucket_name}' exists and is owned by another account, or you lack permission to access it. Choose a different --bucket name.",
                    file=sys.stderr,
                )
                raise SystemExit(1)

    config.tprint(f"[1/3] Creating bucket '{bucket_name}' in region '{region}'...")
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region})
        config.tprint(f"      Bucket '{bucket_name}' created.")
        _tag_bucket(s3, bucket_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            config.tprint(f"      Bucket '{bucket_name}' already owned by you -- continuing.")
            _tag_bucket(s3, bucket_name)
        else:
            config.tprint(f"ERROR creating bucket: {exc}", file=sys.stderr)
            raise SystemExit(1)


def upload_csvs(s3, bucket_name: str, data_dir: str, recursive: bool) -> list[str]:
    csv_paths = discover_csv_files(data_dir, recursive)
    if not csv_paths:
        config.tprint(f"WARNING: no CSV or GZIP-compressed CSV files found under '{data_dir}'.")
        return []

    config.tprint(f"[2/3] Uploading up to {len(csv_paths)} file(s) to s3://{bucket_name}/ "
          f"(skipping any that already exist with the same size) ...")
    uploaded_prefixes: list[str] = []
    uploaded = skipped = 0
    for path in csv_paths:
        filename = os.path.basename(path)
        if filename.endswith(".csv.gz"):
            logical_name = filename[:-3]
        elif filename.endswith(".gz"):
            logical_name = filename[:-3]
        else:
            logical_name = filename
        base_name = os.path.splitext(logical_name)[0]
        # The demo subject-id list ships in its own `subject/` folder; land it in a
        # dedicated prefix so the crawler catalogs it as `demo_subject_raw`.
        folder = "demo_subject_raw" if filename == "demo_subject_id.csv" else f"{base_name}_raw"
        key = f"{folder}/{filename}"
        uploaded_prefixes.append(folder)
        local_size = os.path.getsize(path)

        existing_size = None
        try:
            existing_size = s3.head_object(Bucket=bucket_name, Key=key)["ContentLength"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise

        if existing_size == local_size:
            skipped += 1
            continue

        note = "" if existing_size is None else f" (replacing {existing_size:,}-byte object)"
        config.tprint(f"      {path}  ->  s3://{bucket_name}/{key}  ({local_size / 1048576:.2f} MB){note}")
        s3.upload_file(path, bucket_name, key)
        uploaded += 1

    config.tprint(f"      uploaded {uploaded}, skipped {skipped} already present.")
    return sorted(set(uploaded_prefixes))


def ensure_glue_role(iam, role_name: str, bucket_name: str, scripts_bucket: Optional[str] = None) -> str:
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
        config.tprint(f"      Creating IAM role '{role_name}' for Glue...")
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role used by AWS Glue crawler for mimic4-datalake-v3-2",
            Tags=config.project_tags_list(),
        )
        role_arn = response["Role"]["Arn"]
        time.sleep(10)

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    )

    s3_policy_name = "MimicGlueS3ReadAccess"
    statements = [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*",
            ],
        }
    ]
    if scripts_bucket:
        statements.append(
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"],
                "Resource": [
                    f"arn:aws:s3:::{scripts_bucket}",
                    f"arn:aws:s3:::{scripts_bucket}/*",
                ],
            }
        )
    s3_policy_doc = {"Version": "2012-10-17", "Statement": statements}
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=s3_policy_name,
        PolicyDocument=json.dumps(s3_policy_doc),
    )

    return role_arn


def ensure_database(glue, database_name: str) -> None:
    try:
        glue.get_database(Name=database_name)
        config.tprint(f"      Glue database '{database_name}' already exists -- reusing it.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating Glue database '{database_name}'...")
        glue.create_database(DatabaseInput={"Name": database_name})


def build_crawler_request(
    role_arn: str,
    database_name: str,
    s3_target: str,
    table_prefix: str,
    classifier_name: str = "mimic4_csv_classifier",
) -> dict:
    return {
        "Role": role_arn,
        "DatabaseName": database_name,
        "Targets": {
            "S3Targets": [
                {
                    "Path": s3_target,
                    # Notes are owned end-to-end by etl/notes_ingest.py (see the
                    # CRAWLER_EXCLUDED_NOTE_PREFIXES comment).
                    "Exclusions": [f"{prefix}/**" for prefix in CRAWLER_EXCLUDED_NOTE_PREFIXES],
                }
            ]
        },
        "TablePrefix": table_prefix,
        "Classifiers": [classifier_name],
    }


def ensure_csv_classifier(glue, classifier_name: str = "mimic4_csv_classifier") -> None:
    try:
        glue.get_classifier(Name=classifier_name)
        config.tprint(f"      Glue classifier '{classifier_name}' already exists -- reusing it.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating Glue classifier '{classifier_name}'...")
        glue.create_classifier(
            CsvClassifier={
                "Name": classifier_name,
                "Delimiter": ",",
                "ContainsHeader": "UNKNOWN",
            }
        )


def create_or_update_crawler(
    glue,
    crawler_name: str,
    role_arn: str,
    database_name: str,
    s3_target: str,
    table_prefix: str,
    classifier_name: str = "mimic4_csv_classifier",
) -> None:
    request = build_crawler_request(
        role_arn=role_arn,
        database_name=database_name,
        s3_target=s3_target,
        table_prefix=table_prefix,
        classifier_name=classifier_name,
    )
    try:
        glue.get_crawler(Name=crawler_name)
        config.tprint(f"      Crawler '{crawler_name}' already exists -- updating its configuration.")
        glue.update_crawler(Name=crawler_name, **request)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating crawler '{crawler_name}' targeting {s3_target} ...")
        glue.create_crawler(
            Name=crawler_name,
            Description="Crawls mimic4-datalake-v3-2 raw CSV folders into the Glue Catalog",
            Tags=config.project_tags(),
            **request,
        )


def overwrite_caregiver_table(glue, database_name: str, bucket_name: str) -> None:
    table_name = "caregiver_raw"
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        config.tprint(f"      Replacing Glue table '{table_name}' with the requested external-table definition.")
        glue.delete_table(DatabaseName=database_name, Name=table_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv"},
        "StorageDescriptor": {
            "Columns": [{"Name": "caregiver_id", "Type": "bigint"}],
            "Location": f"s3://{bucket_name}/{table_name}/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ",", "serialization.format": ","},
            },
        },
    }
    glue.create_table(DatabaseName=database_name, TableInput=table_input)


def overwrite_demo_subject_table(glue, database_name: str, bucket_name: str) -> None:
    # demo_subject_id.csv is a single-column CSV (one `subject_id` per line plus a
    # header), so it hits the same crawler issue as caregiver_raw/provider_raw: the
    # built-in classifier fails to recognize it as CSV and catalogs it as Ion with a
    # single `choice` string column. Replace it with an explicit external-table
    # definition. subject_id is numeric, so use BIGINT (like caregiver_id), and skip
    # the one header row so `SELECT *` returns exactly the 100 demo subject ids.
    table_name = "demo_subject_raw"
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        config.tprint(f"      Replacing Glue table '{table_name}' with the requested external-table definition.")
        glue.delete_table(DatabaseName=database_name, Name=table_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": {
            "Columns": [{"Name": "subject_id", "Type": "bigint"}],
            "Location": f"s3://{bucket_name}/{table_name}/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ",", "serialization.format": ","},
            },
        },
    }
    glue.create_table(DatabaseName=database_name, TableInput=table_input)


# Tables whose real schema is written by overwrite_*_table() after the crawl; the
# crawler tends to leave a hash-suffixed, zero-column duplicate of each one behind.
_HAND_MAINTAINED_RAW_TABLES = ("provider_raw", "caregiver_raw", "demo_subject_raw")


def drop_stale_note_crawler_tables(glue, database_name: str) -> None:
    # Clean up tables a crawl may have created: (a) the un-parseable note CSV tables
    # (OpenCSVSerde over newline-bearing quoted fields returns BAD_DATA), (b)
    # hash-suffixed duplicates of the Parquet note tables (the crawler mis-catalogs
    # Spark-written Parquet as a new `<name>_<32 hex>` table instead of updating),
    # and (c) the same hash-suffixed duplicates the crawler leaves for the
    # single-column CSV tables that overwrite_*_table() rewrites. The real tables
    # are owned by etl/notes_ingest.py and aws_workflow's overwrite_* helpers.
    dup_prefixes = NOTE_PARQUET_PREFIXES + _HAND_MAINTAINED_RAW_TABLES
    dup_pattern = re.compile(r"^(?:" + "|".join(dup_prefixes) + r")_[0-9a-f]{32}$")
    try:
        existing = [t["Name"] for t in _iter_glue_tables(glue, database_name)]
    except ClientError:
        existing = []

    targets = [n for n in existing if n in RAW_NOTE_CSV_PREFIXES or dup_pattern.match(n)]
    for name in targets:
        config.tprint(f"      Dropping stale crawler table '{name}' (real schema is owned elsewhere).")
        glue.delete_table(DatabaseName=database_name, Name=name)


def _iter_glue_tables(glue, database_name: str):
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database_name):
        yield from page["TableList"]


def overwrite_provider_table(glue, database_name: str, bucket_name: str) -> None:
    # provider_raw hits the same single-column-CSV crawler issue as caregiver_raw
    # (see overwrite_caregiver_table above) -- provider.csv has a header but the built-in
    # CSV classifier still fails to identify it, crawling it with zero columns
    # (classification=UNKNOWN). Unlike caregiver_id, provider_id is alphanumeric
    # (e.g. "P003F3"), so this uses STRING, not BIGINT.
    table_name = "provider_raw"
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        config.tprint(f"      Replacing Glue table '{table_name}' with the requested external-table definition.")
        glue.delete_table(DatabaseName=database_name, Name=table_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv"},
        "StorageDescriptor": {
            "Columns": [{"Name": "provider_id", "Type": "string"}],
            "Location": f"s3://{bucket_name}/{table_name}/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ",", "serialization.format": ","},
            },
        },
    }
    glue.create_table(DatabaseName=database_name, TableInput=table_input)


def overwrite_d_icd_procedures_table(glue, database_name: str) -> None:
    # Unlike caregiver_raw/provider_raw (single-column CSVs the crawler fails to
    # classify at all), d_icd_procedures_raw crawls fine as a proper 3-column CSV --
    # it just infers icd_code as BIGINT. That's correct for ICD-9 procedure codes
    # (purely numeric, e.g. "0039") but wrong for ICD-10-PCS codes, which are
    # alphanumeric (e.g. "001U3J6"): casting those to bigint at read time nulls out
    # ~95% of rows (80,814 of 85,257 in the demo dataset). Patch just that column's
    # type in place rather than reconstructing the table definition from scratch --
    # long_title contains embedded commas inside quoted values, and re-guessing the
    # crawler's SerDe/format settings risks breaking parsing that already works.
    table_name = "d_icd_procedures_raw"
    try:
        table = glue.get_table(DatabaseName=database_name, Name=table_name)["Table"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            config.tprint(f"      Table '{table_name}' does not exist yet -- skipping icd_code type fix.")
            return
        raise

    columns = table["StorageDescriptor"]["Columns"]
    if not any(c["Name"] == "icd_code" and c["Type"] != "string" for c in columns):
        config.tprint(f"      Table '{table_name}': icd_code is already non-numeric -- nothing to patch.")
        return

    for column in columns:
        if column["Name"] == "icd_code":
            column["Type"] = "string"

    table_input = {
        key: table[key]
        for key in ("Name", "StorageDescriptor", "PartitionKeys", "TableType", "Parameters")
        if key in table
    }
    config.tprint(f"      Patching '{table_name}.icd_code' column type to STRING.")
    glue.update_table(DatabaseName=database_name, TableInput=table_input)


def run_crawler_and_wait(glue, crawler_name: str, poll_interval: int) -> None:
    config.tprint(f"[3/3] Starting crawler '{crawler_name}'...")
    try:
        glue.start_crawler(Name=crawler_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "CrawlerRunningException":
            config.tprint("      Crawler is already running -- will just poll it.")
        else:
            raise

    while True:
        response = glue.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        config.tprint(f"      Crawler state: {state}")
        if state == "READY":
            break
        time.sleep(poll_interval)

    last_crawl = response["Crawler"].get("LastCrawl", {})
    status = last_crawl.get("Status", "UNKNOWN")
    config.tprint(f"      Crawler finished with last-run status: {status}")
    if status == "FAILED":
        config.tprint(f"      Error message: {last_crawl.get('ErrorMessage', 'n/a')}", file=sys.stderr)
        raise SystemExit(1)


def strip_full_line_sql_comments(sql_text: str) -> str:
    kept_lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    return "\n".join(kept_lines)


def split_sql_statements(sql_text: str) -> list[str]:
    """Split a .sql file into individual statements, honoring ';' inside quoted string literals."""
    cleaned = strip_full_line_sql_comments(sql_text)

    statements: list[str] = []
    current: list[str] = []
    in_string = False
    for ch in cleaned:
        if ch == "'":
            in_string = not in_string
        if ch == ";" and not in_string:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(ch)

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)

    return statements


def run_athena_statement(athena, query: str, output_location: str, workgroup: str, poll_interval: int) -> None:
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    query_execution_id = response["QueryExecutionId"]

    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll_interval)

    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "n/a")
        config.tprint(f"ERROR: Athena query failed ({state}): {reason}", file=sys.stderr)
        config.tprint(f"      Query: {query[:200]}", file=sys.stderr)
        raise SystemExit(1)


def gold_table_names(ddl_text: str, gold_database: str) -> list[str]:
    """Every `CREATE TABLE IF NOT EXISTS <gold_database>.<name>` in (dataset-substituted)
    Gold DDL text, in file order -- used by --fresh-reload to know what to drop without
    hand-maintaining a second copy of the table list."""
    pattern = rf"CREATE TABLE IF NOT EXISTS {re.escape(gold_database)}\.(\w+)"
    return re.findall(pattern, ddl_text, re.IGNORECASE)


def apply_gold_ddl(athena, ddl_file: str, output_location: str, workgroup: str, poll_interval: int,
                   substitutions: Optional[dict] = None, fresh_reload: bool = False) -> None:
    ddl_path = Path(ddl_file)
    if not ddl_path.is_file():
        config.tprint(f"ERROR: Gold DDL file '{ddl_path}' does not exist.", file=sys.stderr)
        raise SystemExit(1)

    text = ddl_path.read_text(encoding="utf-8")
    for old, new in (substitutions or {}).items():
        if old != new:
            text = text.replace(old, new)

    if fresh_reload:
        gold_database = (substitutions or {}).get("mimic4_db_business", "mimic4_db_business")
        tables = [t for t in gold_table_names(text, gold_database) if t not in _KEEP_ON_FRESH_RELOAD]
        config.tprint(f"[4/4] --fresh-reload: dropping {len(tables)} existing Gold-zone table(s) in "
              f"'{gold_database}' (keeping {', '.join(sorted(_KEEP_ON_FRESH_RELOAD))})...")
        for index, table in enumerate(tables, start=1):
            config.tprint(f"      ({index}/{len(tables)}) DROP TABLE IF EXISTS {gold_database}.{table}")
            run_athena_statement(athena, f"DROP TABLE IF EXISTS {gold_database}.{table}",
                                 output_location, workgroup, poll_interval)

    statements = split_sql_statements(text)
    if not statements:
        config.tprint(f"WARNING: no SQL statements found in '{ddl_path}'.")
        return

    config.tprint(f"[4/4] Applying {len(statements)} Gold-zone DDL statement(s) from '{ddl_path}' via Athena workgroup '{workgroup}'...")
    for index, statement in enumerate(statements, start=1):
        first_line = statement.splitlines()[0][:100]
        config.tprint(f"      ({index}/{len(statements)}) {first_line}...")
        run_athena_statement(athena, statement, output_location, workgroup, poll_interval)

    config.tprint("      Gold-zone DDL applied successfully.")


def run_workflow(args: argparse.Namespace) -> None:
    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    region = get_region(session)

    s3 = session.client("s3")
    glue = session.client("glue")
    iam = session.client("iam")
    athena = session.client("athena")

    if args.scripts_bucket:
        create_bucket(s3, args.scripts_bucket, region)

    if args.skip_load:
        config.tprint("[2/3] Skipping S3 upload and Glue crawler execution because --skip-load was supplied.")
    else:
        create_bucket(s3, args.bucket, region)
        upload_csvs(s3, args.bucket, args.data_dir, args.recursive)

        if args.create_role:
            role_arn = ensure_glue_role(iam, args.role_name, args.bucket, scripts_bucket=args.scripts_bucket)
        else:
            role_arn = args.iam_role_arn

        ensure_database(glue, args.database)
        ensure_csv_classifier(glue)
        s3_target = f"s3://{args.bucket}/"
        create_or_update_crawler(
            glue,
            args.crawler_name,
            role_arn,
            args.database,
            s3_target,
            args.table_prefix,
            classifier_name="mimic4_csv_classifier",
        )
        run_crawler_and_wait(glue, args.crawler_name, args.poll_interval)

    overwrite_caregiver_table(glue, args.database, args.bucket)
    overwrite_provider_table(glue, args.database, args.bucket)
    overwrite_demo_subject_table(glue, args.database, args.bucket)
    overwrite_d_icd_procedures_table(glue, args.database)
    drop_stale_note_crawler_tables(glue, args.database)

    profile = getattr(args, "profile", None) or config.dataset_profile(getattr(args, "dataset", None))
    if args.skip_gold_ddl:
        config.tprint("[4/4] Skipping Gold-zone DDL because --skip-gold-ddl was supplied.")
    else:
        create_bucket(s3, args.gold_bucket, region)
        athena_output_location = args.athena_output_location or f"s3://{args.gold_bucket}/athena-results/"
        # The committed DDL is written for the default (un-suffixed) names; rewrite the
        # catalog DB + Iceberg bucket for this dataset before running it.
        substitutions = {
            "mimic4_db_business": profile.gold_database,
            "mimic4_db_raw": profile.raw_database,
            "mimic4-lakehouse-v3-2": args.gold_bucket,
        }
        apply_gold_ddl(athena, args.gold_ddl_file, athena_output_location, args.athena_workgroup,
                       args.athena_poll_interval, substitutions=substitutions,
                       fresh_reload=args.fresh_reload)

    config.tprint(f"\nDone ({args.dataset}). Check the Glue Data Catalog:")
    config.tprint(f"  Raw database:  {args.database}")
    config.tprint(f"  aws glue get-tables --database-name {args.database} --region {region}")
    if not args.skip_gold_ddl:
        config.tprint(f"  Gold database: {profile.gold_database}")
        config.tprint(f"  aws glue get-tables --database-name {profile.gold_database} --region {region}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    resolve_dataset_defaults(args)

    if not args.iam_role_arn and not args.create_role:
        parser.error("You must supply either --iam-role-arn <existing role> or --create-role to let the script create one for the Glue crawler.")
    if args.fresh_reload and args.skip_gold_ddl:
        parser.error("--fresh-reload drops and recreates the Gold-zone tables via the DDL step -- it cannot be combined with --skip-gold-ddl.")

    run_workflow(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
