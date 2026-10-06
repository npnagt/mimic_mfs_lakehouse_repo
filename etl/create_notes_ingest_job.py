#!/usr/bin/env python3
"""Create or update an AWS Glue script job that ingests MIMIC-IV-Note CSVs.

Uploads the root-level notes_ingest.py Glue script and wires it to a job. The job
reads discharge.csv.gz / radiology.csv.gz (already uploaded to the raw bucket by
setup_mimic4_s3_glue.py) with a multiLine-aware CSV reader, optionally filters to
the demo subject cohort, and writes Parquet + a Glue table per source.

Why a script job and not the crawler / a Visual ETL job: the note `text` column has
embedded newlines inside quoted fields, which no Hive/Athena CSV SerDe can parse.

Usage:
  python etl/create_notes_ingest_job.py \\
      --create-role \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket \\
      --raw-s3-bucket mimic4-datalake-v3-2 \\
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


def ensure_glue_etl_role(iam, role_name, scripts_bucket, raw_bucket) -> str:
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
        config.tprint(f"      Creating IAM role '{role_name}' for the notes-ingest Glue job...")
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role used by the AWS Glue notes-ingest script job for MIMIC-IV-Note",
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
            ],
        },
        {
            # Reads the raw note CSVs and writes the *_note_raw Parquet back into the
            # same bucket, so it needs read + write there.
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"],
            "Resource": [
                f"arn:aws:s3:::{raw_bucket}",
                f"arn:aws:s3:::{raw_bucket}/*",
            ],
        },
    ]
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueNotesIngestS3Access",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )

    return role_arn


def upload_script(s3, bucket_name: str, job_name: str) -> str:
    script_path = Path(__file__).resolve().parents[1] / "notes_ingest.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    script_key = f"scripts/{job_name}/notes_ingest.py"
    config.tprint(f"      Uploading script to s3://{bucket_name}/{script_key}")
    try:
        s3.put_object(Bucket=bucket_name, Key=script_key, Body=script_path.read_bytes())
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AccessDenied":
            raise RuntimeError(
                "Permission denied uploading the script to the scripts bucket. The "
                "calling user/role must have s3:PutObject on the target bucket."
            ) from exc
        raise
    return f"s3://{bucket_name}/{script_key}"


def create_or_update_job(glue, job_name, script_location, role_arn, scripts_bucket,
                         raw_bucket, raw_database, demo_only, glue_version,
                         worker_type, num_workers):
    common_kwargs = dict(
        Description="Glue script job: ingest MIMIC-IV-Note CSVs to Parquet (auto-generated)",
        Role=role_arn,
        GlueVersion=glue_version,
        WorkerType=worker_type,
        NumberOfWorkers=num_workers,
        Timeout=120,
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--raw_s3_bucket": raw_bucket,
            "--raw_database": raw_database,
            "--demo_only": "true" if demo_only else "false",
            "--TempDir": f"s3://{scripts_bucket}/temp/",
            # Required for spark.table()/spark.sql() to resolve against the Glue Data
            # Catalog (reading demo_subject_raw, registering the *_note_raw tables).
            "--enable-glue-datacatalog": "true",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
        },
    )

    try:
        glue.get_job(JobName=job_name)
        config.tprint(f"      Job '{job_name}' already exists -- updating it.")
        glue.update_job(JobName=job_name, JobUpdate=common_kwargs)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating job '{job_name}'...")
        glue.create_job(Name=job_name, Tags=config.project_tags(), **common_kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    config.add_dataset_arg(parser)
    parser.add_argument("--role-arn", default=None, help="IAM role ARN for Glue to assume when running the job")
    parser.add_argument("--create-role", action="store_true", help="Create or reuse an IAM role for Glue instead of passing --role-arn")
    parser.add_argument("--role-name", default="MimicGlueNotesIngestRole", help="IAM role base name (suffixed per --dataset)")
    parser.add_argument("--scripts-bucket", default=None, help=f"Glue script/temp bucket (default: {config.SCRIPTS_BUCKET})")
    parser.add_argument("--raw-s3-bucket", default=None, help="Raw S3 bucket for note CSVs + Parquet (default: from --dataset)")
    parser.add_argument("--raw-database", default=None, help="Glue raw database (default: from --dataset)")
    parser.add_argument("--all-subjects", action="store_true", help="Ingest every note (demodataset default: filter to demo_subject_raw; fulldataset: always all)")
    parser.add_argument("--job-name", default="notes-ingest-job", help="Glue job name base (suffixed per --dataset)")
    parser.add_argument("--glue-version", default="4.0", help="Glue version (default: 4.0)")
    parser.add_argument("--worker-type", default="G.2X", help="Worker type (default: G.2X -- multiLine CSV loads a whole file per partition)")
    parser.add_argument("--num-workers", type=int, default=5, help="Number of workers (default: 5)")
    parser.add_argument("--run-now", action="store_true", help="Start the Glue job immediately after creating/updating it")
    parser.add_argument("--region", default=None, help="AWS region override")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    profile = config.resolve_job_defaults(args, raw=True, gold=False, nlp=False, role_base=args.role_name)

    if args.create_role and args.role_arn:
        parser.error("Cannot specify both --create-role and --role-arn; choose one.")
    if not args.role_arn and not args.create_role:
        parser.error("--role-arn or --create-role is required.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3 = session.client("s3")
    glue = session.client("glue")

    if args.create_role:
        iam = session.client("iam")
        args.role_arn = ensure_glue_etl_role(
            iam, args.role_name, args.scripts_bucket, args.raw_s3_bucket
        )
        config.tprint(f"Using Glue ETL role ARN: {args.role_arn}")

    script_location = upload_script(s3, args.scripts_bucket, args.job_name)
    create_or_update_job(
        glue,
        args.job_name,
        script_location,
        args.role_arn,
        args.scripts_bucket,
        args.raw_s3_bucket,
        args.raw_database,
        demo_only=(profile.demo_only and not args.all_subjects),
        glue_version=args.glue_version,
        worker_type=args.worker_type,
        num_workers=args.num_workers,
    )

    if args.run_now:
        resp = glue.start_job_run(JobName=args.job_name)
        config.tprint(f"Started job run: {resp['JobRunId']}")

    config.tprint(f"Glue job '{args.job_name}' created/updated successfully.")
    config.tprint(f"Script location: {script_location}")


if __name__ == "__main__":
    main()
