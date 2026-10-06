#!/usr/bin/env python3
"""Create/update the Glue job(s) that roll discharge_note_raw / radiology_note_raw up from
note_id grain to subject_id/hadm_id grain, producing fact_discharge_note and
fact_radiology_note -- giving the Unstructured Data Fact archetype a Gold instance at the
same grain as every other archetype in the Multimodal Fusion Schema.

One script, parameterized by --note-type, creates either job (or both, the default) so the
near-identical discharge/radiology definitions aren't duplicated across two files.

Prerequisites:
  * notes-ingest-job has run (discharge_note_raw / radiology_note_raw populated).
  * ddl/gold/mimic_iv_ddl_gold_combined_v4.sql has been applied (fact_discharge_note /
    fact_radiology_note must already exist as Gold tables -- this script does not issue
    DDL, matching every other fact/NLP job creator in this repo).

Usage:
  python etl/create_fact_note_concat_job.py --dataset fulldataset --create-role --run-now
  python etl/create_fact_note_concat_job.py --dataset fulldataset --note-type discharge --run-now
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

GLUE_ROLE_NAME_DEFAULT = "MimicGlueFactNoteConcatRole"

# note-type -> (source raw table, target Gold table, Glue job name suffix). Shared by both
# job creation and run_lakehouse_pipeline.py's wiring (see that file's _NOTE_CONCAT_JOBS).
NOTE_TYPES = {
    "discharge": ("discharge_note_raw", "fact_discharge_note", "fact-discharge-note-job"),
    "radiology": ("radiology_note_raw", "fact_radiology_note", "fact-radiology-note-job"),
}


def ensure_glue_role(iam, role_name, scripts_bucket, gold_bucket, raw_bucket) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
    try:
        arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
        config.tprint(f"      IAM role '{role_name}' already exists -- reusing it.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
        config.tprint(f"      Creating Glue job role '{role_name}'...")
        arn = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Role for the fact_discharge_note / fact_radiology_note rollup jobs",
            Tags=config.project_tags_list(),
        )["Role"]["Arn"]
        time.sleep(10)
    iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueFactNoteConcatAccess",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": [f"arn:aws:s3:::{scripts_bucket}", f"arn:aws:s3:::{scripts_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": [f"arn:aws:s3:::{raw_bucket}", f"arn:aws:s3:::{raw_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"], "Resource": [f"arn:aws:s3:::{gold_bucket}", f"arn:aws:s3:::{gold_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["glue:GetTable", "glue:GetDatabase", "glue:UpdateTable", "glue:GetPartitions", "glue:BatchCreatePartition", "glue:BatchGetPartition"], "Resource": "*"},
                ],
            }
        ),
    )
    return arn


def upload_script(s3, bucket: str, job_name: str) -> str:
    script_path = Path(__file__).resolve().parents[1] / "fact_note_concat.py"
    key = f"scripts/{job_name}/fact_note_concat.py"
    config.tprint(f"      Uploading script to s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=script_path.read_bytes())
    return f"s3://{bucket}/{key}"


def create_or_update_job(glue, job_name, script_location, role_arn, source_table, target_table, args):
    iceberg_conf = " ".join(
        [
            "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
            f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{args.gold_s3_bucket}/",
            "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
            "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            "--conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
        ]
    )
    payload = dict(
        Description=f"Roll {source_table} (note_id grain) up to {target_table} (subject_id/hadm_id grain)",
        Role=role_arn,
        GlueVersion=args.glue_version,
        WorkerType=args.worker_type,
        NumberOfWorkers=args.num_workers,
        Timeout=60,
        Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--conf": iceberg_conf,
            "--raw_database": args.raw_database,
            "--gold_database": args.gold_database,
            "--source_table": source_table,
            "--target_table": target_table,
            "--enable-glue-datacatalog": "true",
            "--TempDir": f"s3://{args.scripts_bucket}/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
        },
    )
    try:
        glue.get_job(JobName=job_name)
        config.tprint(f"      Job '{job_name}' already exists -- updating it.")
        glue.update_job(JobName=job_name, JobUpdate=payload)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating job '{job_name}'...")
        glue.create_job(Name=job_name, Tags=config.project_tags(), **payload)

    config.tag_glue_resource(glue, "job", job_name, args.region)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--role-arn", default=None)
    parser.add_argument("--create-role", action="store_true")
    config.add_dataset_arg(parser)
    parser.add_argument("--glue-role-name", default=GLUE_ROLE_NAME_DEFAULT, help="IAM role base name (suffixed per --dataset)")
    parser.add_argument("--scripts-bucket", default=None)
    parser.add_argument("--raw-database", default=None, help="Glue raw database (default: from --dataset)")
    parser.add_argument("--raw-s3-bucket", default=None, help="Raw S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database (default: from --dataset)")
    parser.add_argument("--note-type", choices=["discharge", "radiology", "both"], default="both",
                        help="Which job(s) to create/update (default: both)")
    parser.add_argument("--glue-version", default="4.0")
    parser.add_argument("--worker-type", default="G.1X")
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--run-now", action="store_true")
    parser.add_argument("--region", default=config.get("AWS_REGION"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config.resolve_job_defaults(args, raw=True, gold=True, nlp=False)
    if not args.create_role and not args.role_arn:
        raise SystemExit("--role-arn or --create-role is required.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3 = session.client("s3")
    glue = session.client("glue")

    config.tprint("[1/2] Ensuring IAM role...")
    if args.create_role and not args.role_arn:
        args.role_arn = ensure_glue_role(session.client("iam"), args.glue_role_name,
                                          args.scripts_bucket, args.gold_s3_bucket, args.raw_s3_bucket)
    config.tprint(f"      Glue job role: {args.role_arn}")

    note_types = list(NOTE_TYPES) if args.note_type == "both" else [args.note_type]
    for note_type in note_types:
        source_table, target_table, job_name_base = NOTE_TYPES[note_type]
        # dataset suffix convention matches every other job creator in this repo (e.g.
        # "fact-load-fact_admission-full"):
        job_name = f"{job_name_base}{config.dataset_profile(args.dataset).suffix}"
        config.tprint(f"[2/2] {note_type}: uploading script and creating/updating '{job_name}'...")
        script_location = upload_script(s3, args.scripts_bucket, job_name)
        create_or_update_job(glue, job_name, script_location, args.role_arn, source_table, target_table, args)
        if args.run_now:
            run_id = glue.start_job_run(JobName=job_name)["JobRunId"]
            config.tprint(f"      Started job run: {run_id}")
        config.tprint(f"Glue job '{job_name}' ready ({source_table} -> {target_table}).")


if __name__ == "__main__":
    main()
