#!/usr/bin/env python3
"""Create/update the Glue 6.0 job that combines fact_discharge_note_nlp and
fact_radiology_note_nlp (STRING+JSON, Athena-queryable) into fact_clinician_note_nlp_v, a
second Inference Fact instance with real Iceberg VARIANT columns covering both note types
at the shared admission grain (full outer join on hadm_id -- an admission with only one
note type still gets exactly one row).

Runs on Glue 6.0 (Spark 4.1 / Iceberg 1.11) because VARIANT needs Iceberg format-version 3
-- readable only from Spark / EMR / Athena-for-Apache-Spark, not Athena SQL (engine v3).
Does no NLP: reads the two STRING tables, parse_json()s the JSON-shaped columns, writes the
combined VARIANT table. Does NOT replace or modify fact_radiology_note_nlp_v.

Prerequisites:
  * medspacy-nlp-job has run (mimic4_db_business.fact_discharge_note_nlp populated).
  * radiology-nlp-job has run (mimic4_db_business.fact_radiology_note_nlp populated).

Usage:
  python etl/create_clinician_note_variant_job.py --dataset fulldataset --create-role --run-now
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

GLUE_ROLE_NAME_DEFAULT = "MimicGlueClinicianNlpVariantRole"


def ensure_glue_role(iam, role_name, scripts_bucket, gold_bucket) -> str:
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
            Description="Role for the fact_clinician_note_nlp_v combined-inference Glue job",
            Tags=config.project_tags_list(),
        )["Role"]["Arn"]
        time.sleep(10)
    iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueClinicianNlpVariantAccess",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": [f"arn:aws:s3:::{scripts_bucket}", f"arn:aws:s3:::{scripts_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"], "Resource": [f"arn:aws:s3:::{gold_bucket}", f"arn:aws:s3:::{gold_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["glue:GetTable", "glue:GetDatabase", "glue:CreateTable", "glue:UpdateTable", "glue:GetPartitions", "glue:BatchCreatePartition", "glue:BatchGetPartition"], "Resource": "*"},
                ],
            }
        ),
    )
    return arn


def upload_script(s3, bucket: str, job_name: str) -> str:
    script_path = Path(__file__).resolve().parents[1] / "clinician_note_nlp_variant.py"
    key = f"scripts/{job_name}/clinician_note_nlp_variant.py"
    config.tprint(f"      Uploading script to s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=script_path.read_bytes())
    return f"s3://{bucket}/{key}"


def create_or_update_job(glue, job_name, script_location, role_arn, args):
    iceberg_conf = " ".join(
        [
            "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
            f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{args.gold_s3_bucket}/",
            "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
            "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        ]
    )
    payload = dict(
        Description="Combine fact_discharge_note_nlp + fact_radiology_note_nlp into fact_clinician_note_nlp_v (Iceberg VARIANT, Glue 6.0)",
        Role=role_arn,
        GlueVersion=args.glue_version,
        WorkerType=args.worker_type,
        NumberOfWorkers=args.num_workers,
        Timeout=60,
        Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--conf": iceberg_conf,
            "--gold_database": args.gold_database,
            "--gold_s3_bucket": args.gold_s3_bucket,
            "--target_table": args.target_table,
            "--refresh_mode": args.refresh_mode,
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
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database (default: from --dataset)")
    parser.add_argument("--target-table", default="fact_clinician_note_nlp_v")
    parser.add_argument("--refresh-mode", choices=["auto", "full"], default="auto",
                        help="auto (default): project only admissions touched in either source since the "
                             "etl_control watermark and MERGE them in. full: rebuild the whole projection.")
    parser.add_argument("--job-name", default="clinician-note-variant-job")
    parser.add_argument("--glue-version", default="6.0", help="Must be 6.0+ (Spark 4 / Iceberg 1.11) for VARIANT")
    parser.add_argument("--worker-type", default="G.1X")
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--run-now", action="store_true")
    parser.add_argument("--region", default=config.get("AWS_REGION"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config.resolve_job_defaults(args, raw=False, gold=True, nlp=False)
    if not args.create_role and not args.role_arn:
        raise SystemExit("--role-arn or --create-role is required.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3 = session.client("s3")
    glue = session.client("glue")

    config.tprint("[1/2] Ensuring IAM role...")
    if args.create_role and not args.role_arn:
        args.role_arn = ensure_glue_role(session.client("iam"), args.glue_role_name, args.scripts_bucket, args.gold_s3_bucket)
    config.tprint(f"      Glue job role: {args.role_arn}")

    config.tprint("[2/2] Uploading script and creating/updating the Glue job...")
    script_location = upload_script(s3, args.scripts_bucket, args.job_name)
    create_or_update_job(glue, args.job_name, script_location, args.role_arn, args)

    if args.run_now:
        run_id = glue.start_job_run(JobName=args.job_name)["JobRunId"]
        config.tprint(f"      Started job run: {run_id}")
    config.tprint(f"Glue job '{args.job_name}' ready (Glue {args.glue_version} -> {args.target_table}, VARIANT / format-v3).")


if __name__ == "__main__":
    main()
