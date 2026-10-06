#!/usr/bin/env python3
"""Create or update an AWS Glue script job that runs the dim_date seed script.

This script uploads the actual root-level dim_date_seed.py Glue script, not the
helper generator script under etl/.
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


def ensure_glue_etl_role(iam, role_name, scripts_bucket, gold_bucket=None) -> str:
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
        config.tprint(f"      Creating IAM role '{role_name}' for Glue script job...")
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role used by AWS Glue script jobs for MIMIC-IV dim_date seeding",
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
        }
    ]

    if gold_bucket:
        statements.append(
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:PutObject",
                    "s3:DeleteObject",
                ],
                "Resource": [
                    f"arn:aws:s3:::{gold_bucket}",
                    f"arn:aws:s3:::{gold_bucket}/*",
                ],
            }
        )

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueDimDateSeedS3Access",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )

    return role_arn


def upload_seed_script(s3, bucket_name: str, job_name: str) -> str:
    script_path = Path(__file__).resolve().parents[1] / "dim_date_seed.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Seed script not found: {script_path}")

    script_key = f"scripts/{job_name}/dim_date_seed.py"
    config.tprint(f"      Uploading seed script to s3://{bucket_name}/{script_key}")
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=script_key,
            Body=script_path.read_bytes(),
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AccessDenied":
            raise RuntimeError(
                "Permission denied uploading the seed script to the scripts bucket. "
                "The calling user/role must have s3:PutObject on the target bucket, "
                "even if the Glue job role itself is created in this script."
            ) from exc
        raise
    return f"s3://{bucket_name}/{script_key}"


def create_or_update_job(glue, job_name, script_location, role_arn, scripts_bucket, gold_s3_bucket,
                          glue_version, worker_type, num_workers, gold_database):
    # dim_date is registered as an Iceberg table (see ddl/gold), and the seed script
    # writes it via the 3-part identifier glue_catalog.mimic4_db_business.dim_date.
    # --datalake-formats=iceberg only puts the Iceberg jars on the classpath -- it does
    # NOT register a catalog named "glue_catalog". Without these --conf entries, Spark
    # has no catalog by that name and silently resolves the identifier against its
    # built-in default catalog instead, which fails with:
    #   AnalysisException: spark_catalog requires a single-part namespace, but got [...]
    iceberg_conf = " ".join([
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
        f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{gold_s3_bucket}/",
        "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        # dim_date.created_ts/updated_ts are plain TIMESTAMP (timestamp-without-timezone
        # in Iceberg's spec). Glue 4.0's Spark (3.3) doesn't natively support that Iceberg
        # type unless this compatibility flag is set, and fails with:
        #   IllegalArgumentException: Cannot handle timestamp without timezone fields in Spark.
        "--conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
    ])

    common_kwargs = dict(
        Description=f"Glue script job to seed dim_date (auto-generated)",
        Role=role_arn,
        GlueVersion=glue_version,
        WorkerType=worker_type,
        NumberOfWorkers=num_workers,
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--conf": iceberg_conf,
            "--gold_database": gold_database,
            "--enable-glue-datacatalog": "true",
            "--TempDir": f"s3://{scripts_bucket}/temp/",
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
    parser.add_argument("--role-name", default="MimicGlueDimDateSeedRole", help="IAM role base name (suffixed per --dataset)")
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket / Iceberg warehouse (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database holding dim_date (default: from --dataset)")
    parser.add_argument("--scripts-bucket", default=None, help=f"Glue script/temp bucket (default: {config.SCRIPTS_BUCKET})")
    parser.add_argument("--job-name", default="dim-date-seed-job", help="Glue job name base (suffixed per --dataset)")
    parser.add_argument("--glue-version", default="4.0", help="Glue version (must be 4.0+ for Iceberg support; default: 4.0)")
    parser.add_argument("--worker-type", default="G.1X", help="Worker type (default: G.1X)")
    parser.add_argument("--num-workers", type=int, default=2, help="Number of workers (default: 2)")
    parser.add_argument("--run-now", action="store_true", help="Start the Glue job immediately after creating/updating it")
    parser.add_argument("--region", default=None, help="AWS region override")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config.resolve_job_defaults(args, raw=False, gold=True, nlp=False, role_base=args.role_name)

    if args.create_role and args.role_arn:
        parser.error("Cannot specify both --create-role and --role-arn; choose one.")
    if not args.role_arn and not args.create_role:
        parser.error("--role-arn or --create-role is required.")
    if args.create_role and not args.scripts_bucket:
        parser.error("--scripts-bucket is required when using --create-role.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3 = session.client("s3")
    glue = session.client("glue")

    if args.create_role:
        iam = session.client("iam")
        args.role_arn = ensure_glue_etl_role(
            iam,
            args.role_name,
            args.scripts_bucket,
            args.gold_s3_bucket,
        )
        config.tprint(f"Using Glue ETL role ARN: {args.role_arn}")

    script_location = upload_seed_script(s3, args.scripts_bucket, args.job_name)
    create_or_update_job(
        glue,
        args.job_name,
        script_location,
        args.role_arn,
        args.scripts_bucket,
        args.gold_s3_bucket,
        args.glue_version,
        args.worker_type,
        args.num_workers,
        args.gold_database,
    )

    if args.run_now:
        resp = glue.start_job_run(JobName=args.job_name)
        config.tprint(f"Started job run: {resp['JobRunId']}")

    config.tprint(f"Glue job '{args.job_name}' created/updated successfully.")
    config.tprint(f"Script location: {script_location}")


if __name__ == "__main__":
    main()
