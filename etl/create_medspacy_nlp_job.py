#!/usr/bin/env python3
"""Create/update the AWS Glue script job that runs medSpaCy over the discharge
notes and loads mimic4_db_business.fact_discharge_note_nlp.

Derives tobacco/alcohol/obesity levels + a tobacco-cessation code with medSpaCy
(spaCy + PyRuSH + a rule-based TargetMatcher + the ConText negation/historical/
hypothetical/family algorithm) -- an entirely local, open-source pipeline: no external
service, no data-access role, no per-character billing.

medSpaCy is installed at job start via --additional-python-modules (needs internet
egress from the Glue job, which the default no-VPC connection has). Defaults to a
100,000-note cap (--note-limit) -- lowered from the 1M cap the fact tables use because
medSpaCy's per-document cost (~1.5s/note observed) makes a 1M-note backfill take far
longer than the Glue job's 120-minute Timeout on the provisioned worker count;
--all-notes processes every discharge note.

Prerequisites:
  * etl/notes_ingest.py has been run (mimic4_db_raw.discharge_note_raw exists).
  * mimic4_db_business.fact_discharge_note_nlp exists (ddl/gold/...v4.sql via Athena).

Usage:
  python etl/create_medspacy_nlp_job.py --create-role \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket --run-now
  python etl/create_medspacy_nlp_job.py --create-role \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket --all-notes --run-now
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

GLUE_ROLE_NAME_DEFAULT = "MimicGlueMedspacyNlpRole"
# medSpaCy 1.1.x pulls a compatible spaCy (<3.8); pin only medSpaCy and let it resolve
# the rest. Adjust if the Glue runtime's Python version needs a different wheel set.
MEDSPACY_MODULES = "medspacy==1.1.5"


def ensure_bucket(s3, bucket: str, region: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        config.tprint(f"      Bucket '{bucket}' already exists -- using it.")
        return
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "403":
            raise SystemExit(f"Bucket '{bucket}' exists in another account or is not accessible.")
        if code not in ("404", "NoSuchBucket"):
            raise
    config.tprint(f"      Creating bucket '{bucket}' in {region}...")
    if region == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
    try:
        s3.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": config.project_tags_list()})
    except ClientError as exc:
        config.tprint(f"      WARNING: could not tag bucket '{bucket}': {exc}", file=sys.stderr)


def ensure_prefix(s3, bucket: str, prefix: str) -> None:
    prefix = prefix.strip("/") + "/"
    if s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get("KeyCount", 0) > 0:
        config.tprint(f"      Folder 's3://{bucket}/{prefix}' already exists -- using it.")
        return
    config.tprint(f"      Creating folder 's3://{bucket}/{prefix}'.")
    s3.put_object(Bucket=bucket, Key=prefix)


def ensure_glue_role(iam, role_name, scripts_bucket, nlp_bucket, raw_bucket, gold_bucket) -> str:
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
            Description="Role for the MIMIC medSpaCy NLP Glue job",
            Tags=config.project_tags_list(),
        )["Role"]["Arn"]
        time.sleep(10)

    iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueMedspacyNlpAccess",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": [f"arn:aws:s3:::{scripts_bucket}", f"arn:aws:s3:::{scripts_bucket}/*", f"arn:aws:s3:::{raw_bucket}", f"arn:aws:s3:::{raw_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"], "Resource": [f"arn:aws:s3:::{nlp_bucket}", f"arn:aws:s3:::{nlp_bucket}/*", f"arn:aws:s3:::{gold_bucket}", f"arn:aws:s3:::{gold_bucket}/*"]},
                    {"Effect": "Allow", "Action": ["glue:GetTable", "glue:GetDatabase", "glue:CreateTable", "glue:UpdateTable", "glue:GetPartitions", "glue:BatchCreatePartition", "glue:BatchGetPartition"], "Resource": "*"},
                ],
            }
        ),
    )
    return arn


def upload_script(s3, bucket: str, job_name: str) -> str:
    script_path = Path(__file__).resolve().parents[1] / "medspacy_nlp.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    key = f"scripts/{job_name}/medspacy_nlp.py"
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
            "--conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
        ]
    )
    note_limit = "all" if args.all_notes else str(args.note_limit)
    payload = dict(
        Description="MIMIC discharge-note NLP via medSpaCy -> fact_discharge_note_nlp (auto-generated)",
        Role=role_arn,
        GlueVersion=args.glue_version,
        WorkerType=args.worker_type,
        NumberOfWorkers=args.num_workers,
        Timeout=120,
        Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--additional-python-modules": args.medspacy_modules,
            "--conf": iceberg_conf,
            "--raw_database": args.raw_database,
            "--gold_database": args.gold_database,
            "--gold_s3_bucket": args.gold_s3_bucket,
            "--nlp_bucket": args.nlp_bucket,
            "--medspacy_results_prefix": args.medspacy_results_prefix,
            "--note_limit": note_limit,
            "--write_note_json": "false" if args.no_note_json else "true",
            "--refresh_mode": args.refresh_mode,
            "--num_buckets": str(args.num_buckets),
            "--region": args.region,
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
        glue.create_job(Name=job_name, Tags=config.project_tags("LAKEHSE-NLP"), **payload)

    # UpdateJob has no Tags parameter, so a pre-existing job (or one whose desired tag
    # value just changed to LAKEHSE-NLP) never gets it from the branches above alone --
    # apply it explicitly every run so it's correct regardless of create vs. update path.
    config.tag_glue_resource(glue, "job", job_name, args.region, "LAKEHSE-NLP")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    config.add_dataset_arg(parser)
    parser.add_argument("--role-arn", default=None, help="Glue job role ARN (skip role creation)")
    parser.add_argument("--create-role", action="store_true", help="Create/reuse the Glue job role")
    parser.add_argument("--glue-role-name", default=GLUE_ROLE_NAME_DEFAULT, help="IAM role base name (suffixed per --dataset)")
    parser.add_argument("--scripts-bucket", default=None)
    parser.add_argument("--raw-s3-bucket", default=None, help="Raw S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket (default: from --dataset)")
    parser.add_argument("--raw-database", default=None, help="Glue raw database (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database (default: from --dataset)")
    parser.add_argument("--nlp-bucket", default=None, help="Per-note / NLP-results S3 bucket (default: from --dataset)")
    parser.add_argument("--medspacy-results-prefix", default=config.get("MEDSPACY_RESULTS_PREFIX", "medspacy-results"))
    parser.add_argument("--medspacy-modules", default=MEDSPACY_MODULES, help=f"--additional-python-modules value (default: {MEDSPACY_MODULES})")
    parser.add_argument("--note-limit", type=int, default=100000, help="Number of discharge notes to process (default: 100000 -- lowered from the 1M fact-table cap since medSpaCy can't tag 1M notes within the job's 120-minute Timeout). A capped sample run never advances the etl_control watermark.")
    parser.add_argument("--all-notes", action="store_true", help="Process every discharge note (overrides --note-limit)")
    parser.add_argument("--refresh-mode", choices=["auto", "full"], default="auto",
                        help="auto (default): first run backfills, later runs reprocess only admissions with a discharge note "
                             "newer than the etl_control watermark and MERGE them in. full: reprocess every note, then advance the watermark.")
    parser.add_argument("--no-note-json", action="store_true", help="Do not write a per-note JSON to the results folder")
    parser.add_argument("--num-buckets", type=int, default=64,
                        help="Must match fact_discharge_note_nlp's PARTITIONED BY (bucket(N, hadm_id)) in "
                             "the Gold DDL (default 64). Each hadm_id bucket is processed and written as "
                             "its own Iceberg partition overwrite, so a Glue Timeout only loses the "
                             "in-flight bucket's work -- resuming the job skips admissions already written.")
    parser.add_argument("--job-name", default="medspacy-nlp-job")
    parser.add_argument("--glue-version", default="4.0")
    parser.add_argument("--worker-type", default="G.1X")
    parser.add_argument("--num-workers", type=int, default=10)
    parser.add_argument("--run-now", action="store_true")
    parser.add_argument("--region", default=config.get("AWS_REGION"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config.resolve_job_defaults(args, raw=True, gold=True, nlp=True)
    if not args.create_role and not args.role_arn:
        raise SystemExit("--role-arn or --create-role is required.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    args.region = session.region_name or "us-east-1"
    s3 = session.client("s3")
    glue = session.client("glue")

    config.tprint("[1/3] Ensuring NLP bucket and medSpaCy results folder...")
    ensure_bucket(s3, args.nlp_bucket, args.region)
    ensure_prefix(s3, args.nlp_bucket, args.medspacy_results_prefix)

    config.tprint("[2/3] Ensuring IAM role...")
    if args.create_role and not args.role_arn:
        iam = session.client("iam")
        args.role_arn = ensure_glue_role(
            iam, args.glue_role_name, args.scripts_bucket, args.nlp_bucket, args.raw_s3_bucket, args.gold_s3_bucket
        )
    config.tprint(f"      Glue job role: {args.role_arn}")

    config.tprint("[3/3] Uploading script and creating/updating the Glue job...")
    script_location = upload_script(s3, args.scripts_bucket, args.job_name)
    create_or_update_job(glue, args.job_name, script_location, args.role_arn, args)

    if args.run_now:
        run_id = glue.start_job_run(JobName=args.job_name)["JobRunId"]
        config.tprint(f"      Started job run: {run_id}")
    scope = "ALL discharge notes" if args.all_notes else f"{args.note_limit} note(s)"
    config.tprint(f"Glue job '{args.job_name}' ready (scope: {scope}). Script: {script_location}")


if __name__ == "__main__":
    main()
