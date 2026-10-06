#!/usr/bin/env python3
"""
create_dim_visual_etl_jobs.py

Creates (or updates) an AWS Glue Studio VISUAL ETL job (JobMode='VISUAL' with
CodeGenConfigurationNodes) for every raw-sourced Gold dimension table -- one job per
dimension, all generated from this single script. Each job appears in the Glue Studio
console as an editable drag-and-drop DAG:

    <raw table> (Data Catalog)
        --> ApplyMapping (rename/cast to Gold schema)
        --> Add audit columns (Custom Transform -- PySpark DataFrame API, no SQL)
        --> Select single output (unwraps the Custom Transform's DynamicFrameCollection)
        --> <gold table> (Gold, Iceberg Data Catalog target)

The audit columns (created_ts/updated_ts/created_by/updated_by) are populated with a
Custom Transform node using DataFrame.withColumn(...) rather than a SparkSQL node --
these jobs intentionally avoid SQL-statement-based transforms.

NOTE ON dim_date: dim_date is a generated calendar spine (one row per day over the
study's date range), not sourced from any raw CSV -- see etl/seed_dim_date.py /
run_dim_date_seed.py instead.

Prerequisites:
  1. The raw CSVs have already been crawled into Glue tables in --raw-database
     (e.g. via setup_mimic4_s3_glue.py / src/mimic_lakehouse/aws_workflow.py).
     provider_raw and caregiver_raw are single-column CSVs that Glue's built-in CSV
     classifier does not reliably identify (crawled with zero/wrong columns), so
     aws_workflow.py's main() overwrites both with an explicit schema
     (overwrite_provider_table() / overwrite_caregiver_table()) after the crawler runs.
  2. The Gold Iceberg tables already exist in --gold-database (created by
     ddl/gold/mimic_iv_ddl_gold_combined_v4.sql via Athena) -- this script does not
     create tables, only the ETL jobs that load them.
  3. An IAM role Glue can assume, with AWSGlueServiceRole plus S3 read on the raw
     bucket and S3 read/write on the Gold bucket (use --create-role to have this
     script set one up; the same role is shared across every dimension job).

Usage:
  python3 create_dim_visual_etl_jobs.py \\
      --role-arn arn:aws:iam::123456789012:role/GlueETLRole \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket

  # Or create/reuse the shared IAM role automatically, and run every job immediately:
  python3 create_dim_visual_etl_jobs.py \\
      --create-role \\
      --scripts-bucket mimic4-glue-scripts-v3-2-bucket \\
      --raw-s3-bucket mimic4-datalake-v3-2 \\
      --gold-s3-bucket mimic4-lakehouse-v3-2 \\
      --run-now

  # Only (re)create a subset of jobs:
  python3 create_dim_visual_etl_jobs.py --role-arn ... --scripts-bucket ... \\
      --only dim_diagnosis,dim_procedure
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

# ---------------------------------------------------------------------------------------
# Per-dimension configuration, derived from the Gold DDL (ddl/gold/mimic_iv_ddl_gold_combined_v4.sql)
# cross-checked against each raw table's ACTUAL crawled schema (aws glue get-table) --
# the raw crawler often infers wider/looser types than the Gold DDL declares (e.g. sparse
# numeric/date columns with many blanks get crawled as string), so FromType reflects what
# the raw table really has, not what its CSV conceptually represents.
#
# Each entry:
#   raw_table   -- name of the crawled raw table in --raw-database
#   gold_table  -- name of the existing Iceberg table in --gold-database
#   mapping     -- list of (from_path, from_type, to_key, to_type) for ApplyMapping.
#                  from_type/to_type use Glue DynamicFrame type names (string, int, long,
#                  double, date, timestamp, boolean) -- NOT the Athena/Iceberg DDL type
#                  names, which differ (STRING vs string, BIGINT vs long, etc.).
# ---------------------------------------------------------------------------------------
DIMENSIONS = [
    {
        "name": "dim_hcpcs",
        "raw_table": "d_hcpcs_raw",
        "gold_table": "dim_hcpcs",
        "mapping": [
            ("code", "string", "code", "string"),
            # category is crawled as string (many blank values in the raw CSV prevent the
            # crawler from inferring a numeric type); Gold declares it BIGINT.
            ("category", "string", "category", "long"),
            ("long_description", "string", "long_description", "string"),
            ("short_description", "string", "short_description", "string"),
        ],
    },
    {
        "name": "dim_diagnosis",
        "raw_table": "d_icd_diagnoses_raw",
        "gold_table": "dim_diagnosis",
        "mapping": [
            ("icd_code", "string", "icd_code", "string"),
            ("icd_version", "long", "icd_version", "int"),
            ("long_title", "string", "long_title", "string"),
        ],
    },
    {
        "name": "dim_procedure",
        "raw_table": "d_icd_procedures_raw",
        "gold_table": "dim_procedure",
        "mapping": [
            # icd_code is alphanumeric for ICD-10-PCS procedure codes (e.g. "001U3J6"),
            # not numeric-only -- the raw crawler originally inferred bigint here (only
            # ICD-9 procedure codes are purely numeric), which nulled ~95% of rows.
            # Patched at the raw layer via overwrite_d_icd_procedures_table() in
            # aws_workflow.py; see the Gold DDL for the corresponding STRING column fix.
            ("icd_code", "string", "icd_code", "string"),
            ("icd_version", "long", "icd_version", "int"),
            ("long_title", "string", "long_title", "string"),
        ],
    },
    {
        "name": "dim_lab_item",
        "raw_table": "d_labitems_raw",
        "gold_table": "dim_lab_item",
        "mapping": [
            ("itemid", "long", "item_id", "long"),
            ("label", "string", "label", "string"),
            ("fluid", "string", "fluid", "string"),
            ("category", "string", "category", "string"),
        ],
    },
    {
        "name": "dim_patient",
        "raw_table": "patients_raw",
        "gold_table": "dim_patient",
        "mapping": [
            ("subject_id", "long", "subject_id", "long"),
            ("gender", "string", "gender", "string"),
            ("anchor_age", "long", "anchor_age", "int"),
            ("anchor_year", "long", "anchor_year", "int"),
            ("anchor_year_group", "string", "anchor_year_group", "string"),
            # dod is crawled as string (most patients have no date of death, so the
            # crawler can't confidently infer date); Gold declares it DATE.
            ("dod", "string", "dod", "date"),
        ],
    },
    {
        # provider_raw is recreated with an explicit 'provider_id STRING' column (see
        # overwrite_provider_table() in src/mimic_lakehouse/aws_workflow.py), replacing
        # the zero-column schema the Glue CSV classifier otherwise infers for
        # hosp/provider.csv's single column -- so no rename/cast is needed here.
        "name": "dim_provider",
        "raw_table": "provider_raw",
        "gold_table": "dim_provider",
        "mapping": [
            ("provider_id", "string", "provider_id", "string"),
        ],
    },
    {
        # caregiver_raw is recreated with an explicit 'caregiver_id BIGINT' column (see
        # overwrite_caregiver_table() in src/mimic_lakehouse/aws_workflow.py), replacing
        # the erroneous schema the Glue CSV classifier would otherwise infer for
        # icu/caregiver.csv's single column -- so no rename/cast is needed here.
        "name": "dim_caregiver",
        "raw_table": "caregiver_raw",
        "gold_table": "dim_caregiver",
        "mapping": [
            ("caregiver_id", "long", "caregiver_id", "long"),
        ],
    },
    {
        "name": "dim_chart_item",
        "raw_table": "d_items_raw",
        "gold_table": "dim_chart_item",
        "mapping": [
            ("itemid", "long", "item_id", "long"),
            ("label", "string", "label", "string"),
            ("abbreviation", "string", "abbreviation", "string"),
            ("linksto", "string", "links_to", "string"),
            ("category", "string", "category", "string"),
            ("unitname", "string", "unit_name", "string"),
            ("param_type", "string", "param_type", "string"),
            # low/highnormalvalue are crawled as string (most rows are blank, so the
            # crawler can't confidently infer double); Gold declares both DOUBLE.
            ("lownormalvalue", "string", "low_normal_value", "double"),
            ("highnormalvalue", "string", "high_normal_value", "double"),
        ],
    },
]

AUDIT_TRANSFORM_CLASS_NAME = "AddAuditColumns"

#  Glue Studio's CustomCode node generates its own
#     def {AUDIT_TRANSFORM_CLASS_NAME}(glueContext, dfc) -> DynamicFrameCollection:
#  wrapper around whatever is in `Code` (using ClassName as the function name) --
#  `Code` must be just the body, not another `def` line, or the inner def shadows the
#  outer one and the outer function returns None with no error.
#
# IMPORTANT: this node also performs the Iceberg write itself, via native Spark
# (df.writeTo(...).overwritePartitions()), instead of relying on a downstream
# S3IcebergCatalogTarget node's write_data_frame.from_catalog(additional_options=
# {{"overwrite": "true"}}). That call turned out to silently call Iceberg's .append()
# internally, NOT a real overwrite -- confirmed the hard way: every dimension job that
# was ever re-run (e.g. when dim_provider was added and the full batch re-ran) silently
# appended a second copy of its data instead of replacing it, leaving every dimension
# table except the once-only-run dim_provider with exactly 2x duplicated rows (row
# count = 2 x distinct key count). Bypassing Glue's DataSink wrapper the same way the
# fact-table jobs do (see etl/create_fact_visual_etl_jobs.py's build_transform_code)
# is the fix -- there is no downstream SelectFromCollection/S3IcebergCatalogTarget node.
def build_audit_transform_code(gold_database, gold_table):
    return f'''\
from pyspark.sql import functions as F

input_key = list(dfc.keys())[0]
df = dfc.select(input_key).toDF()
df = (
    df.withColumn("created_ts", F.current_timestamp())
      .withColumn("updated_ts", F.current_timestamp())
      .withColumn("created_by", F.lit("glue_visual_etl"))
      .withColumn("updated_by", F.lit("glue_visual_etl"))
)
df.writeTo("glue_catalog.{gold_database}.{gold_table}").option("fanout-enabled", "true").overwritePartitions()
dyf = DynamicFrame.fromDF(df, glueContext, "{AUDIT_TRANSFORM_CLASS_NAME}")
return DynamicFrameCollection({{"{AUDIT_TRANSFORM_CLASS_NAME}": dyf}}, glueContext)
'''


def ensure_glue_etl_role(iam, role_name, scripts_bucket, raw_bucket, gold_bucket) -> str:
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
        config.tprint(f"      Creating IAM role '{role_name}' for Glue Visual ETL jobs...")
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Shared role used by all AWS Glue Visual ETL jobs that load Gold dimension tables",
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
                f"arn:aws:s3:::{raw_bucket}",
                f"arn:aws:s3:::{raw_bucket}/*",
            ],
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"],
            "Resource": [
                f"arn:aws:s3:::{gold_bucket}",
                f"arn:aws:s3:::{gold_bucket}/*",
            ],
        },
    ]

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MimicGlueDimLoadS3Access",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )

    return role_arn


def build_dag(dim, raw_database, gold_database):
    source_node = {
        "S3CatalogSource": {
            "Name": f"{dim['raw_table']} (raw)",
            "Database": raw_database,
            "Table": dim["raw_table"],
        }
    }

    mapping_node = {
        "ApplyMapping": {
            "Name": "Rename & cast to Gold schema",
            "Inputs": ["node-source"],
            "Mapping": [
                {
                    "ToKey": to_key,
                    "FromPath": [from_path],
                    "FromType": from_type,
                    "ToType": to_type,
                    "Dropped": False,
                }
                for (from_path, from_type, to_key, to_type) in dim["mapping"]
            ],
        }
    }

    # This node both adds the audit columns AND performs the Iceberg write itself (see
    # build_audit_transform_code's comment for why) -- it is the DAG's terminal node;
    # there is no downstream SelectFromCollection or S3IcebergCatalogTarget.
    audit_node = {
        "CustomCode": {
            "Name": "Add audit columns & write to Gold",
            "Inputs": ["node-mapping"],
            "ClassName": AUDIT_TRANSFORM_CLASS_NAME,
            "Code": build_audit_transform_code(gold_database, dim["gold_table"]),
        }
    }

    return {
        "node-source": source_node,
        "node-mapping": mapping_node,
        "node-audit": audit_node,
    }


def create_or_update_job(glue, job_name, dag, role_arn, scripts_bucket, gold_s3_bucket,
                          glue_version, worker_type, num_workers):
    # write_data_frame.from_catalog() (the API Glue's S3IcebergCatalogTarget node codegens
    # to) resolves the table through a Spark catalog named "glue_catalog". --datalake-
    # formats=iceberg only puts the Iceberg jars on the classpath -- it does NOT register
    # that catalog. Without these --conf entries, Spark has no catalog by that name and
    # resolves the identifier against its built-in default catalog instead, which fails
    # with: AnalysisException: spark_catalog requires a single-part namespace, but got [...]
    iceberg_conf = " ".join([
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
        f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{gold_s3_bucket}/",
        "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        # Several Gold dimension tables (dim_date, dim_diagnosis, ...) have plain
        # TIMESTAMP audit columns (timestamp-without-timezone in Iceberg's spec) --
        # Glue 4.0's Spark (3.3) needs this compatibility flag or the write fails with:
        #   IllegalArgumentException: Cannot handle timestamp without timezone fields in Spark.
        "--conf spark.sql.iceberg.handle-timestamp-without-timezone=true",
    ])

    common_kwargs = dict(
        Description=f"Visual ETL: raw -> Gold for {job_name} (auto-generated)",
        Role=role_arn,
        GlueVersion=glue_version,
        WorkerType=worker_type,
        NumberOfWorkers=num_workers,
        Command={
            "Name": "glueetl",
            "ScriptLocation": f"s3://{scripts_bucket}/scripts/{job_name}",
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--job-language": "python",
            "--conf": iceberg_conf,
            "--TempDir": f"s3://{scripts_bucket}/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
        },
        CodeGenConfigurationNodes=dag,
        JobMode="VISUAL",
    )

    try:
        glue.get_job(JobName=job_name)
        config.tprint(f"      Job '{job_name}' already exists -- updating its DAG/config.")
        glue.update_job(JobName=job_name, JobUpdate=common_kwargs)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        config.tprint(f"      Creating job '{job_name}'...")
        glue.create_job(Name=job_name, Tags=config.project_tags(), **common_kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config.add_dataset_arg(parser)
    parser.add_argument("--role-arn", default=None, help="IAM role ARN for Glue to assume when running these jobs")
    parser.add_argument("--create-role", action="store_true", help="Create or reuse a single shared IAM role for all dimension jobs instead of passing --role-arn")
    parser.add_argument("--role-name", default="MimicGlueDimLoadRole", help="IAM role base name (suffixed per --dataset; default: MimicGlueDimLoadRole)")
    parser.add_argument("--raw-database", default=None, help="Glue raw database (default: from --dataset)")
    parser.add_argument("--raw-s3-bucket", default=None, help="Raw S3 bucket (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Glue gold database (default: from --dataset)")
    parser.add_argument("--gold-s3-bucket", default=None, help="Gold S3 bucket / Iceberg warehouse (default: from --dataset)")
    parser.add_argument("--scripts-bucket", default=None, help=f"Glue script/temp bucket (default: {config.SCRIPTS_BUCKET})")
    parser.add_argument("--job-prefix", default="dim-load-", help="Prefix for generated job names (default: dim-load-)")
    parser.add_argument("--glue-version", default="4.0", help="Glue version (must be 4.0+ for native Iceberg support; default: 4.0)")
    parser.add_argument("--worker-type", default="G.1X", help="Worker type (default: G.1X)")
    parser.add_argument("--num-workers", type=int, default=2, help="Number of workers (default: 2)")
    parser.add_argument("--only", default=None, help="Comma-separated subset of dimension names to generate (default: all)")
    parser.add_argument("--run-now", action="store_true", help="Also start a run of each job immediately after creating/updating it")
    parser.add_argument("--region", default=None, help="AWS region override")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config.resolve_job_defaults(args, role_base=args.role_name)

    if args.create_role and args.role_arn:
        parser.error("Cannot specify both --create-role and --role-arn; choose one.")
    if not args.role_arn and not args.create_role:
        parser.error("--role-arn or --create-role is required.")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    glue = session.client("glue")

    if args.create_role:
        iam = session.client("iam")
        args.role_arn = ensure_glue_etl_role(
            iam, args.role_name, args.scripts_bucket, args.raw_s3_bucket, args.gold_s3_bucket,
        )
        config.tprint(f"Using Glue ETL role ARN: {args.role_arn}")

    selected = DIMENSIONS
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        selected = [d for d in DIMENSIONS if d["name"] in wanted]
        missing = wanted - {d["name"] for d in selected}
        if missing:
            config.tprint(f"WARNING: unknown dimension name(s) ignored: {missing}", file=sys.stderr)

    if not selected:
        config.tprint("Nothing to do -- no dimensions selected.", file=sys.stderr)
        sys.exit(1)

    created_jobs = []
    for dim in selected:
        job_name = f"{args.job_prefix}{dim['name']}{args.profile.suffix}"
        config.tprint(f"[{dim['name']}] raw:{args.raw_database}.{dim['raw_table']} -> gold:{args.gold_database}.{dim['gold_table']}")
        dag = build_dag(dim, args.raw_database, args.gold_database)
        create_or_update_job(
            glue, job_name, dag, args.role_arn, args.scripts_bucket, args.gold_s3_bucket,
            args.glue_version, args.worker_type, args.num_workers,
        )
        created_jobs.append(job_name)

    if args.run_now:
        config.tprint("\nStarting job runs...")
        for job_name in created_jobs:
            resp = glue.start_job_run(JobName=job_name)
            config.tprint(f"      {job_name}: run id {resp['JobRunId']}")

    config.tprint("\nDone. Jobs created/updated:")
    for job_name in created_jobs:
        config.tprint(f"  - {job_name}  (view/edit the DAG in AWS Glue Studio -> Jobs -> {job_name})")

    config.tprint(
        "\nNOTE: dim_date was intentionally NOT generated here -- it's a calendar spine "
        "with no raw CSV source; see run_dim_date_seed.py instead."
    )


if __name__ == "__main__":
    main()
