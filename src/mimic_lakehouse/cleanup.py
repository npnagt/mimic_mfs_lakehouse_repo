"""Full environment teardown for the MIMIC-IV lakehouse project.

Deletes every AWS object this project's setup/ETL scripts create -- Glue databases and
their tables, Glue jobs, Glue crawlers, Glue classifiers, and IAM roles -- and empties
(but does not delete) the S3 buckets, so a full setup -> load -> tear down -> setup
again cycle can be repeated without manually hunting down leftover resources.

--dataset {demodataset,fulldataset} picks which dataset's resources to tear down; every
name below is suffixed -demo / -full to match what the load / ETL scripts create, so the
two datasets can be torn down independently. Defaults for demodataset:
  - Glue databases:  mimic4_db_raw_demo, mimic4_db_business_demo   (aws_workflow.py / DDL)
  - Glue jobs:       dim-date-seed-job-demo              (etl/create_dim_date_seed_job.py)
                     dim-load-<dimension>-demo            (etl/create_dim_visual_etl_jobs.py)
                     fact-load-<table>-demo               (etl/create_fact_visual_etl_jobs.py)
                     agg-refresh-<name>-demo              (etl/create_agg_visual_etl_jobs.py)
                     notes-ingest-job-demo                (etl/create_notes_ingest_job.py)
                     medspacy-nlp-job-demo                (etl/create_medspacy_nlp_job.py)
                     radiology-nlp-job-demo               (etl/create_radiology_nlp_job.py)
                     radiology-nlp-variant-job-demo       (etl/create_radiology_variant_job.py)
  - Glue crawler:    mimic4-data-v3-2-crawler-demo          (src/mimic_lakehouse/aws_workflow.py)
  - Glue classifier: mimic4_csv_classifier  (shared, format-only -- not deleted by default)
  - IAM roles:       MimicGlue*Role-demo
  - S3 buckets (emptied, not deleted): mimic4-datalake-v3-2-demo, mimic4-lakehouse-v3-2-demo,
    mimic4-glue-scripts-v3-2-bucket (shared), mimic4-nlp-v3-2-demo

Use --extra-crawlers/--extra-classifiers/--extra-buckets for stray resources outside
these conventions (e.g. a legacy crawler/bucket from before a naming change).

To tear down a deployment created BEFORE the --dataset naming change (unsuffixed
mimic4_db_raw / dim-load-* / MimicGlue*Role ...), pass --resource-suffix "" together
with the explicit legacy names via --raw-database/--gold-database/--crawler-names/
--nlp-bucket/etc.

--delete-buckets also removes the (emptied) bucket shells; --delete-log-groups removes
the named CloudWatch log groups (default: the /aws-glue/* groups -- note these are
shared with the data-warehouse repo's Glue jobs, which will recreate them on next run).

This defaults to a DRY RUN -- pass --yes to actually delete/empty anything.
"""

import argparse
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from mimic_lakehouse import config  # noqa: E402

_ROLE_BASES = (
    "MimicGlueCrawlerRole", "MimicGlueDimDateSeedRole", "MimicGlueDimLoadRole",
    "MimicGlueFactLoadRole", "MimicGlueAggRefreshRole", "MimicGlueNotesIngestRole",
    "MimicGlueMedspacyNlpRole", "MimicGlueRadiologyNlpRole",
)
_JOB_EXACT_BASES = (
    "dim-date-seed-job", "notes-ingest-job",
    "medspacy-nlp-job", "radiology-nlp-job", "radiology-nlp-variant-job",
)
_JOB_PREFIXES = ("dim-load-", "fact-load-", "agg-refresh-")
_DEFAULT_GLUE_LOG_GROUPS = (
    "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2", "/aws-glue/jobs/output",
    "/aws-glue/python-jobs/error", "/aws-glue/python-jobs/output",
    "/aws-glue/sessions/error", "/aws-glue/sessions/output",
    "/aws-glue/crawlers", "/aws-glue/crawlers-role", "/aws-glue/column-statistics",
)


def _split_csv(value: str) -> list:
    return [v.strip() for v in value.split(",") if v.strip()]


def delete_database_and_tables(glue, database_name: str, dry_run: bool) -> None:
    try:
        table_names = []
        paginator = glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database_name):
            table_names.extend(t["Name"] for t in page["TableList"])
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"      Database '{database_name}' does not exist -- skipping.")
            return
        raise

    print(f"      Database '{database_name}': {len(table_names)} table(s) to delete.")
    if dry_run:
        return

    # BatchDeleteTable accepts at most 100 names per call.
    for i in range(0, len(table_names), 100):
        chunk = table_names[i:i + 100]
        if chunk:
            glue.batch_delete_table(DatabaseName=database_name, TablesToDelete=chunk)

    try:
        glue.delete_database(Name=database_name)
        print(f"      Deleted database '{database_name}'.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise


def delete_matching_jobs(glue, exact_names, job_prefixes, suffix: str, dry_run: bool) -> None:
    all_jobs = []
    paginator = glue.get_paginator("get_jobs")
    for page in paginator.paginate():
        all_jobs.extend(j["Name"] for j in page["Jobs"])

    exact = set(exact_names)
    # suffix "" (legacy, pre-naming-change deployment) -> prefix jobs match by prefix
    # alone; a non-empty suffix additionally requires the job name to end with it so a
    # demo run never touches -full jobs (or vice versa).
    targets = sorted(
        j for j in all_jobs
        if j in exact
        or (j.endswith(suffix) and any(j.startswith(prefix) for prefix in job_prefixes))
    )
    print(f"      {len(targets)} Glue job(s) to delete: {targets or '(none)'}")
    if dry_run:
        return
    for name in targets:
        glue.delete_job(JobName=name)
        print(f"      Deleted job '{name}'.")


def delete_crawlers(glue, crawler_names, dry_run: bool) -> None:
    for name in crawler_names:
        try:
            glue.get_crawler(Name=name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "EntityNotFoundException":
                print(f"      Crawler '{name}' does not exist -- skipping.")
                continue
            raise
        print(f"      Crawler '{name}' to delete.")
        if not dry_run:
            glue.delete_crawler(Name=name)
            print(f"      Deleted crawler '{name}'.")


def delete_classifiers(glue, classifier_names, dry_run: bool) -> None:
    for name in classifier_names:
        try:
            glue.get_classifier(Name=name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "EntityNotFoundException":
                print(f"      Classifier '{name}' does not exist -- skipping.")
                continue
            raise
        print(f"      Classifier '{name}' to delete.")
        if not dry_run:
            glue.delete_classifier(Name=name)
            print(f"      Deleted classifier '{name}'.")


def delete_roles(iam, role_names, dry_run: bool) -> None:
    for role_name in role_names:
        try:
            iam.get_role(RoleName=role_name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchEntity":
                print(f"      Role '{role_name}' does not exist -- skipping.")
                continue
            raise

        print(f"      Role '{role_name}' to delete.")
        if dry_run:
            continue

        for policy_name in iam.list_role_policies(RoleName=role_name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
        for policy in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
        iam.delete_role(RoleName=role_name)
        print(f"      Deleted role '{role_name}'.")


def empty_bucket(s3, bucket_name: str, dry_run: bool, delete_shell: bool = False) -> None:
    # list_object_versions (rather than list_objects_v2) so this also clears out old
    # versions/delete-markers if versioning was ever turned on for the bucket -- on an
    # unversioned bucket it behaves the same as a plain object listing.
    to_delete = []
    try:
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            for v in page.get("Versions", []):
                to_delete.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                to_delete.append({"Key": m["Key"], "VersionId": m["VersionId"]})
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchBucket", "404"):
            print(f"      Bucket '{bucket_name}' does not exist -- skipping.")
            return
        raise

    action = "empty + DELETE" if delete_shell else "empty"
    print(f"      Bucket '{bucket_name}': {len(to_delete)} object version(s) to {action}.")
    if dry_run:
        return

    for i in range(0, len(to_delete), 1000):
        chunk = to_delete[i:i + 1000]
        if chunk:
            s3.delete_objects(Bucket=bucket_name, Delete={"Objects": chunk, "Quiet": True})
    if delete_shell:
        s3.delete_bucket(Bucket=bucket_name)
        print(f"      Deleted bucket '{bucket_name}'.")
    else:
        print(f"      Emptied bucket '{bucket_name}' (bucket itself left intact).")


def delete_log_groups(logs, group_names, dry_run: bool) -> None:
    for name in group_names:
        try:
            resp = logs.describe_log_groups(logGroupNamePrefix=name, limit=50)
        except ClientError:
            resp = {"logGroups": []}
        exact = [g for g in resp.get("logGroups", []) if g["logGroupName"] == name]
        if not exact:
            print(f"      Log group '{name}' does not exist -- skipping.")
            continue
        stored = exact[0].get("storedBytes", 0)
        print(f"      Log group '{name}' to delete ({stored:,} stored bytes).")
        if dry_run:
            continue
        try:
            logs.delete_log_group(logGroupName=name)
            print(f"      Deleted log group '{name}'.")
        except ClientError as exc:
            print(f"      WARNING: could not delete log group '{name}': {exc}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--yes", action="store_true", help="Actually perform deletions (default: dry run, prints what would be deleted)")
    config.add_dataset_arg(parser)
    parser.add_argument("--raw-database", default=None, help="Raw Glue database to delete, with all its tables (default: from --dataset)")
    parser.add_argument("--gold-database", default=None, help="Gold Glue database to delete, with all its tables (default: from --dataset)")
    parser.add_argument("--crawler-names", default=None, help="Comma-separated Glue crawler names to delete (default: from --dataset)")
    parser.add_argument("--extra-crawlers", default="", help="Comma-separated additional/legacy Glue crawler names to delete")
    parser.add_argument("--classifier-names", default="", help="Comma-separated Glue classifier names to delete (default: none -- mimic4_csv_classifier is shared, format-only)")
    parser.add_argument("--extra-classifiers", default="", help="Comma-separated additional/legacy Glue classifier names to delete")
    parser.add_argument("--role-names", default=None, help="Comma-separated IAM role names to delete (default: the MimicGlue*Role set, suffixed per --dataset)")
    parser.add_argument("--resource-suffix", default=None, help="Override the -demo/-full suffix used to match Glue jobs and derive role names. Pass \"\" to target a pre-naming-change (unsuffixed) deployment.")
    parser.add_argument("--raw-bucket", default=None, help="Raw S3 bucket to empty (default: from --dataset)")
    parser.add_argument("--gold-bucket", default=None, help="Gold S3 bucket to empty (default: from --dataset)")
    parser.add_argument("--scripts-bucket", default=None, help=f"Glue scripts/temp S3 bucket to empty (default: {config.SCRIPTS_BUCKET}, shared)")
    parser.add_argument("--nlp-bucket", default=None, help="NLP S3 bucket to empty (default: from --dataset)")
    parser.add_argument("--empty-scripts-bucket", action="store_true", help="Also empty the shared Glue scripts bucket (off by default -- it is shared between datasets)")
    parser.add_argument("--extra-buckets", default="", help="Comma-separated additional/legacy S3 buckets to empty (bucket shells are kept unless --delete-buckets)")
    parser.add_argument("--delete-buckets", action="store_true", help="After emptying, also delete the bucket shells (default: empty only)")
    parser.add_argument("--delete-log-groups", action="store_true", help="Also delete the CloudWatch log groups in --log-groups")
    parser.add_argument("--log-groups", default=",".join(_DEFAULT_GLUE_LOG_GROUPS), help="Comma-separated CloudWatch log group names for --delete-log-groups")
    parser.add_argument("--region", default=None, help="AWS region override")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    profile = config.dataset_profile(args.dataset)
    sfx = args.resource_suffix if args.resource_suffix is not None else profile.suffix
    args.raw_database = args.raw_database or profile.raw_database
    args.gold_database = args.gold_database or profile.gold_database
    args.crawler_names = args.crawler_names or profile.crawler_name
    args.raw_bucket = args.raw_bucket or profile.raw_bucket
    args.gold_bucket = args.gold_bucket or profile.gold_bucket
    args.scripts_bucket = args.scripts_bucket or profile.scripts_bucket
    args.nlp_bucket = args.nlp_bucket or profile.nlp_bucket
    role_names = args.role_names or ",".join(b + sfx for b in _ROLE_BASES)
    job_exact = [b + sfx for b in _JOB_EXACT_BASES]

    dry_run = not args.yes
    n_steps = 6 if args.delete_log_groups else 5
    print(f"Dataset: {profile.dataset} (resource suffix '{sfx}')\n")
    if dry_run:
        print("DRY RUN -- nothing will be deleted. Pass --yes to actually apply these changes.\n")

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    glue = session.client("glue")
    iam = session.client("iam")
    s3 = session.client("s3")

    print(f"[1/{n_steps}] Glue databases and tables")
    delete_database_and_tables(glue, args.raw_database, dry_run)
    delete_database_and_tables(glue, args.gold_database, dry_run)

    print(f"\n[2/{n_steps}] Glue jobs")
    delete_matching_jobs(glue, job_exact, _JOB_PREFIXES, sfx, dry_run)

    print(f"\n[3/{n_steps}] Glue crawlers and classifiers")
    crawler_names = _split_csv(args.crawler_names) + _split_csv(args.extra_crawlers)
    delete_crawlers(glue, crawler_names, dry_run)
    classifier_names = _split_csv(args.classifier_names) + _split_csv(args.extra_classifiers)
    delete_classifiers(glue, classifier_names, dry_run)

    print(f"\n[4/{n_steps}] IAM roles")
    delete_roles(iam, _split_csv(role_names), dry_run)

    verb = "contents + shells" if args.delete_buckets else "contents (shells kept)"
    print(f"\n[5/{n_steps}] S3 bucket {verb}")
    buckets = [args.raw_bucket, args.gold_bucket, args.nlp_bucket] + _split_csv(args.extra_buckets)
    if args.empty_scripts_bucket:
        buckets.append(args.scripts_bucket)
    seen: set = set()
    for bucket_name in buckets:
        if bucket_name and bucket_name not in seen:
            seen.add(bucket_name)
            empty_bucket(s3, bucket_name, dry_run, delete_shell=args.delete_buckets)

    if args.delete_log_groups:
        print(f"\n[6/{n_steps}] CloudWatch log groups")
        logs = session.client("logs")
        delete_log_groups(logs, _split_csv(args.log_groups), dry_run)

    print()
    if dry_run:
        print("Dry run complete -- re-run with --yes to apply.")
    else:
        print("Cleanup complete.")


if __name__ == "__main__":
    main()
