"""Minimal .env loader (no python-dotenv dependency) + dataset-mode profiles.

Reads KEY=VALUE lines from the repo-root .env into a dict; os.environ still wins so
CI / shell exports override the file. Used by the CLI drivers to pick up bucket
names and the AWS region without hard-coding them.

`dataset_profile()` resolves the `--dataset` / DATASET mode -- demodataset (100
patients, data/raw/mimic-iv-clinical-database-demo-2.2) or fulldataset
(data/raw/mimic-iv-3.1-fulldataset) -- into the full set of per-dataset resource
names (S3 buckets, Glue catalog databases, crawler). Every load / ETL / measurement
driver takes `--dataset` and reads its resources from the profile, so the two
datasets stay fully isolated and can be built side by side.
"""

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]


def tprint(msg: str = "", **kwargs) -> None:
    """print(), prefixed with a wall-clock timestamp and always flushed. Accepts the same
    kwargs as print() (e.g. file=sys.stderr for error messages).

    These setup/job-creator scripts are normally run as children of
    run_lakehouse_pipeline.py with stdout piped through `tee` into a log file, where
    Python's default block buffering can sit on output for many minutes with nothing
    visible on screen or in the log -- making a slow-but-healthy step (a Glue crawl, an
    Athena DDL statement) look hung. A leading '\\n' in msg (used throughout for
    blank-line spacing) prints as a real blank line ahead of the timestamp rather than
    after it.
    """
    if msg.startswith("\n"):
        print(file=kwargs.get("file"))
        msg = msg[1:]
    kwargs.setdefault("flush", True)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", **kwargs)


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


_FILE_VALUES = _parse_env_file(_REPO_ROOT / ".env")


def get(key: str, default: Optional[str] = None) -> Optional[str]:
    """Return the value of `key`: os.environ first, then .env, then `default`."""
    if key in os.environ and os.environ[key] != "":
        return os.environ[key]
    return _FILE_VALUES.get(key, default)


def require(key: str) -> str:
    value = get(key)
    if value is None or value == "":
        raise SystemExit(
            f"Required config '{key}' is not set. Add it to {_REPO_ROOT / '.env'} "
            f"(see .env.example) or export it in your shell."
        )
    return value


# ---------------------------------------------------------------------------------------
# Cost-allocation tag
# ---------------------------------------------------------------------------------------
# Every AWS resource this repo provisions (S3 buckets, IAM roles, Glue jobs / crawlers)
# is tagged Project=<PROJECT_TAG_VALUE> so spend can be split from the data-warehouse
# repo (which tags Project=DWHS). Activate "Project" under Billing > Cost allocation
# tags to see it broken out in Cost Explorer.

PROJECT_TAG_KEY = "Project"
PROJECT_TAG_VALUE = get("PROJECT_TAG", "LAKEHSE")

# Athena workgroup every script submits queries to. Athena bills per-TB-scanned to the
# workgroup, not the bucket, so the Project tag only reaches query cost through a tagged
# workgroup (mimic4-lakehouse, Project=LAKEHSE). Falls back to the untagged "primary".
ATHENA_WORKGROUP = get("ATHENA_WORKGROUP", "primary")


def project_tags(value: Optional[str] = None) -> dict[str, str]:
    """Cost-allocation tags as a {key: value} dict (Glue create_* APIs take this shape).

    `value` overrides PROJECT_TAG_VALUE for callers that need a more specific Project tag
    than the repo-wide default -- e.g. the medSpaCy/radiology NLP jobs, tagged
    Project=LAKEHSE-NLP instead of the shared Project=LAKEHSE so their cost breaks out as
    its own Cost Explorer bucket instead of rolling into the rest of the pipeline."""
    return {PROJECT_TAG_KEY: value or PROJECT_TAG_VALUE}


def project_tags_list(value: Optional[str] = None) -> list[dict[str, str]]:
    """Same tags as a [{"Key":.., "Value":..}] list (IAM / S3 / RDS APIs take this shape)."""
    return [{"Key": k, "Value": v} for k, v in project_tags(value).items()]


def tag_glue_resource(glue_client, arn_kind: str, resource_name: str, region: str,
                      value: Optional[str] = None) -> None:
    """Explicitly (re)apply the Project cost-allocation tag to an EXISTING Glue resource
    (job, crawler, ...) via TagResource. Needed because Glue's UpdateJob API has no Tags
    parameter -- create_job(..., Tags=...) only ever applies on first creation, so a job
    that already existed before this call was added (or whose desired tag value changes,
    e.g. moving an NLP job from Project=LAKEHSE to Project=LAKEHSE-NLP) would otherwise
    never actually get re-tagged just by re-running its creator script."""
    import boto3
    # --region defaults to AWS_REGION, which may be unset (e.g. no .env) while boto3 still
    # resolves a region from ~/.aws/config -- use the client's actual region so the ARN
    # never reads "arn:aws:glue:None:..." (TagResource rejects it with InvalidInputException).
    region = region or glue_client.meta.region_name
    account_id = boto3.client("sts", region_name=region).get_caller_identity()["Account"]
    arn = f"arn:aws:glue:{region}:{account_id}:{arn_kind}/{resource_name}"
    glue_client.tag_resource(ResourceArn=arn, TagsToAdd=project_tags(value))


# ---------------------------------------------------------------------------------------
# Credential-refresh resilience for long-running measurement scripts
# ---------------------------------------------------------------------------------------
_CREDENTIAL_ERROR_CODES = {
    "ExpiredToken", "ExpiredTokenException", "InvalidSignatureException",
    "RequestExpired", "UnrecognizedClientException", "AuthFailure",
    "TokenRefreshRequired", "InvalidClientTokenId",
}


def is_credential_error(exc: Exception) -> bool:
    """True if `exc` is a botocore ClientError caused by expired/invalid AWS
    credentials (a temporary session token outliving its validity window), as opposed
    to any other API error. Error codes vary somewhat by AWS service, so this also
    falls back to a substring check on the message."""
    from botocore.exceptions import ClientError
    if not isinstance(exc, ClientError):
        return False
    err = exc.response.get("Error", {})
    if err.get("Code") in _CREDENTIAL_ERROR_CODES:
        return True
    msg = (err.get("Message") or "").lower()
    return "signature expired" in msg or "security token" in msg or "token has expired" in msg


class RefreshingClient:
    """Transparent proxy for a boto3 client that rebuilds the underlying boto3 Session
    -- re-resolving credentials from whatever provider chain is configured (SSO,
    assumed role, static keys, ...) -- and retries, if a call fails with an
    expired-credentials error.

    Several measurement scripts in this repo now default to hundreds of repetitions
    and can run for hours (measure_time_to_insight.py's --repeats 200 default has taken
    upward of 8 hours); a temporary credential set that outlives a shorter session
    window will expire partway through, and every subsequent AWS call then fails until
    the process is restarted. This is exactly what happened on 2026-09-21/22: a
    confirmatory time-to-insight run crashed about 7.5 hours in on an
    InvalidSignatureException, several hours of work lost. Rebuilding the Session picks
    up newly refreshed credentials automatically for provider chains that support it
    (SSO, assumed role); for a genuinely dead session (e.g. an SSO login that itself
    expired) it cannot help, since interactive re-authentication is not something
    boto3 can do unattended -- in that case this raises a clear, actionable RuntimeError
    instead of leaving a bare botocore traceback.

    Usage: `ath = RefreshingClient("athena", region)` in place of
    `boto3.Session(region_name=region).client("athena")` -- every method call
    (`start_query_execution`, `list_objects_v2`, `download_file`, ... whatever the
    wrapped service exposes) is proxied transparently via __getattr__, so this is a
    drop-in replacement requiring no changes at any call site."""

    def __init__(self, service_name: str, region: str, max_retries: int = 4,
                 backoff_seconds: float = 5.0):
        import boto3
        self._service_name = service_name
        self._region = region
        self._max_retries = max_retries
        self._backoff = backoff_seconds
        self._client = boto3.Session(region_name=region).client(service_name)

    def _refresh(self) -> None:
        import boto3
        tprint(f"  [credentials] refreshing the AWS session ({self._service_name}, region "
               f"{self._region}) after an expired-credentials error ...")
        self._client = boto3.Session(region_name=self._region).client(self._service_name)

    def _call(self, method_name: str, *args, **kwargs):
        last_exc = None
        for attempt in range(self._max_retries + 1):
            try:
                return getattr(self._client, method_name)(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 -- re-raised below if not a credential error
                if not is_credential_error(exc):
                    raise
                last_exc = exc
                if attempt < self._max_retries:
                    wait = self._backoff * (attempt + 1)
                    tprint(f"  [credentials] {self._service_name}.{method_name} failed with an "
                           f"expired-credentials error (attempt {attempt + 1}/"
                           f"{self._max_retries + 1}); waiting {wait:.0f}s and refreshing "
                           "before retrying ...")
                    time.sleep(wait)
                    self._refresh()
        raise RuntimeError(
            "AWS credentials appear to be expired and could not be refreshed after "
            f"{self._max_retries + 1} attempts ({self._service_name}, region {self._region}). "
            "If you are using AWS SSO or an assumed role, the underlying session itself has "
            "most likely expired and needs interactive re-authentication (e.g. `aws sso "
            "login`) before this run can continue -- boto3 cannot do that unattended. "
            f"Original error: {last_exc}"
        ) from last_exc

    def __getattr__(self, name):
        def method(*args, **kwargs):
            return self._call(name, *args, **kwargs)
        return method


# ---------------------------------------------------------------------------------------
# Dataset-mode profiles
# ---------------------------------------------------------------------------------------

DEFAULT_DATASET = "demodataset"
_DATASET_CHOICES = ("demodataset", "fulldataset")

# Shared across both datasets (job code is identical; only data + catalog differ).
SCRIPTS_BUCKET = get("SCRIPTS_BUCKET", "mimic4-glue-scripts-v3-2-bucket")


@dataclass(frozen=True)
class DatasetProfile:
    dataset: str          # "demodataset" | "fulldataset"
    mode: str             # short tag: "demo" | "full"
    data_dir: str         # local raw-CSV directory
    raw_bucket: str       # landing / bronze S3 bucket
    gold_bucket: str      # Iceberg gold S3 bucket (glue_catalog warehouse)
    raw_database: str     # Glue catalog DB for the crawled raw tables
    gold_database: str    # Glue catalog DB for the gold Iceberg tables
    nlp_bucket: str       # per-note text / NLP-results S3 bucket
    crawler_name: str     # Glue crawler name
    scripts_bucket: str   # shared
    demo_only: bool       # filter notes to the demo_subject list (demo) or not (full)

    @property
    def suffix(self) -> str:
        return f"-{self.mode}"

    @property
    def role_suffix(self) -> str:
        """Appended to IAM role names so demo and full keep separate roles."""
        return f"-{self.mode}"

    def role(self, base_name: str) -> str:
        """Per-dataset IAM role name, e.g. role('MimicGlueFactLoadRole') -> '...-demo'."""
        return f"{base_name}{self.role_suffix}"


_PROFILES: dict[str, DatasetProfile] = {
    "demodataset": DatasetProfile(
        dataset="demodataset", mode="demo",
        data_dir=str(_REPO_ROOT / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"),
        raw_bucket="mimic4-datalake-v3-2-demo",
        gold_bucket="mimic4-lakehouse-v3-2-demo",
        raw_database="mimic4_db_raw_demo",
        gold_database="mimic4_db_business_demo",
        nlp_bucket="mimic4-nlp-v3-2-demo",
        crawler_name="mimic4-data-v3-2-crawler-demo",
        scripts_bucket=SCRIPTS_BUCKET,
        demo_only=True,
    ),
    "fulldataset": DatasetProfile(
        dataset="fulldataset", mode="full",
        data_dir=str(_REPO_ROOT / "data" / "raw" / "mimic-iv-3.1-fulldataset"),
        raw_bucket="mimic4-datalake-v3-2-full",
        gold_bucket="mimic4-lakehouse-v3-2-full",
        raw_database="mimic4_db_raw_full",
        gold_database="mimic4_db_business_full",
        nlp_bucket="mimic4-nlp-v3-2-full",
        crawler_name="mimic4-data-v3-2-crawler-full",
        scripts_bucket=SCRIPTS_BUCKET,
        demo_only=False,
    ),
}
# convenience aliases
_PROFILES["demo"] = _PROFILES["demodataset"]
_PROFILES["full"] = _PROFILES["fulldataset"]


def dataset_profile(dataset: Optional[str] = None) -> DatasetProfile:
    """Resolve a dataset mode to its DatasetProfile.

    Precedence: explicit arg > DATASET env var > .env DATASET > DEFAULT_DATASET.
    """
    name = (dataset or get("DATASET") or DEFAULT_DATASET).strip().lower()
    if name not in _PROFILES:
        raise SystemExit(
            f"Unknown dataset '{name}'. Use one of: {', '.join(_DATASET_CHOICES)}."
        )
    return _PROFILES[name]


def add_dataset_arg(parser: argparse.ArgumentParser) -> None:
    """Add the standard `--dataset {demodataset,fulldataset}` option to a parser."""
    parser.add_argument(
        "--dataset", choices=list(_DATASET_CHOICES),
        default=(get("DATASET") or DEFAULT_DATASET),
        help=f"dataset + resource set to use (default: {DEFAULT_DATASET})",
    )


def resolve_job_defaults(args, *, raw=True, gold=True, nlp=False, role_base=None):
    """Fill dataset-derived job args (raw/gold database + bucket, nlp bucket, scripts
    bucket) that were left as None, from the --dataset profile. Also suffixes
    ``args.role_name`` (or ``role_base``) so demo / full keep separate IAM roles.
    Returns the profile (also stored on ``args.profile``)."""
    p = dataset_profile(getattr(args, "dataset", None))
    if raw:
        if getattr(args, "raw_database", None) is None:
            args.raw_database = p.raw_database
        if getattr(args, "raw_s3_bucket", None) is None:
            args.raw_s3_bucket = p.raw_bucket
    if gold:
        if getattr(args, "gold_database", None) is None:
            args.gold_database = p.gold_database
        if getattr(args, "gold_s3_bucket", None) is None:
            args.gold_s3_bucket = p.gold_bucket
    if nlp and getattr(args, "nlp_bucket", None) is None:
        args.nlp_bucket = p.nlp_bucket
    if getattr(args, "scripts_bucket", None) in (None, ""):
        args.scripts_bucket = p.scripts_bucket

    def _suffix(value):
        return value if value.endswith(p.role_suffix) else value + p.role_suffix

    base = getattr(args, "role_name", None) or role_base
    if base:
        args.role_name = _suffix(base)
    if getattr(args, "glue_role_name", None):
        args.glue_role_name = _suffix(args.glue_role_name)
    # Glue job names: append -demo / -full so the two datasets keep separate jobs.
    if getattr(args, "job_name", None) and not args.job_name.endswith(p.suffix):
        args.job_name = args.job_name + p.suffix
    args.profile = p
    return p
