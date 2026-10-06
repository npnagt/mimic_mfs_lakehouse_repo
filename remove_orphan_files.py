# Standalone PySpark script (Glue 6.0 / Spark 4.1 / Iceberg 1.11): reclaim the physical S3
# bytes OPTIMIZE + VACUUM leave behind.
#
# KNOWN LIMITATION (documented elsewhere in this repo -- ddl/maintenance/optimize_gold_tables.sql,
# docs/storage_footprint_analysis.md): Athena's VACUUM reliably EXPIRES a table's old Iceberg
# snapshots (after it, "<table>$snapshots" shows only the current one) but does NOT physically
# delete the data files those expired snapshots referenced -- they become true orphans, backing
# no snapshot, sitting in S3 accruing storage cost until something removes them. Athena has no
# command for this; it requires Spark's Iceberg system.remove_orphan_files procedure, which is
# what this job runs.
#
# older_than defaults to "3 days ago" in Iceberg (a safety margin against deleting files an
# in-flight concurrent write might still need). The SQL CALL procedure additionally hard-
# refuses any older_than under 24 hours regardless of the caller's confidence level
# ("Cannot remove orphan files with an interval less than 24 hours ... If you are absolutely
# confident that no concurrent operations will be affected ... you can use the Action API to
# remove orphan files with an arbitrary interval" -- verified 2026-09-26). A same-day
# override via that Action API was attempted and hit a py4j method-overload-resolution
# failure (BaseTable vs. the Table interface) that isn't worth fighting for a maintenance
# job -- see git history for that attempt. This script uses the standard, safe SQL CALL
# syntax, so run it 24+ hours after the compact_gold_tables.py OPTIMIZE+VACUUM pass whose
# orphans it is meant to reclaim -- by then this table's superseded files are genuinely
# older than the floor and the CALL succeeds without needing to override anything.
#
# Usage (via the job creator): python run_remove_orphan_files.py --create-role --run-now \
#     --tables fact_admission,obt_admission_features,fact_discharge_note_nlp,fact_radiology_note_nlp
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ARGS = getResolvedOptions(sys.argv, ["gold_database", "tables"])
GOLD_DB = ARGS["gold_database"]
TABLES = [t.strip() for t in ARGS["tables"].split(",") if t.strip()]

spark = GlueContext(SparkContext()).spark_session
now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
print(f"spark={spark.version}; remove_orphan_files -> {GOLD_DB} ({len(TABLES)} table(s)), older_than={now}")

total_removed = 0
for t in TABLES:
    print(f"CALL glue_catalog.system.remove_orphan_files(table => '{GOLD_DB}.{t}', older_than => TIMESTAMP '{now}')")
    try:
        result = spark.sql(
            f"CALL glue_catalog.system.remove_orphan_files("
            f"table => '{GOLD_DB}.{t}', older_than => TIMESTAMP '{now}')"
        )
        rows = result.collect()
        removed = len(rows)
        # remove_orphan_files' result schema is a single column of removed file paths
        # (orphan_file_location); byte size isn't returned, so we only report a count here.
        total_removed += removed
        print(f"  {t}: removed {removed} orphan file(s)")
        for r in rows[:10]:
            print(f"    - {r[0]}")
        if removed > 10:
            print(f"    ... and {removed - 10} more")
    except Exception as exc:  # noqa: BLE001
        print(f"  {t}: FAILED -- {exc}")

print(f"remove_orphan_files: done. {total_removed} orphan file(s) removed across {len(TABLES)} table(s).")
