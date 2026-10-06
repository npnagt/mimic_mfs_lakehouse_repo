# Standalone PySpark script (Glue 6.0 / Spark 4.1 / Iceberg 1.11): full compaction sweep for
# an Iceberg format-version-3 table -- rewrite_data_files, then expire_snapshots, then
# remove_orphan_files, in that order. Exists because compact_gold_tables.py (Athena OPTIMIZE
# + VACUUM) explicitly cannot touch a format-v3 table at all -- Athena engine v3 refuses any
# operation against one, not just SELECT -- so this is the only route to compacting or
# reclaiming space on fact_radiology_note_nlp_v.
#
# WHY ALL THREE STEPS, IN THIS ORDER:
#   1. rewrite_data_files -- bin-packs the CURRENT snapshot's many small files (this table
#      accumulates one file per Spark output partition per commit; see radiology_nlp_variant.py)
#      into fewer, larger ones. This adds a NEW snapshot; it does not by itself shrink total
#      S3 bytes, since the pre-compaction files are still referenced by older snapshots.
#   2. expire_snapshots -- unlike Athena's VACUUM (which only expires snapshot pointers
#      without deleting files, a documented Athena-engine-specific limitation -- see
#      ddl/maintenance/optimize_gold_tables.sql), Spark's system.expire_snapshots action
#      actually deletes the data/manifest files that no remaining snapshot references, once
#      old snapshots are expired. retain_last=1 keeps only the just-created compacted
#      snapshot; older_than=now expires everything before it.
#   3. remove_orphan_files -- catches files that were written to S3 but never referenced by
#      any manifest at all (e.g. left behind by a failed/aborted write), which
#      expire_snapshots' snapshot-graph-based cleanup cannot see. Same 24-hour older_than
#      floor as remove_orphan_files.py; not an issue here since this sweep is run well after
#      any of the runs that could have left stray files.
#
# Usage (via the job creator): python run_compact_variant_table.py --create-role --run-now \
#     --tables fact_radiology_note_nlp_v
import sys
from datetime import datetime, timedelta, timezone

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ARGS = getResolvedOptions(sys.argv, ["gold_database", "tables"])
GOLD_DB = ARGS["gold_database"]
TABLES = [t.strip() for t in ARGS["tables"].split(",") if t.strip()]

spark = GlueContext(SparkContext()).spark_session
now = datetime.now(timezone.utc)
now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
orphan_cutoff_str = (now - timedelta(hours=25)).strftime("%Y-%m-%d %H:%M:%S.%f")
print(f"spark={spark.version}; compact_variant_table -> {GOLD_DB} ({len(TABLES)} table(s))")

for t in TABLES:
    fq = f"{GOLD_DB}.{t}"
    print(f"\n=== {fq} ===")

    print(f"[1/3] CALL glue_catalog.system.rewrite_data_files(table => '{fq}')")
    try:
        spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{fq}')").show(truncate=False)
    except Exception as exc:  # noqa: BLE001
        print(f"  rewrite_data_files FAILED -- {exc}")

    print(f"[2/3] CALL glue_catalog.system.expire_snapshots(table => '{fq}', "
          f"older_than => TIMESTAMP '{now_str}', retain_last => 1)")
    try:
        spark.sql(
            f"CALL glue_catalog.system.expire_snapshots("
            f"table => '{fq}', older_than => TIMESTAMP '{now_str}', retain_last => 1)"
        ).show(truncate=False)
    except Exception as exc:  # noqa: BLE001
        print(f"  expire_snapshots FAILED -- {exc}")

    print(f"[3/3] CALL glue_catalog.system.remove_orphan_files(table => '{fq}', "
          f"older_than => TIMESTAMP '{orphan_cutoff_str}')")
    try:
        result = spark.sql(
            f"CALL glue_catalog.system.remove_orphan_files("
            f"table => '{fq}', older_than => TIMESTAMP '{orphan_cutoff_str}')"
        )
        rows = result.collect()
        print(f"  removed {len(rows)} orphan file(s)")
        for r in rows[:10]:
            print(f"    - {r[0]}")
        if len(rows) > 10:
            print(f"    ... and {len(rows) - 10} more")
    except Exception as exc:  # noqa: BLE001
        print(f"  remove_orphan_files FAILED -- {exc}")

print("\ncompact_variant_table: done.")
