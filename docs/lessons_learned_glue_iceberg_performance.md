# Chapter 12: Diagnosing Performance in a Serverless Iceberg Lakehouse

## Where Slowness Actually Comes From

Every engineer who arrives at a data lakehouse from a relational-database background carries the same mental model with them: when a query is slow, you look for a missing index, and when a write is slow, you look for a lock. Neither instinct is wrong, exactly — it is simply aimed at a machine that no longer exists. Apache Iceberg tables, the format underlying every Gold-layer table in this lakehouse, have no indexes in the relational sense. There is no B-tree to build, no statistics to refresh, no query planner deciding whether to use one. What Iceberg offers instead is *partitioning* — a physical arrangement of files on object storage — and what Spark offers on top of it is a *join strategy* chosen, sometimes badly, by a cost-based optimizer working from incomplete information.

This distinction matters because it changes where you look when something is slow. Four incidents from building out this project's Gold layer illustrate the four places performance problems actually hide in a system like this one: a false lead chasing an "index" that could never have existed, a join strategy that guessed wrong under memory pressure, a partitioning scheme that multiplied file-open overhead past what two workers could shuffle, and — cutting across all three — the simple, compounding cost of scanning tables that don't need to be scanned. Each is presented here as it was encountered: as a problem, a root cause, and a solution, in the order an engineer would actually have to reconstruct them from a stack trace and a hunch.

---

## 12.1 The Missing Index That Never Existed

**The problem.** Midway through backfilling `obt_admission_features` — a wide table built by joining nine source tables — the job stalled for over twenty minutes before failing outright. The natural question, and the one actually asked at the time, was whether an index on the table was slowing the ETL down.

**The cause.** There was no index to slow anything down, because Iceberg tables backed by Parquet files simply don't have one. This is worth dwelling on precisely because the instinct is so reasonable: in a transactional database, a write that touches an indexed column pays a real, visible cost, and diagnosing slowness by asking "what's indexed here?" is usually the right first move. In an Iceberg table, the only structural artifacts are the partition columns (which affect file *layout*, not lookup speed) and Parquet's own internal column statistics (min/max per row-group, used for scan pruning, and irrelevant to write performance). Neither one explains a stalled write. Ruling out the index took one sentence; finding the real cause took reading the actual Spark stage logs, which is the generalizable lesson — a plausible-sounding cause that matches an old mental model is still a guess until the logs confirm it.

**The solution.** There wasn't one, because there was nothing to fix. The value of this incident is entirely diagnostic: it establishes that "check for an index" is not a transferable habit in this environment, and redirects the question from *structure* to the two things that actually govern performance here — join strategy and partitioning — which are the subjects of the next two sections.

---

## 12.2 Sort-Merge vs. Broadcast: When the Optimizer Guesses Wrong

**The problem.** Once the index was ruled out, the actual error surfaced: `Could not execute broadcast in 300 secs. You can increase the timeout for broadcasts via spark.sql.broadcastTimeout or disable broadcast join by setting spark.sql.autoBroadcastJoinThreshold to -1`. The job had been running for over half an hour before failing on this timeout.

**The cause.** Spark's query optimizer chooses between two fundamentally different ways to execute a join. A *sort-merge join* is the safe, general-purpose default: both sides of the join are shuffled across the cluster, sorted by join key, and merged — predictable, scales to any data size, but requires a shuffle. A *broadcast join* is the fast path: if one side of the join is small enough, Spark copies it in full to every executor, so the other (larger) side never has to be shuffled at all. The decision is made automatically, based on Spark's *estimate* of each side's size — and that estimate is exactly where this incident went wrong.

`obt_admission_features` joins `fact_admission` against eight other fact tables, several of them filtered and grouped down to a handful of rows per admission before the join happens. Spark saw those small *result* sizes and reasonably concluded a broadcast would be cheap. What Spark's cost model does not account for is how expensive that result was to *produce*: several of those "small" join sides required scanning and aggregating a genuinely large upstream table first. On a cluster of two `G.1X` workers — the minimum viable Glue configuration, chosen deliberately to keep this project's demo footprint small — that computation simply could not finish inside the 300-second broadcast window, however small its final output would have been.

**The solution.** `--conf spark.sql.autoBroadcastJoinThreshold=-1` was added to every aggregate and OBT job's Spark configuration, disabling automatic broadcast selection entirely and forcing sort-merge joins everywhere. This is a conservative choice, not a clever one, and it was made deliberately: a predictable, somewhat slower join that always completes is worth more than a fast join that works until the day a "small" result quietly takes 301 seconds to compute. The fix moved the backfill from a 35-minute failure to a 2-minute success — the timeout, once removed, revealed that the actual computation was fast; only the misjudged broadcast attempt had been slow.

---

## 12.3 Too Many Partitions: When File Overhead Outweighs the Data

**The problem.** Two fact tables — `fact_provider_order` and `fact_lab_result` — failed intermittently during their initial loads with `IllegalStateException: Incoming records violate the writer assumption that records are clustered by spec and by partition within each spec` and, on retry, `MetadataFetchFailedException: Missing an output location for shuffle`. Other, larger fact tables loaded without incident.

**The cause.** Every Gold fact table in this project is partitioned by calendar day (`PARTITIONED BY (day(...))`), which is the right default for time-series clinical data — most queries filter by date range, and day-partitioning lets Athena and Spark skip files outside that range entirely. The right default is not the right choice unconditionally, though, and both failing tables shared the same shape: a small number of rows spread across a very large number of distinct calendar days. `fact_provider_order` and `fact_lab_result` had on the order of tens of rows per day across a date range spanning thousands of days (an artifact of MIMIC-IV's date-shifting for de-identification, which spreads a modest cohort's real-world encounter dates across a century-plus of shifted timestamps). Iceberg's partitioned writer opens one file per partition value it encounters per task; with thousands of sparse partitions and only two workers to shuffle the write across, the writer either ran out of memory holding that many open file handles or lost track of which files were still open when a shuffle stage failed and had to retry — both symptoms of the same underlying mismatch between partition *cardinality* and cluster size.

**The solution.** For these two tables specifically, day-partitioning was removed from the DDL (`-- PARTITIONED BY (day(...))`, commented out rather than deleted, preserving the record of the decision) and the tables were recreated unpartitioned. The fix was validated, not assumed: after recreating `fact_provider_order` without partitioning, the same load that had failed twice completed in roughly two minutes. The generalizable diagnostic is a ratio, not a row count — before assuming a fact-table load failure is a code defect, check the table's row-count-to-distinct-partition-value ratio. A table with millions of rows across a handful of partitions is well served by day-partitioning; a table with a few hundred rows spread across thousands of days is not, regardless of how "correct" that partitioning looks on paper.

---

## 12.4 Long ETL Execution Time: The Sum of Everything Else

**The problem.** Considered on its own, "the ETL job is slow" is not a diagnosis — it is the symptom that motivated every investigation in this chapter. `obt_admission_features` took over thirty minutes before failing; `obt_icu_stay_features`, joining `fact_chart_observation` (at 1.3 million-plus rows, the largest table in the schema), took twenty-three minutes to succeed on its first run. Both numbers are worth explaining rather than simply accepted as "big data is slow."

**The cause.** Execution time in this system is the compound product of three independent variables, and slowness in any one masquerades as slowness in the job as a whole: the *join strategy* (Section 12.2 — a misjudged broadcast doesn't just underperform, it can time out entirely rather than degrade gracefully), the *partitioning scheme* (Section 12.3 — too many small partitions turns a write into thousands of tiny, overhead-dominated file operations), and, cutting across both, the *volume actually scanned* on each run. This last variable is the one a full-reload architecture pays on every single execution: a job that re-reads its entire source table every time it runs will get slower in direct proportion to that source table's growth, with no way to opt out.

**The solution.** The other two problems in this chapter were fixed with a configuration flag and a DDL change; this one required a different architecture, applied consistently across every aggregate and OBT built in this project. Each table tracks a watermark — the newest source-row `updated_ts` it has already accounted for — in a shared `etl_control` table, and each run recomputes only the partitions touched by rows newer than that watermark, writing them back with a partition-scoped `overwritePartitions()` call that leaves every untouched partition alone. The payoff compounds with scale precisely because the cost no longer does: `obt_icu_stay_features`'s first backfill, computing every ICU stay from scratch, took twenty-three minutes; a second run immediately afterward, with no new source data to account for, printed `no source rows changed since watermark ... -- nothing to refresh` and finished in about two minutes — the time to check nine tables' watermarks and confirm there was nothing to do. A production system at several thousand times this demo's data volume will see that same asymmetry, and it is the reason a job's execution time should be evaluated against how much *changed*, not how much *exists*.

---

## Summary

| Symptom | Looked like | Actually was | Fix |
|---|---|---|---|
| Slow write, no clear cause | A missing index | Nothing — Iceberg has no indexes; the real causes are join strategy and partitioning | Redirect the investigation, don't chase a structure that doesn't exist |
| `Could not execute broadcast in 300 secs` | An oversized broadcast | An *undersized* broadcast whose upstream computation was too slow for 2 workers to finish in time | `spark.sql.autoBroadcastJoinThreshold=-1` — force sort-merge everywhere |
| `IllegalStateException` / `MetadataFetchFailedException` on write | A code defect in the ETL job | Partition cardinality (thousands of sparse days) exceeding what 2 workers can shuffle | Remove day-partitioning from low-density tables; verify with the actual reload |
| A job simply "taking a long time" | An inherent cost of the data volume | The compounding effect of join strategy, partition count, and full-table rescanning | Incremental refresh: watermark in `etl_control` + partition-scoped `overwritePartitions()` |

The thread connecting all four sections is the same one that opened this chapter: performance problems in a system like this one are rarely where twenty years of relational-database intuition points first. The productive move, each time, was the same — read the actual error text, check the actual data (row counts, partition cardinality, column values), and let the evidence overrule the plausible-sounding guess.
