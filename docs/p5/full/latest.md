# P5 -- native in-lake join vs externalized two-stage

- **Generated:** 2026-10-01T01:18:24.801093+00:00  ·  200 repeats (first discarded)
- **Benchmarks:** 2 -- lab, radiology

## Benchmark: lab  (structured)

fact_lab_result x dim_lab_item x fact_admission -> abnormal-rate agg  ·  1,547,679 input rows

| Arm | Wall (median) | Throughput | $ cost | Notes |
|---|--:|--:|--:|---|
| **Native** — one Athena query over Iceberg | **1,938 ms** (±374) | 798,548 rows/s | $0.00002 | scans 4,559 KiB at $5/TiB |
| **External** — export + DuckDB join | 4,820 ms (export 4,700 + join 120) | 321,108 rows/s | $0.00002+ | DuckDB runs on the local machine; on a dedicated EC2/EMR box add its instance-hour cost. |
| External — DuckDB join **alone** | 120 ms (±4) | 12,869,033 rows/s | ~$0 | the compute step once the data is already extracted |

**Acceptance (native wall < external mean + 1 SD): PASS.**

## Benchmark: radiology  (cross-modal (structured + NLP-derived JSON))

fact_radiology_note_nlp (disorders, JSON UNNEST) x fact_admission -> non-negated disorder mentions by admission type  ·  548,560 input rows

| Arm | Wall (median) | Throughput | $ cost | Notes |
|---|--:|--:|--:|---|
| **Native** — one Athena query over Iceberg | **1,582 ms** (±552) | 346,855 rows/s | $0.00001 | scans 2,210 KiB at $5/TiB |
| **External** — export + DuckDB join | 4,038 ms (export 4,000 + join 39) | 135,833 rows/s | $0.00001+ | DuckDB runs on the local machine; on a dedicated EC2/EMR box add its instance-hour cost. |
| External — DuckDB join **alone** | 39 ms (±2) | 14,066,489 rows/s | ~$0 | the compute step once the data is already extracted |

**Acceptance (native wall < external mean + 1 SD): PASS.**

## Summary across benchmarks

| Benchmark | Modality | Native median | External median | Ratio | Acceptance |
|---|---|--:|--:|--:|:--:|
| lab | structured | 1,938 ms | 4,820 ms | 2.5x | PASS |
| radiology | cross-modal (structured + NLP-derived JSON) | 1,582 ms | 4,038 ms | 2.6x | PASS |

The external arm's cost is the *extract hop* — the Athena scan to pull the tables out, including (for the radiology benchmark) reconstructing the same JSON-array-to-rows unnesting in DuckDB that Athena performs natively — plus, on a real deployment, the instance-hours of whatever box runs the second stage. DuckDB's join/unnest itself is fast once the data is local; the two-stage penalty is the extract-and-reload, not the processing engine, for either a plain structured join or a cross-modal JSON-bearing one.

