# RQ1 — query coverage catalog

Supports the paper's RQ1: *"Does the Multimodal Fusion Schema increase the proportion of
realistic analytical workflows spanning structured and unstructured clinical data that can
be answered without an extract-transform-load detour, relative to a base dimensional model
that lacks its multimodal fact archetypes?"*

(File names here — `rq2_workflow_queries.sql`, `validate_rq2_queries.py` — use an earlier
numbering in which this was RQ2; they are the paper's RQ1.)

The Supported / Partial scores are a **manual coverage evaluation**: a person reads each
workflow's requirements and marks whether it is answerable, from a single schema, as an
ordinary SQL join. `validate_rq2_queries.py` then checks that every workflow's query plans
and runs against the deployed schema (see [Representative queries](#representative-queries)).

## Method

A catalog of thirty representative analytical workflows was authored across seven
operational and clinical categories: encounter and administrative reporting,
diagnosis/procedure/billing, laboratory and microbiology diagnostics, medications, care
logistics, ICU monitoring and interventions, and notes/NLP-derived analysis. Each workflow
was scored **twice** — once against the full Multimodal Fusion Schema (this repo's Gold
layer, including the Unstructured Data Fact, Unstructured Features Fact, and Inference Fact
archetypes), and once against an otherwise-identical **base dimensional model**: the same
Source Data Fact / Computed Structured Fact tables and conformed dimensions, but with those
three multimodal archetypes removed. The base dimensional model is a structural ablation of
this same schema, not a separate warehouse — RQ1 isolates the schema's
fact-archetype vocabulary as the variable, not the storage engine.

Each workflow is scored **Supported** (answerable end to end as a single query, no ETL
detour), **Partial** (answerable only via a proxy, an external join, or a manual step
outside the schema), or **Not supported**.

## The four cross-modal workflows the comparison turns on

Twenty-six of the thirty workflows are purely structured (admission case-mix,
ED-to-inpatient conversion, 30-day readmission, DRG severity vs. observed outcome,
laboratory turnaround time, medication administration delay, ICU length of stay, and
similar) and are **Supported** by both schemas via the Source Data Fact and Computed
Structured Fact archetypes alone. The remaining four require joining a structured admission
or diagnosis fact to a feature or inference payload derived from clinical free text:

1. Social-determinants-of-health status (tobacco, alcohol, obesity) derived from discharge
   notes — needs `fact_discharge_note_nlp` joined to `fact_admission` on `hadm_id`.
2. Radiology procedure/modality mix derived from radiology reports — needs
   `fact_radiology_note_nlp`.
3. Radiology-versus-coded-diagnosis concordance — needs `fact_radiology_note_nlp` joined
   against `fact_diagnosis`.
4. Free-text INDICATION/IMPRESSION retrieval — needs the section-text columns on
   `fact_radiology_note_nlp` (or the combined `fact_clinician_note_nlp_v` Inference Fact).

The full schema answers all four as an ordinary equi-join on the same admission key as
every other archetype (**Supported**); the base dimensional model has no schema position
for a note-derived feature or inference payload to live in at the same grain and key as the
fact it augments, so each is scored **Partial** at best.

## Result

| Schema | Supported | Partial | Not supported | Coverage |
|---|---|---|---|---|
| Multimodal Fusion Schema (this repo) | 30 | 0 | 0 | **100%** |
| Base dimensional model (structural ablation) | 26 | 4 | 0 | **86.67%** (26 of 30) |

This four-workflow gap is not a query-engine limitation; it is the absence, in the base
dimensional model, of a schema position for a note-derived feature or inference payload to
live in at the same grain and key as the fact it augments — exactly the gap the
Unstructured Features Fact and Inference Fact archetypes are designed to close.

## Representative queries

[rq2_workflow_queries.sql](rq2_workflow_queries.sql) holds one runnable Athena query per
workflow (W01–W30, numbered as in the catalog), each answering its workflow end to end
against the Gold layer as a single statement: W01–W26 over the structured facts, OBTs,
aggregates and conformed dimensions; W27–W30 joining `fact_discharge_note_nlp` /
`fact_radiology_note_nlp` to the structured facts on `hadm_id`. Cohort definitions (ICD
prefix lists, lab/chart item labels) are deliberately simple and transparent —
representative of each workflow, not validated phenotypes; proxies are flagged in each
query's header. On the full dataset, tables over 1M rows are capped at 1M rows, so
cross-table cohorts are partial samples.

```bash
python validate_rq2_queries.py --dataset fulldataset             # EXPLAIN all 30 (no data scanned)
python validate_rq2_queries.py --dataset fulldataset --execute   # run all 30, record results
```

Results go to `docs/query_coverage/<demo|full>/latest_{explain,execute}.{md,json}`.
Don't run `--execute` while the join benchmark is running — it competes for Athena
capacity.

## Status and reproducibility gap

**The full thirty-row catalog (every workflow's description and its Supported/Partial/Not
supported score for both schemas) is currently maintained in the author's dissertation
research materials, outside this repository.** This document records the methodology and
the paper's reported top-line result accurately, but does not yet reproduce every row —
adding a versioned `workflow_catalog.csv` (or `.md` table) here, with one row per workflow,
is an open item for full reproducibility of RQ1 from this repo alone. Until then, treat the
100% / 86.67% figures above as the paper's reported result, not as independently
re-derivable from files in this repo.
