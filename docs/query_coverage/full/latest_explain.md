# RQ2 workflow queries -- explain (mimic4_db_business_full)

- **Run:** 20261001T090254Z · mode `explain` · 30/30 passed
- **Source:** `docs/query_coverage/rq2_workflow_queries.sql`

| id | workflow | status | runtime s | scanned | rows |
|---|---|---|---|---|---|
| W01 | Admission registration -> volume / case-mix by type, location, quarter | SUCCEEDED | 0.6 | 0.0 MB |  |
| W02 | ED triage / disposition -> ED-to-inpatient conversion, ED LOS, boarding | SUCCEEDED | 1.1 | 0.0 MB |  |
| W03 | Discharge processing -> 30-day readmission, days-since-prior, prior DRG | SUCCEEDED | 1.0 | 0.0 MB |  |
| W04 | Encounter close-out -> in-hospital mortality, time-to-death by service | SUCCEEDED | 1.0 | 0.0 MB |  |
| W05 | Patient identity management -> longitudinal patient-360 | SUCCEEDED | 0.7 | 0.0 MB |  |
| W06 | Diagnosis coding -> disease cohort (sepsis / CHF / AKI) -> LOS, mortality, readmit | SUCCEEDED | 1.2 | 0.0 MB |  |
| W07 | DRG assignment -> severity / mortality weight vs. observed outcome (case-mix) | SUCCEEDED | 0.8 | 0.0 MB |  |
| W08 | Procedure coding -> ICD-10-PCS utilization -> LOS delta | SUCCEEDED | 1.2 | 0.0 MB |  |
| W09 | Diagnosis coding -> Charlson comorbidity index -> risk-adjusted outcome | SUCCEEDED | 1.1 | 0.0 MB |  |
| W10 | HCPCS / CPT billing events -> per-admission billed-event cost proxy | SUCCEEDED | 1.9 | 0.0 MB |  |
| W11 | Lab testing -> KDIGO AKI (baseline vs. peak creatinine) -> RRT, outcome | SUCCEEDED | 1.3 | 0.0 MB |  |
| W12 | Lab testing -> abnormal-result burden per admission -> LOS, mortality | SUCCEEDED | 2.1 | 0.0 MB |  |
| W13 | Lab workflow -> turnaround time -> care-delay analysis | SUCCEEDED | 0.8 | 0.0 MB |  |
| W14 | Microbiology -> positive blood culture -> time-to-antibiotic, concordant therapy, mortality | SUCCEEDED | 1.2 | 0.0 MB |  |
| W15 | Microbiology -> antibiogram (organism x susceptibility) | SUCCEEDED | 0.6 | 0.0 MB |  |
| W16 | eMAR -> medication administration volume / delay per admission -> outcome | SUCCEEDED | 0.9 | 0.0 MB |  |
| W17 | Pharmacy + eMAR -> high-alert drug exposure (vasopressors, anticoagulants) -> complication proxy | SUCCEEDED | 1.6 | 0.0 MB |  |
| W18 | POE + eMAR -> ordered-not-given reconciliation | SUCCEEDED | 0.9 | 0.0 MB |  |
| W19 | Transfers / bed management -> ward-transfer count, bounce-backs -> LOS | SUCCEEDED | 1.0 | 0.0 MB |  |
| W20 | Clinical service assignment -> MED <-> SURG transitions -> outcome | SUCCEEDED | 0.8 | 0.0 MB |  |
| W21 | Outpatient measurements (OMR) -> pre-admission vitals / BMI -> admission risk | SUCCEEDED | 1.5 | 0.0 MB |  |
| W22 | ICU stay management -> LOS, ICU readmission, time-to-ICU | SUCCEEDED | 0.8 | 0.0 MB |  |
| W23 | Intake / output charting -> net fluid balance trajectory -> AKI, mortality | SUCCEEDED | 2.0 | 0.0 MB |  |
| W24 | ICU procedures -> mechanical ventilation duration, VAP proxy -> outcome | SUCCEEDED | 3.3 | 0.0 MB |  |
| W25 | Nursing charting -> sedation (RASS) vs. delirium (CAM-ICU) -> outcome | SUCCEEDED | 1.2 | 0.0 MB |  |
| W26 | Ingredient events -> caloric / protein intake -> LOS | SUCCEEDED | 1.3 | 0.0 MB |  |
| W27 | Discharge documentation -> SDOH (tobacco / alcohol / obesity) -> readmission, mortality | SUCCEEDED | 1.6 | 0.0 MB |  |
| W28 | Radiology reports -> procedure / modality mix per admission -> imaging cost, LOS | SUCCEEDED | 0.9 | 0.0 MB |  |
| W29 | Radiology findings -> ICD-code concordance (report disorders vs. coded dx) | SUCCEEDED | 1.0 | 0.0 MB |  |
| W30 | Radiology INDICATION / IMPRESSION retrieval for a cohort (structured summaries) | SUCCEEDED | 1.5 | 0.0 MB |  |
