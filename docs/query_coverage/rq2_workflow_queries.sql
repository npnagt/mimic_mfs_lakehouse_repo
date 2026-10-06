-- =====================================================================================
-- RQ2 workflow catalog -- one representative, runnable Athena (Trino) query per workflow
-- =====================================================================================
-- Companion to docs/query_coverage/README.md and the "Multimodal workflows supported"
-- catalog (dissertation research materials). Each query below answers its workflow end to
-- end from the Gold layer of the Multimodal Fusion Schema as a single SQL statement, with
-- no ETL detour -- the operational definition of "Supported" in RQ2.
--
-- Scope: the 30 workflows the pipeline as actually built fully supports. W01-W26 are
-- purely structured (Source Data Fact / Computed Structured Fact / OBT / aggregate tables
-- + conformed dimensions). W27-W30 are the cross-modal workflows that need a note-derived
-- Unstructured Features Fact (fact_discharge_note_nlp) or Inference Fact
-- (fact_radiology_note_nlp) joined on the same admission key -- the four workflows the
-- base dimensional model can only answer Partially.
--
-- Conventions
--   * Database: run with the Gold database as the query context (mimic4_db_business_full
--     or mimic4_db_business_demo); table names are unqualified.
--   * Each query starts with a "-- @W<nn>: <title>" marker line; validate_rq2_queries.py
--     splits the file on these markers.
--   * Clinical definitions are deliberately simple, transparent cohort rules (ICD prefix
--     lists, lab/chart item labels) -- representative of each workflow, not validated
--     phenotypes. Where a definition is a proxy, the query header says so.
--   * Full-dataset caveat: tables over 1M rows are capped at 1M rows at fact ETL
--     (--row-limit), so cross-table cohorts are partial samples of MIMIC-IV 3.1, not
--     population estimates.
--   * MIMIC-IV ICD codes are stored without dots; the radiology NLP emits ICD-10 codes
--     with dots (e.g. J18.9) -- W29 compares at the 3-character ICD-10 category.
-- =====================================================================================


-- @W01: Admission registration -> volume / case-mix by type, location, quarter
SELECT year(a.admit_time)                          AS admit_year,
       quarter(a.admit_time)                       AS admit_quarter,
       a.admission_type,
       a.admission_location,
       count(*)                                    AS admissions,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM fact_admission a
GROUP BY 1, 2, 3, 4
ORDER BY admit_year, admit_quarter, admissions DESC;


-- @W02: ED triage / disposition -> ED-to-inpatient conversion, ED LOS, boarding
-- Hosp-module scope: every row is an admitted encounter, so "conversion" is the share of
-- admissions that arrived through the ED; boarding = hours the patient stayed in the ED
-- after the inpatient admission time was recorded.
SELECT a.admission_type,
       count(*)                                                        AS admissions,
       count_if(a.ed_reg_time IS NOT NULL)                             AS via_ed,
       round(100.0 * count_if(a.ed_reg_time IS NOT NULL) / count(*), 2) AS ed_conversion_pct,
       approx_percentile(a.ed_los_minutes, 0.5)                        AS median_ed_los_min,
       approx_percentile(a.ed_los_minutes, 0.9)                        AS p90_ed_los_min,
       round(avg(CASE WHEN a.ed_out_time > a.admit_time
                      THEN date_diff('minute', a.admit_time, a.ed_out_time) / 60.0 END), 2)
                                                                       AS avg_boarding_hours
FROM fact_admission a
GROUP BY a.admission_type
ORDER BY admissions DESC;


-- @W03: Discharge processing -> 30-day readmission, days-since-prior, prior DRG
WITH adm AS (
    SELECT subject_id, hadm_id, admit_time, is_readmission_flag, days_since_prior_discharge,
           lag(hadm_id) OVER (PARTITION BY subject_id ORDER BY admit_time) AS prior_hadm_id
    FROM fact_admission
)
SELECT d.description                                   AS prior_drg,
       count(*)                                        AS readmissions,
       round(avg(adm.days_since_prior_discharge), 1)   AS avg_days_since_prior,
       approx_percentile(adm.days_since_prior_discharge, 0.5) AS median_days_since_prior
FROM adm
JOIN fact_drg_assignment d
  ON d.hadm_id = adm.prior_hadm_id AND d.drg_type = 'APR'
WHERE adm.is_readmission_flag
GROUP BY d.description
ORDER BY readmissions DESC
LIMIT 20;


-- @W04: Encounter close-out -> in-hospital mortality, time-to-death by service
WITH last_service AS (
    SELECT hadm_id, curr_service,
           row_number() OVER (PARTITION BY hadm_id ORDER BY transfer_time DESC) AS rn
    FROM fact_service_assignment
)
SELECT s.curr_service                                      AS discharge_service,
       count(*)                                            AS admissions,
       sum(a.hospital_expire_flag)                         AS in_hospital_deaths,
       round(avg(a.hospital_expire_flag) * 100, 2)         AS mortality_pct,
       approx_percentile(a.time_to_death_hours, 0.5)       AS median_hours_to_death
FROM fact_admission a
JOIN last_service s ON s.hadm_id = a.hadm_id AND s.rn = 1
GROUP BY s.curr_service
ORDER BY admissions DESC;


-- @W05: Patient identity management -> longitudinal patient-360
SELECT subject_id, gender, anchor_age, total_admission_count, total_readmission_count,
       ever_expired_in_hospital, first_admit_time, most_recent_disch_time,
       distinct_diagnosis_count, total_icu_stay_count, total_icu_los_days,
       overall_abnormal_lab_rate, total_positive_culture_count
FROM obt_patient_360
ORDER BY total_admission_count DESC, subject_id
LIMIT 25;


-- @W06: Diagnosis coding -> disease cohort (sepsis / CHF / AKI) -> LOS, mortality, readmit
WITH adm AS (
    SELECT hadm_id, hospital_los_hours, hospital_expire_flag,
           CASE WHEN date_diff('day', disch_time,
                     lead(admit_time) OVER (PARTITION BY subject_id ORDER BY admit_time)) <= 30
                THEN 1 ELSE 0 END AS readmit_30d
    FROM fact_admission
),
cohort AS (
    SELECT DISTINCT hadm_id,
           CASE WHEN (icd_version = 10 AND (icd_code LIKE 'A40%' OR icd_code LIKE 'A41%' OR icd_code LIKE 'R652%'))
                  OR (icd_version = 9  AND (icd_code LIKE '038%' OR icd_code IN ('99591', '99592', '78552')))
                THEN 'Sepsis'
                WHEN (icd_version = 10 AND icd_code LIKE 'I50%') OR (icd_version = 9 AND icd_code LIKE '428%')
                THEN 'CHF'
                WHEN (icd_version = 10 AND icd_code LIKE 'N17%') OR (icd_version = 9 AND icd_code LIKE '584%')
                THEN 'AKI'
           END AS disease
    FROM fact_diagnosis
)
SELECT c.disease,
       count(*)                                    AS admissions,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct,
       round(avg(a.readmit_30d) * 100, 2)          AS readmit_30d_pct
FROM cohort c
JOIN adm a ON a.hadm_id = c.hadm_id
WHERE c.disease IS NOT NULL
GROUP BY c.disease
ORDER BY admissions DESC;


-- @W07: DRG assignment -> severity / mortality weight vs. observed outcome (case-mix)
SELECT d.drg_severity,
       d.drg_mortality,
       count(*)                                    AS admissions,
       round(avg(a.hospital_expire_flag) * 100, 2) AS observed_mortality_pct,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days
FROM fact_drg_assignment d
JOIN fact_admission a ON a.hadm_id = d.hadm_id
WHERE d.drg_type = 'APR' AND d.drg_severity IS NOT NULL
GROUP BY d.drg_severity, d.drg_mortality
ORDER BY d.drg_severity, d.drg_mortality;


-- @W08: Procedure coding -> ICD-10-PCS utilization -> LOS delta
WITH overall AS (SELECT avg(hospital_los_hours) / 24 AS avg_los_days FROM fact_admission),
proc_adm AS (
    SELECT DISTINCT p.hadm_id, p.icd_code
    FROM fact_procedure p
    WHERE p.icd_version = 10
)
SELECT pa.icd_code,
       dp.long_title,
       count(*)                                                     AS admissions,
       round(avg(a.hospital_los_hours) / 24, 2)                     AS avg_los_days,
       round(avg(a.hospital_los_hours) / 24 - max(o.avg_los_days), 2) AS los_delta_vs_all_days
FROM proc_adm pa
JOIN fact_admission a ON a.hadm_id = pa.hadm_id
LEFT JOIN dim_procedure dp ON dp.icd_code = pa.icd_code AND dp.icd_version = 10
CROSS JOIN overall o
GROUP BY pa.icd_code, dp.long_title
ORDER BY admissions DESC
LIMIT 25;


-- @W09: Diagnosis coding -> Charlson comorbidity index -> risk-adjusted outcome
-- Charlson (Quan 2005 ICD-10 / Deyo ICD-9), 3/4-character prefix approximation.
WITH dx AS (
    SELECT hadm_id,
           CASE WHEN icd_version = 10 THEN icd_code ELSE concat('9:', icd_code) END AS c
    FROM fact_diagnosis
),
flags AS (
    SELECT hadm_id,
      max(CASE WHEN regexp_like(c, '^(I21|I22|I252|9:410|9:412)') THEN 1 ELSE 0 END) AS mi,
      max(CASE WHEN regexp_like(c, '^(I50|I110|I130|I132|9:428)') THEN 1 ELSE 0 END) AS chf,
      max(CASE WHEN regexp_like(c, '^(I70|I71|I739|I771|K551|9:440|9:441|9:4439)') THEN 1 ELSE 0 END) AS pvd,
      max(CASE WHEN regexp_like(c, '^(G45|G46|I6|9:43[0-8])') THEN 1 ELSE 0 END) AS cvd,
      max(CASE WHEN regexp_like(c, '^(F0[0-3]|G30|9:290|9:3310)') THEN 1 ELSE 0 END) AS dementia,
      max(CASE WHEN regexp_like(c, '^(J4[0-7]|J6[0-7]|9:49[0-6]|9:50[0-5])') THEN 1 ELSE 0 END) AS copd,
      max(CASE WHEN regexp_like(c, '^(M05|M06|M32|M33|M34|M353|9:710[014]|9:714)') THEN 1 ELSE 0 END) AS rheum,
      max(CASE WHEN regexp_like(c, '^(K2[5-8]|9:53[1-4])') THEN 1 ELSE 0 END) AS pud,
      max(CASE WHEN regexp_like(c, '^(K70[0-39]|K71[3-57]|K73|K74|K760|9:571)') THEN 1 ELSE 0 END) AS mild_liver,
      max(CASE WHEN regexp_like(c, '^(E1[0-4][019]|9:250[0-3])') THEN 1 ELSE 0 END) AS dm,
      max(CASE WHEN regexp_like(c, '^(E1[0-4][2-8]|9:250[4-9])') THEN 1 ELSE 0 END) AS dm_cx,
      max(CASE WHEN regexp_like(c, '^(G81|G82|G041|9:342|9:344[0-6])') THEN 1 ELSE 0 END) AS plegia,
      max(CASE WHEN regexp_like(c, '^(N18|N19|N052|I120|I131|Z49|Z992|9:585|9:586|9:V56)') THEN 1 ELSE 0 END) AS renal,
      max(CASE WHEN regexp_like(c, '^(C[01][0-9]|C[2-6][0-9]|C7[0-6]|C8[1-58]|C9[0-7]|9:1[4-9][0-9]|9:20[0-8])') THEN 1 ELSE 0 END) AS cancer,
      max(CASE WHEN regexp_like(c, '^(K704|K711|K721|K729|K765|K766|I85|9:572[2-8]|9:4560)') THEN 1 ELSE 0 END) AS severe_liver,
      max(CASE WHEN regexp_like(c, '^(C7[7-9]|C80|9:19[6-9])') THEN 1 ELSE 0 END) AS metastatic,
      max(CASE WHEN regexp_like(c, '^(B2[0-2]|B24|9:04[2-4])') THEN 1 ELSE 0 END) AS hiv
    FROM dx
    GROUP BY hadm_id
),
cci AS (
    SELECT hadm_id,
           mi + chf + pvd + cvd + dementia + copd + rheum + pud
           + mild_liver * (1 - severe_liver)
           + dm * (1 - dm_cx) + 2 * dm_cx + 2 * plegia + 2 * renal
           + 2 * cancer * (1 - metastatic) + 3 * severe_liver + 6 * metastatic + 6 * hiv AS charlson
    FROM flags
)
SELECT CASE WHEN c.charlson = 0 THEN '0'
            WHEN c.charlson <= 2 THEN '1-2'
            WHEN c.charlson <= 4 THEN '3-4'
            ELSE '5+' END                           AS charlson_band,
       count(*)                                    AS admissions,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(CAST(a.is_readmission_flag AS INTEGER)) * 100, 2) AS readmission_pct
FROM cci c
JOIN fact_admission a ON a.hadm_id = c.hadm_id
GROUP BY 1
ORDER BY 1;


-- @W10: HCPCS / CPT billing events -> per-admission billed-event cost proxy
-- Top billed HCPCS/CPT codes, plus each admission's total billed-event count as the cost proxy.
WITH per_adm_code AS (
    SELECT e.hadm_id, e.hcpcs_cd,
           coalesce(h.short_description, e.short_description) AS description,
           count(*) AS events
    FROM fact_hcpcs_event e
    LEFT JOIN dim_hcpcs h ON h.code = e.hcpcs_cd
    GROUP BY e.hadm_id, e.hcpcs_cd, coalesce(h.short_description, e.short_description)
),
adm_total AS (SELECT hadm_id, sum(events) AS billed_events_in_admission FROM per_adm_code GROUP BY hadm_id)
SELECT p.hcpcs_cd,
       arbitrary(p.description)                    AS description,
       count(DISTINCT p.hadm_id)                   AS admissions,
       sum(p.events)                               AS billed_events,
       round(avg(t.billed_events_in_admission), 1) AS avg_admission_billed_events,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days
FROM per_adm_code p
JOIN adm_total t ON t.hadm_id = p.hadm_id
JOIN fact_admission a ON a.hadm_id = p.hadm_id
GROUP BY p.hcpcs_cd
ORDER BY billed_events DESC
LIMIT 25;


-- @W11: Lab testing -> KDIGO AKI (baseline vs. peak creatinine) -> RRT, outcome
-- Baseline = first creatinine of the admission, peak = max; KDIGO creatinine criteria.
-- RRT = ICD-10-PCS 5A1D* (hemodialysis/CRRT) or ICD-9 39.95.
WITH cr AS (
    SELECT l.hadm_id,
           min_by(l.value_num, l.chart_time)                         AS baseline,
           max(l.value_num)                                          AS peak
    FROM fact_lab_result l
    JOIN dim_lab_item i ON i.item_id = l.item_id
    WHERE i.label = 'Creatinine' AND i.fluid = 'Blood'
      AND l.hadm_id IS NOT NULL AND l.value_num > 0
    GROUP BY l.hadm_id
),
staged AS (
    SELECT hadm_id, baseline, peak,
           CASE WHEN peak / baseline >= 3 OR peak >= 4.0     THEN 3
                WHEN peak / baseline >= 2                    THEN 2
                WHEN peak / baseline >= 1.5 OR peak - baseline >= 0.3 THEN 1
                ELSE 0 END AS kdigo_stage
    FROM cr
),
rrt AS (
    SELECT DISTINCT hadm_id FROM fact_procedure
    WHERE (icd_version = 10 AND icd_code LIKE '5A1D%') OR (icd_version = 9 AND icd_code = '3995')
)
SELECT s.kdigo_stage,
       count(*)                                         AS admissions,
       round(avg(s.baseline), 2)                        AS avg_baseline_cr,
       round(avg(s.peak), 2)                            AS avg_peak_cr,
       round(100.0 * count(r.hadm_id) / count(*), 2)    AS rrt_pct,
       round(avg(a.hospital_expire_flag) * 100, 2)      AS mortality_pct,
       round(avg(a.hospital_los_hours) / 24, 2)         AS avg_los_days
FROM staged s
JOIN fact_admission a ON a.hadm_id = s.hadm_id
LEFT JOIN rrt r ON r.hadm_id = s.hadm_id
GROUP BY s.kdigo_stage
ORDER BY s.kdigo_stage;


-- @W12: Lab testing -> abnormal-result burden per admission -> LOS, mortality
WITH burden AS (
    SELECT hadm_id, count(*) AS labs, count_if(is_abnormal_flag) AS abnormal
    FROM fact_lab_result
    WHERE hadm_id IS NOT NULL
    GROUP BY hadm_id
),
q AS (SELECT *, ntile(4) OVER (ORDER BY abnormal) AS burden_quartile FROM burden)
SELECT q.burden_quartile,
       count(*)                                    AS admissions,
       min(q.abnormal)                             AS min_abnormal,
       max(q.abnormal)                             AS max_abnormal,
       round(avg(1.0 * q.abnormal / q.labs), 3)    AS avg_abnormal_rate,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM q
JOIN fact_admission a ON a.hadm_id = q.hadm_id
GROUP BY q.burden_quartile
ORDER BY q.burden_quartile;


-- @W13: Lab workflow -> turnaround time -> care-delay analysis
SELECT i.category,
       coalesce(l.priority, 'UNSPECIFIED')                     AS priority,
       count(*)                                                AS results,
       approx_percentile(l.result_turnaround_minutes, 0.5)     AS median_tat_min,
       approx_percentile(l.result_turnaround_minutes, 0.9)     AS p90_tat_min,
       round(100.0 * count_if(l.result_turnaround_minutes > 60) / count(*), 2) AS pct_over_60_min
FROM fact_lab_result l
JOIN dim_lab_item i ON i.item_id = l.item_id
WHERE l.result_turnaround_minutes >= 0
GROUP BY i.category, coalesce(l.priority, 'UNSPECIFIED')
HAVING count(*) >= 100
ORDER BY median_tat_min DESC;


-- @W14: Microbiology -> positive blood culture -> time-to-antibiotic, concordant therapy, mortality
-- Concordant = an antibiotic administered after the culture whose name matches an agent the
-- isolate tested Susceptible to (name-substring match: a transparent proxy).
WITH bc AS (
    SELECT hadm_id, min(coalesce(chart_time, chart_date)) AS culture_time
    FROM fact_microbiology_result
    WHERE spec_type_desc = 'BLOOD CULTURE' AND is_positive_culture_flag AND hadm_id IS NOT NULL
    GROUP BY hadm_id
),
susceptible AS (
    SELECT DISTINCT hadm_id, upper(split_part(ab_name, '/', 1)) AS agent
    FROM fact_microbiology_result
    WHERE spec_type_desc = 'BLOOD CULTURE' AND interpretation = 'S' AND ab_name IS NOT NULL
),
abx AS (
    SELECT m.hadm_id, m.chart_time, upper(m.medication) AS med
    FROM fact_medication_administration m
    WHERE m.event_txt LIKE 'Administered%'
      AND regexp_like(lower(m.medication),
          'vancomycin|cefepime|ceftriaxone|cefazolin|piperacillin|meropenem|ciprofloxacin|levofloxacin|ampicillin|nafcillin|oxacillin|gentamicin|daptomycin|linezolid|metronidazole|aztreonam|ertapenem|cefuroxime|ceftazidime|trimethoprim')
),
per_adm AS (
    SELECT bc.hadm_id,
           date_diff('minute', bc.culture_time, min(abx.chart_time)) / 60.0 AS hours_to_first_abx,
           max(CASE WHEN s.agent IS NOT NULL THEN 1 ELSE 0 END)             AS concordant
    FROM bc
    LEFT JOIN abx ON abx.hadm_id = bc.hadm_id AND abx.chart_time >= bc.culture_time
    LEFT JOIN susceptible s ON s.hadm_id = bc.hadm_id AND strpos(abx.med, s.agent) > 0
    GROUP BY bc.hadm_id, bc.culture_time
)
SELECT CASE WHEN p.hours_to_first_abx IS NULL THEN 'no abx recorded'
            WHEN p.hours_to_first_abx <= 1  THEN '<=1h'
            WHEN p.hours_to_first_abx <= 6  THEN '1-6h'
            WHEN p.hours_to_first_abx <= 24 THEN '6-24h'
            ELSE '>24h' END                         AS time_to_antibiotic,
       count(*)                                    AS bacteremic_admissions,
       round(avg(p.concordant) * 100, 2)           AS concordant_therapy_pct,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM per_adm p
JOIN fact_admission a ON a.hadm_id = p.hadm_id
GROUP BY 1
ORDER BY 1;


-- @W15: Microbiology -> antibiogram (organism x susceptibility)
SELECT org_name,
       ab_name,
       count(*)                                                 AS isolates_tested,
       round(100.0 * count_if(interpretation = 'S') / count(*), 1) AS pct_susceptible,
       round(100.0 * count_if(interpretation = 'R') / count(*), 1) AS pct_resistant
FROM fact_microbiology_result
WHERE org_name IS NOT NULL AND ab_name IS NOT NULL AND interpretation IN ('S', 'I', 'R')
GROUP BY org_name, ab_name
HAVING count(*) >= 30
ORDER BY isolates_tested DESC
LIMIT 50;


-- @W16: eMAR -> medication administration volume / delay per admission -> outcome
WITH per_adm AS (
    SELECT hadm_id,
           count(*)                                          AS administrations,
           approx_percentile(admin_delay_minutes, 0.5)       AS median_delay_min
    FROM fact_medication_administration
    WHERE event_txt LIKE 'Administered%' AND hadm_id IS NOT NULL
    GROUP BY hadm_id
),
q AS (SELECT *, ntile(4) OVER (ORDER BY median_delay_min) AS delay_quartile FROM per_adm
      WHERE median_delay_min IS NOT NULL)
SELECT q.delay_quartile,
       count(*)                                    AS admissions,
       round(avg(q.median_delay_min), 1)           AS avg_median_delay_min,
       round(avg(q.administrations), 1)            AS avg_administrations,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM q
JOIN fact_admission a ON a.hadm_id = q.hadm_id
GROUP BY q.delay_quartile
ORDER BY q.delay_quartile;


-- @W17: Pharmacy + eMAR -> high-alert drug exposure (vasopressors, anticoagulants) -> complication proxy
-- Complication proxy: in-hospital mortality and hemorrhage-related diagnosis
-- (ICD-10 D62 / K92.2 / R58, ICD-9 285.1 / 578.9 / 459.0).
WITH exposure AS (
    SELECT hadm_id,
           max(CASE WHEN regexp_like(lower(medication), 'norepinephrine|epinephrine|vasopressin|phenylephrine|dopamine') THEN 1 ELSE 0 END) AS vasopressor,
           max(CASE WHEN regexp_like(lower(medication), 'heparin|enoxaparin|warfarin|apixaban|rivaroxaban|argatroban|bivalirudin') THEN 1 ELSE 0 END) AS anticoagulant
    FROM (
        SELECT hadm_id, medication FROM fact_pharmacy_order WHERE hadm_id IS NOT NULL
        UNION ALL
        SELECT hadm_id, medication FROM fact_medication_administration
        WHERE hadm_id IS NOT NULL AND event_txt LIKE 'Administered%'
    ) meds
    GROUP BY hadm_id
),
bleed AS (
    SELECT DISTINCT hadm_id FROM fact_diagnosis
    WHERE (icd_version = 10 AND (icd_code LIKE 'D62%' OR icd_code = 'K922' OR icd_code LIKE 'R58%'))
       OR (icd_version = 9  AND icd_code IN ('2851', '5789', '4590'))
)
SELECT CASE WHEN e.vasopressor = 1 AND e.anticoagulant = 1 THEN 'vasopressor + anticoagulant'
            WHEN e.vasopressor = 1 THEN 'vasopressor only'
            WHEN e.anticoagulant = 1 THEN 'anticoagulant only'
            ELSE 'neither' END                      AS high_alert_exposure,
       count(*)                                    AS admissions,
       round(100.0 * count(b.hadm_id) / count(*), 2) AS hemorrhage_dx_pct,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM exposure e
JOIN fact_admission a ON a.hadm_id = e.hadm_id
LEFT JOIN bleed b ON b.hadm_id = e.hadm_id
GROUP BY 1
ORDER BY admissions DESC;


-- @W18: POE + eMAR -> ordered-not-given reconciliation
WITH given AS (
    SELECT DISTINCT pharmacy_id
    FROM fact_medication_administration
    WHERE pharmacy_id IS NOT NULL AND event_txt LIKE 'Administered%'
)
SELECT p.drug,
       count(*)                                                      AS prescriptions,
       count_if(g.pharmacy_id IS NULL)                               AS never_administered,
       round(100.0 * count_if(g.pharmacy_id IS NULL) / count(*), 2)  AS not_given_pct
FROM fact_prescription p
LEFT JOIN given g ON g.pharmacy_id = p.pharmacy_id
WHERE p.pharmacy_id IS NOT NULL
  AND p.hadm_id IN (SELECT DISTINCT hadm_id FROM fact_medication_administration)
GROUP BY p.drug
HAVING count(*) >= 200
ORDER BY not_given_pct DESC
LIMIT 25;


-- @W19: Transfers / bed management -> ward-transfer count, bounce-backs -> LOS
-- Bounce-back = re-entering a care unit already visited earlier in the same admission.
WITH moves AS (
    SELECT hadm_id, care_unit, in_time,
           count(*) OVER (PARTITION BY hadm_id, care_unit ORDER BY in_time
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_visits,
           lag(care_unit) OVER (PARTITION BY hadm_id ORDER BY in_time)    AS prev_unit
    FROM fact_transfer
    WHERE event_type IN ('admit', 'transfer') AND hadm_id IS NOT NULL
),
per_adm AS (
    SELECT hadm_id,
           count_if(prev_unit IS NOT NULL)                       AS transfers,
           count_if(prior_visits > 0 AND prev_unit <> care_unit) AS bounce_backs
    FROM moves
    GROUP BY hadm_id
)
SELECT CASE WHEN p.transfers = 0 THEN '0'
            WHEN p.transfers <= 2 THEN '1-2'
            WHEN p.transfers <= 4 THEN '3-4'
            ELSE '5+' END                            AS ward_transfers,
       count(*)                                     AS admissions,
       round(100.0 * count_if(p.bounce_backs > 0) / count(*), 2) AS bounce_back_pct,
       round(avg(a.hospital_los_hours) / 24, 2)     AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2)  AS mortality_pct
FROM per_adm p
JOIN fact_admission a ON a.hadm_id = p.hadm_id
GROUP BY 1
ORDER BY 1;


-- @W20: Clinical service assignment -> MED <-> SURG transitions -> outcome
WITH per_adm AS (
    SELECT hadm_id,
           max(CASE WHEN prev_service = 'MED' AND curr_service LIKE '%SURG' THEN 1 ELSE 0 END) AS med_to_surg,
           max(CASE WHEN prev_service LIKE '%SURG' AND curr_service = 'MED' THEN 1 ELSE 0 END) AS surg_to_med,
           count(*) AS service_changes
    FROM fact_service_assignment
    GROUP BY hadm_id
)
SELECT CASE WHEN med_to_surg = 1 AND surg_to_med = 1 THEN 'both directions'
            WHEN med_to_surg = 1 THEN 'MED -> SURG'
            WHEN surg_to_med = 1 THEN 'SURG -> MED'
            ELSE 'no MED/SURG transition' END       AS transition,
       count(*)                                    AS admissions,
       round(avg(p.service_changes), 2)            AS avg_service_records,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM per_adm p
JOIN fact_admission a ON a.hadm_id = p.hadm_id
GROUP BY 1
ORDER BY admissions DESC;


-- @W21: Outpatient measurements (OMR) -> pre-admission vitals / BMI -> admission risk
-- Most recent outpatient BMI within 365 days before the admission.
WITH bmi AS (
    SELECT subject_id, chart_date, try_cast(result_value AS DOUBLE) AS bmi
    FROM fact_outpatient_measurement
    WHERE result_name LIKE 'BMI%'
),
pre AS (
    SELECT a.hadm_id, a.hospital_expire_flag, a.hospital_los_hours, a.is_readmission_flag,
           max_by(b.bmi, b.chart_date) AS last_bmi
    FROM fact_admission a
    JOIN bmi b ON b.subject_id = a.subject_id
             AND b.chart_date <  CAST(a.admit_time AS DATE)
             AND b.chart_date >= date_add('day', -365, CAST(a.admit_time AS DATE))
    WHERE b.bmi BETWEEN 10 AND 80
    GROUP BY a.hadm_id, a.hospital_expire_flag, a.hospital_los_hours, a.is_readmission_flag
)
SELECT CASE WHEN last_bmi < 18.5 THEN '1 underweight (<18.5)'
            WHEN last_bmi < 25   THEN '2 normal (18.5-25)'
            WHEN last_bmi < 30   THEN '3 overweight (25-30)'
            WHEN last_bmi < 40   THEN '4 obese (30-40)'
            ELSE '5 morbidly obese (40+)' END       AS pre_admission_bmi,
       count(*)                                    AS admissions,
       round(avg(hospital_expire_flag) * 100, 2)   AS mortality_pct,
       round(avg(hospital_los_hours) / 24, 2)      AS avg_los_days,
       round(avg(CAST(is_readmission_flag AS INTEGER)) * 100, 2) AS readmission_pct
FROM pre
GROUP BY 1
ORDER BY 1;


-- @W22: ICU stay management -> LOS, ICU readmission, time-to-ICU
SELECT first_careunit,
       count(*)                                                    AS icu_stays,
       round(avg(los), 2)                                          AS avg_icu_los_days,
       approx_percentile(los, 0.5)                                 AS median_icu_los_days,
       round(avg(CAST(is_icu_readmission_flag AS INTEGER)) * 100, 2) AS icu_readmission_pct,
       approx_percentile(time_to_icu_hours, 0.5)                   AS median_hours_to_icu,
       round(avg(hospital_expire_flag) * 100, 2)                   AS hospital_mortality_pct
FROM obt_icu_stay_features
GROUP BY first_careunit
ORDER BY icu_stays DESC;


-- @W23: Intake / output charting -> net fluid balance trajectory -> AKI, mortality
-- Cumulative net balance over each stay's first 3 charted ICU days.
WITH daily AS (
    SELECT stay_id, date_key, net_balance_ml,
           row_number() OVER (PARTITION BY stay_id ORDER BY date_key) AS icu_day
    FROM agg_icu_fluid_balance_daily
),
first3 AS (
    SELECT stay_id, sum(net_balance_ml) AS net_72h_ml
    FROM daily WHERE icu_day <= 3
    GROUP BY stay_id
),
aki AS (
    SELECT DISTINCT hadm_id FROM fact_diagnosis
    WHERE (icd_version = 10 AND icd_code LIKE 'N17%') OR (icd_version = 9 AND icd_code LIKE '584%')
)
SELECT CASE WHEN f.net_72h_ml < 0     THEN '1 negative'
            WHEN f.net_72h_ml < 2000  THEN '2 0-2 L'
            WHEN f.net_72h_ml < 5000  THEN '3 2-5 L'
            ELSE '4 >5 L' END                       AS net_balance_first_72h,
       count(*)                                    AS icu_stays,
       round(avg(f.net_72h_ml), 0)                 AS avg_net_ml,
       round(100.0 * count(k.hadm_id) / count(*), 2) AS aki_dx_pct,
       round(avg(s.hospital_expire_flag) * 100, 2) AS hospital_mortality_pct
FROM first3 f
JOIN obt_icu_stay_features s ON s.stay_id = f.stay_id
LEFT JOIN aki k ON k.hadm_id = s.hadm_id
GROUP BY 1
ORDER BY 1;


-- @W24: ICU procedures -> mechanical ventilation duration, VAP proxy -> outcome
-- VAP proxy = coded ventilator-associated pneumonia (ICD-10 J95.851, ICD-9 997.31).
WITH vent AS (
    SELECT pe.stay_id, pe.hadm_id, sum(pe.duration_minutes) / 60.0 AS vent_hours
    FROM fact_procedure_event pe
    JOIN dim_chart_item ci ON ci.item_id = pe.item_id
    WHERE ci.label = 'Invasive Ventilation'
    GROUP BY pe.stay_id, pe.hadm_id
),
vap AS (
    SELECT DISTINCT hadm_id FROM fact_diagnosis
    WHERE (icd_version = 10 AND icd_code = 'J95851') OR (icd_version = 9 AND icd_code = '99731')
)
SELECT CASE WHEN v.vent_hours < 24  THEN '1 <24h'
            WHEN v.vent_hours < 96  THEN '2 1-4 days'
            WHEN v.vent_hours < 168 THEN '3 4-7 days'
            ELSE '4 >7 days' END                    AS ventilation_duration,
       count(*)                                    AS ventilated_stays,
       round(100.0 * count(p.hadm_id) / count(*), 2) AS vap_dx_pct,
       round(avg(s.los), 2)                        AS avg_icu_los_days,
       round(avg(s.hospital_expire_flag) * 100, 2) AS hospital_mortality_pct
FROM vent v
JOIN obt_icu_stay_features s ON s.stay_id = v.stay_id
LEFT JOIN vap p ON p.hadm_id = v.hadm_id
GROUP BY 1
ORDER BY 1;


-- @W25: Nursing charting -> sedation (RASS) vs. delirium (CAM-ICU) -> outcome
-- Delirium-positive proxy: any CAM-ICU item charted as 'Positive' or 'Yes' during the stay.
WITH obs AS (
    SELECT co.stay_id, ci.label, co.value, co.value_num
    FROM fact_chart_observation co
    JOIN dim_chart_item ci ON ci.item_id = co.item_id
    WHERE ci.label = 'Richmond-RAS Scale' OR ci.label LIKE 'CAM-ICU%'
),
per_stay AS (
    SELECT stay_id,
           avg(CASE WHEN label = 'Richmond-RAS Scale' THEN value_num END) AS mean_rass,
           max(CASE WHEN label LIKE 'CAM-ICU%' AND value IN ('Positive', 'Yes') THEN 1 ELSE 0 END) AS cam_positive,
           max(CASE WHEN label LIKE 'CAM-ICU%' THEN 1 ELSE 0 END) AS cam_assessed
    FROM obs
    GROUP BY stay_id
)
SELECT CASE WHEN p.mean_rass IS NULL THEN '0 no RASS'
            WHEN p.mean_rass <= -3 THEN '1 deep sedation (<= -3)'
            WHEN p.mean_rass <  0  THEN '2 light sedation (-2..-1)'
            WHEN p.mean_rass =  0  THEN '3 alert and calm (0)'
            ELSE '4 agitated (> 0)' END             AS sedation_level,
       count(*)                                    AS icu_stays,
       round(100.0 * sum(p.cam_positive) / nullif(sum(p.cam_assessed), 0), 2) AS cam_icu_positive_pct,
       round(avg(s.los), 2)                        AS avg_icu_los_days,
       round(avg(s.hospital_expire_flag) * 100, 2) AS hospital_mortality_pct
FROM per_stay p
JOIN obt_icu_stay_features s ON s.stay_id = p.stay_id
GROUP BY 1
ORDER BY 1;


-- @W26: Ingredient events -> caloric / protein intake -> LOS
WITH intake AS (
    SELECT ie.stay_id,
           sum(CASE WHEN lower(ci.label) LIKE '%calorie%' THEN ie.amount END) AS kcal,
           sum(CASE WHEN lower(ci.label) LIKE '%protein%' THEN ie.amount END) AS protein_g
    FROM fact_ingredient_event ie
    JOIN dim_chart_item ci ON ci.item_id = ie.item_id
    GROUP BY ie.stay_id
),
per_day AS (
    SELECT i.stay_id, s.los, s.hospital_expire_flag,
           i.kcal / greatest(s.los, 1.0)      AS kcal_per_day,
           i.protein_g / greatest(s.los, 1.0) AS protein_g_per_day
    FROM intake i
    JOIN obt_icu_stay_features s ON s.stay_id = i.stay_id
    WHERE i.kcal > 0
)
SELECT CASE WHEN kcal_per_day < 500  THEN '1 <500 kcal/day'
            WHEN kcal_per_day < 1000 THEN '2 500-1000'
            WHEN kcal_per_day < 1500 THEN '3 1000-1500'
            ELSE '4 >=1500' END                     AS caloric_intake,
       count(*)                                    AS icu_stays,
       round(avg(protein_g_per_day), 1)            AS avg_protein_g_per_day,
       round(avg(los), 2)                          AS avg_icu_los_days,
       round(avg(hospital_expire_flag) * 100, 2)   AS hospital_mortality_pct
FROM per_day
GROUP BY 1
ORDER BY 1;


-- =====================================================================================
-- Cross-modal workflows (structured x note-derived) -- Partial in the base dimensional
-- model, Supported here via the Unstructured Features / Inference Fact archetypes.
-- =====================================================================================

-- @W27: Discharge documentation -> SDOH (tobacco / alcohol / obesity) -> readmission, mortality
WITH adm AS (
    SELECT hadm_id, hospital_expire_flag, hospital_los_hours,
           CASE WHEN date_diff('day', disch_time,
                     lead(admit_time) OVER (PARTITION BY subject_id ORDER BY admit_time)) <= 30
                THEN 1 ELSE 0 END AS readmit_30d
    FROM fact_admission
)
SELECT n.tobacco_use,
       n.alcohol_use,
       n.obesity_level,
       count(*)                                    AS admissions,
       round(avg(a.readmit_30d) * 100, 2)          AS readmit_30d_pct,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days
FROM fact_discharge_note_nlp n
JOIN adm a ON a.hadm_id = n.hadm_id
GROUP BY n.tobacco_use, n.alcohol_use, n.obesity_level
HAVING count(*) >= 20
ORDER BY admissions DESC;


-- @W28: Radiology reports -> procedure / modality mix per admission -> imaging cost, LOS
WITH exams AS (
    SELECT r.hadm_id,
           coalesce(json_extract_scalar(p, '$.modality'), 'UNKNOWN') AS modality
    FROM fact_radiology_note_nlp r
    CROSS JOIN UNNEST(CAST(json_parse(r.radiology_procedure_types) AS ARRAY(JSON))) AS t(p)
),
per_adm AS (
    SELECT hadm_id, modality, count(*) AS exams FROM exams GROUP BY hadm_id, modality
)
SELECT p.modality,
       count(DISTINCT p.hadm_id)                   AS admissions,
       sum(p.exams)                                AS exams,
       round(avg(p.exams), 2)                      AS avg_exams_per_admission,
       round(avg(a.hospital_los_hours) / 24, 2)    AS avg_los_days,
       round(avg(a.hospital_expire_flag) * 100, 2) AS mortality_pct
FROM per_adm p
JOIN fact_admission a ON a.hadm_id = p.hadm_id
GROUP BY p.modality
ORDER BY exams DESC;


-- @W29: Radiology findings -> ICD-code concordance (report disorders vs. coded dx)
-- Compared at the 3-character ICD-10 category; only ICD-10-coded admissions are scored.
WITH nlp_codes AS (
    SELECT DISTINCT r.hadm_id,
           substr(replace(json_extract_scalar(c, '$.code'), '.', ''), 1, 3) AS category,
           json_extract_scalar(c, '$.term')                                AS term
    FROM fact_radiology_note_nlp r
    CROSS JOIN UNNEST(CAST(json_parse(r.icd_codes) AS ARRAY(JSON))) AS t(c)
    WHERE json_extract_scalar(c, '$.source') = 'lookup'
),
coded AS (
    SELECT DISTINCT hadm_id, substr(icd_code, 1, 3) AS category
    FROM fact_diagnosis
    WHERE icd_version = 10
)
SELECT n.category                                               AS icd10_category,
       arbitrary(n.term)                                        AS example_report_term,
       count(*)                                                 AS admissions_with_finding,
       count(c.hadm_id)                                         AS also_coded,
       round(100.0 * count(c.hadm_id) / count(*), 2)            AS concordance_pct
FROM nlp_codes n
LEFT JOIN coded c ON c.hadm_id = n.hadm_id AND c.category = n.category
WHERE n.hadm_id IN (SELECT hadm_id FROM coded)
GROUP BY n.category
HAVING count(*) >= 20
ORDER BY admissions_with_finding DESC;


-- @W30: Radiology INDICATION / IMPRESSION retrieval for a cohort (structured summaries)
-- Cohort: sepsis admissions with an ICU stay; returns each report's indication and impression.
WITH cohort AS (
    SELECT DISTINCT d.hadm_id
    FROM fact_diagnosis d
    JOIN fact_icu_stay_accumulating i ON i.hadm_id = d.hadm_id
    WHERE (d.icd_version = 10 AND (d.icd_code LIKE 'A40%' OR d.icd_code LIKE 'A41%'))
       OR (d.icd_version = 9 AND d.icd_code LIKE '038%')
),
ind AS (
    SELECT r.hadm_id, json_extract_scalar(x, '$.note_id') AS note_id, json_extract_scalar(x, '$.text') AS indication
    FROM fact_radiology_note_nlp r
    CROSS JOIN UNNEST(CAST(json_parse(r.indication_summary) AS ARRAY(JSON))) AS t(x)
    WHERE r.hadm_id IN (SELECT hadm_id FROM cohort)
),
imp AS (
    SELECT r.hadm_id, json_extract_scalar(x, '$.note_id') AS note_id, json_extract_scalar(x, '$.text') AS impression
    FROM fact_radiology_note_nlp r
    CROSS JOIN UNNEST(CAST(json_parse(r.conclusion) AS ARRAY(JSON))) AS t(x)
    WHERE r.hadm_id IN (SELECT hadm_id FROM cohort)
)
SELECT a.subject_id, a.hadm_id, a.admit_time, coalesce(ind.note_id, imp.note_id) AS note_id,
       substr(ind.indication, 1, 300) AS indication,
       substr(imp.impression, 1, 500) AS impression
FROM ind
FULL OUTER JOIN imp ON imp.hadm_id = ind.hadm_id AND imp.note_id = ind.note_id
JOIN fact_admission a ON a.hadm_id = coalesce(ind.hadm_id, imp.hadm_id)
ORDER BY a.hadm_id, note_id
LIMIT 50;
