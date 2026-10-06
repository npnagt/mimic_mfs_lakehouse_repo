# RQ2 workflow queries -- execute (mimic4_db_business_full)

- **Run:** 20261001T091542Z · mode `execute` · 30/30 passed
- **Source:** `docs/query_coverage/rq2_workflow_queries.sql`

| id | workflow | status | runtime s | scanned | rows |
|---|---|---|---|---|---|
| W01 | Admission registration -> volume / case-mix by type, location, quarter | SUCCEEDED | 2.5 | 5.3 MB | 16444 |
| W02 | ED triage / disposition -> ED-to-inpatient conversion, ED LOS, boarding | SUCCEEDED | 0.9 | 9.7 MB | 10 |
| W03 | Discharge processing -> 30-day readmission, days-since-prior, prior DRG | SUCCEEDED | 1.8 | 12.1 MB | 20 |
| W04 | Encounter close-out -> in-hospital mortality, time-to-death by service | SUCCEEDED | 1.6 | 8.4 MB | 20 |
| W05 | Patient identity management -> longitudinal patient-360 | SUCCEEDED | 1.1 | 5.3 MB | 25 |
| W06 | Diagnosis coding -> disease cohort (sepsis / CHF / AKI) -> LOS, mortality, readmit | SUCCEEDED | 4.6 | 16.3 MB | 3 |
| W07 | DRG assignment -> severity / mortality weight vs. observed outcome (case-mix) | SUCCEEDED | 1.2 | 6.6 MB | 16 |
| W08 | Procedure coding -> ICD-10-PCS utilization -> LOS delta | SUCCEEDED |  | 10.0 MB | 25 |
| W09 | Diagnosis coding -> Charlson comorbidity index -> risk-adjusted outcome | SUCCEEDED | 2.0 | 7.3 MB | 4 |
| W10 | HCPCS / CPT billing events -> per-admission billed-event cost proxy | SUCCEEDED | 4.2 | 5.3 MB | 25 |
| W11 | Lab testing -> KDIGO AKI (baseline vs. peak creatinine) -> RRT, outcome | SUCCEEDED | 3.8 | 18.4 MB | 4 |
| W12 | Lab testing -> abnormal-result burden per admission -> LOS, mortality | SUCCEEDED | 2.1 | 5.1 MB | 4 |
| W13 | Lab workflow -> turnaround time -> care-delay analysis | SUCCEEDED | 2.9 | 2.7 MB | 5 |
| W14 | Microbiology -> positive blood culture -> time-to-antibiotic, concordant therapy, mortality | SUCCEEDED | 3.7 | 24.8 MB | 5 |
| W15 | Microbiology -> antibiogram (organism x susceptibility) | SUCCEEDED | 1.0 | 1.4 MB | 50 |
| W16 | eMAR -> medication administration volume / delay per admission -> outcome | SUCCEEDED | 3.6 | 7.2 MB | 4 |
| W17 | Pharmacy + eMAR -> high-alert drug exposure (vasopressors, anticoagulants) -> complication proxy | SUCCEEDED | 4.4 | 15.0 MB | 4 |
| W18 | POE + eMAR -> ordered-not-given reconciliation | SUCCEEDED | 1.8 | 14.2 MB | 25 |
| W19 | Transfers / bed management -> ward-transfer count, bounce-backs -> LOS | SUCCEEDED | 3.1 | 14.0 MB | 4 |
| W20 | Clinical service assignment -> MED <-> SURG transitions -> outcome | SUCCEEDED | 2.2 | 5.6 MB | 4 |
| W21 | Outpatient measurements (OMR) -> pre-admission vitals / BMI -> admission risk | SUCCEEDED | 2.5 | 14.9 MB | 5 |
| W22 | ICU stay management -> LOS, ICU readmission, time-to-ICU | SUCCEEDED | 1.6 | 1.9 MB | 17 |
| W23 | Intake / output charting -> net fluid balance trajectory -> AKI, mortality | SUCCEEDED |  | 6.2 MB | 4 |
| W24 | ICU procedures -> mechanical ventilation duration, VAP proxy -> outcome | SUCCEEDED | 2.6 | 12.5 MB | 4 |
| W25 | Nursing charting -> sedation (RASS) vs. delirium (CAM-ICU) -> outcome | SUCCEEDED | 2.1 | 5.3 MB | 4 |
| W26 | Ingredient events -> caloric / protein intake -> LOS | SUCCEEDED | 1.9 | 6.9 MB | 4 |
| W27 | Discharge documentation -> SDOH (tobacco / alcohol / obesity) -> readmission, mortality | SUCCEEDED | 1.7 | 12.4 MB | 20 |
| W28 | Radiology reports -> procedure / modality mix per admission -> imaging cost, LOS | SUCCEEDED | 1.6 | 3.5 MB | 7 |
| W29 | Radiology findings -> ICD-code concordance (report disorders vs. coded dx) | SUCCEEDED | 3.1 | 8.1 MB | 26 |
| W30 | Radiology INDICATION / IMPRESSION retrieval for a cohort (structured summaries) | SUCCEEDED | 3.8 | 18.1 MB | 50 |

## W01 -- Admission registration -> volume / case-mix by type, location, quarter

| admit_year | admit_quarter | admission_type | admission_location | admissions | avg_los_days | mortality_pct |
|---|---|---|---|---|---|---|
| 2105 | 4 | URGENT | TRANSFER FROM HOSPITAL | 1 | 7.74 | 0.0 |
| 2106 | 1 | EU OBSERVATION | EMERGENCY ROOM | 1 | 0.55 | 0.0 |
| 2109 | 4 | EW EMER. | EMERGENCY ROOM | 1 | 31.67 | 0.0 |
| 2110 | 1 | EW EMER. | EMERGENCY ROOM | 130 | 4.7 | 3.85 |
| 2110 | 1 | EU OBSERVATION | EMERGENCY ROOM | 87 | 0.8 | 1.15 |
| 2110 | 1 | SURGICAL SAME DAY ADMISSION | PHYSICIAN REFERRAL | 47 | 4.42 | 0.0 |
| 2110 | 1 | URGENT | TRANSFER FROM HOSPITAL | 41 | 10.65 | 17.07 |
| 2110 | 1 | OBSERVATION ADMIT | TRANSFER FROM HOSPITAL | 26 | 8.11 | 3.85 |
| 2110 | 1 | OBSERVATION ADMIT | EMERGENCY ROOM | 21 | 8.28 | 4.76 |
| 2110 | 1 | URGENT | PHYSICIAN REFERRAL | 18 | 3.53 | 0.0 |
| 2110 | 1 | OBSERVATION ADMIT | PHYSICIAN REFERRAL | 16 | 5.38 | 0.0 |
| 2110 | 1 | DIRECT EMER. | PHYSICIAN REFERRAL | 15 | 6.69 | 0.0 |
| 2110 | 1 | EW EMER. | TRANSFER FROM HOSPITAL | 14 | 7.81 | 7.14 |
| 2110 | 1 | EU OBSERVATION | PHYSICIAN REFERRAL | 14 | 0.95 | 0.0 |
| 2110 | 1 | EU OBSERVATION | TRANSFER FROM HOSPITAL | 12 | 1.13 | 0.0 |

## W02 -- ED triage / disposition -> ED-to-inpatient conversion, ED LOS, boarding

| admission_type | admissions | via_ed | ed_conversion_pct | median_ed_los_min | p90_ed_los_min | avg_boarding_hours |
|---|---|---|---|---|---|---|
| EW EMER. | 177459 | 169782 | 95.67 | 363.8128003152231 | 718.8307040379142 | 2.09 |
| EU OBSERVATION | 119456 | 118932 | 99.56 | 749.6112350601607 | 1685.9475481538916 | 10.86 |
| OBSERVATION ADMIT | 84437 | 72225 | 85.54 | 513.1215878136633 | 1296.73524105725 | 4.22 |
| URGENT | 54929 | 9598 | 17.47 | 417.0261127849418 | 1037.5782856425978 | 1.92 |
| SURGICAL SAME DAY ADMISSION | 42898 | 32 | 0.07 | 255.0 | 551.0 | 6.58 |
| DIRECT OBSERVATION | 24551 | 6069 | 24.72 | 723.8026925254762 | 1628.419364138045 | 9.61 |
| DIRECT EMER. | 21973 | 2377 | 10.82 | 511.059555270311 | 1065.207809847199 | 1.63 |
| ELECTIVE | 13130 | 114 | 0.87 | 506.25 | 987.0 | 3.01 |
| AMBULATORY OBSERVATION | 7195 | 111 | 1.54 | 276.3333333333333 | 615.0 | 7.44 |
| admission_type | 1 | 0 | 0.0 |  |  |  |

## W03 -- Discharge processing -> 30-day readmission, days-since-prior, prior DRG

| prior_drg | readmissions | avg_days_since_prior | median_days_since_prior |
|---|---|---|---|
| HEART FAILURE | 8183 | 187.1 | 58 |
| SEPTICEMIA AND DISSEMINATED INFECTIONS | 5572 | 235.1 | 55 |
| VAGINAL DELIVERY | 5487 | 1031.6 | 848 |
| OTHER PNEUMONIA | 4304 | 347.4 | 115 |
| "MALFUNCTION | 4130 | 215.6 | 49 |
| PERCUTANEOUS CARDIAC INTERVENTION WITHOUT AMI | 3746 | 504.5 | 159 |
| KIDNEY AND URINARY TRACT INFECTIONS | 3698 | 287.5 | 77 |
| OTHER DIGESTIVE SYSTEM DIAGNOSES | 3630 | 264.1 | 59 |
| "POST-OPERATIVE | 3598 | 278.8 | 57 |
| CARDIAC ARRHYTHMIA AND CONDUCTION DISORDERS | 3418 | 385.2 | 107 |
| CELLULITIS AND OTHER SKIN INFECTIONS | 3343 | 411.7 | 113 |
| DISORDERS OF PANCREAS EXCEPT MALIGNANCY | 3077 | 237.8 | 53 |
| "OTHER GASTROENTERITIS | 2824 | 350.1 | 92 |
| CESAREAN SECTION WITHOUT STERILIZATION | 2780 | 980.4 | 794 |
| CHRONIC OBSTRUCTIVE PULMONARY DISEASE | 2552 | 273.0 | 94 |

## W04 -- Encounter close-out -> in-hospital mortality, time-to-death by service

| discharge_service | admissions | in_hospital_deaths | mortality_pct | median_hours_to_death |
|---|---|---|---|---|
| MED | 266856 | 6412 | 2.4 | 145.4867302762133 |
| CMED | 48969 | 1288 | 2.63 | 116.54793250860666 |
| SURG | 46841 | 820 | 1.75 | 182.00306686777273 |
| OMED | 29900 | 967 | 3.23 | 237.66151503915688 |
| ORTHO | 24714 | 43 | 0.17 | 98.0 |
| OBS | 24622 | 1 | 0.0 | 12.133333333333333 |
| NMED | 23631 | 947 | 4.01 | 82.82911519670593 |
| NSURG | 14567 | 581 | 3.99 | 73.99085901027078 |
| CSURG | 12648 | 260 | 2.06 | 238.99654761904762 |
| VSURG | 11486 | 158 | 1.38 | 99.9 |
| PSYCH | 9586 | 2 | 0.02 | 246.23333333333332 |
| GYN | 7474 | 5 | 0.07 | 154.61666666666667 |
| TRAUM | 7250 | 266 | 3.67 | 52.11517857142857 |
| GU | 6709 | 5 | 0.07 | 81.6 |
| TSURG | 5046 | 44 | 0.87 | 309.3833333333333 |

## W05 -- Patient identity management -> longitudinal patient-360

| subject_id | gender | anchor_age | total_admission_count | total_readmission_count | ever_expired_in_hospital | first_admit_time | most_recent_disch_time | distinct_diagnosis_count | total_icu_stay_count | total_icu_los_days | overall_abnormal_lab_rate | total_positive_culture_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 15496609 | M | 57 | 238 | 237 | false | 2154-08-06 00:17:00.000000 | 2165-05-30 07:09:00.000000 | 0 | 4 | 7.405185185185186 |  | 0 |
| 15464144 | M | 55 | 185 | 184 | false | 2189-03-23 22:10:00.000000 | 2201-01-26 14:27:00.000000 | 0 | 0 | 0.0 |  | 0 |
| 10714009 | M | 52 | 163 | 162 | false | 2127-03-12 23:04:00.000000 | 2138-03-30 06:58:00.000000 | 113 | 4 | 5.796099537037037 |  | 9 |
| 16662316 | M | 56 | 142 | 141 | true | 2171-05-20 06:48:00.000000 | 2181-09-04 03:55:00.000000 | 0 | 16 | 38.184652777777785 |  | 0 |
| 14394983 | M | 46 | 138 | 137 | false | 2196-05-06 05:26:00.000000 | 2209-02-06 12:01:00.000000 | 0 | 0 | 0.0 |  | 0 |
| 15229574 | M | 48 | 130 | 129 | false | 2120-02-15 01:57:00.000000 | 2130-08-28 10:09:00.000000 | 0 | 1 | 0.659212962962963 |  | 0 |
| 11582633 | F | 49 | 105 | 104 | false | 2132-08-29 19:30:00.000000 | 2147-05-06 17:10:00.000000 | 404 | 0 | 0.0 |  | 1 |
| 17011846 | M | 57 | 104 | 103 | false | 2163-08-29 00:52:00.000000 | 2178-04-02 17:15:00.000000 | 0 | 9 | 11.964756944444444 |  | 0 |
| 13475033 | M | 66 | 103 | 102 | false | 2173-10-19 04:11:00.000000 | 2186-11-02 20:38:00.000000 | 0 | 9 | 10.528414351851852 |  | 0 |
| 11965254 | F | 24 | 101 | 100 | false | 2141-11-28 12:26:00.000000 | 2156-03-24 11:47:00.000000 | 0 | 2 | 2.785972222222222 |  | 119 |
| 16233333 | M | 48 | 101 | 100 | false | 2110-11-07 18:23:00.000000 | 2122-01-27 11:20:00.000000 | 0 | 1 | 0.7736226851851852 |  | 0 |
| 18284271 | F | 68 | 101 | 100 | false | 2139-10-08 14:40:00.000000 | 2154-01-03 16:30:00.000000 | 0 | 5 | 9.59423611111111 |  | 0 |
| 11553072 | M | 46 | 99 | 98 | false | 2158-04-13 21:24:00.000000 | 2169-05-25 16:18:00.000000 | 137 | 1 | 1.802534722222222 |  | 5 |
| 12468016 | M | 50 | 99 | 98 | false | 2127-08-23 19:44:00.000000 | 2140-09-16 13:30:00.000000 | 0 | 41 | 109.39697916666668 |  | 302 |
| 11389314 | F | 53 | 95 | 94 | false | 2183-11-13 13:00:00.000000 | 2194-12-29 15:43:00.000000 | 368 | 16 | 27.123530092592592 |  | 77 |

## W06 -- Diagnosis coding -> disease cohort (sepsis / CHF / AKI) -> LOS, mortality, readmit

| disease | admissions | avg_los_days | mortality_pct | readmit_30d_pct |
|---|---|---|---|---|
| CHF | 12802 | 7.21 | 4.95 | 23.26 |
| AKI | 11651 | 9.53 | 8.47 | 23.48 |
| Sepsis | 3552 | 13.5 | 20.19 | 20.13 |

## W07 -- DRG assignment -> severity / mortality weight vs. observed outcome (case-mix)

| drg_severity | drg_mortality | admissions | observed_mortality_pct | avg_los_days |
|---|---|---|---|---|
| 1 | 1 | 51405 | 0.03 | 3.11 |
| 1 | 2 | 5172 | 0.52 | 3.52 |
| 1 | 3 | 372 | 1.34 | 4.44 |
| 1 | 4 | 26 | 7.69 | 5.35 |
| 2 | 1 | 57372 | 0.05 | 4.29 |
| 2 | 2 | 42567 | 0.51 | 4.29 |
| 2 | 3 | 7491 | 1.7 | 4.76 |
| 2 | 4 | 200 | 12.0 | 5.72 |
| 3 | 1 | 10558 | 0.09 | 6.57 |
| 3 | 2 | 35797 | 0.56 | 6.15 |
| 3 | 3 | 52023 | 2.43 | 6.8 |
| 3 | 4 | 6532 | 11.74 | 7.21 |
| 4 | 1 | 474 | 0.21 | 22.56 |
| 4 | 2 | 1895 | 0.74 | 12.12 |
| 4 | 3 | 12187 | 4.67 | 12.03 |

## W08 -- Procedure coding -> ICD-10-PCS utilization -> LOS delta

| icd_code | long_title | admissions | avg_los_days | los_delta_vs_all_days |
|---|---|---|---|---|
| 02HV33Z | Insertion of Infusion Device into Superior Vena Cava, Percutaneous Approach | 13561 | 17.37 | 12.61 |
| 3E0G76Z | Introduction of Nutritional Substance into Upper GI, Via Natural or Artificial Opening | 8651 | 21.44 | 16.68 |
| 10E0XZZ | Delivery of Products of Conception, External Approach | 5815 | 3.0 | -1.76 |
| 0BH17EZ | Insertion of Endotracheal Airway into Trachea, Via Natural or Artificial Opening | 5612 | 19.48 | 14.72 |
| 5A1221Z | Performance of Cardiac Output, Continuous | 5451 | 10.32 | 5.56 |
| 0DJ08ZZ | Inspection of Upper Intestinal Tract, Via Natural or Artificial Opening Endoscopic | 5297 | 13.22 | 8.46 |
| B211YZZ | Fluoroscopy of Multiple Coronary Arteries using Other Contrast | 4600 | 7.97 | 3.21 |
| 5A1D70Z | Performance of Urinary Filtration, Intermittent, Less than 6 Hours Per Day | 4573 | 14.7 | 9.94 |
| 5A1945Z | Respiratory Ventilation, 24-96 Consecutive Hours | 4551 | 14.87 | 10.11 |
| 5A1955Z | Respiratory Ventilation, Greater than 96 Consecutive Hours | 4106 | 27.93 | 23.17 |
| 3E04305 | Introduction of Other Antineoplastic into Central Vein, Percutaneous Approach | 3728 | 15.08 | 10.32 |
| 02H633Z | Insertion of Infusion Device into Right Atrium, Percutaneous Approach | 3255 | 20.89 | 16.13 |
| 0W9G3ZZ | Drainage of Peritoneal Cavity, Percutaneous Approach | 3111 | 14.4 | 9.64 |
| 10D00Z1 | Extraction of Products of Conception, Low, Open Approach | 3002 | 5.41 | 0.65 |
| 02100Z9 | Bypass Coronary Artery, One Artery from Left Internal Mammary, Open Approach | 2991 | 9.62 | 4.85 |

## W09 -- Diagnosis coding -> Charlson comorbidity index -> risk-adjusted outcome

| charlson_band | admissions | mortality_pct | avg_los_days | readmission_pct |
|---|---|---|---|---|
| 0 | 31931 | 0.45 | 3.14 | 46.58 |
| 1-2 | 27910 | 1.82 | 4.73 | 59.67 |
| 3-4 | 13090 | 3.93 | 6.51 | 69.31 |
| 5+ | 12589 | 5.7 | 7.43 | 77.16 |

## W10 -- HCPCS / CPT billing events -> per-admission billed-event cost proxy

| hcpcs_cd | description | admissions | billed_events | avg_admission_billed_events | avg_los_days |
|---|---|---|---|---|---|
| G0378 | Hospital observation per hr | 68533 | 68571 | 1.3 | 1.12 |
| 99219 | Hospital observation services | 52379 | 52408 | 1.2 | 1.01 |
| 99218 | Hospital observation services | 12083 | 12091 | 1.7 | 0.87 |
| 99220 | Hospital observation services | 11065 | 11069 | 1.0 | 0.85 |
| 44970 | Digestive system | 1174 | 1174 | 2.0 | 0.97 |
| 93454 | Cardiovascular | 1144 | 1144 | 2.7 | 1.24 |
| 43262 | Digestive system | 1068 | 1073 | 3.5 | 1.27 |
| 43264 | Digestive system | 965 | 965 | 3.5 | 1.28 |
| 47562 | Digestive system | 950 | 952 | 2.2 | 1.61 |
| 43239 | Digestive system | 861 | 862 | 2.5 | 2.33 |
| 64447 | Nervous system | 431 | 824 | 4.6 | 2.06 |
| C9600 | Perc drug-el cor stent sing | 680 | 759 | 3.2 | 1.17 |
| 43235 | Digestive system | 562 | 564 | 2.3 | 1.97 |
| 45380 | Digestive system | 461 | 477 | 2.7 | 2.16 |
| 64415 | Nervous system | 250 | 470 | 5.1 | 1.49 |

## W11 -- Lab testing -> KDIGO AKI (baseline vs. peak creatinine) -> RRT, outcome

| kdigo_stage | admissions | avg_baseline_cr | avg_peak_cr | rrt_pct | mortality_pct | avg_los_days |
|---|---|---|---|---|---|---|
| 0 | 2223 | 1.02 | 1.06 | 0.18 | 1.39 | 4.99 |
| 1 | 194 | 1.34 | 1.87 | 2.58 | 6.7 | 11.68 |
| 2 | 32 | 0.97 | 2.23 | 3.13 | 18.75 | 13.63 |
| 3 | 146 | 4.67 | 6.16 | 52.74 | 15.07 | 11.64 |

## W12 -- Lab testing -> abnormal-result burden per admission -> LOS, mortality

| burden_quartile | admissions | min_abnormal | max_abnormal | avg_abnormal_rate | avg_los_days | mortality_pct |
|---|---|---|---|---|---|---|
| 1 | 699 | 0 | 9 | 0.176 | 2.32 | 0.14 |
| 2 | 698 | 9 | 25 | 0.272 | 3.15 | 0.86 |
| 3 | 698 | 25 | 68 | 0.326 | 4.88 | 1.58 |
| 4 | 698 | 68 | 2459 | 0.394 | 12.71 | 7.88 |

## W13 -- Lab workflow -> turnaround time -> care-delay analysis

| category | priority | results | median_tat_min | p90_tat_min | pct_over_60_min |
|---|---|---|---|---|---|
| Chemistry | ROUTINE | 227598 | 133.73400119937392 | 443.5393326190537 | 89.55 |
| Hematology | ROUTINE | 211716 | 105.09334459520527 | 386.1634138856799 | 70.53 |
| Chemistry | STAT | 219556 | 69.25227459512782 | 150.35705196762484 | 62.53 |
| Hematology | STAT | 275019 | 38.72639187114929 | 108.15527648922773 | 27.8 |
| Blood Gas |  | 49997 | 2.993044771638716 | 17.388622299905432 | 1.02 |

## W14 -- Microbiology -> positive blood culture -> time-to-antibiotic, concordant therapy, mortality

| time_to_antibiotic | bacteremic_admissions | concordant_therapy_pct | mortality_pct |
|---|---|---|---|
| 1-6h | 40 | 0.0 | 25.0 |
| 6-24h | 31 | 0.0 | 32.26 |
| <=1h | 22 | 0.0 | 22.73 |
| >24h | 16 | 0.0 | 25.0 |
| no abx recorded | 2037 | 0.0 | 18.9 |

## W15 -- Microbiology -> antibiogram (organism x susceptibility)

| org_name | ab_name | isolates_tested | pct_susceptible | pct_resistant |
|---|---|---|---|---|
| ESCHERICHIA COLI | CIPROFLOXACIN | 10536 | 69.2 | 30.1 |
| ESCHERICHIA COLI | CEFAZOLIN | 10533 | 78.4 | 20.8 |
| ESCHERICHIA COLI | GENTAMICIN | 10533 | 88.2 | 11.6 |
| ESCHERICHIA COLI | CEFTAZIDIME | 10532 | 91.9 | 6.3 |
| ESCHERICHIA COLI | MEROPENEM | 10531 | 99.9 | 0.1 |
| ESCHERICHIA COLI | TOBRAMYCIN | 10531 | 88.9 | 4.1 |
| ESCHERICHIA COLI | AMPICILLIN/SULBACTAM | 10531 | 60.0 | 22.1 |
| ESCHERICHIA COLI | CEFTRIAXONE | 10527 | 87.1 | 12.5 |
| ESCHERICHIA COLI | TRIMETHOPRIM/SULFA | 10516 | 69.2 | 30.8 |
| ESCHERICHIA COLI | CEFEPIME | 10487 | 91.0 | 8.3 |
| ESCHERICHIA COLI | AMPICILLIN | 10480 | 48.0 | 50.6 |
| ESCHERICHIA COLI | NITROFURANTOIN | 9516 | 95.0 | 1.8 |
| ESCHERICHIA COLI | PIPERACILLIN/TAZO | 8908 | 98.7 | 0.7 |
| STAPH AUREUS COAG + | OXACILLIN | 6100 | 63.7 | 36.3 |
| STAPH AUREUS COAG + | GENTAMICIN | 6098 | 98.9 | 0.8 |

## W16 -- eMAR -> medication administration volume / delay per admission -> outcome

| delay_quartile | admissions | avg_median_delay_min | avg_administrations | avg_los_days | mortality_pct |
|---|---|---|---|---|---|
| 1 | 1678 | -1.6 | 57.5 | 3.91 | 2.21 |
| 2 | 1677 | 0.2 | 64.0 | 4.22 | 3.82 |
| 3 | 1677 | 8.4 | 149.6 | 7.81 | 3.04 |
| 4 | 1677 | 12111.9 | 113.8 | 6.19 | 0.95 |

## W17 -- Pharmacy + eMAR -> high-alert drug exposure (vasopressors, anticoagulants) -> complication proxy

| high_alert_exposure | admissions | hemorrhage_dx_pct | mortality_pct |
|---|---|---|---|
| anticoagulant only | 18000 | 4.77 | 0.91 |
| neither | 6348 | 5.91 | 1.43 |
| vasopressor + anticoagulant | 2024 | 27.47 | 17.39 |
| vasopressor only | 251 | 34.26 | 25.9 |

## W18 -- POE + eMAR -> ordered-not-given reconciliation

| drug | prescriptions | never_administered | not_given_pct |
|---|---|---|---|
| Prismasate (B32 K2) | 228 | 228 | 100.0 |
| Lidocaine 5% Patch | 922 | 922 | 100.0 |
| Insulin Human Regular | 318 | 318 | 100.0 |
| Heparin Flush (100 units/ml) | 297 | 297 | 100.0 |
| NORepinephrine | 550 | 550 | 100.0 |
| Nicotine Patch | 322 | 322 | 100.0 |
| Nitroglycerin | 422 | 422 | 100.0 |
| Dexmedetomidine | 306 | 306 | 100.0 |
| Potassium Chl 20 mEq / 1000 mL D5 1/2 NS | 372 | 371 | 99.73 |
| Glucagon | 2134 | 2125 | 99.58 |
| PHENYLEPHrine | 665 | 662 | 99.55 |
| Neomycin-Polymyxin-Bacitracin | 333 | 328 | 98.5 |
| D5 1/2NS | 974 | 945 | 97.02 |
| HYDROmorphone | 514 | 496 | 96.5 |
| Glucose Gel | 2129 | 2050 | 96.29 |

## W19 -- Transfers / bed management -> ward-transfer count, bounce-backs -> LOS

| ward_transfers | admissions | bounce_back_pct | avg_los_days | mortality_pct |
|---|---|---|---|---|
| 0 | 114017 | 0.0 | 2.73 | 1.46 |
| 1-2 | 85064 | 4.26 | 5.27 | 2.23 |
| 3-4 | 20164 | 40.81 | 9.53 | 3.89 |
| 5+ | 7024 | 74.54 | 18.81 | 7.56 |

## W20 -- Clinical service assignment -> MED <-> SURG transitions -> outcome

| transition | admissions | avg_service_records | avg_los_days | mortality_pct |
|---|---|---|---|---|
| no MED/SURG transition | 533227 | 1.06 | 4.59 | 2.08 |
| SURG -> MED | 5666 | 1.85 | 9.63 | 4.54 |
| MED -> SURG | 5287 | 2.08 | 11.84 | 6.07 |
| both directions | 1832 | 3.31 | 20.58 | 8.02 |

## W21 -- Outpatient measurements (OMR) -> pre-admission vitals / BMI -> admission risk

| pre_admission_bmi | admissions | mortality_pct | avg_los_days | readmission_pct |
|---|---|---|---|---|
| 1 underweight (<18.5) | 821 | 4.26 | 5.76 | 85.99 |
| 2 normal (18.5-25) | 8791 | 2.45 | 5.16 | 81.72 |
| 3 overweight (25-30) | 9643 | 2.02 | 4.77 | 79.03 |
| 4 obese (30-40) | 9328 | 1.34 | 4.65 | 78.18 |
| 5 morbidly obese (40+) | 2414 | 0.62 | 4.68 | 79.87 |

## W22 -- ICU stay management -> LOS, ICU readmission, time-to-ICU

| first_careunit | icu_stays | avg_icu_los_days | median_icu_los_days | icu_readmission_pct | median_hours_to_icu | hospital_mortality_pct |
|---|---|---|---|---|---|---|
| Medical Intensive Care Unit (MICU) | 20703 | 3.76 | 1.9146994729710904 | 39.54 | 1.558696767912258 | 16.55 |
| Medical/Surgical Intensive Care Unit (MICU/SICU) | 15449 | 3.09 | 1.783496060020571 | 34.93 | 2.48545169668758 | 15.52 |
| Cardiac Vascular Intensive Care Unit (CVICU) | 14771 | 3.32 | 1.9770657766044712 | 22.01 | 18.018194928573436 | 4.37 |
| Surgical Intensive Care Unit (SICU) | 13009 | 3.9 | 1.9865716877327364 | 30.29 | 2.645045027928065 | 11.88 |
| Coronary Care Unit (CCU) | 10775 | 3.09 | 2.0174723511277852 | 31.59 | 1.7488247677824944 | 13.7 |
| Trauma SICU (TSICU) | 10474 | 3.64 | 1.8879427756226326 | 24.44 | 1.7752793015006083 | 11.04 |
| Neuro Intermediate | 5776 | 5.02 | 3.0145951684433254 | 25.31 | 2.083170456792182 | 2.04 |
| Neuro Surgical Intensive Care Unit (Neuro SICU) | 1751 | 4.48 | 2.253417827579171 | 25.01 | 1.3571848455095876 | 27.07 |
| Neuro Stepdown | 1421 | 4.07 | 2.224399327422902 | 22.45 | 1.9558620480848885 | 1.69 |
| Surgery/Vascular/Intermediate | 145 | 15.71 | 13.587534722222223 | 34.48 | 1.7722222222222221 | 31.72 |
| PACU | 122 | 4.02 | 2.0141753472222224 | 54.92 | 7.273796296296297 | 10.66 |
| Intensive Care Unit (ICU) | 33 | 8.79 | 5.761678240740741 | 33.33 | 2.2666666666666666 | 51.52 |
| Medicine | 16 | 15.79 | 13.954548611111111 | 18.75 | 2.5833333333333335 | 31.25 |
| Surgery/Trauma | 10 | 10.64 | 11.955405092592592 | 40.0 | 36.36055555555556 | 50.0 |
| Neurology | 1 | 28.25 | 28.247592592592593 | 0.0 | 0.9666666666666667 | 0.0 |

## W23 -- Intake / output charting -> net fluid balance trajectory -> AKI, mortality

| net_balance_first_72h | icu_stays | avg_net_ml | aki_dx_pct | hospital_mortality_pct |
|---|---|---|---|---|
| 1 negative | 11740 | -3758.0 | 25.2 | 11.08 |
| 2 0-2 L | 2167 | 930.0 | 24.97 | 8.95 |
| 3 2-5 L | 1915 | 3335.0 | 28.36 | 10.76 |
| 4 >5 L | 1494 | 9428.0 | 51.87 | 24.63 |

## W24 -- ICU procedures -> mechanical ventilation duration, VAP proxy -> outcome

| ventilation_duration | ventilated_stays | vap_dx_pct | avg_icu_los_days | hospital_mortality_pct |
|---|---|---|---|---|
| 1 <24h | 15967 | 0.15 | 2.35 | 9.94 |
| 2 1-4 days | 8483 | 0.75 | 4.6 | 23.11 |
| 3 4-7 days | 2865 | 2.09 | 8.3 | 31.8 |
| 4 >7 days | 4654 | 5.2 | 18.44 | 31.87 |

## W25 -- Nursing charting -> sedation (RASS) vs. delirium (CAM-ICU) -> outcome

| sedation_level | icu_stays | cam_icu_positive_pct | avg_icu_los_days | hospital_mortality_pct |
|---|---|---|---|---|
| 0 no RASS | 7 | 100.0 | 2.11 | 85.71 |
| 2 light sedation (-2..-1) | 94 | 72.09 | 5.72 | 14.89 |
| 3 alert and calm (0) | 63 | 11.86 | 2.21 | 3.17 |
| 4 agitated (> 0) | 9 | 100.0 | 8.63 | 0.0 |

## W26 -- Ingredient events -> caloric / protein intake -> LOS

| caloric_intake | icu_stays | avg_protein_g_per_day | avg_icu_los_days | hospital_mortality_pct |
|---|---|---|---|---|
| 1 <500 kcal/day | 3966 | 8.3 | 2.69 | 11.32 |
| 2 500-1000 | 519 | 27.4 | 8.09 | 27.55 |
| 3 1000-1500 | 234 | 53.4 | 13.55 | 23.08 |
| 4 >=1500 | 104 | 78.5 | 13.07 | 22.12 |

## W27 -- Discharge documentation -> SDOH (tobacco / alcohol / obesity) -> readmission, mortality

| tobacco_use | alcohol_use | obesity_level | admissions | readmit_30d_pct | mortality_pct | avg_los_days |
|---|---|---|---|---|---|---|
| NOT_MENTIONED | NOT_MENTIONED | NONE | 5456 | 21.28 | 3.21 | 5.19 |
| NOT_MENTIONED | CURRENT | NONE | 1040 | 23.56 | 2.02 | 6.24 |
| NOT_MENTIONED | DENIES | NONE | 777 | 15.32 | 0.77 | 4.39 |
| CURRENT | NOT_MENTIONED | NONE | 601 | 22.13 | 2.0 | 5.65 |
| NOT_MENTIONED | NOT_MENTIONED | MORBID | 498 | 21.29 | 3.61 | 6.21 |
| NOT_MENTIONED | NOT_MENTIONED | OBESE | 389 | 20.57 | 1.29 | 4.56 |
| CURRENT | CURRENT | NONE | 308 | 18.18 | 0.65 | 6.89 |
| NOT_MENTIONED | DENIES | MORBID | 92 | 15.22 | 0.0 | 4.91 |
| NOT_MENTIONED | CURRENT | OBESE | 91 | 26.37 | 2.2 | 4.65 |
| NOT_MENTIONED | CURRENT | MORBID | 89 | 21.35 | 3.37 | 6.89 |
| CURRENT | DENIES | NONE | 83 | 18.07 | 0.0 | 5.3 |
| DENIES | NOT_MENTIONED | NONE | 79 | 17.72 | 1.27 | 5.78 |
| NOT_MENTIONED | FORMER | NONE | 64 | 23.44 | 0.0 | 4.72 |
| CURRENT | NOT_MENTIONED | MORBID | 62 | 4.84 | 0.0 | 6.43 |
| FORMER | NOT_MENTIONED | NONE | 59 | 25.42 | 3.39 | 4.73 |

## W28 -- Radiology reports -> procedure / modality mix per admission -> imaging cost, LOS

| modality | admissions | exams | avg_exams_per_admission | avg_los_days | mortality_pct |
|---|---|---|---|---|---|
| XR | 1384 | 2391 | 1.73 | 7.0 | 4.62 |
| CT | 1228 | 1966 | 1.6 | 6.63 | 3.91 |
| OTHER | 909 | 1261 | 1.39 | 7.58 | 4.4 |
| US | 548 | 730 | 1.33 | 8.93 | 4.74 |
| MR | 299 | 351 | 1.17 | 8.66 | 4.01 |
| FLUORO | 40 | 42 | 1.05 | 12.95 | 2.5 |
| ANGIO | 12 | 12 | 1.0 | 9.81 | 0.0 |

## W29 -- Radiology findings -> ICD-code concordance (report disorders vs. coded dx)

| icd10_category | example_report_term | admissions_with_finding | also_coded | concordance_pct |
|---|---|---|---|---|
| J98 | atelectasis | 337 | 25 | 7.42 |
| J90 | pleural effusion | 264 | 15 | 5.68 |
| J18 | pneumonia | 231 | 38 | 16.45 |
| I51 | cardiomegaly | 172 | 0 | 0.0 |
| J81 | pulmonary edema | 164 | 7 | 4.27 |
| I63 | infarct | 106 | 34 | 32.08 |
| R22 | mass | 104 | 0 | 0.0 |
| I82 | dvt | 103 | 28 | 27.18 |
| R91 | nodule | 83 | 13 | 15.66 |
| R18 | ascites | 83 | 16 | 19.28 |
| K74 | cirrhosis | 68 | 22 | 32.35 |
| K56 | bowel obstruction | 60 | 25 | 41.67 |
| J93 | pneumothorax | 59 | 4 | 6.78 |
| L02 | abscess | 54 | 5 | 9.26 |
| K80 | cholelithiasis | 52 | 12 | 23.08 |

## W30 -- Radiology INDICATION / IMPRESSION retrieval for a cohort (structured summaries)

| subject_id | hadm_id | admit_time | note_id | indication | impression |
|---|---|---|---|---|---|
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-10 | ___ female with duodenal perforation from ERCP. Question duodenal leak. | 1. Persistent free air adjacent to mid second portion of duodenum, consistent with retroperitoneal perforation post ERCP |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-11 | ___ with duodenal perforation, now with increasing O2 sats wheezing, crackles, concern for edema or pneumonia. | Increased interstitial markings bilaterally concerning for aspiration with component of interstitial edema. |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-13 | Increased oxygen demand. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-14 | Post-intubation, endotracheal tube placement. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-15 | Nasogastric tube placement. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-16 | Bilateral opacities. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-18 | PICC line placement. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-19 | ARDS, assessment for interval change. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-20 | Evaluation for interval change. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-21 | ___ woman status post ERCP and duodenal puncture who was improving; now with ARDS. Evaluate for consolidation versus dif | 1. Multifocal consolidations with air-bronchograms, worst at the right lung base and apices bilaterally, with intersitia |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-22 | Duodenal perforation, now with ARDS and effusions, to assess for change. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-23 | To assess for change. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-24 | Evaluation for interval change. |  |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-25 | ___ female status post ERCP with duodenal perforation, with several days of increasing respiratory distress. Evaluate ch | 1. Interval worsening of multifocal, bilateral airspace consolidations within the lungs, worst at the apices. Differenti |
| 10032381 | 20176432 | 2115-06-27 13:38:00.000000 | 10032381-RR-26 | ___ woman with ARDS. | Minimal interval improvement in aeration, with persisting widespread abnormalities compatible with ARDS. |
