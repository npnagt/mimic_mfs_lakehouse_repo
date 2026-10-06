# Standalone PySpark script: NLP feature extraction from RADIOLOGY notes with
# medSpaCy, loading mimic4_db_business.fact_radiology_note_nlp. The radiology parallel
# of medspacy_nlp.py (discharge notes). Run as a script-mode Glue job -- see
# etl/create_radiology_nlp_job.py.
#
# Radiology reports are rigidly sectioned (EXAMINATION: / INDICATION: / TECHNIQUE: /
# COMPARISON: / FINDINGS: / IMPRESSION: / PROCEDURE: ...), so a regex section splitter
# does most of the work; medSpaCy (TargetMatcher + ConText) supplies negation-aware
# symptom / disorder recognition over the relevant sections.
#
# Grain: one row per admission (subject_id, hadm_id). Derived columns, all JSON held
# in STRING (Athena has no VARIANT/JSON column type and neither Glue 4.0 nor Glue 5.0
# /Spark 3.5 expose Iceberg's variant -- verified; query with json_extract/json_parse):
#   radiology_procedure_types  [{exam, modality, body_region, note_id}]
#   radiology_reasons          [{reason, note_id}]              (INDICATION "// ..." question)
#   symptoms                   [{text, negated, section, note_id}]
#   disorders                  [{text, negated, section, note_id}]
#   icd_codes                  [{code, term, source}]           (cited in text + disorder lookup)
#   procedures                 [{text, note_id}]                (PROCEDURE section / procedure terms)
#   findings_summary           [{note_id, text}]                (FINDINGS section text)
#   indication_summary         [{note_id, text}]                (INDICATION/HISTORY section text)
#   conclusion                 [{note_id, text}]                (IMPRESSION section text)
#   ner_json                   the entire per-note medSpaCy output for the admission
#
# Resumable, partition-aligned batching: the same fix applied to medspacy_nlp.py for the
# identical failure mode -- radiology-nlp-job-full also timed out at the 120-minute Glue
# limit on 2x G.1X with zero partial progress saved (one mapPartitions(...).collect() over
# the entire corpus, nothing written until the very end), and up to ~310,000 admissions
# can carry a radiology note, so it's at least as exposed. Every write now processes one
# hadm_bucket = hadm_id % NUM_BUCKETS group at a time -- a real column, matching
# fact_radiology_note_nlp's PARTITIONED BY (hadm_bucket) plain IDENTITY partition (not an
# Iceberg bucket() transform: glue_catalog's SparkCatalog doesn't implement Iceberg's
# function catalog here, confirmed live) -- so overwritePartitions() only ever replaces
# that one partition, and a Timeout only loses the in-flight bucket's work. Resuming
# re-reads TARGET_TABLE, skips every admission a prior attempt already wrote (left-anti
# join), and continues. See medspacy_nlp.py's module docstring for the full rationale --
# this mirrors it exactly.
import json
import re
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

ARGS = getResolvedOptions(
    sys.argv,
    [
        "raw_database",
        "gold_database",
        "gold_s3_bucket",
        "nlp_bucket",
        "radiology_results_prefix",
        "note_limit",
        "region",
        "write_note_json",
        "refresh_mode",
        "num_buckets",
    ],
)
RAW_DB = ARGS["raw_database"]
GOLD_DB = ARGS["gold_database"]
NLP_BUCKET = ARGS["nlp_bucket"]
RESULTS_PREFIX = ARGS["radiology_results_prefix"].strip("/")
REGION = ARGS["region"]
WRITE_NOTE_JSON = ARGS["write_note_json"].strip().lower() == "true"
_raw_limit = ARGS["note_limit"].strip().lower()
NOTE_LIMIT = None if _raw_limit in ("", "0", "all", "-1") else int(_raw_limit)
# Must match fact_radiology_note_nlp's PARTITIONED BY (bucket(N, hadm_id)) -- see module
# docstring and medspacy_nlp.py's identical NUM_BUCKETS comment.
NUM_BUCKETS = int(ARGS["num_buckets"])

MAX_SECTION_CHARS = 2500
TARGET_TABLE = f"glue_catalog.{GOLD_DB}.fact_radiology_note_nlp"
REFRESH_MODE = ARGS["refresh_mode"].strip().lower()  # "auto" | "full"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---------------------------------------------------------------------------------------
# Incremental refresh via mimic4_db_business.etl_control  (proposition P7) -- same
# watermark pattern as the agg/OBT jobs and medspacy_nlp.py.
#   auto : no watermark -> full backfill; else reprocess only admissions with a radiology
#          note newer than the watermark, and MERGE those rows into the target.
#   full : reprocess every note, then advance the watermark.
# A --note_limit sample run never advances the watermark.
# ---------------------------------------------------------------------------------------
from pyspark.sql import functions as _F  # noqa: E402

ETL_CONTROL = f"glue_catalog.{GOLD_DB}.etl_control"
CONTROL_KEY = "fact_radiology_note_nlp"
NOTE_TS = _F.coalesce(_F.to_timestamp("storetime"), _F.to_timestamp("charttime"),
                      _F.lit("1900-01-01 00:00:00").cast("timestamp"))


def read_watermark():
    try:
        r = (spark.table(ETL_CONTROL)
             .filter(_F.col("aggregate_table") == CONTROL_KEY)
             .select(_F.max("last_processed_ts").alias("wm")).collect())
        return r[0]["wm"] if r and r[0]["wm"] is not None else None
    except Exception as exc:  # noqa: BLE001
        print(f"etl_control not readable ({exc}) -- treating as first run.")
        return None


def advance_watermark(new_ts):
    if new_ts is None:
        return
    (spark.createDataFrame([(CONTROL_KEY, new_ts)], ["aggregate_table", "last_processed_ts"])
     .withColumn("updated_ts", _F.current_timestamp())
     .writeTo(ETL_CONTROL).option("fanout-enabled", "true").overwritePartitions())
    print(f"etl_control: {CONTROL_KEY} watermark advanced to {new_ts}")

# ---------------------------------------------------------------------------------------
# Section splitting -- radiology headers are '^  HEADER:' at line start.
# ---------------------------------------------------------------------------------------
SECTION_HEADERS = {
    "EXAMINATION": "examination", "EXAM": "examination", "STUDY": "examination",
    "INDICATION": "indication", "HISTORY": "indication", "CLINICAL HISTORY": "indication",
    "CLINICAL INFORMATION": "indication", "REASON FOR EXAM": "indication",
    "REASON FOR EXAMINATION": "indication", "REASON FOR STUDY": "indication",
    "TECHNIQUE": "technique", "COMPARISON": "comparison", "COMPARISONS": "comparison",
    "FINDINGS": "findings", "FINDING": "findings",
    "IMPRESSION": "impression", "CONCLUSION": "impression", "INTERPRETATION": "impression",
    "PROCEDURE": "procedure", "PROCEDURE DETAILS": "procedure", "PROCEDURE COMMENTS": "procedure",
    "WET READ": "wet_read", "RECOMMENDATION": "recommendation", "RECOMMENDATIONS": "recommendation",
    "NOTIFICATION": "notification", "DOSE": "dose",
}
_HEADER_RX = re.compile(r"(?m)^[ \t]*([A-Z][A-Z0-9 /()_.\-]{2,45}?):[ \t]*")


def split_sections(text):
    text = text or ""
    matches = list(_HEADER_RX.finditer(text))
    sections = {}
    if not matches:
        sections["_body"] = text.strip()
        return sections
    if matches[0].start() > 0:
        sections["_preamble"] = text[: matches[0].start()].strip()
    for i, m in enumerate(matches):
        raw_header = m.group(1).strip().upper()
        canon = SECTION_HEADERS.get(raw_header)
        if canon is None:
            # tolerate minor variants ("FINAL REPORT EXAMINATION", trailing words)
            for key, val in SECTION_HEADERS.items():
                if raw_header.startswith(key) or raw_header.endswith(key):
                    canon = val
                    break
        if canon is None:
            continue
        body = text[m.end() : matches[i + 1].start() if i + 1 < len(matches) else len(text)]
        body = re.sub(r"\s+\n", "\n", body).strip()
        sections[canon] = (sections.get(canon, "") + "\n" + body).strip() if canon in sections else body
    return sections


def clean(s):
    return re.sub(r"[ \t]+", " ", re.sub(r"\n{3,}", "\n\n", (s or "").replace("\r", ""))).strip()[:MAX_SECTION_CHARS]


# ---------------------------------------------------------------------------------------
# Radiology procedure / modality classification (from EXAMINATION + TECHNIQUE text).
# ---------------------------------------------------------------------------------------
# Order matters: the first match wins, so specific modalities precede the XR catch-all.
MODALITY_RX = [
    ("CT", re.compile(r"(?i)\b(ct|computed tomograph|cta|ctu|cect|mdct|multidetector)\b")),
    ("MR", re.compile(r"(?i)\b(mr|mri|mra|mrcp|magnetic resonance)\b")),
    ("US", re.compile(r"(?i)\b(us|ultrasound|sonogra|doppler|echo)\b")),
    ("MAMMO", re.compile(r"(?i)\b(mammogra|tomosynthesis)\b")),
    ("FLUORO", re.compile(r"(?i)\b(fluorosc|barium|esophagram|upper gi|small bowel follow|cystogram|voiding)\b")),
    ("NM", re.compile(r"(?i)\b(nuclear|scintigraph|hida|v/?q scan|bone scan|spect)\b")),
    ("PET", re.compile(r"(?i)\bpet\b")),
    ("ANGIO", re.compile(r"(?i)\b(angiogra|arteriogram|venogram|embolization)\b")),
    ("XR", re.compile(r"(?i)\b(x-?ray|radiograph|chest pa|portable|kub|plain film|\bxr\b|frontal|"
                       r"lateral view|two views|single view|pa and lat|chest film|ap (chest|abdomen|pelvis|view))\b")),
]
BODY_REGION_RX = [
    ("chest", re.compile(r"(?i)\b(chest|thorax|thoracic|lung|pulmonary|pa and lat)\b")),
    ("abdomen", re.compile(r"(?i)\b(abdom|liver|gallbladder|pancrea|renal|kidney|pelvi|paracentesis)\b")),
    ("head", re.compile(r"(?i)\b(head|brain|skull|cranial|intracranial)\b")),
    ("neck", re.compile(r"(?i)\b(neck|carotid|cervical spine|thyroid)\b")),
    ("spine", re.compile(r"(?i)\b(spine|spinal|lumbar|thoracic spine|vertebr)\b")),
    ("extremity", re.compile(r"(?i)\b(extremity|femur|tibia|humerus|hand|foot|ankle|knee|shoulder|hip|wrist|elbow)\b")),
    ("cardiac", re.compile(r"(?i)\b(cardiac|coronary|echocardiogram)\b")),
]
PROCEDURE_RX = re.compile(
    r"(?i)\b(paracentesis|thoracentesis|thoracostomy|biopsy|aspiration|drainage|drain placement|"
    r"catheter placement|picc|central line|line placement|angioplasty|embolization|thrombectomy|"
    r"nephrostomy|cholecystostomy|lumbar puncture|myelogram|arthrogram|ablation|stent placement|"
    r"gastrostomy|g-?tube|j-?tube|port placement|vertebroplasty|kyphoplasty)\b"
)


def classify_exam(exam_text, technique_text):
    blob = f"{exam_text} {technique_text}"
    modality = next((name for name, rx in MODALITY_RX if rx.search(blob)), "OTHER")
    body_region = next((name for name, rx in BODY_REGION_RX if rx.search(blob)), None)
    src = exam_text or technique_text or ""
    exam = re.sub(r"_{2,}", "", clean(src).split("\n")[0]).strip(" .-\t")[:200] or None
    return exam, modality, body_region


# ---------------------------------------------------------------------------------------
# medSpaCy target vocabulary (plain data -> pickles into the Spark closure).
# ---------------------------------------------------------------------------------------
SYMPTOM_PATTERNS = [
    "pain", "chest pain", "abdominal pain", "abd pain", "flank pain", "back pain",
    "shortness of breath", "dyspnea", "cough", "hemoptysis", "fever", "chills",
    "nausea", "vomiting", "diarrhea", "constipation", "jaundice", "distension",
    "distention", "abdominal distension", "swelling", "edema", "weakness", "numbness",
    "dizziness", "headache", "syncope", "altered mental status", "confusion",
    "weight loss", "fatigue", "hematuria", "dysuria", "dysphagia", "melena",
    "hematochezia", "seizure", "vision loss", "dizziness",
]
DISORDER_PATTERNS = [
    "consolidation", "pneumonia", "pleural effusion", "effusion", "pneumothorax",
    "pulmonary edema", "edema", "atelectasis", "nodule", "pulmonary nodule", "mass",
    "lung mass", "opacity", "ground glass opacity", "fracture", "compression fracture",
    "cirrhosis", "hepatomegaly", "splenomegaly", "hepatic steatosis", "steatosis",
    "cholelithiasis", "gallstone", "gallstones", "cholecystitis", "hydronephrosis",
    "nephrolithiasis", "kidney stone", "renal calculus", "hemorrhage",
    "intracranial hemorrhage", "subdural hematoma", "subarachnoid hemorrhage",
    "hematoma", "infarct", "infarction", "ischemia", "stroke", "aneurysm",
    "aortic aneurysm", "aortic dissection", "dissection", "pulmonary embolism",
    "embolism", "deep vein thrombosis", "dvt", "thrombosis", "thrombus",
    "bowel obstruction", "small bowel obstruction", "obstruction", "ileus",
    "perforation", "free air", "abscess", "appendicitis", "diverticulitis",
    "pancreatitis", "portal hypertension", "ascites", "malignancy", "metastasis",
    "metastases", "metastatic disease", "lymphadenopathy", "cardiomegaly", "emphysema",
    "copd", "pulmonary fibrosis", "fibrosis", "hydrocephalus", "midline shift",
    "herniation", "mass effect", "stenosis", "bronchiectasis",
]
ICD10_LOOKUP = {
    "pneumonia": "J18.9", "consolidation": "J18.9", "pleural effusion": "J90", "effusion": "J90",
    "pneumothorax": "J93.9", "pulmonary edema": "J81.1", "atelectasis": "J98.11",
    "pulmonary embolism": "I26.99", "copd": "J44.9", "emphysema": "J43.9",
    "pulmonary fibrosis": "J84.10", "fibrosis": "J84.10", "bronchiectasis": "J47.9",
    "cardiomegaly": "I51.7", "cirrhosis": "K74.60", "ascites": "R18.8",
    "hepatomegaly": "R16.0", "splenomegaly": "R16.1", "hepatic steatosis": "K76.0",
    "steatosis": "K76.0", "portal hypertension": "K76.6", "cholelithiasis": "K80.20",
    "gallstone": "K80.20", "gallstones": "K80.20", "cholecystitis": "K81.9",
    "pancreatitis": "K85.90", "appendicitis": "K37", "diverticulitis": "K57.92",
    "bowel obstruction": "K56.60", "small bowel obstruction": "K56.609", "ileus": "K56.7",
    "perforation": "K63.1", "hydronephrosis": "N13.30", "nephrolithiasis": "N20.0",
    "kidney stone": "N20.0", "renal calculus": "N20.0", "aortic aneurysm": "I71.9",
    "aortic dissection": "I71.00", "aneurysm": "I72.9", "deep vein thrombosis": "I82.409",
    "dvt": "I82.409", "intracranial hemorrhage": "I62.9", "subdural hematoma": "I62.00",
    "subarachnoid hemorrhage": "I60.9", "stroke": "I63.9", "infarct": "I63.9",
    "infarction": "I63.9", "hydrocephalus": "G91.9", "lymphadenopathy": "R59.9",
    "pulmonary nodule": "R91.1", "nodule": "R91.1", "lung mass": "R91.8", "mass": "R22.9",
    "metastasis": "C79.9", "metastases": "C79.9", "metastatic disease": "C79.9",
    "abscess": "L02.91",
}
CITED_ICD_RX = re.compile(r"\b([A-TV-Z][0-9][0-9AB](?:\.[0-9A-Z]{1,4})?)\b")
REASON_SPLIT_RX = re.compile(r"//|\bfor eval\b|\beval(?:uate)? for\b|\brule out\b|\br/o\b|\bassess for\b|\bconcern for\b", re.I)


def _trait(ent, name):
    return bool(getattr(ent._, name, False))


def process_partition(rows):
    import medspacy
    from medspacy.ner import TargetRule

    try:
        nlp = medspacy.load()
    except Exception:  # noqa: BLE001
        nlp = medspacy.load(medspacy_disable=["medspacy_pyrush"])
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer", first=True)
    matcher = nlp.get_pipe("medspacy_target_matcher")
    matcher.add([TargetRule(literal=p, category="SYMPTOM") for p in SYMPTOM_PATTERNS])
    matcher.add([TargetRule(literal=p, category="DISORDER") for p in DISORDER_PATTERNS])

    for r in rows:
        note_id = str(r["note_id"])
        sections = split_sections(r["text"])
        exam_txt = sections.get("examination", "") or sections.get("_preamble", "")
        tech_txt = sections.get("technique", "")
        indication_txt = sections.get("indication", "")
        findings_txt = sections.get("findings", "")
        impression_txt = sections.get("impression", "")
        procedure_txt = sections.get("procedure", "")
        full_text = r["text"] or ""

        exam, modality, body_region = classify_exam(exam_txt, tech_txt)
        proc_types = (
            [{"exam": exam, "modality": modality, "body_region": body_region, "note_id": note_id}]
            if (exam or modality != "OTHER")
            else []
        )

        reasons = []
        if indication_txt:
            parts = REASON_SPLIT_RX.split(indication_txt)
            tail = parts[-1] if len(parts) > 1 else indication_txt
            for chunk in re.split(r"[.;\n]", tail):
                c = re.sub(r"_{2,}", "", chunk).strip(" -\t\r\n")
                if 3 <= len(c) <= 200:
                    reasons.append({"reason": c, "note_id": note_id})
                    if len(reasons) >= 3:
                        break

        # medSpaCy over the sections most likely to carry findings/dx language
        ner_text = "\n".join(t for t in (indication_txt, findings_txt, impression_txt) if t)
        doc = nlp(ner_text)
        symptoms, disorders, all_ents = [], [], []
        for ent in doc.ents:
            rec = {
                "text": ent.text.lower(),
                "label": ent.label_,
                "negated": _trait(ent, "is_negated"),
                "historical": _trait(ent, "is_historical"),
                "hypothetical": _trait(ent, "is_hypothetical"),
                "family": _trait(ent, "is_family"),
                "sentence": ent.sent.text if ent.sent is not None else "",
                "note_id": note_id,
            }
            all_ents.append(rec)
            bucket = symptoms if ent.label_ == "SYMPTOM" else disorders
            bucket.append({"text": rec["text"], "negated": rec["negated"], "note_id": note_id})

        icd = []
        seen_codes = set()
        for code in CITED_ICD_RX.findall(full_text):
            if code not in seen_codes:
                seen_codes.add(code)
                icd.append({"code": code, "term": None, "source": "cited"})
        for d in disorders:
            if d["negated"]:
                continue
            code = ICD10_LOOKUP.get(d["text"])
            if code and code not in seen_codes:
                seen_codes.add(code)
                icd.append({"code": code, "term": d["text"], "source": "lookup"})

        procedures = []
        if procedure_txt:
            procedures.append({"text": clean(procedure_txt)[:400], "note_id": note_id})
        for term in set(PROCEDURE_RX.findall(f"{exam_txt} {tech_txt} {procedure_txt}")):
            procedures.append({"text": term.lower(), "note_id": note_id})

        yield (
            int(r["subject_id"]),
            int(r["hadm_id"]),
            r["admit_provider_id"],
            r["admit_date_key"],
            note_id,
            json.dumps(
                {
                    "proc_types": proc_types,
                    "reasons": reasons,
                    "symptoms": symptoms,
                    "disorders": disorders,
                    "icd": icd,
                    "procedures": procedures,
                    "findings": clean(findings_txt),
                    "indication": clean(indication_txt),
                    "impression": clean(impression_txt),
                    "entities": all_ents,
                }
            ),
        )


def _dedup(items, key):
    seen, out = set(), []
    for it in items:
        k = key(it)
        if k not in seen:
            seen.add(k)
            out.append(it)
    return out


SCHEMA = StructType(
    [
        StructField("subject_id", LongType()),
        StructField("hadm_id", LongType()),
        StructField("admit_provider_id", StringType()),
        StructField("admit_date_key", IntegerType()),
        StructField("hadm_bucket", IntegerType()),
        StructField("note_count", IntegerType()),
        StructField("radiology_procedure_types", StringType()),
        StructField("radiology_reasons", StringType()),
        StructField("symptoms", StringType()),
        StructField("disorders", StringType()),
        StructField("icd_codes", StringType()),
        StructField("procedures", StringType()),
        StructField("findings_summary", StringType()),
        StructField("indication_summary", StringType()),
        StructField("conclusion", StringType()),
        StructField("ner_json", StringType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
        StructField("created_by", StringType()),
        StructField("updated_by", StringType()),
    ]
)


def tag_and_derive(batch_notes_df):
    """Phase 2+3 for one batch: medSpaCy + section parsing on the executors, aggregate to
    (subject_id, hadm_id), build the JSON columns. Returns a DataFrame (possibly 0 rows)."""
    tagged = batch_notes_df.rdd.mapPartitions(process_partition).collect()
    if not tagged:
        return spark.createDataFrame([], SCHEMA)

    s3 = boto3.client("s3", region_name=REGION)
    per_admission = {}
    for subject_id, hadm_id, admit_provider_id, admit_date_key, note_id, payload_json in tagged:
        payload = json.loads(payload_json)
        if WRITE_NOTE_JSON:
            s3.put_object(
                Bucket=NLP_BUCKET,
                Key=f"{RESULTS_PREFIX}/{subject_id}_{hadm_id}_{note_id}.json",
                Body=payload_json.encode("utf-8"),
            )
        agg = per_admission.setdefault(
            (subject_id, hadm_id),
            {"notes": [], "payloads": [], "admit_provider_id": admit_provider_id, "admit_date_key": admit_date_key},
        )
        agg["notes"].append(note_id)
        agg["payloads"].append((note_id, payload))

    now = datetime.now(timezone.utc)
    rows = []
    for (subject_id, hadm_id), agg in per_admission.items():
        payloads = [p for _, p in agg["payloads"]]
        proc_types = _dedup([x for p in payloads for x in p["proc_types"]], lambda x: (x["exam"], x["modality"]))
        reasons = _dedup([x for p in payloads for x in p["reasons"]], lambda x: x["reason"].lower())
        symptoms = _dedup([x for p in payloads for x in p["symptoms"]], lambda x: (x["text"], x["negated"]))
        disorders = _dedup([x for p in payloads for x in p["disorders"]], lambda x: (x["text"], x["negated"]))
        icd = _dedup([x for p in payloads for x in p["icd"]], lambda x: x["code"])
        procedures = _dedup([x for p in payloads for x in p["procedures"]], lambda x: x["text"].lower())
        findings = [{"note_id": nid, "text": p["findings"]} for nid, p in agg["payloads"] if p["findings"]]
        indication = [{"note_id": nid, "text": p["indication"]} for nid, p in agg["payloads"] if p["indication"]]
        conclusion = [{"note_id": nid, "text": p["impression"]} for nid, p in agg["payloads"] if p["impression"]]
        ner = {
            "engine": "medspacy",
            "note_count": len(agg["notes"]),
            "notes": [{"note_id": nid, "entities": p["entities"]} for nid, p in agg["payloads"]],
        }
        rows.append(
            Row(
                subject_id=subject_id,
                hadm_id=hadm_id,
                admit_provider_id=agg["admit_provider_id"],
                admit_date_key=agg["admit_date_key"],
                hadm_bucket=hadm_id % NUM_BUCKETS,
                note_count=len(agg["notes"]),
                radiology_procedure_types=json.dumps(proc_types),
                radiology_reasons=json.dumps(reasons),
                symptoms=json.dumps(symptoms),
                disorders=json.dumps(disorders),
                icd_codes=json.dumps(icd),
                procedures=json.dumps(procedures),
                findings_summary=json.dumps(findings),
                indication_summary=json.dumps(indication),
                conclusion=json.dumps(conclusion),
                ner_json=json.dumps(ner),
                created_ts=now,
                updated_ts=now,
                created_by="glue_radiology_nlp",
                updated_by="glue_radiology_nlp",
            )
        )
    return spark.createDataFrame(rows, SCHEMA)


def write_batch(result_df) -> int:
    """Write one bucket's worth of results into TARGET_TABLE -- see medspacy_nlp.py's
    write_batch for the full rationale (identical pattern: fact_radiology_note_nlp is
    also PARTITIONED BY (hadm_bucket), a plain IDENTITY partition on the hadm_bucket =
    hadm_id % NUM_BUCKETS column tag_and_derive computes above, matching the single value
    every caller below groups by, so overwritePartitions() only replaces that one
    partition).

    coalesce(1): without it this table measured close to one physical Parquet file PER
    ADMISSION (2,711 files for 2,711 rows) -- result_df comes from spark.createDataFrame(rows,
    SCHEMA) over a plain Python list, which Spark spreads across its default parallelism
    regardless of how few rows a bucket holds, and every one of those tiny files still pays
    Parquet's fixed per-file footer/statistics cost. See medspacy_nlp.py's write_batch for
    the full rationale (identical fix, identical cause)."""
    result_df.coalesce(1).writeTo(TARGET_TABLE).overwritePartitions()
    return result_df.count()


# ---------------------------------------------------------------------------------------
# Phase 1 -- decide the note set (incremental vs full), read radiology notes
# ---------------------------------------------------------------------------------------
# admit_provider_id / admit_date_key are pulled from fact_admission so this table shares
# fact_admission's grain, matching every other archetype instance in the Multimodal Fusion
# Schema -- same value for every note of a given admission.
admission_keys = spark.table(f"glue_catalog.{GOLD_DB}.fact_admission").select(
    "hadm_id", "admit_provider_id", "admit_date_key"
)
src = (
    spark.table(f"{RAW_DB}.radiology_note_raw")
    .select("subject_id", "hadm_id", "note_id", "text", NOTE_TS.alias("_note_ts"))
    .where("hadm_id IS NOT NULL")
    .join(admission_keys, "hadm_id", "left")
)

watermark = None if REFRESH_MODE == "full" else read_watermark()
watermark_to_set = None

if watermark is not None:
    new_notes = src.filter(_F.col("_note_ts") > _F.lit(watermark).cast("timestamp"))
    touched_hadm = [r["hadm_id"] for r in new_notes.select("hadm_id").distinct().collect()]
    if not touched_hadm:
        # os._exit(0), not sys.exit(0): Glue's script runner reports a raised SystemExit
        # as FAILED, but a clean process exit with code 0 as SUCCEEDED -- which is correct
        # for a no-op refresh.
        print(f"radiology_nlp: no radiology note newer than watermark {watermark} -- nothing to refresh.")
        import os as _os
        _os._exit(0)
    print(f"radiology_nlp: incremental -- {len(touched_hadm)} admission(s) have a new radiology note "
          f"since {watermark}; reprocessing all notes for those admissions.")
    notes_df = src.where(_F.col("hadm_id").isin(touched_hadm))
    watermark_to_set = new_notes.select(_F.max("_note_ts").alias("m")).collect()[0]["m"]
else:
    print(f"radiology_nlp: full {'refresh' if REFRESH_MODE == 'full' else 'backfill (no watermark)'}.")
    notes_df = src
    watermark_to_set = src.select(_F.max("_note_ts").alias("m")).collect()[0]["m"]

notes_df = notes_df.select("subject_id", "hadm_id", "admit_provider_id", "admit_date_key", "note_id", "text")
if NOTE_LIMIT is not None:
    notes_df = notes_df.orderBy("note_id").limit(NOTE_LIMIT)
    watermark_to_set = None

# Resume-skip only for a fresh-or-interrupted backfill -- see medspacy_nlp.py's identical
# comment for the full rationale (NOTE_LIMIT does not gate this: the default
# --note_limit 1000000 IS the large-scale case that timed out, not a small sample).
USE_RESUME_SKIP = watermark is None and REFRESH_MODE != "full"

if USE_RESUME_SKIP:
    try:
        already_done_df = spark.table(TARGET_TABLE).select("hadm_id").distinct()
        already_done_count = already_done_df.count()
    except Exception as exc:  # noqa: BLE001 -- target table unreadable: treat as empty
        print(f"{TARGET_TABLE} not readable yet ({exc}) -- treating as empty (first attempt).")
        already_done_df, already_done_count = None, 0
    if already_done_count:
        before = notes_df.select("hadm_id").distinct().count()
        notes_df = notes_df.join(already_done_df, on="hadm_id", how="left_anti")
        after = notes_df.select("hadm_id").distinct().count()
        print(f"radiology_nlp: resuming backfill -- {already_done_count} admission(s) already in "
              f"{TARGET_TABLE} from a prior attempt; {before} -> {after} admission(s) remaining to process.")

# ---------------------------------------------------------------------------------------
# Phase 2+3 -- medSpaCy + section parsing on the executors, aggregate, write -- one bucket
# (partition) at a time, see medspacy_nlp.py's identical loop for the full rationale.
# ---------------------------------------------------------------------------------------
notes_df = notes_df.withColumn("hadm_bucket", _F.col("hadm_id") % NUM_BUCKETS)
bucket_ids = [r["hadm_bucket"] for r in notes_df.select("hadm_bucket").distinct().orderBy("hadm_bucket").collect()]
if not bucket_ids:
    print("radiology_nlp: nothing to process -- either no notes matched, or a prior attempt already "
          "covered every admission.")
    advance_watermark(watermark_to_set)
    import os as _os
    _os._exit(0)

print(f"radiology_nlp: processing {len(bucket_ids)} of {NUM_BUCKETS} hadm_id bucket(s).")
run_start = datetime.now(timezone.utc)
total_written = 0
for i, bucket_id in enumerate(bucket_ids, start=1):
    batch_start = datetime.now(timezone.utc)
    batch_notes_df = notes_df.where(_F.col("hadm_bucket") == bucket_id).drop("hadm_bucket").repartition(8)
    result_df = tag_and_derive(batch_notes_df)
    written = write_batch(result_df)
    total_written += written
    now_ts = datetime.now(timezone.utc)
    print(f"radiology_nlp: bucket {i}/{len(bucket_ids)} (id={bucket_id}) -- wrote {written} admission(s) in "
          f"{(now_ts - batch_start).total_seconds():.0f}s "
          f"(total {total_written} written, {(now_ts - run_start).total_seconds():.0f}s elapsed).")
advance_watermark(watermark_to_set)
print(f"phase 3: complete -- wrote {total_written} admission(s) across {len(bucket_ids)} bucket(s) "
      f"to {TARGET_TABLE}.")
