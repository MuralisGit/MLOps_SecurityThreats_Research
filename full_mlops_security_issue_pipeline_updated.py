#!/usr/bin/env python3
"""
full_mlops_security_issue_pipeline.py

End-to-end pipeline for GitHub issue extracts:
1. Identify likely security-related issues
2. Resolve first-pass "Review" issues with a stricter second pass
3. Tag issues to MLOps stages:
   - Acquisition
   - Preparation
   - Modeling
   - Training
   - Evaluation
   - Prediction
4. Capture evidence and reasons for both threat tagging and stage mapping
5. Write an Excel workbook with full results

Usage:
    python full_mlops_security_issue_pipeline.py \
        --input issues_extracted_last_3_years_all_repos.xlsx \
        --output issues_security_mlops_final.xlsx

Optional:
    --sheet Issues
    --min-yes-score 4
    --min-review-score 2
"""

import argparse
import math
import re
from collections import Counter
from pathlib import Path

import pandas as pd


# ============================================================
# Configuration
# ============================================================

STAGES = ["Acquisition", "Preparation", "Modeling", "Training", "Evaluation", "Prediction"]

THREAT_PATTERNS = {
    "Data Poisoning": [
        r"\bdata poisoning\b", r"\bpoison(ed|ing)? data\b", r"\bcorrupt(ed|ion)? dataset\b",
        r"\bmalicious dataset\b", r"\btainted data\b", r"\bchecksum\b", r"\bhash mismatch\b",
        r"\bunexpected labels?\b", r"\blabel corruption\b"
    ],
    "Supply Chain Poisoning": [
        r"\bsupply chain\b", r"\bdependency confusion\b", r"\bpoisoned package\b",
        r"\bmalicious package\b", r"\bcompromised dependency\b", r"\btyposquat(ting)?\b",
        r"\buntrusted dependency\b", r"\bpackage integrity\b"
    ],
    "Unauthorized Data Extraction": [
        r"\bdata exfiltration\b", r"\bunauthorized access\b", r"\bexposed data\b",
        r"\bleaked data\b", r"\bsecret(s)? exposed\b", r"\bcredential(s)? exposed\b",
        r"\btoken(s)? exposed\b", r"\bapi key\b", r"\bprivate data\b", r"\bpii\b"
    ],
    "Malicious Preprocessing Scripts": [
        r"\bexec\(", r"\beval\(", r"\bremote code\b", r"\barbitrary code execution\b",
        r"\bdynamic import\b", r"\bos\.system\b", r"\bsubprocess\b", r"\bdownload script\b",
        r"\bpreprocess(ing)? script\b"
    ],
    "Feature Manipulation": [
        r"\bfeature manipulation\b", r"\bfeature tampering\b", r"\bmalicious feature(s)?\b",
        r"\bunsafe feature engineering\b", r"\bfeature leak(age)?\b"
    ],
    "Data Leakage": [
        r"\bdata leakage\b", r"\btarget leakage\b", r"\btest data.*train(ing)?\b",
        r"\btrain(ing)? data.*test\b", r"\bsensitive attributes?\b", r"\bpii\b"
    ],
    "Model Poisoning": [
        r"\bmodel poisoning\b", r"\bpoisoned model\b", r"\bbackdoor\b",
        r"\btrojan(ed)? model\b", r"\bcompromised model weights?\b", r"\bmalicious weights?\b"
    ],
    "Hyperparameter Manipulation": [
        r"\bhyperparameter manipulation\b", r"\bconfig override\b",
        r"\boverride(d)? .*hyperparameter\b", r"\bunvalidated config\b",
        r"\benvironment variable\b", r"\bunsafe config\b"
    ],
    "Adversarial Training Data Injection": [
        r"\badversarial training data\b", r"\binjected training data\b",
        r"\bmalicious samples?\b", r"\bcrafted examples?\b", r"\badversarial samples?\b"
    ],
    "Training Pipeline Compromise": [
        r"\btraining pipeline\b", r"\btraining job compromise\b", r"\bcheckpoint tampering\b",
        r"\bunauthorized training\b", r"\bmalicious callback\b", r"\bunsafe training script\b"
    ],
    "Evaluation Data Poisoning": [
        r"\bevaluation data poisoning\b", r"\bpoisoned evaluation set\b",
        r"\bcorrupt(ed)? validation set\b", r"\btainted benchmark\b"
    ],
    "Metric Manipulation": [
        r"\bmetric manipulation\b", r"\baccuracy inflation\b",
        r"\bfilter(s|ed)? out .* before calculating\b", r"\bincorrect metric\b",
        r"\bmetric spoofing\b", r"\bbenchmark gaming\b"
    ],
    "Overfitting Exploitation": [
        r"\boverfit(ting)?\b", r"\bmemorization\b", r"\bdata memorization\b", r"\btraining leakage\b"
    ],
    "Model Tampering": [
        r"\bmodel tampering\b", r"\btampered model\b", r"\bunauthorized model change\b",
        r"\bmodified weights?\b", r"\bmodel integrity\b"
    ],
    "Model Extraction": [
        r"\bmodel extraction\b", r"\bsteal(ing)? model\b", r"\bfull probability distribution\b",
        r"\blogits exposed\b", r"\bconfidence scores exposed\b", r"\btoo much output detail\b"
    ],
    "Adversarial Input Attacks": [
        r"\badversarial input\b", r"\badversarial example\b", r"\bevasion attack\b",
        r"\bprompt injection\b", r"\bjailbreak\b", r"\bmalicious prompt\b"
    ],
    "Container Vulnerabilities": [
        r"\bcontainer vulnerabilit(y|ies)\b", r"\bdocker\b", r"\bimage vulnerability\b",
        r"\broot user\b", r"\bprivileged container\b", r"\bunsafe container\b", r"\bbase image\b"
    ],
    "Data Drift Attacks": [
        r"\bdata drift\b", r"\bconcept drift\b", r"\bdistribution shift\b", r"\bdrift detection\b"
    ],
    "Logging Manipulation": [
        r"\blog(ging)? manipulation\b", r"\blog tampering\b", r"\baudit log\b",
        r"\bmissing logs?\b", r"\blog suppression\b"
    ],
    "Alert Suppression": [
        r"\balert suppression\b", r"\balert(s)? disabled\b", r"\bmonitoring disabled\b",
        r"\bsilent failure\b", r"\bnotification suppressed\b"
    ],
}

STAGE_PATTERNS = {
    "Acquisition": [
        r"\bdataset\b", r"\bdata source\b", r"\bdownload\b", r"\bingest(ion|ed|ing)?\b",
        r"\bexternal url\b", r"\bhuggingface hub\b", r"\bs3\b", r"\bcollection\b"
    ],
    "Preparation": [
        r"\bpreprocess(ing)?\b", r"\bclean(ing)?\b", r"\btransform(ation|ed|ing)?\b",
        r"\btokeniz(er|ation)\b", r"\bfeature engineering\b", r"\blabel(s|ing)?\b",
        r"\bdata split\b", r"\bnormaliz(e|ation)\b"
    ],
    "Modeling": [
        r"\bmodel architecture\b", r"\blayer\b", r"\bembedding\b",
        r"\bquantiz(ed|ation)\b", r"\bmodel config\b", r"\bmodel design\b"
    ],
    "Training": [
        r"\btrain(ing|ed)?\b", r"\bepoch\b", r"\boptimizer\b", r"\bloss\b",
        r"\bcheckpoint\b", r"\bhyperparameter\b", r"\bfine[- ]?tuning\b", r"\bbackprop\b"
    ],
    "Evaluation": [
        r"\bevaluat(ion|e|ed)\b", r"\bvalidation\b", r"\btest set\b", r"\bmetric\b",
        r"\baccuracy\b", r"\bprecision\b", r"\brecall\b", r"\bf1\b", r"\bbenchmark\b"
    ],
    "Prediction": [
        r"\binference\b", r"\bpredict(ion|ed)?\b", r"\bserv(ing|e)\b", r"\bapi\b",
        r"\bendpoint\b", r"\bdeploy(ment|ed)?\b", r"\bruntime\b", r"\bproduction\b"
    ],
}

THREAT_TO_DEFAULT_STAGE = {
    "Data Poisoning": ["Acquisition"],
    "Supply Chain Poisoning": ["Acquisition", "Training"],
    "Unauthorized Data Extraction": ["Acquisition", "Preparation", "Prediction"],
    "Malicious Preprocessing Scripts": ["Preparation"],
    "Feature Manipulation": ["Preparation"],
    "Data Leakage": ["Preparation", "Evaluation"],
    "Model Poisoning": ["Training"],
    "Hyperparameter Manipulation": ["Training"],
    "Adversarial Training Data Injection": ["Training"],
    "Training Pipeline Compromise": ["Training"],
    "Evaluation Data Poisoning": ["Evaluation"],
    "Metric Manipulation": ["Evaluation"],
    "Overfitting Exploitation": ["Evaluation"],
    "Model Tampering": ["Modeling", "Training", "Prediction"],
    "Model Extraction": ["Prediction"],
    "Adversarial Input Attacks": ["Prediction"],
    "Container Vulnerabilities": ["Prediction"],
    "Data Drift Attacks": ["Prediction"],
    "Logging Manipulation": ["Prediction"],
    "Alert Suppression": ["Prediction"],
}

SECURITY_GENERAL_PATTERNS = [
    r"\bsecurity\b", r"\bvulnerab(ility|le)\b", r"\bcve[- ]?\d{4}-\d+\b", r"\battack\b",
    r"\bmalicious\b", r"\bunauthorized\b", r"\bsecret\b", r"\btoken\b", r"\bcredential\b",
    r"\bleak(age|ed)?\b", r"\bexploit\b", r"\btamper(ing|ed)?\b", r"\bpoison(ing|ed)?\b",
    r"\bbackdoor\b", r"\btrojan\b", r"\bprivacy\b", r"\bintegrity\b"
]

NEGATIVE_HINTS = [
    r"\btypo\b", r"\bdocumentation\b", r"\bdoc(s)?\b", r"\bquestion\b",
    r"\bhelp wanted\b", r"\bhow to\b", r"\bfeature request\b", r"\benhancement\b"
]

STRONG_SECURITY_IMPACT = [
    r"\bunauthorized\b", r"\bexfiltration\b", r"\bcredential(s)? exposed\b",
    r"\btoken(s)? exposed\b", r"\bsecret(s)? exposed\b", r"\bprivate data\b", r"\bpii\b",
    r"\bremote code execution\b", r"\barbitrary code execution\b", r"\bbackdoor\b",
    r"\btrojan\b", r"\bexploit\b", r"\bvulnerability\b", r"\bcve[- ]?\d{4}-\d+\b",
    r"\bcheckpoint tampering\b", r"\bmodel extraction\b", r"\bprompt injection\b", r"\bjailbreak\b"
]

AMBIGUOUS_NON_SECURITY = [
    r"\bmemory leak\b", r"\bleak in performance\b", r"\bperformance drift\b",
    r"\bconcept drift\b", r"\bmodel drift\b", r"\baccuracy drop\b", r"\bbenchmark issue\b",
    r"\bslow training\b", r"\btraining unstable\b", r"\bbug\b", r"\bfeature request\b",
    r"\benhancement\b", r"\bdocumentation\b", r"\bhow to\b", r"\bquestion\b"
]

MALICIOUS_INTENT_PATTERNS = [
    r"\battack\b", r"\bmalicious\b", r"\badversarial\b", r"\bpoison(ing|ed)?\b",
    r"\btamper(ing|ed)?\b", r"\bunauthorized\b", r"\bcompromised\b", r"\bexploit\b",
    r"\bsteal(ing)?\b", r"\bexposed\b"
]

NORMAL_BUG_PATTERNS = [
    r"\bcrash\b", r"\bexception\b", r"\berror\b", r"\bincorrect output\b",
    r"\bfailed\b", r"\btimeout\b", r"\bslow\b", r"\bnot working\b"
]

COLUMN_ALIASES = {
    "repo": ["repo", "repository", "full_name"],
    "issue_number": ["issue_number", "number", "id"],
    "title": ["title", "issue_title"],
    "labels": ["labels", "label_names"],
    "issue_body": ["issue_body", "body", "description"],
    "comments_text": ["comments_text", "comments", "issue_comments"],
    "url": ["url", "html_url", "issue_url"],
}


# ============================================================
# Helpers
# ============================================================

def normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", text).strip()


def compile_patterns(pattern_map):
    return {k: [re.compile(p, re.IGNORECASE) for p in pats] for k, pats in pattern_map.items()}


COMPILED_THREATS = compile_patterns(THREAT_PATTERNS)
COMPILED_STAGES = compile_patterns(STAGE_PATTERNS)
COMPILED_SECURITY = [re.compile(p, re.IGNORECASE) for p in SECURITY_GENERAL_PATTERNS]
COMPILED_NEGATIVE = [re.compile(p, re.IGNORECASE) for p in NEGATIVE_HINTS]
COMPILED_STRONG = [re.compile(p, re.IGNORECASE) for p in STRONG_SECURITY_IMPACT]
COMPILED_AMBIG = [re.compile(p, re.IGNORECASE) for p in AMBIGUOUS_NON_SECURITY]
COMPILED_INTENT = [re.compile(p, re.IGNORECASE) for p in MALICIOUS_INTENT_PATTERNS]
COMPILED_BUG = [re.compile(p, re.IGNORECASE) for p in NORMAL_BUG_PATTERNS]


def find_column(df, logical_name):
    aliases = COLUMN_ALIASES[logical_name]
    lower_to_real = {str(c).strip().lower(): c for c in df.columns}
    for alias in aliases:
        if alias.lower() in lower_to_real:
            return lower_to_real[alias.lower()]
    return None


def excerpt(text, match, window=80):
    start = max(0, match.start() - window)
    end = min(len(text), match.end() + window)
    return text[start:end].strip()[:250]


def unique_preserve_order(items):
    seen, out = set(), []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


# ============================================================
# Pass 1: broad security identification
# ============================================================

def classify_security_issue(text, min_yes_score=4, min_review_score=2):
    threat_hits = []
    for threat, patterns in COMPILED_THREATS.items():
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                threat_hits.append((threat, excerpt(text, match)))
                break

    general_hits = []
    for pattern in COMPILED_SECURITY:
        match = pattern.search(text)
        if match:
            general_hits.append(excerpt(text, match))

    negative_hits = []
    for pattern in COMPILED_NEGATIVE:
        match = pattern.search(text)
        if match:
            negative_hits.append(excerpt(text, match))

    score = 0
    score += 3 * len(threat_hits)
    score += min(len(general_hits), 3)
    score -= min(len(negative_hits), 2)

    if threat_hits and score >= min_yes_score:
        flag = "Yes"
    elif threat_hits or (general_hits and score >= min_review_score):
        flag = "Review"
    else:
        flag = "No"

    evidence = [f"{threat}: '{snippet}'" for threat, snippet in threat_hits]
    signals = [threat for threat, _ in threat_hits]

    if threat_hits:
        reason = (
            "Identified as security-related because the issue text matches threat indicators: "
            + ", ".join(signals[:5])
            + "."
        )
    elif general_hits:
        reason = (
            "Marked for review because the issue contains general security signals but lacks strong "
            "threat-specific evidence."
        )
    elif negative_hits:
        reason = "Likely non-security because the issue is dominated by documentation/help/enhancement signals."
    else:
        reason = "No strong security-related evidence found."

    return {
        "security_threat_flag": flag,
        "security_signals": "; ".join(unique_preserve_order(signals)),
        "security_score": score,
        "security_evidence": " | ".join(unique_preserve_order(evidence))[:4000],
        "threat_reason": reason,
        "matched_threats": unique_preserve_order(signals),
    }


# ============================================================
# Pass 2: resolve review issues
# ============================================================

def resolve_review_issue(text):
    strong_hits, ambiguous_hits, intent_hits, bug_hits = [], [], [], []

    for rgx in COMPILED_STRONG:
        m = rgx.search(text)
        if m:
            strong_hits.append(excerpt(text, m))

    for rgx in COMPILED_AMBIG:
        m = rgx.search(text)
        if m:
            ambiguous_hits.append(excerpt(text, m))

    for rgx in COMPILED_INTENT:
        m = rgx.search(text)
        if m:
            intent_hits.append(excerpt(text, m))

    for rgx in COMPILED_BUG:
        m = rgx.search(text)
        if m:
            bug_hits.append(excerpt(text, m))

    score = 0
    score += 3 * len(strong_hits)
    score += 2 * len(intent_hits)
    score -= 2 * len(ambiguous_hits)
    score -= 1 * len(bug_hits)

    if len(strong_hits) >= 1 and len(intent_hits) >= 1:
        final_label = "Yes"
        reason = "Escalated from Review to Yes because the issue contains both strong security-impact evidence and malicious/security intent."
    elif len(strong_hits) >= 2:
        final_label = "Yes"
        reason = "Escalated from Review to Yes because multiple strong security indicators were found."
    elif len(ambiguous_hits) >= 1 and len(strong_hits) == 0:
        final_label = "No"
        reason = "Downgraded from Review to No because the issue appears operational/ambiguous rather than security-related."
    elif score >= 4:
        final_label = "Yes"
        reason = "Escalated from Review to Yes based on cumulative second-pass evidence."
    elif score <= 0:
        final_label = "No"
        reason = "Downgraded from Review to No because the second-pass evidence is too weak or non-security."
    else:
        final_label = "Needs Manual Review"
        reason = "Still ambiguous after second-pass analysis."

    evidence_parts = []
    if strong_hits:
        evidence_parts.append("Strong security evidence: " + " | ".join(strong_hits[:3]))
    if intent_hits:
        evidence_parts.append("Intent evidence: " + " | ".join(intent_hits[:3]))
    if ambiguous_hits:
        evidence_parts.append("Ambiguity evidence: " + " | ".join(ambiguous_hits[:3]))
    if bug_hits:
        evidence_parts.append("Bug/operational evidence: " + " | ".join(bug_hits[:3]))

    return {
        "review_resolution": final_label,
        "review_resolution_score": score,
        "review_resolution_reason": reason,
        "review_resolution_evidence": " || ".join(evidence_parts)[:4000],
    }


def derive_final_label(row):
    first_pass = row.get("security_threat_flag", "")
    second_pass = row.get("review_resolution", "")

    if first_pass == "Yes":
        return "Yes"
    if first_pass == "No":
        return "No"
    if first_pass == "Review":
        if second_pass in ["Yes", "No"]:
            return second_pass
        return "Needs Manual Review"
    return "No"


# ============================================================
# Stage tagging and traceability
# ============================================================

def infer_stage_with_traceability(text, matched_threats):
    stage_scores = Counter()
    stage_evidence = {}

    for stage, patterns in COMPILED_STAGES.items():
        for pattern in patterns:
            m = pattern.search(text)
            if m:
                stage_scores[stage] += 2
                stage_evidence.setdefault(stage, excerpt(text, m))
                break

    for threat in matched_threats:
        for stage in THREAT_TO_DEFAULT_STAGE.get(threat, []):
            stage_scores[stage] += 1

    if not stage_scores:
        return [], ""

    max_score = max(stage_scores.values())
    chosen = [s for s, v in stage_scores.items() if v == max_score]
    chosen = sorted(chosen, key=lambda x: STAGES.index(x))

    justifications = []
    for stage in chosen:
        reasons = []
        related_threats = [t for t in matched_threats if stage in THREAT_TO_DEFAULT_STAGE.get(t, [])]
        if related_threats:
            reasons.append(
                f"mapped here because threat(s) {', '.join(related_threats)} typically manifest in this stage"
            )
        if stage in stage_evidence:
            reasons.append(f"matched stage-specific language '{stage_evidence[stage]}'")

        if reasons:
            justifications.append(f"{stage}: " + "; ".join(reasons) + ".")
        else:
            justifications.append(f"{stage}: assigned by highest stage score from available indicators.")

    return chosen, " ".join(justifications)


# ============================================================
# I/O
# ============================================================

def sanitize_for_excel(df):
    illegal = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].apply(lambda x: illegal.sub("", x) if isinstance(x, str) else x)
    return out


def read_input(input_path, sheet_name=None):
    suffix = Path(input_path).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(input_path)
    if suffix in [".xlsx", ".xlsm", ".xls"]:
        if sheet_name:
            return pd.read_excel(input_path, sheet_name=sheet_name)
        xls = pd.ExcelFile(input_path)
        if "Issues" in xls.sheet_names:
            return pd.read_excel(input_path, sheet_name="Issues")
        return pd.read_excel(input_path)
    raise ValueError("Unsupported input format. Use CSV or Excel.")


def build_summary(df):
    final_counts = df["final_security_label"].fillna("No").value_counts().to_dict()
    first_pass_counts = df["security_threat_flag"].fillna("No").value_counts().to_dict()

    stage_counter = Counter()
    flagged_mask = df["final_security_label"].isin(["Yes", "Needs Manual Review"])
    for value in df.loc[flagged_mask, "mlops_stage_tags"].fillna(""):
        for stage in [x.strip() for x in str(value).split(",") if x.strip()]:
            stage_counter[stage] += 1

    rows = [
        ["Metric", "Value"],
        ["Total issues", len(df)],
        ["First pass - Yes", first_pass_counts.get("Yes", 0)],
        ["First pass - Review", first_pass_counts.get("Review", 0)],
        ["First pass - No", first_pass_counts.get("No", 0)],
        ["Final - Yes", final_counts.get("Yes", 0)],
        ["Final - Needs Manual Review", final_counts.get("Needs Manual Review", 0)],
        ["Final - No", final_counts.get("No", 0)],
        ["Flagged Acquisition", stage_counter.get("Acquisition", 0)],
        ["Flagged Preparation", stage_counter.get("Preparation", 0)],
        ["Flagged Modeling", stage_counter.get("Modeling", 0)],
        ["Flagged Training", stage_counter.get("Training", 0)],
        ["Flagged Evaluation", stage_counter.get("Evaluation", 0)],
        ["Flagged Prediction", stage_counter.get("Prediction", 0)],
    ]
    return pd.DataFrame(rows, columns=["Metric", "Value"])


def build_method_sheet():
    rows = [
        ["Field", "Description"],
        ["security_threat_flag", "First-pass broad classification: Yes / Review / No."],
        ["review_resolution", "Second-pass resolution for only Review rows."],
        ["final_security_label", "Final resolved label: Yes / No / Needs Manual Review."],
        ["security_evidence", "Threat evidence snippets from title/body/comments/labels."],
        ["threat_reason", "Reason why issue was tagged as security-related."],
        ["mlops_stage_tags", "Assigned MLOps stage(s) for final flagged issues."],
        ["stage_justification", "Why the stage was assigned."],
        ["Important note", "This is a heuristic pipeline; Needs Manual Review rows should be validated by a human."],
    ]
    return pd.DataFrame(rows, columns=["Field", "Description"])


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input Excel or CSV issue extract")
    parser.add_argument("--output", required=True, help="Output Excel workbook")
    parser.add_argument("--sheet", default=None, help="Optional sheet name")
    parser.add_argument("--min-yes-score", type=int, default=4)
    parser.add_argument("--min-review-score", type=int, default=2)
    args = parser.parse_args()

    df = read_input(args.input, args.sheet)

    title_col = find_column(df, "title")
    body_col = find_column(df, "issue_body")
    labels_col = find_column(df, "labels")
    comments_col = find_column(df, "comments_text")

    missing = [name for name, col in [("title", title_col), ("issue_body", body_col)] if col is None]
    if missing:
        raise ValueError(
            f"Missing required input column(s): {missing}. Found columns: {list(df.columns)}"
        )

    df["_combined_text"] = (
        df[title_col].apply(normalize_text)
        + " | labels: " + (df[labels_col].apply(normalize_text) if labels_col else "")
        + " | body: " + df[body_col].apply(normalize_text)
        + " | comments: " + (df[comments_col].apply(normalize_text) if comments_col else "")
    )

    # First pass
    pass1 = df["_combined_text"].apply(
        lambda text: classify_security_issue(
            text,
            min_yes_score=args.min_yes_score,
            min_review_score=args.min_review_score,
        )
    )

    df["security_threat_flag"] = pass1.apply(lambda x: x["security_threat_flag"])
    df["security_signals"] = pass1.apply(lambda x: x["security_signals"])
    df["security_score"] = pass1.apply(lambda x: x["security_score"])
    df["security_evidence"] = pass1.apply(lambda x: x["security_evidence"])
    df["threat_reason"] = pass1.apply(lambda x: x["threat_reason"])

    # Second pass only for Review
    df["review_resolution"] = ""
    df["review_resolution_score"] = ""
    df["review_resolution_reason"] = ""
    df["review_resolution_evidence"] = ""

    review_mask = df["security_threat_flag"] == "Review"
    if review_mask.any():
        pass2 = df.loc[review_mask, "_combined_text"].apply(resolve_review_issue)
        df.loc[review_mask, "review_resolution"] = pass2.apply(lambda x: x["review_resolution"])
        df.loc[review_mask, "review_resolution_score"] = pass2.apply(lambda x: x["review_resolution_score"])
        df.loc[review_mask, "review_resolution_reason"] = pass2.apply(lambda x: x["review_resolution_reason"])
        df.loc[review_mask, "review_resolution_evidence"] = pass2.apply(lambda x: x["review_resolution_evidence"])

    df["final_security_label"] = df.apply(derive_final_label, axis=1)

    # Stage tagging only for final flagged rows
    df["mlops_stage_tags"] = ""
    df["stage_justification"] = ""

    final_flag_mask = df["final_security_label"].isin(["Yes", "Needs Manual Review"])
    for idx in df.index[final_flag_mask]:
        matched_threats = pass1.iloc[idx]["matched_threats"]
        stages, just = infer_stage_with_traceability(df.at[idx, "_combined_text"], matched_threats)
        df.at[idx, "mlops_stage_tags"] = ", ".join(stages)
        df.at[idx, "stage_justification"] = just

    df = df.drop(columns=["_combined_text"])

    security_issues_df = df[df["final_security_label"].isin(["Yes", "Needs Manual Review"])].copy()
    review_resolved_df = df[df["security_threat_flag"] == "Review"].copy()
    summary_df = build_summary(df)
    method_df = build_method_sheet()

    df = sanitize_for_excel(df)
    security_issues_df = sanitize_for_excel(security_issues_df)
    review_resolved_df = sanitize_for_excel(review_resolved_df)
    summary_df = sanitize_for_excel(summary_df)
    method_df = sanitize_for_excel(method_df)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        df.to_excel(writer, sheet_name="Issues", index=False)
        security_issues_df.to_excel(writer, sheet_name="Security_Issues", index=False)
        review_resolved_df.to_excel(writer, sheet_name="Review_Resolution", index=False)
        method_df.to_excel(writer, sheet_name="Method", index=False)

    print(f"Saved output to: {out}")


if __name__ == "__main__":
    main()
