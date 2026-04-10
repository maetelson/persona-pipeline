"""Deterministic repair and per-axis detail builders for label quality."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.utils.pipeline_schema import LABEL_CODE_COLUMNS, is_unknown_like, split_pipe_codes
from src.utils.record_access import get_record_text

AXIS_TEXT_FIELDS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
]


def apply_label_repairs(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    labelability_df: pd.DataFrame,
    policy: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Repair broad labels only when the row has enough signal."""
    if labeled_df.empty:
        return labeled_df.copy(), pd.DataFrame()
    merged = labeled_df.merge(
        episodes_df[["episode_id", "source", *[column for column in AXIS_TEXT_FIELDS if column in episodes_df.columns]]],
        on="episode_id",
        how="left",
    ).merge(
        labelability_df[["episode_id", "labelability_status", "labelability_score"]],
        on="episode_id",
        how="left",
    )
    min_status = str(((policy.get("repair", {}) or {}).get("min_labelability_for_repair", "borderline")) or "borderline")
    allowed_statuses = {"labelable", "borderline"} if min_status == "borderline" else {"labelable"}
    repair_cfg = dict((policy.get("repair", {}) or {}).get("broad_role_rules", {}) or {})

    result = labeled_df.copy()
    repair_rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        episode_id = str(row.get("episode_id", ""))
        if str(row.get("labelability_status", "low_signal")) not in allowed_statuses:
            continue
        combined = get_record_text(row, fields=AXIS_TEXT_FIELDS).lower()
        notes: list[str] = []

        if is_unknown_like(row.get("role_codes", "unknown")):
            repaired_role = _repair_role(combined, repair_cfg)
            if repaired_role:
                result.loc[result["episode_id"] == episode_id, "role_codes"] = repaired_role
                notes.append(f"role_codes={repaired_role}")

        if is_unknown_like(row.get("output_codes", "unknown")):
            repaired_output = _repair_output(combined)
            if repaired_output:
                result.loc[result["episode_id"] == episode_id, "output_codes"] = repaired_output
                notes.append(f"output_codes={repaired_output}")

        if is_unknown_like(row.get("pain_codes", "unknown")):
            repaired_pain = _repair_pain(combined)
            if repaired_pain:
                result.loc[result["episode_id"] == episode_id, "pain_codes"] = repaired_pain
                notes.append(f"pain_codes={repaired_pain}")

        if is_unknown_like(row.get("question_codes", "unknown")):
            repaired_question = _repair_question(combined)
            if repaired_question:
                result.loc[result["episode_id"] == episode_id, "question_codes"] = repaired_question
                notes.append(f"question_codes={repaired_question}")

        if notes:
            current_reason = str(result.loc[result["episode_id"] == episode_id, "label_reason"].iloc[0] or "")
            result.loc[result["episode_id"] == episode_id, "label_reason"] = f"{current_reason} | repair<{'; '.join(notes)}>"
            result.loc[result["episode_id"] == episode_id, "label_confidence"] = result.loc[
                result["episode_id"] == episode_id, "label_confidence"
            ].astype(float).clip(lower=0.68)
            repair_rows.append(
                {
                    "episode_id": episode_id,
                    "source": str(row.get("source", "")),
                    "labelability_status": str(row.get("labelability_status", "")),
                    "repair_notes": " ; ".join(notes),
                }
            )
    return result, pd.DataFrame(repair_rows)


def build_axis_label_details(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    labelability_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create one row per axis label with confidence, evidence, and unknown cause."""
    merged = labeled_df.merge(episodes_df, on="episode_id", how="left").merge(labelability_df, on=["episode_id", "source"], how="left")
    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        text = get_record_text(row, fields=AXIS_TEXT_FIELDS).lower()
        for family in LABEL_CODE_COLUMNS:
            raw_value = str(row.get(family, "unknown") or "unknown")
            labels = split_pipe_codes(raw_value)
            known = labels if labels else []
            if known:
                evidence = _build_evidence(text, known)
                axis_confidence = _axis_confidence(row, family, evidence)
                unknown_reason = ""
                predicted_label = "|".join(known)
            else:
                evidence = ""
                axis_confidence = 0.0
                unknown_reason = _infer_unknown_reason(row, family, text)
                predicted_label = "unknown"
            rows.append(
                {
                    "episode_id": str(row.get("episode_id", "")),
                    "source": str(row.get("source", "")),
                    "axis_name": family,
                    "predicted_label": predicted_label,
                    "confidence_score": round(axis_confidence, 3),
                    "evidence_span": evidence,
                    "unknown_reason": unknown_reason,
                    "labelability_status": str(row.get("labelability_status", "")),
                }
            )
    return pd.DataFrame(rows)


def _repair_role(text: str, repair_cfg: dict[str, list[str]]) -> str:
    for label, terms in repair_cfg.items():
        if any(term.lower() in text for term in terms):
            mapping = {
                "analyst": "R_ANALYST",
                "manager": "R_MANAGER",
                "marketer": "R_MARKETER",
            }
            return mapping.get(label, "")
    return ""


def _repair_output(text: str) -> str:
    if any(term in text for term in ["excel", "spreadsheet", "board report", "business review", "presentation-ready", "presentation ready"]):
        return "O_XLSX"
    if any(
        term in text
        for term in [
            "dashboard",
            "monitoring",
            "chart",
            "impressions",
            "clicks",
            "campaigns",
            "advertising account",
            "products section",
            "product status",
            "diagnostics",
            "free listings",
            "advertiser verification",
            "verification failure",
            "manual reviews",
            "no physical stores found",
            "countries of sale",
            "profile filter",
        ]
    ):
        return "O_DASHBOARD"
    if any(
        term in text
        for term in [
            "reconciled dataset",
            "validated dataset",
            "source of truth",
            "analytics discrepancy",
            "wrong analytics data",
            "price mismatch",
            "shipping mismatch",
            "feed sync",
            "products not syncing",
            "sessions not tracking",
            "inventory feed",
            "data feed",
            "incorrect pricing",
            "duplicate listings",
            "products visible",
            "tracking",
            "third party tracking",
            "lead tracking",
            "manual reviews",
            "no specific feedback",
            "verification failure",
        ]
    ):
        return "O_VALIDATED_DATASET"
    if any(term in text for term in ["scheduled", "automate", "automation", "template", "flow", "flows", "campaign not sending"]):
        return "O_AUTOMATION_JOB"
    return ""


def _repair_pain(text: str) -> str:
    if any(term in text for term in ["manual reporting", "export to excel", "spreadsheet", "copy paste", "every week"]):
        return "P_MANUAL_REPORTING"
    if any(
        term in text
        for term in [
            "numbers don't match",
            "source of truth",
            "reconcile",
            "definition",
            "metric mismatch",
            "analytics discrepancy",
            "wrong analytics data",
            "price mismatch",
            "shipping mismatch",
            "feed sync",
            "products not syncing",
            "sessions not tracking",
            "incorrect pricing",
            "duplicate listings",
            "inventory feed",
            "data feed",
            "live visitors",
            "incorrect data",
            "third party tracking",
        ]
    ):
        return "P_DATA_QUALITY"
    if any(
        term in text
        for term in [
            "not enough",
            "tool limitation",
            "can't drill",
            "cannot explain",
            "calc issue",
            "not working",
            "not showing",
            "not serving",
            "not sending",
            "not recording",
            "disapproved",
            "misrepresentation",
            "suspension",
            "manual review",
            "store not approved",
            "limited by budget",
            "deliverability",
            "advertiser verification failure",
            "no impressions",
            "not spending",
            "not approved",
            "products not being approved",
            "free listings",
            "products from showing",
            "no physical stores",
            "profile filter",
            "added to cart",
            "no physical stores found",
            "countries of sale",
        ]
    ):
        return "P_TOOL_LIMITATION"
    if any(term in text for term in ["stakeholders keep asking", "follow-up", "follow up", "leadership wants", "handoff"]):
        return "P_HANDOFF"
    return ""


def _repair_question(text: str) -> str:
    if any(
        term in text
        for term in [
            "manual reporting",
            "every week",
            "presentation-ready",
            "report for leadership",
            "monthly report",
            "sales report",
            "form submissions report",
            "revenue target report",
        ]
    ):
        return "Q_REPORT_SPEED"
    if any(
        term in text
        for term in [
            "numbers don't match",
            "reconcile",
            "source of truth",
            "metric definition",
            "analytics discrepancy",
            "wrong analytics data",
            "price mismatch",
            "shipping mismatch",
            "sessions not tracking",
        ]
    ):
        return "Q_VALIDATE_NUMBERS"
    if any(
        term in text
        for term in [
            "why did",
            "can't explain",
            "root cause",
            "segment",
            "channel",
            "device",
            "conversion rate",
            "sales have dropped",
            "open rate",
            "no impressions",
            "not spending",
            "not serving",
            "not showing",
            "disapproved",
            "misrepresentation",
            "account suspended",
            "manual review",
            "store not approved",
            "deliverability",
            "advertiser verification failure",
            "single impression",
            "no products visible",
            "not approved",
            "products not being approved",
            "free listings",
            "limited",
            "products from showing",
            "no physical stores",
            "incorrect pricing",
            "duplicate listings",
            "live visitors",
            "third party tracking",
            "profile filter",
            "added to cart",
            "no physical stores found",
            "countries of sale",
            "manual reviews",
            "no specific feedback",
        ]
    ):
        return "Q_DIAGNOSE_ISSUE"
    if any(term in text for term in ["automate", "scheduled", "template away", "repeated steps", "flow", "flows", "sync", "not syncing", "not sending"]):
        return "Q_AUTOMATE_WORKFLOW"
    return ""


def _build_evidence(text: str, labels: list[str]) -> str:
    if not labels:
        return ""
    tokens = {
        "R_ANALYST": ["analyst", "report owner", "dashboard owner"],
        "R_MANAGER": ["leadership", "stakeholder", "executive"],
        "R_MARKETER": ["campaign", "channel", "attribution", "marketing"],
        "Q_REPORT_SPEED": ["report", "weekly", "monthly", "export"],
        "Q_VALIDATE_NUMBERS": ["reconcile", "trust", "source of truth", "numbers don't match"],
        "Q_DIAGNOSE_ISSUE": ["why did", "explain", "segment", "channel", "device"],
        "Q_AUTOMATE_WORKFLOW": ["automate", "scheduled", "template"],
        "P_MANUAL_REPORTING": ["manual", "spreadsheet", "copy paste", "excel"],
        "P_DATA_QUALITY": ["mismatch", "reconcile", "definition", "source of truth"],
        "P_TOOL_LIMITATION": ["not enough", "can't explain", "drill down", "limitation"],
        "P_HANDOFF": ["stakeholder", "follow-up", "follow up", "leadership"],
        "O_XLSX": ["excel", "spreadsheet", "presentation-ready", "board report"],
        "O_DASHBOARD": ["dashboard", "chart", "monitoring"],
        "O_VALIDATED_DATASET": ["validated", "reconciled dataset", "source of truth"],
        "O_AUTOMATION_JOB": ["scheduled", "automate", "automation"],
    }
    hits: list[str] = []
    for label in labels:
        for token in tokens.get(label, []):
            if token in text and token not in hits:
                hits.append(token)
    return ", ".join(hits[:4])


def _axis_confidence(row: pd.Series, family: str, evidence: str) -> float:
    base = float(row.get("label_confidence", 0.0) or 0.0)
    if not evidence:
        return max(base - 0.15, 0.35)
    if "|" in str(row.get(family, "")):
        return max(base - 0.08, 0.45)
    return max(base, 0.55)


def _infer_unknown_reason(row: pd.Series, family: str, text: str) -> str:
    if str(row.get("labelability_status", "")) == "low_signal":
        return "low_relevance_input"
    if len(text.strip()) < 50:
        return "truly_no_evidence"
    if family == "role_codes" and any(term in text for term in ["campaign", "stakeholder", "analyst", "leadership"]):
        return "conflicting_evidence"
    if family in {"pain_codes", "question_codes"} and any(term in text for term in ["problem", "issue", "workflow"]):
        return "taxonomy_gap"
    return "truly_no_evidence"
