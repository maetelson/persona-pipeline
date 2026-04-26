"""Diagnostic-only boundary analysis for persona_05 evidence quality."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.source_tiers import annotate_source_tiers


ROOT_BOUNDARY_DIAGNOSTIC_CSV = "artifacts/readiness/persona05_boundary_diagnostic.csv"
ROOT_BOUNDARY_DIAGNOSTIC_SUMMARY_JSON = "artifacts/readiness/persona05_boundary_diagnostic_summary.json"
ROOT_BOUNDARY_RULE_REPORT_JSON = "artifacts/readiness/persona05_boundary_rule_report.json"
ROOT_BOUNDARY_RULE_REPORT_MD = "docs/operational/PERSONA05_BOUNDARY_RULE_REPORT.md"

BOUNDARY_COLUMNS = [
    "persona05_boundary_status",
    "persona05_boundary_reason",
    "persona05_positive_signal_count",
    "persona05_negative_signal_count",
    "persona05_reporting_delivery_context",
    "persona05_output_construction_blocker",
    "persona05_boundary_confidence",
]

SUMMARY_COLUMNS = [
    "persona05_clean_evidence_count",
    "persona05_overlap_risk_count",
    "persona05_support_noise_count",
    "persona05_boundary_readiness",
    "persona05_boundary_rule_status",
]

PERSONA_05_ID = "persona_05"

DELIVERY_CONTEXT_TERMS = [
    "end users",
    "stakeholder",
    "shareable",
    "shared dashboard",
    "consumer",
    "consumer usability",
    "report consumer",
    "target tracker",
    "target",
    "monthly",
    "quarterly",
    "board",
    "presentation",
    "presentable",
    "usability",
    "experience for our end users",
    "where are we at vs target",
]

REPORTING_SURFACE_TERMS = [
    "dashboard",
    "report",
    "reporting",
    "table",
    "pivot",
    "view",
    "visual",
    "chart",
    "widget",
    "layout",
    "trend",
    "series",
    "goal",
]

DELIVERY_PRESSURE_TERMS = [
    "presentation",
    "presentable",
    "share",
    "shared",
    "end user",
    "usability",
    "experience for our end users",
    "where are we at vs target",
    "trend",
]

OUTPUT_SURFACE_TERMS = [
    "dashboard",
    "filter",
    "table",
    "pivot",
    "visual",
    "chart",
    "widget",
    "layout",
    "rows",
    "columns",
    "series",
    "dimensions",
    "measures",
    "trend",
]

BLOCKER_TERMS = [
    "can't",
    "cannot",
    "unable",
    "not possible",
    "is there any way",
    "doesn't work",
    "do not work",
    "wrong position",
    "incorrect place",
    "blank",
    "indistinguishable",
    "confusing",
    "can't move",
    "can't add",
    "can't configure",
    "can't distinguish",
    "preventing me",
    "in the way",
    "produce different query results",
]

EXPORT_OVERLAP_TERMS = [
    "export",
    "excel",
    "csv",
    "spreadsheet",
    "google sheets",
    "copy paste",
    "power query",
    "vlookup",
    "reconcile",
]

SUPPORT_NOISE_TERMS = [
    "service principal",
    "gateway",
    "permission",
    "permissions",
    "auth",
    "oauth",
    "login",
    "connector",
    "refresh credentials",
    "api",
    "sdk",
    "sandbox",
    "support email",
    "trial",
    "upgrade",
    "onboarding checklist",
    "terraform",
]

WEAK_GENERIC_TERMS = [
    "best practices",
    "any advice",
    "beginner",
    "free trial",
    "what part of this workflow should be automated first",
    "how can we diagnose and resolve analytics issues faster",
]


def build_persona05_boundary_outputs(
    persona_assignments_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    persona_summary_df: pd.DataFrame,
    cluster_stats_df: pd.DataFrame,
) -> dict[str, Any]:
    """Build persona_05 boundary diagnostics without changing persona assignment."""
    diagnostic_df = _build_persona05_boundary_diagnostic_df(persona_assignments_df, episodes_df)
    summary = _build_persona05_boundary_summary(diagnostic_df)
    persona_summary_with_boundary = _merge_boundary_summary(persona_summary_df, summary)
    cluster_stats_with_boundary = _merge_boundary_summary(cluster_stats_df, summary)
    report = {
        "persona_id": PERSONA_05_ID,
        "total_persona_05_rows_evaluated": int(summary["total_persona_05_rows_evaluated"]),
        "clean_persona05_count": int(summary["clean_persona05_count"]),
        "persona01_overlap_count": int(summary["persona01_overlap_count"]),
        "persona03_overlap_count": int(summary["persona03_overlap_count"]),
        "support_troubleshooting_noise_count": int(summary["support_troubleshooting_noise_count"]),
        "weak_generic_count": int(summary["weak_generic_count"]),
        "ambiguous_count": int(summary["ambiguous_count"]),
        "clean_evidence_share": float(summary["clean_evidence_share"]),
        "persona05_boundary_readiness": str(summary["persona05_boundary_readiness"]),
        "persona05_boundary_rule_status": str(summary["persona05_boundary_rule_status"]),
        "next_action_recommendation": (
            "persona_05_claim_eligibility_recheck"
            if str(summary["persona05_boundary_readiness"]) == "pass"
            else "keep_persona_05_blocked_and_use_boundary_diagnostics_for_future_curation"
            if str(summary["persona05_boundary_readiness"]) == "borderline"
            else "merge_or_suppression_review"
        ),
    }
    return {
        "diagnostic_df": diagnostic_df,
        "summary": summary,
        "report": report,
        "persona_summary_df": persona_summary_with_boundary,
        "cluster_stats_df": cluster_stats_with_boundary,
    }


def write_persona05_boundary_artifacts(
    root_dir: Path,
    diagnostic_df: pd.DataFrame,
    summary: dict[str, Any],
    report: dict[str, Any],
) -> dict[str, Path]:
    """Write persona_05 boundary diagnostics and reviewer-facing report artifacts."""
    csv_path = root_dir / ROOT_BOUNDARY_DIAGNOSTIC_CSV
    summary_path = root_dir / ROOT_BOUNDARY_DIAGNOSTIC_SUMMARY_JSON
    report_json_path = root_dir / ROOT_BOUNDARY_RULE_REPORT_JSON
    report_md_path = root_dir / ROOT_BOUNDARY_RULE_REPORT_MD
    for path in [csv_path, summary_path, report_json_path, report_md_path]:
        path.parent.mkdir(parents=True, exist_ok=True)
    diagnostic_df.to_csv(csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    report_json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    report_md_path.write_text(_report_markdown(report), encoding="utf-8")
    return {
        "persona05_boundary_diagnostic_csv": csv_path,
        "persona05_boundary_diagnostic_summary_json": summary_path,
        "persona05_boundary_rule_report_json": report_json_path,
        "persona05_boundary_rule_report_md": report_md_path,
    }


def _build_persona05_boundary_diagnostic_df(persona_assignments_df: pd.DataFrame, episodes_df: pd.DataFrame) -> pd.DataFrame:
    """Attach diagnostic boundary labels to persona_05 rows only."""
    if persona_assignments_df.empty:
        return pd.DataFrame(columns=["episode_id", "persona_id", "source", "source_tier", *BOUNDARY_COLUMNS])
    required_episode_columns = [
        column
        for column in ["episode_id", "source", "normalized_episode", "business_question", "bottleneck_text", "desired_output"]
        if column in episodes_df.columns
    ]
    episode_lookup = annotate_source_tiers(
        episodes_df[required_episode_columns].drop_duplicates(subset=["episode_id"], keep="first").copy()
        if required_episode_columns
        else pd.DataFrame(columns=["episode_id", "source"])
    )
    merged = persona_assignments_df.merge(episode_lookup, on="episode_id", how="left")
    merged = merged[merged.get("persona_id", pd.Series(dtype=str)).astype(str).eq(PERSONA_05_ID)].copy()
    if merged.empty:
        frame = merged.copy()
        for column in BOUNDARY_COLUMNS:
            frame[column] = pd.Series(dtype=object)
        return frame
    annotations = merged.apply(_boundary_annotation, axis=1, result_type="expand")
    return pd.concat([merged.reset_index(drop=True), annotations.reset_index(drop=True)], axis=1)


def _boundary_annotation(row: pd.Series) -> pd.Series:
    """Classify one persona_05 row for boundary-only diagnostic purposes."""
    text = _combined_text(row)
    positive_hits = _unique_hits(text, DELIVERY_CONTEXT_TERMS + DELIVERY_PRESSURE_TERMS + OUTPUT_SURFACE_TERMS + BLOCKER_TERMS)
    support_hits = _unique_hits(text, SUPPORT_NOISE_TERMS)
    export_hits = _unique_hits(text, EXPORT_OVERLAP_TERMS)
    weak_hits = _unique_hits(text, WEAK_GENERIC_TERMS)
    reporting_delivery_context = _has_reporting_delivery_context(text)
    output_construction_blocker = _has_output_construction_blocker(text)
    positive_signal_count = int(reporting_delivery_context) + int(output_construction_blocker) + int(bool(_unique_hits(text, DELIVERY_PRESSURE_TERMS)))
    negative_signal_count = int(bool(support_hits)) + int(bool(export_hits)) + int(bool(weak_hits))

    if support_hits and not (reporting_delivery_context and output_construction_blocker):
        status = "support_troubleshooting_noise"
        reason = "Support or troubleshooting markers dominate without enough reporting-delivery context."
    elif reporting_delivery_context and output_construction_blocker and not support_hits:
        if export_hits and not _has_specific_output_terms(text):
            status = "persona01_overlap"
            reason = "Manual reporting and export-style overlap are stronger than final-output construction specificity."
        elif _is_generic_tool_limitation(text) and not _has_delivery_pressure_or_consumer_context(text):
            status = "persona03_overlap"
            reason = "Tool limitation is clear, but delivery-specific reporting context is still too weak."
        else:
            status = "clean_persona05"
            reason = "Row shows both reporting-delivery context and a concrete last-mile output-construction blocker."
    elif reporting_delivery_context and export_hits:
        status = "persona01_overlap"
        reason = "Row looks closer to recurring manual reporting or export burden than last-mile output construction."
    elif output_construction_blocker and not reporting_delivery_context:
        status = "persona03_overlap"
        reason = "Row shows tool limitation but not enough stakeholder-facing reporting delivery context."
    elif _is_generic_tool_limitation(text) and not reporting_delivery_context:
        status = "persona03_overlap"
        reason = "Row reads like generic tool limitation or feature friction without enough delivery-specific reporting context."
    elif support_hits:
        status = "support_troubleshooting_noise"
        reason = "Support or setup language dominates the row."
    elif weak_hits or (positive_signal_count == 0 and negative_signal_count > 0):
        status = "weak_generic"
        reason = "Row is too generic or low-context to support persona_05 claim wording."
    else:
        status = "ambiguous"
        reason = "Row has mixed or incomplete evidence and does not separate cleanly from nearby personas."

    return pd.Series(
        {
            "persona05_boundary_status": status,
            "persona05_boundary_reason": reason,
            "persona05_positive_signal_count": positive_signal_count,
            "persona05_negative_signal_count": negative_signal_count,
            "persona05_reporting_delivery_context": reporting_delivery_context,
            "persona05_output_construction_blocker": output_construction_blocker,
            "persona05_boundary_confidence": _boundary_confidence(status, positive_signal_count, negative_signal_count),
        }
    )


def _build_persona05_boundary_summary(diagnostic_df: pd.DataFrame) -> dict[str, Any]:
    """Summarize persona_05 boundary diagnostics into reviewer-facing counts."""
    total = int(len(diagnostic_df))
    status = diagnostic_df.get("persona05_boundary_status", pd.Series(dtype=str)).astype(str)
    clean_count = int(status.eq("clean_persona05").sum())
    p1_overlap = int(status.eq("persona01_overlap").sum())
    p3_overlap = int(status.eq("persona03_overlap").sum())
    support_noise = int(status.eq("support_troubleshooting_noise").sum())
    weak_generic = int(status.eq("weak_generic").sum())
    ambiguous = int(status.eq("ambiguous").sum())
    overlap_risk = p1_overlap + p3_overlap
    clean_share = round(float(clean_count) / float(total), 4) if total > 0 else 0.0
    overlap_noise_share = round(float(overlap_risk + support_noise) / float(total), 4) if total > 0 else 0.0
    if clean_share >= 0.5 and overlap_noise_share <= 0.3:
        readiness = "pass"
    elif clean_share >= 0.3:
        readiness = "borderline"
    else:
        readiness = "fail"
    return {
        "total_persona_05_rows_evaluated": total,
        "clean_persona05_count": clean_count,
        "persona01_overlap_count": p1_overlap,
        "persona03_overlap_count": p3_overlap,
        "support_troubleshooting_noise_count": support_noise,
        "weak_generic_count": weak_generic,
        "ambiguous_count": ambiguous,
        "clean_evidence_share": clean_share,
        "overlap_noise_share": overlap_noise_share,
        "persona05_clean_evidence_count": clean_count,
        "persona05_overlap_risk_count": overlap_risk,
        "persona05_support_noise_count": support_noise,
        "persona05_boundary_readiness": readiness,
        "persona05_boundary_rule_status": "diagnostic_only_rule_implemented",
    }


def _merge_boundary_summary(frame: pd.DataFrame, summary: dict[str, Any]) -> pd.DataFrame:
    """Merge persona_05-only boundary summary fields into one persona-facing frame."""
    if frame.empty or "persona_id" not in frame.columns:
        return frame.copy()
    annotated = frame.copy()
    for column in SUMMARY_COLUMNS:
        if column in {"persona05_boundary_readiness", "persona05_boundary_rule_status"}:
            annotated[column] = "not_applicable"
        else:
            annotated[column] = 0
    mask = annotated["persona_id"].astype(str).eq(PERSONA_05_ID)
    for column in SUMMARY_COLUMNS:
        annotated.loc[mask, column] = summary[column]
    return annotated


def _combined_text(row: pd.Series) -> str:
    """Return one normalized lowercase text bundle for heuristic boundary checks."""
    parts = [
        str(row.get("normalized_episode", "") or ""),
        str(row.get("business_question", "") or ""),
        str(row.get("bottleneck_text", "") or ""),
        str(row.get("desired_output", "") or ""),
    ]
    return " ".join(" ".join(parts).lower().split())


def _unique_hits(text: str, terms: list[str]) -> list[str]:
    """Return stable unique term hits for simple phrase-based diagnostics."""
    hits: list[str] = []
    for term in terms:
        pattern = rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])"
        if re.search(pattern, text) and term not in hits:
            hits.append(term)
    return hits


def _has_reporting_delivery_context(text: str) -> bool:
    """Return whether one row shows clear reporting-delivery context."""
    strong_context = bool(_unique_hits(text, DELIVERY_CONTEXT_TERMS)) or bool(_unique_hits(text, DELIVERY_PRESSURE_TERMS))
    report_surface = bool(_unique_hits(text, REPORTING_SURFACE_TERMS))
    return strong_context and report_surface


def _has_output_construction_blocker(text: str) -> bool:
    """Return whether one row shows a concrete last-mile output-construction blocker."""
    return bool(_unique_hits(text, OUTPUT_SURFACE_TERMS)) and bool(_unique_hits(text, BLOCKER_TERMS))


def _has_specific_output_terms(text: str) -> bool:
    """Return whether one row names a specific report surface rather than generic reporting burden."""
    specific_terms = ["pivot", "filter", "dashboard", "table", "rows", "columns", "series", "trend", "layout", "visual", "widget"]
    return bool(_unique_hits(text, specific_terms))


def _is_generic_tool_limitation(text: str) -> bool:
    """Return whether one row still reads like generic tool limitation rather than output delivery."""
    generic_terms = ["feature request", "default filters", "api", "sdk", "automation", "permissions", "sandbox", "service principal", "gateway"]
    return bool(_unique_hits(text, generic_terms)) or ("tool_limitation" in text and not _has_delivery_pressure_or_consumer_context(text))


def _has_delivery_pressure_or_consumer_context(text: str) -> bool:
    """Return whether one row includes reviewer-facing delivery pressure or consumer usability context."""
    terms = ["presentation", "shareable", "shared dashboard", "end users", "stakeholder", "target", "trend", "consumer", "usability", "dashboard_update"]
    return bool(_unique_hits(text, terms))


def _boundary_confidence(status: str, positive_signal_count: int, negative_signal_count: int) -> str:
    """Return a simple reviewer-facing confidence label for one boundary classification."""
    if status == "clean_persona05" and positive_signal_count >= 2 and negative_signal_count == 0:
        return "high"
    if status in {"clean_persona05", "persona01_overlap", "persona03_overlap", "support_troubleshooting_noise"}:
        return "medium"
    return "low"


def _report_markdown(report: dict[str, Any]) -> str:
    """Render one concise Markdown report for the persona_05 boundary rule pass."""
    return "\n".join(
        [
            "## Persona 05 Boundary Rule Report",
            "",
            f"- persona: `{report['persona_id']}`",
            f"- total rows evaluated: `{report['total_persona_05_rows_evaluated']}`",
            f"- clean persona_05 count: `{report['clean_persona05_count']}`",
            f"- persona_01 overlap count: `{report['persona01_overlap_count']}`",
            f"- persona_03 overlap count: `{report['persona03_overlap_count']}`",
            f"- support/troubleshooting noise count: `{report['support_troubleshooting_noise_count']}`",
            f"- weak generic count: `{report['weak_generic_count']}`",
            f"- ambiguous count: `{report['ambiguous_count']}`",
            f"- clean evidence share: `{report['clean_evidence_share']}`",
            f"- boundary readiness: `{report['persona05_boundary_readiness']}`",
            f"- boundary rule status: `{report['persona05_boundary_rule_status']}`",
            f"- next action recommendation: `{report['next_action_recommendation']}`",
            "",
            "This pass adds diagnostic boundary flags only. It does not change persona assignment, readiness, or deck-ready claim eligibility.",
        ]
    )
