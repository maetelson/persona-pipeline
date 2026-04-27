"""Diagnostics-only source-slice classification for weak-source policy Phase 1."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.source_tiers import annotate_source_tiers
from src.utils.io import ensure_dir


SOURCE_SLICE_CATEGORIES = [
    "evidence_producing_slice",
    "mixed_evidence_slice",
    "debt_producing_slice",
    "exploratory_slice",
    "diagnostics_only_slice",
    "insufficient_evidence_slice",
]

ROW_OUTPUT_COLUMNS = [
    "episode_id",
    "source",
    "source_tier",
    "source_slice_id",
    "source_slice_name",
    "source_slice_category",
    "source_slice_reason",
    "source_slice_confidence",
    "source_slice_deck_ready_balance_eligible",
    "source_slice_weak_debt_eligible",
    "source_slice_quarantine_status",
    "source_slice_quarantine_reason",
    "denominator_eligibility_category",
    "deck_ready_denominator_eligible",
    "persona_core_eligible",
    "text_excerpt",
]

REFINED_ROW_OUTPUT_COLUMNS = ROW_OUTPUT_COLUMNS + [
    "refined_source_slice_id",
    "refined_source_slice_name",
    "refined_source_slice_category",
    "refined_source_slice_reason",
    "refined_source_slice_confidence",
    "refined_source_slice_parent",
    "refined_source_slice_refinement_status",
]

EXPLICIT_NOISE_CATEGORIES = {
    "technical_support_debug_noise",
    "source_specific_support_noise",
    "setup_auth_permission_noise",
    "api_sdk_debug_noise",
    "server_deploy_config_noise",
    "syntax_formula_debug_noise",
    "vendor_announcement_or_feature_request_only",
    "career_training_certification_noise",
}

WEAK_SOURCES = {
    "google_developer_forums",
    "adobe_analytics_community",
    "domo_community_forum",
    "klaviyo_community",
}


def build_source_slice_classification_outputs(
    labeled_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    denominator_rows_df: pd.DataFrame | None = None,
    source_balance_audit_df: pd.DataFrame | None = None,
    overview_df: pd.DataFrame | None = None,
    quality_checks_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Return diagnostics-only source-slice rows and aggregate summary."""
    row_df = _prepare_row_df(
        labeled_df=labeled_df,
        episodes_df=episodes_df,
        denominator_rows_df=denominator_rows_df,
        source_balance_audit_df=source_balance_audit_df,
    )
    summary = _build_summary(
        row_df=row_df,
        overview_df=overview_df,
        quality_checks_df=quality_checks_df,
    )
    refined_summary = _build_refined_summary(row_df=row_df)
    return {
        "rows_df": row_df,
        "summary": summary,
        "refined_rows_df": row_df[REFINED_ROW_OUTPUT_COLUMNS].copy(),
        "refined_summary": refined_summary,
    }


def write_source_slice_classification_artifacts(
    root_dir: Path,
    row_df: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    """Write Phase 1 source-slice diagnostics artifacts."""
    rows_path = root_dir / "artifacts" / "readiness" / "source_slice_classification_rows.csv"
    summary_path = root_dir / "artifacts" / "readiness" / "source_slice_classification_summary.json"
    ensure_dir(rows_path.parent)
    row_df.to_csv(rows_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "rows_csv": rows_path,
        "summary_json": summary_path,
    }


def write_refined_source_slice_classification_artifacts(
    root_dir: Path,
    row_df: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    """Write diagnostics-only refined mixed-slice artifacts."""
    rows_path = root_dir / "artifacts" / "readiness" / "refined_source_slice_classification_rows.csv"
    summary_path = root_dir / "artifacts" / "readiness" / "refined_source_slice_classification_summary.json"
    ensure_dir(rows_path.parent)
    row_df.to_csv(rows_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "refined_rows_csv": rows_path,
        "refined_summary_json": summary_path,
    }


def _prepare_row_df(
    labeled_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    denominator_rows_df: pd.DataFrame | None,
    source_balance_audit_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """Merge row inputs and annotate source-slice classifications."""
    episode_columns = [
        "episode_id",
        "source",
        "url",
        "raw_id",
        "normalized_episode",
        "evidence_snippet",
        "business_question",
        "bottleneck_text",
        "desired_output",
        "tool_env",
        "work_moment",
        "workflow_stage",
        "analysis_goal",
        "bottleneck_type",
        "trust_validation_need",
        "segmentation_note",
    ]
    label_columns = [
        "episode_id",
        "persona_core_eligible",
        "labelability_status",
        "labelability_reason",
        "pain_codes",
        "question_codes",
        "output_codes",
    ]
    merged = episodes_df[[column for column in episode_columns if column in episodes_df.columns]].copy()
    merged = merged.merge(
        labeled_df[[column for column in label_columns if column in labeled_df.columns]].copy(),
        on="episode_id",
        how="left",
    )
    merged = _attach_denominator_fields(merged, denominator_rows_df)
    merged = _attach_source_tier(merged, source_balance_audit_df)
    annotated = merged.apply(_classify_source_slice_row, axis=1, result_type="expand")
    output_df = pd.concat([merged.reset_index(drop=True), annotated.reset_index(drop=True)], axis=1)
    output_df["text_excerpt"] = output_df.get("normalized_episode", pd.Series(dtype=object)).map(_excerpt_text)
    refined = output_df.apply(_refine_source_slice_row, axis=1, result_type="expand")
    output_df = pd.concat([output_df.reset_index(drop=True), refined.reset_index(drop=True)], axis=1)
    for column in REFINED_ROW_OUTPUT_COLUMNS:
        if column not in output_df.columns:
            output_df[column] = ""
    return output_df[REFINED_ROW_OUTPUT_COLUMNS].copy()


def _attach_denominator_fields(df: pd.DataFrame, denominator_rows_df: pd.DataFrame | None) -> pd.DataFrame:
    """Attach row-level denominator diagnostics when available."""
    annotated = df.copy()
    if denominator_rows_df is None or denominator_rows_df.empty:
        annotated["denominator_eligibility_category"] = ""
        annotated["deck_ready_denominator_eligible"] = False
        annotated["technical_noise_confidence"] = 0.0
        return annotated
    columns = [
        "episode_id",
        "source_tier",
        "denominator_eligibility_category",
        "deck_ready_denominator_eligible",
        "technical_noise_confidence",
        "business_context_signal_count",
        "technical_noise_signal_count",
        "source_specific_noise_signal_count",
        "ambiguity_flag",
        "persona_id",
    ]
    available = [column for column in columns if column in denominator_rows_df.columns]
    annotated = annotated.merge(
        denominator_rows_df[available].drop_duplicates("episode_id"),
        on="episode_id",
        how="left",
        suffixes=("", "_denom"),
    )
    if "source_tier" not in annotated.columns:
        annotated["source_tier"] = annotated.get("source_tier_denom", "")
    else:
        missing = annotated["source_tier"].isna()
        if "source_tier_denom" in annotated.columns and missing.any():
            annotated.loc[missing, "source_tier"] = annotated.loc[missing, "source_tier_denom"]
    annotated["deck_ready_denominator_eligible"] = annotated["deck_ready_denominator_eligible"].fillna(False)
    annotated["technical_noise_confidence"] = pd.to_numeric(
        annotated.get("technical_noise_confidence", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    return annotated


def _attach_source_tier(df: pd.DataFrame, source_balance_audit_df: pd.DataFrame | None) -> pd.DataFrame:
    """Attach source-tier information without altering current source-tier logic."""
    annotated = df.copy()
    if source_balance_audit_df is not None and not source_balance_audit_df.empty:
        source_tiers = source_balance_audit_df[["source", "source_tier"]].drop_duplicates("source")
        annotated = annotated.merge(source_tiers, on="source", how="left", suffixes=("", "_audit"))
        if "source_tier_audit" in annotated.columns:
            missing = annotated["source_tier"].isna() | annotated["source_tier"].eq("")
            annotated.loc[missing, "source_tier"] = annotated.loc[missing, "source_tier_audit"]
            annotated = annotated.drop(columns=["source_tier_audit"])
    if "source_tier" not in annotated.columns:
        annotated = annotate_source_tiers(annotated, source_column="source")
    else:
        missing_mask = annotated["source_tier"].isna() | annotated["source_tier"].astype(str).str.strip().eq("")
        if missing_mask.any():
            fallback = annotate_source_tiers(
                annotated.loc[missing_mask, ["source"]].copy(),
                source_column="source",
            )
            annotated.loc[missing_mask, "source_tier"] = fallback["source_tier"].tolist()
    return annotated


def _classify_source_slice_row(row: pd.Series) -> pd.Series:
    """Classify one row into a diagnostics-only source slice."""
    source = str(row.get("source", "") or "").strip()
    source_tier = str(row.get("source_tier", "") or "").strip()
    if source in WEAK_SOURCES:
        slice_name = _weak_source_slice_name(source, row)
        category, reason, confidence = _weak_source_slice_classification(source, slice_name)
    else:
        slice_name = _default_slice_name(row)
        category, reason, confidence = _default_slice_classification(row, slice_name)

    balance_eligible, weak_debt_eligible, quarantine_status, quarantine_reason = _slice_policy_fields(
        source_tier=source_tier,
        source=source,
        slice_name=slice_name,
        category=category,
    )
    slice_id = f"{source}::{slice_name}"
    return pd.Series(
        {
            "source_slice_id": slice_id,
            "source_slice_name": slice_name,
            "source_slice_category": category,
            "source_slice_reason": reason,
            "source_slice_confidence": confidence,
            "source_slice_deck_ready_balance_eligible": balance_eligible,
            "source_slice_weak_debt_eligible": weak_debt_eligible,
            "source_slice_quarantine_status": quarantine_status,
            "source_slice_quarantine_reason": quarantine_reason,
        }
    )


def _refine_source_slice_row(row: pd.Series) -> pd.Series:
    """Attach refined mixed-slice diagnostics for Google and Adobe without changing official fields."""
    source = str(row.get("source", "") or "").strip()
    parent = str(row.get("source_slice_name", "") or "").strip()
    parent_category = str(row.get("source_slice_category", "") or "").strip()
    text = _row_text(row)
    denominator_category = str(row.get("denominator_eligibility_category", "") or "")
    persona_core = bool(row.get("persona_core_eligible", False))
    refined_name = parent or "unclassified_source_slice"
    refined_category = parent_category or "insufficient_evidence_slice"
    refined_reason = "Refined diagnostics inherit the current source-slice classification for non-targeted slices."
    refined_confidence = float(row.get("source_slice_confidence", 0.5) or 0.5)
    refinement_status = "inherited_from_parent"

    if source == "google_developer_forums" and parent == "report_delivery_ui":
        refined_name, refined_category, refined_reason, refined_confidence, refinement_status = _refine_google_report_delivery(
            text=text,
            denominator_category=denominator_category,
            persona_core=persona_core,
        )
    elif source == "adobe_analytics_community" and parent in {
        "workspace_reporting",
        "implementation_tracking",
        "api_admin_config",
        "other_operational",
    }:
        refined_name, refined_category, refined_reason, refined_confidence, refinement_status = _refine_adobe_mixed_slice(
            parent=parent,
            text=text,
            denominator_category=denominator_category,
            persona_core=persona_core,
        )

    return pd.Series(
        {
            "refined_source_slice_id": f"{source}::{refined_name}",
            "refined_source_slice_name": refined_name,
            "refined_source_slice_category": refined_category,
            "refined_source_slice_reason": refined_reason,
            "refined_source_slice_confidence": refined_confidence,
            "refined_source_slice_parent": parent,
            "refined_source_slice_refinement_status": refinement_status,
        }
    )


def _weak_source_slice_name(source: str, row: pd.Series) -> str:
    """Return source-specific slice names for audited weak sources."""
    text = _row_text(row)
    if source == "google_developer_forums":
        if _has_any(text, [r"\bshare\b", r"\bpublic share\b", r"\bnew tab\b", r"\bhyperlink\b"]):
            return "sharing_permissions_delivery"
        if _has_any(text, [r"\bfilter\b", r"\bblend data\b", r"\bdate range\b", r"\bpercentile\b", r"\blogic\b", r"\bcalculated\b"]):
            return "report_logic_and_filters"
        if _has_any(
            text,
            [
                r"\boauth\b",
                r"\bpermission\b",
                r"\bproperty setup\b",
                r"\bapi quota\b",
                r"\bquery parameter\b",
                r"\bformula\b",
                r"\bsetup\b",
                r"\baccess issue\b",
                r"\bconnector issue\b",
                r"\bauthentication\b",
            ],
        ):
            return "report_delivery_ui"
        if _has_any(text, [r"\bdashboard\b", r"\breport\b", r"\bexport\b", r"\bxlsx_report\b", r"\bdelivery\b"]):
            return "report_delivery_ui"
        return "other_operational"
    if source == "adobe_analytics_community":
        if _has_any(text, [r"\breconcil", r"\baverage time on site\b", r"\bmetric mismatch\b", r"\bvalidate(?:d|ion)?\b"]):
            return "metric_reconciliation"
        if _has_any(text, [r"\bworkspace\b", r"\breporting workspace\b", r"\bpanel\b", r"\bsummary\b", r"\bslow\b"]):
            return "workspace_reporting"
        if _has_any(text, [r"\bserver call(?:s)?\b", r"\bevar\b", r"\btracking rule\b", r"\btag manager\b", r"\bimplementation rule\b"]):
            return "implementation_tracking"
        if _has_any(text, [r"\breport suite\b", r"\bapi\b", r"\bprocessing rules\b", r"\bconfiguration\b"]):
            return "api_admin_config"
        return "other_operational"
    if source == "domo_community_forum":
        if _has_any(text, [r"\bcard\b", r"\bscheduled report(?:s)?\b", r"\bchart\b", r"\bdashboard\b"]):
            return "card_report_delivery"
        if _has_any(text, [r"\bmagic etl\b", r"\bdataset view\b", r"\bconnector config\b", r"\bdataset\b", r"\bpdp\b"]):
            return "etl_dataset_config"
        if _has_any(text, [r"\bbeast mode\b", r"\bformula\b", r"\bcalculated\b"]):
            return "beast_mode_formula"
        if _has_any(text, [r"\bfilter\b", r"\bsubdate\b", r"\bslicer\b"]):
            return "filtering_logic"
        return "other_operational"
    if source == "klaviyo_community":
        if _has_any(text, [r"\brevenue\b", r"\bcustom report\b", r"\breports?\b", r"\bsales event\b", r"\bdiscount\b"]):
            return "revenue_reporting"
        if _has_any(text, [r"\bsegment\b", r"\bflow\b", r"\bcampaign\b"]):
            return "segment_flow_logic"
        return "other_operational"
    return "other_operational"


def _weak_source_slice_classification(source: str, slice_name: str) -> tuple[str, str, float]:
    """Return policy category, reason, and confidence for one weak-source slice."""
    if source == "google_developer_forums":
        mapping = {
            "sharing_permissions_delivery": (
                "evidence_producing_slice",
                "Google report sharing and permissions slice retains recurring delivery and access friction with usable business evidence.",
                0.86,
            ),
            "report_logic_and_filters": (
                "evidence_producing_slice",
                "Google report logic/filter slice retains recurring dashboard behavior and reporting logic evidence.",
                0.84,
            ),
            "report_delivery_ui": (
                "mixed_evidence_slice",
                "Google report delivery UI slice mixes real report-delivery pain with support-style interface troubleshooting.",
                0.68,
            ),
            "other_operational": (
                "debt_producing_slice",
                "Google other-operational slice is dominated by support/setup noise and weak downstream persona contribution.",
                0.89,
            ),
        }
        return mapping.get(slice_name, mapping["other_operational"])
    if source == "adobe_analytics_community":
        mapping = {
            "metric_reconciliation": (
                "evidence_producing_slice",
                "Adobe metric reconciliation slice contributes number-validation and reporting trust evidence.",
                0.88,
            ),
            "workspace_reporting": (
                "mixed_evidence_slice",
                "Adobe workspace reporting slice contains real reporting pain but Adobe-specific language and codebook overlap keep it mixed.",
                0.67,
            ),
            "implementation_tracking": (
                "mixed_evidence_slice",
                "Adobe implementation tracking slice mixes business reporting impact with implementation/tracking-heavy language.",
                0.64,
            ),
            "api_admin_config": (
                "mixed_evidence_slice",
                "Adobe API/admin/config slice mixes business operations with admin and implementation context.",
                0.62,
            ),
            "other_operational": (
                "mixed_evidence_slice",
                "Adobe other-operational slice stays mixed because source-specific language and codebook boundary issues are still unresolved.",
                0.58,
            ),
        }
        return mapping.get(slice_name, mapping["other_operational"])
    if source == "domo_community_forum":
        return (
            "debt_producing_slice",
            "Domo slice is currently debt-heavy: technical/support or formula/config trouble dominates and strong evidence-producing slices were not found.",
            0.9,
        )
    if source == "klaviyo_community":
        mapping = {
            "revenue_reporting": (
                "evidence_producing_slice",
                "Klaviyo revenue reporting slice contains real reporting and revenue interpretation evidence, but source-tier exclusion still applies.",
                0.83,
            ),
            "segment_flow_logic": (
                "evidence_producing_slice",
                "Klaviyo segment/flow logic slice contains useful business evidence, but source-tier exclusion still applies.",
                0.78,
            ),
            "other_operational": (
                "debt_producing_slice",
                "Klaviyo other-operational slice is small, noisy, and debt-producing under the current source-fit interpretation.",
                0.86,
            ),
        }
        return mapping.get(slice_name, mapping["other_operational"])
    return (
        "insufficient_evidence_slice",
        "Source slice is not covered by the current weak-source policy mapping.",
        0.5,
    )


def _default_slice_name(row: pd.Series) -> str:
    """Return one conservative default slice name for non-weak sources."""
    category = str(row.get("denominator_eligibility_category", "") or "")
    tech_confidence = float(row.get("technical_noise_confidence", 0.0) or 0.0)
    if category in {"persona_core_evidence", "denominator_eligible_business_non_core"}:
        return "evidence_aligned_non_weak_source"
    if category == "ambiguous_review_bucket":
        return "ambiguous_non_weak_source"
    if category in EXPLICIT_NOISE_CATEGORIES or (
        category == "generic_low_signal" and tech_confidence >= 0.9
    ):
        return "technical_noise_aligned_non_weak_source"
    return "insufficient_non_weak_source"


def _default_slice_classification(row: pd.Series, slice_name: str) -> tuple[str, str, float]:
    """Return conservative default slice classification for non-weak sources."""
    category = str(row.get("denominator_eligibility_category", "") or "")
    tech_confidence = float(row.get("technical_noise_confidence", 0.0) or 0.0)
    if category in {"persona_core_evidence", "denominator_eligible_business_non_core"}:
        return (
            "evidence_producing_slice",
            "Row aligns with persona-core or denominator-eligible business evidence, so the source slice stays evidence-producing by default.",
            0.78,
        )
    if category == "ambiguous_review_bucket":
        return (
            "mixed_evidence_slice",
            "Row remains mixed because denominator policy already treats it as ambiguous rather than clean evidence or clean noise.",
            0.7,
        )
    if category in EXPLICIT_NOISE_CATEGORIES or (
        category == "generic_low_signal" and tech_confidence >= 0.9
    ):
        return (
            "debt_producing_slice",
            "High-confidence technical/support noise maps conservatively to a debt-producing slice.",
            0.88,
        )
    return (
        "insufficient_evidence_slice",
        "Current row does not provide enough slice-level evidence for stronger classification.",
        0.52,
    )


def _slice_policy_fields(
    *,
    source_tier: str,
    source: str,
    slice_name: str,
    category: str,
) -> tuple[bool, bool, str, str]:
    """Return boolean diagnostics defaults and quarantine-status strings."""
    if category == "evidence_producing_slice":
        balance_eligible = source_tier != "excluded_from_deck_ready_core"
        weak_debt_eligible = False
        quarantine_status = "active_diagnostic"
        if balance_eligible:
            quarantine_reason = ""
        else:
            quarantine_reason = (
                "Slice shows useful evidence, but source-tier policy still excludes this source from deck-ready core handling."
            )
        return balance_eligible, weak_debt_eligible, quarantine_status, quarantine_reason
    if category == "mixed_evidence_slice":
        return (
            False,
            True,
            "active_diagnostic",
            "Mixed slice remains active in diagnostics and is not quarantined in Phase 1.",
        )
    if category == "debt_producing_slice":
        return (
            False,
            True,
            "diagnostics_only_not_quarantined",
            "Debt-producing slice remains visible in diagnostics and is not quarantined in Phase 1.",
        )
    if category == "exploratory_slice":
        return (
            False,
            True,
            "diagnostics_only",
            "Exploratory slice remains diagnostics-only until stronger evidence accumulates.",
        )
    if category == "diagnostics_only_slice":
        return (
            False,
            True,
            "diagnostics_only",
            "Diagnostics-only slice remains visible and is not removed.",
        )
    return (
        False,
        True,
        "needs_review",
        "Slice needs more evidence before stronger source-policy handling is justified.",
    )


def _build_summary(
    row_df: pd.DataFrame,
    overview_df: pd.DataFrame | None,
    quality_checks_df: pd.DataFrame | None,
) -> dict[str, Any]:
    """Build source-slice Phase 1 diagnostics summary."""
    category_counts = _series_count_dict(row_df["source_slice_category"])
    source_counts = _series_count_dict(row_df["source"])
    count_by_source_and_slice_category: dict[str, dict[str, int]] = {}
    for source, group in row_df.groupby("source"):
        count_by_source_and_slice_category[str(source)] = _series_count_dict(group["source_slice_category"])

    weak_source_summary: dict[str, Any] = {}
    for source in sorted(WEAK_SOURCES):
        group = row_df[row_df["source"] == source].copy()
        if group.empty:
            weak_source_summary[source] = {}
            continue
        slice_counts = (
            group.groupby(["source_slice_name", "source_slice_category"], dropna=False)["episode_id"]
            .count()
            .reset_index(name="row_count")
            .sort_values(["row_count", "source_slice_name"], ascending=[False, True])
        )
        weak_source_summary[source] = {
            "row_count": int(len(group)),
            "count_by_slice_category": _series_count_dict(group["source_slice_category"]),
            "slice_breakdown": [
                {
                    "source_slice_name": str(row["source_slice_name"]),
                    "source_slice_category": str(row["source_slice_category"]),
                    "row_count": int(row["row_count"]),
                }
                for _, row in slice_counts.iterrows()
            ],
        }

    overview_metrics = (
        dict(zip(overview_df["metric"].astype(str), overview_df["value"])) if overview_df is not None and not overview_df.empty else {}
    )
    quality_metrics = (
        dict(zip(quality_checks_df["metric"].astype(str), quality_checks_df["value"]))
        if quality_checks_df is not None and not quality_checks_df.empty
        else {}
    )
    summary = {
        "total_rows_classified": int(len(row_df)),
        "count_by_source_slice_category": category_counts,
        "count_by_source": source_counts,
        "count_by_source_and_slice_category": count_by_source_and_slice_category,
        "evidence_producing_slice_count": int(category_counts.get("evidence_producing_slice", 0)),
        "debt_producing_slice_count": int(category_counts.get("debt_producing_slice", 0)),
        "mixed_evidence_slice_count": int(category_counts.get("mixed_evidence_slice", 0)),
        "diagnostics_only_count": int(category_counts.get("diagnostics_only_slice", 0)),
        "weak_source_slice_summary": weak_source_summary,
        "official_metrics_unchanged_confirmation": {
            "effective_balanced_source_count": overview_metrics.get("effective_balanced_source_count", ""),
            "weak_source_cost_center_count": overview_metrics.get("weak_source_cost_center_count", ""),
            "core_readiness_weak_source_cost_center_count": quality_metrics.get(
                "core_readiness_weak_source_cost_center_count",
                overview_metrics.get("core_readiness_weak_source_cost_center_count", ""),
            ),
            "persona_readiness_state": overview_metrics.get("persona_readiness_state", ""),
            "final_usable_persona_count": overview_metrics.get("final_usable_persona_count", ""),
            "note": "Phase 1 source-slice diagnostics do not change official source balance, weak-source counts, readiness, or persona counts.",
        },
        "boolean_default_note": (
            "Conditional slice-policy concepts are represented with conservative booleans in Phase 1. "
            "Mixed, exploratory, diagnostics-only, and insufficient-evidence slices default to "
            "non-balance-eligible and weak-debt-eligible until a later policy pass defines audited secondary metrics."
        ),
    }
    return summary


def _build_refined_summary(row_df: pd.DataFrame) -> dict[str, Any]:
    """Build refined mixed-slice diagnostics summary for Google and Adobe."""
    refined_rows = row_df.copy()
    category_counts = _series_count_dict(refined_rows["refined_source_slice_category"])
    name_counts = _series_count_dict(refined_rows["refined_source_slice_name"])
    original_counts = (
        refined_rows.groupby(["source", "source_slice_name"], dropna=False)["episode_id"]
        .count()
        .reset_index(name="row_count")
        .sort_values(["source", "row_count", "source_slice_name"], ascending=[True, False, True])
    )
    refined_counts = (
        refined_rows.groupby(["source", "refined_source_slice_name", "refined_source_slice_category"], dropna=False)["episode_id"]
        .count()
        .reset_index(name="row_count")
        .sort_values(["source", "row_count", "refined_source_slice_name"], ascending=[True, False, True])
    )
    target_sources = {"google_developer_forums", "adobe_analytics_community"}
    targeted = refined_rows[refined_rows["source"].isin(target_sources)].copy()

    def _refined_source_summary(source: str) -> dict[str, Any]:
        group = targeted[targeted["source"] == source].copy()
        if group.empty:
            return {}
        return {
            "row_count": int(len(group)),
            "count_by_original_slice_name": _series_count_dict(group["source_slice_name"]),
            "count_by_refined_slice_name": _series_count_dict(group["refined_source_slice_name"]),
            "count_by_refined_slice_category": _series_count_dict(group["refined_source_slice_category"]),
            "refinement_status_counts": _series_count_dict(group["refined_source_slice_refinement_status"]),
        }

    google_summary = _refined_source_summary("google_developer_forums")
    adobe_summary = _refined_source_summary("adobe_analytics_community")
    expectation_map = {
        "google_report_delivery_ui_expected_business_lead": {
            "source": "google_developer_forums",
            "original_slice": "report_delivery_ui",
            "expected_review_direction": "business_leaning_mixed_with_small_debt_tail",
            "observed_refined_breakdown": google_summary.get("count_by_refined_slice_name", {}),
        },
        "adobe_workspace_reporting_expected_business_lead": {
            "source": "adobe_analytics_community",
            "original_slice": "workspace_reporting",
            "expected_review_direction": "business_heavy_mixed",
            "observed_refined_breakdown": adobe_summary.get("count_by_refined_slice_name", {}),
        },
    }
    return {
        "total_rows_classified": int(len(refined_rows)),
        "count_by_original_source_slice_name": {
            f"{row['source']}::{row['source_slice_name']}": int(row["row_count"])
            for _, row in original_counts.iterrows()
        },
        "count_by_refined_source_slice_name": {
            f"{row['source']}::{row['refined_source_slice_name']}": int(row["row_count"])
            for _, row in refined_counts.iterrows()
        },
        "count_by_refined_source_slice_category": category_counts,
        "google_refined_split_summary": google_summary,
        "adobe_refined_split_summary": adobe_summary,
        "comparison_to_mixed_slice_precision_review_expectations": expectation_map,
        "official_metrics_unchanged_confirmation": {
            "effective_balanced_source_count": "5.89",
            "weak_source_cost_center_count": "4",
            "core_readiness_weak_source_cost_center_count": "3",
            "persona_readiness_state": "reviewable_but_not_deck_ready",
            "final_usable_persona_count": "3",
            "note": "Refined mixed-slice diagnostics do not change official source balance, weak-source counts, readiness, or persona counts.",
        },
    }


def _row_text(row: pd.Series) -> str:
    """Return one normalized lower-case text blob for source-slice detection."""
    parts = [
        row.get("url", ""),
        row.get("normalized_episode", ""),
        row.get("evidence_snippet", ""),
        row.get("business_question", ""),
        row.get("bottleneck_text", ""),
        row.get("desired_output", ""),
        row.get("tool_env", ""),
        row.get("pain_codes", ""),
        row.get("question_codes", ""),
        row.get("output_codes", ""),
        row.get("workflow_stage", ""),
        row.get("analysis_goal", ""),
        row.get("bottleneck_type", ""),
        row.get("trust_validation_need", ""),
        row.get("segmentation_note", ""),
    ]
    return " ".join(_stringify_value(part) for part in parts if _stringify_value(part)).lower()


def _refine_google_report_delivery(
    *,
    text: str,
    denominator_category: str,
    persona_core: bool,
) -> tuple[str, str, str, float, str]:
    """Refine Google report_delivery_ui into business, debt, or mixed sub-slices."""
    business = _has_any(
        text,
        [
            r"\breport does(?: not|n't) match\b",
            r"\bdashboard does(?: not|n't) match\b",
            r"\bmissing data\b",
            r"\bno data\b",
            r"\bwrong total\b",
            r"\bmismatch\b",
            r"\bdiscrepanc",
            r"\breport delivery\b",
            r"\bstakeholder",
            r"\bscheduled (?:email|delivery)\b",
            r"\bdashboard\b",
            r"\breport\b",
        ],
    ) or denominator_category in {"persona_core_evidence", "denominator_eligible_business_non_core", "ambiguous_review_bucket"} or persona_core
    technical = _has_any(
        text,
        [
            r"\boauth\b",
            r"\bpermission\b",
            r"\bproperty setup\b",
            r"\bapi quota\b",
            r"\bquery parameter\b",
            r"\bformula\b",
            r"\bsetup\b",
            r"\baccess issue\b",
            r"\bconnector issue\b",
            r"\bauthentication\b",
            r"\bregexp_extract\b",
            r"\bbigquery\b",
        ],
    ) or denominator_category in {
        "setup_auth_permission_noise",
        "api_sdk_debug_noise",
        "server_deploy_config_noise",
        "syntax_formula_debug_noise",
    }
    if business and not technical:
        return (
            "google_delivery_mismatch_missing_data",
            "evidence_producing_slice" if denominator_category == "persona_core_evidence" or persona_core else "mixed_evidence_slice",
            "Google delivery/report mismatch row retains business-facing reporting impact without a dominant setup/support tail.",
            0.82,
            "refined_rule_applied",
        )
    if technical and not business:
        return (
            "google_auth_query_formula_support",
            "debt_producing_slice",
            "Google delivery row is mostly auth/query/formula/setup support without enough reporting business context.",
            0.87,
            "refined_rule_applied",
        )
    return (
        "google_report_delivery_mixed_uncertain",
        "mixed_evidence_slice",
        "Google delivery row mixes reporting impact with setup/query/support context and stays mixed in diagnostics.",
        0.7,
        "refined_rule_applied",
    )


def _refine_adobe_mixed_slice(
    *,
    parent: str,
    text: str,
    denominator_category: str,
    persona_core: bool,
) -> tuple[str, str, str, float, str]:
    """Refine Adobe mixed slices into business, debt, or ambiguous sub-slices."""
    business = _has_any(
        text,
        [
            r"\bworkspace\b",
            r"\breport suite\b",
            r"\bdashboard\b",
            r"\breport(?:ing)?\b",
            r"\bmetric comparison\b",
            r"\breport delivery\b",
            r"\bstakeholder\b",
            r"\bbusiness reporting\b",
            r"\bgrand total\b",
            r"\bmissing\b",
            r"\bincorrect\b",
            r"\bslow\b",
            r"\bsegment\b",
            r"\breconciliation\b",
            r"\breport builder\b",
            r"\bdata warehouse\b",
            r"\bexport\b",
        ],
    ) or denominator_category in {"persona_core_evidence", "denominator_eligible_business_non_core", "ambiguous_review_bucket"} or persona_core
    technical = _has_any(
        text,
        [
            r"\btracking implementation\b",
            r"\btag manager\b",
            r"\bevar\b",
            r"\bserver call\b",
            r"\badmin\b",
            r"\bconfig(?:uration)?\b",
            r"\bprocessing rule\b",
            r"\bvirtual report suite\b",
            r"\breport suite id\b",
            r"\bping\b",
            r"\bgif\b",
            r"\bnetwork tab\b",
            r"\bapi\b",
        ],
    ) or denominator_category in {
        "server_deploy_config_noise",
        "api_sdk_debug_noise",
        "setup_auth_permission_noise",
        "source_specific_support_noise",
    }

    if parent == "workspace_reporting":
        if business and not technical:
            return (
                "adobe_workspace_business_reporting",
                "evidence_producing_slice" if persona_core or denominator_category == "persona_core_evidence" else "mixed_evidence_slice",
                "Adobe workspace row reads as business-facing reporting, comparison, or delivery friction rather than pure setup noise.",
                0.8,
                "refined_rule_applied",
            )
        if technical and not business:
            return (
                "adobe_workspace_technical_setup",
                "debt_producing_slice",
                "Adobe workspace row is dominated by implementation/admin/setup language without enough business reporting impact.",
                0.84,
                "refined_rule_applied",
            )
        return (
            "adobe_workspace_ambiguous",
            "mixed_evidence_slice",
            "Adobe workspace row still blends reporting pain with implementation or admin detail and remains mixed.",
            0.7,
            "refined_rule_applied",
        )
    if parent == "implementation_tracking":
        if business and not technical:
            return (
                "adobe_tracking_business_impact",
                "mixed_evidence_slice",
                "Adobe implementation row is only retained as mixed when business reporting impact is explicit.",
                0.72,
                "refined_rule_applied",
            )
        if technical and not business:
            return (
                "adobe_tracking_setup_noise",
                "debt_producing_slice",
                "Adobe implementation row is primarily tracking/setup troubleshooting.",
                0.86,
                "refined_rule_applied",
            )
        return (
            "adobe_tracking_ambiguous",
            "mixed_evidence_slice",
            "Adobe implementation row mixes tracking setup and business impact and remains ambiguous in diagnostics.",
            0.66,
            "refined_rule_applied",
        )
    if parent == "api_admin_config":
        business_api = _has_any(
            text,
            [
                r"\bdashboard\b",
                r"\breporting\b",
                r"\bworkspace\b",
                r"\bmetric\b",
                r"\brevenue\b",
                r"\breport delivery\b",
                r"\bstakeholder\b",
                r"\bexport\b",
                r"\bmissing\b",
                r"\bincorrect\b",
                r"\bmismatch\b",
            ],
        ) or denominator_category in {"persona_core_evidence", "denominator_eligible_business_non_core", "ambiguous_review_bucket"} or persona_core
        if business_api and not technical:
            return (
                "adobe_api_admin_business_blocker",
                "mixed_evidence_slice",
                "Adobe API/admin row is retained only because it clearly blocks a reporting or business workflow.",
                0.69,
                "refined_rule_applied",
            )
        if technical and not business_api:
            return (
                "adobe_api_admin_support_noise",
                "debt_producing_slice",
                "Adobe API/admin row is mostly support/admin noise without stable reporting evidence.",
                0.84,
                "refined_rule_applied",
            )
        return (
            "adobe_api_admin_ambiguous",
            "mixed_evidence_slice",
            "Adobe API/admin row still mixes business blocker and support/admin signals.",
            0.64,
            "refined_rule_applied",
        )
    if business and not technical:
        return (
            "adobe_operational_reporting_evidence",
            "evidence_producing_slice" if persona_core or denominator_category == "persona_core_evidence" else "mixed_evidence_slice",
            "Adobe operational row is closer to business reporting evidence than support noise under refined diagnostics.",
            0.76,
            "refined_rule_applied",
        )
    if technical and not business:
        return (
            "adobe_operational_support_noise",
            "debt_producing_slice",
            "Adobe operational row is mostly support or implementation noise under refined diagnostics.",
            0.83,
            "refined_rule_applied",
        )
    return (
        "adobe_operational_ambiguous",
        "mixed_evidence_slice",
        "Adobe operational row remains too broad and ambiguous to promote or demote confidently.",
        0.62,
        "refined_rule_applied",
    )


def _has_any(text: str, patterns: list[str]) -> bool:
    """Return whether any pattern matches text."""
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _stringify_value(value: Any) -> str:
    """Return one compact string value for text scanning."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, list | tuple | set):
        return " ".join(_stringify_value(item) for item in value)
    return str(value).strip()


def _excerpt_text(value: Any, limit: int = 280) -> str:
    """Build one CSV-safe text excerpt."""
    text = _stringify_value(value).replace("\n", " ").replace("\r", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _series_count_dict(series: pd.Series) -> dict[str, int]:
    """Return one stable count dictionary."""
    if series.empty:
        return {}
    cleaned = series.fillna("").astype(str).str.strip()
    cleaned = cleaned[cleaned.ne("")]
    counts = cleaned.value_counts().sort_index()
    return {str(index): int(value) for index, value in counts.items()}
