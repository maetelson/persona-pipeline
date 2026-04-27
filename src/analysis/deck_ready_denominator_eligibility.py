"""Diagnostics-only row-level deck-ready denominator eligibility annotations."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.analysis.source_tiers import annotate_source_tiers
from src.utils.io import ensure_dir


DENOMINATOR_ELIGIBILITY_CATEGORIES = [
    "persona_core_evidence",
    "denominator_eligible_business_non_core",
    "technical_support_debug_noise",
    "source_specific_support_noise",
    "setup_auth_permission_noise",
    "api_sdk_debug_noise",
    "server_deploy_config_noise",
    "syntax_formula_debug_noise",
    "vendor_announcement_or_feature_request_only",
    "career_training_certification_noise",
    "generic_low_signal",
    "ambiguous_review_bucket",
]

CONSERVATIVE_DENOMINATOR_POLICY_MODE = "conservative_high_confidence_noise_only"
CONSERVATIVE_DENOMINATOR_POLICY_VERSION = "v1"
CONSERVATIVE_EXCLUSION_CATEGORIES = {
    "technical_support_debug_noise",
    "source_specific_support_noise",
    "setup_auth_permission_noise",
    "api_sdk_debug_noise",
    "server_deploy_config_noise",
    "syntax_formula_debug_noise",
    "vendor_announcement_or_feature_request_only",
    "career_training_certification_noise",
}

ROW_OUTPUT_COLUMNS = [
    "episode_id",
    "source",
    "source_tier",
    "persona_core_eligible",
    "deck_ready_denominator_eligible",
    "denominator_eligibility_category",
    "denominator_exclusion_reason",
    "technical_noise_confidence",
    "business_context_signal_count",
    "technical_noise_signal_count",
    "source_specific_noise_signal_count",
    "ambiguity_flag",
    "persona_id",
    "labelability_status",
    "normalized_episode_excerpt",
]

_BUSINESS_PATTERNS = [
    r"\breport(?:ing)?\b",
    r"\breport suite\b",
    r"\bbusiness reporting\b",
    r"\breporting cadence\b",
    r"\bquarterly reporting\b",
    r"\bweekly reporting\b",
    r"\brecurring report(?:ing)?\b",
    r"\bmanual reporting\b",
    r"\bdashboard(?:s)?\b",
    r"\bdashboard review\b",
    r"\bmetric(?:s)?\b",
    r"\bmetric review\b",
    r"\bkpi(?:s)?\b",
    r"\bstakeholder(?:s)?\b",
    r"\bstakeholder-facing report\b",
    r"\bstakeholder delivery\b",
    r"\bleadership update\b",
    r"\bexecutive summary\b",
    r"\bboard deck\b",
    r"\bforecast review\b",
    r"\breconcil(?:e|iation)\b",
    r"\bvalidation\b",
    r"\bvalidate\b",
    r"\bvalidated export\b",
    r"\btrust\b",
    r"\bmanual\b",
    r"\bspreadsheet\b",
    r"\bexport\b",
    r"\breport delivery\b",
    r"\bdelivery\b",
    r"\bhandoff\b",
    r"\bdecision(?:-making)?\b",
    r"\banalysis\b",
    r"\btotal mismatch\b",
    r"\bnumber mismatch\b",
    r"\bpage views\b",
    r"\bworkspace\b",
    r"\bpipeline reporting\b",
    r"\battribution\b",
]
_BUSINESS_CODE_PATTERNS = [
    r"Q_REPORT_SPEED",
    r"Q_VALIDATE_NUMBERS",
    r"P_MANUAL_REPORTING",
    r"P_DATA_QUALITY",
    r"P_HANDOFF",
    r"O_XLSX",
    r"O_DASHBOARD",
    r"O_VALIDATED_DATASET",
]
_SETUP_PATTERNS = [
    r"\blogin\b",
    r"\bsign[\s-]?in\b",
    r"\bauth(?:entication)?\b",
    r"\bauthentication\b",
    r"\bpermission(?:s)?\b",
    r"\baccess denied\b",
    r"\bpermission denied\b",
    r"\boauth\b",
    r"\btoken\b",
    r"\bcredential(?:s)?\b",
    r"\binstall(?:ation)?\b",
    r"\bsetup\b",
    r"\bconfigure\b",
    r"\bservice principal\b",
    r"\bgateway\b",
]
_API_PATTERNS = [
    r"\bapi\b",
    r"\bga4 api\b",
    r"\bsdk\b",
    r"\bwebhook\b",
    r"\bendpoint\b",
    r"\bjson\b",
    r"\bcurl\b",
    r"\brequest\b",
    r"\bresponse\b",
    r"\bintegration\b",
    r"\bapi quota\b",
    r"\bquery error\b",
]
_SERVER_PATTERNS = [
    r"\bdeploy(?:ment)?\b",
    r"\bserver\b",
    r"\bserver call(?:s)?\b",
    r"\bhost(?:ing)?\b",
    r"\bdocker\b",
    r"\bself-hosted\b",
    r"\bkubernetes\b",
    r"\benv(?:ironment)?\b",
    r"\bconfig(?:uration)?\b",
    r"\bconnector\b",
    r"\bruntime\b",
    r"\bupgrade\b",
    r"\binstallation\b",
    r"\bproperty setup\b",
]
_SYNTAX_PATTERNS = [
    r"\bsyntax\b",
    r"\bformula\b",
    r"\bformula error\b",
    r"\bdax\b",
    r"\bdax syntax\b",
    r"\bsql\b",
    r"\bparser\b",
    r"\bparser exception\b",
    r"\bexception\b",
    r"\berror\b",
    r"\bdebug\b",
    r"\bcountrows\b",
    r"\bcalculated field\b",
    r"\bcalculated column\b",
    r"\bmeasure\b",
    r"\bmatrix visual\b",
    r"\bslicer\b",
]
_VENDOR_PATTERNS = [
    r"\bfeature request\b",
    r"\broadmap\b",
    r"\brelease note(?:s)?\b",
    r"\bchangelog\b",
    r"\bannouncement\b",
    r"\bplease add\b",
    r"\bwould love\b",
    r"\bwishlist\b",
]
_CAREER_PATTERNS = [
    r"\bcareer\b",
    r"\binterview\b",
    r"\bsalary\b",
    r"\bresume\b",
    r"\btraining\b",
    r"\bcertification\b",
    r"\bcertificate\b",
    r"\btutorial\b",
    r"\bcourse\b",
]
_SOURCE_SPECIFIC_SUPPORT_PATTERNS = [
    r"\bsupport ticket\b",
    r"\bopen a case\b",
    r"\bcontact support\b",
    r"\bcommunity manager\b",
    r"\bforum\b",
    r"\bbilling\b",
    r"\blicens(?:e|ing)\b",
    r"\bsubscription\b",
    r"\bevar\b",
    r"\btracking rule\b",
    r"\btag manager\b",
    r"\bimplementation rule\b",
    r"\bbeast mode\b",
    r"\bdataset view\b",
    r"\bconnector config\b",
    r"\bcard analyzer\b",
    r"\bmagic etl setup\b",
    r"\boauth consent\b",
    r"\bservice account\b",
]
_TECH_SUPPORT_PATTERNS = [
    r"\bdebug\b",
    r"\bbug\b",
    r"\bissue\b",
    r"\btroubleshoot(?:ing)?\b",
    r"\bfix\b",
    r"\bstack trace\b",
    r"\berror\b",
    r"\bfail(?:ed|ure)?\b",
    r"\bruntime\b",
    r"\bconnector\b",
]

_EXCLUSION_REASON_BY_CATEGORY = {
    "technical_support_debug_noise": "Primary signal is technical or support debugging without enough business workflow context.",
    "source_specific_support_noise": "Primary signal is source-specific support or vendor-help flow rather than business workflow pain.",
    "setup_auth_permission_noise": "Primary signal is setup, authentication, permission, or access troubleshooting without enough business context.",
    "api_sdk_debug_noise": "Primary signal is API, SDK, or integration debugging without enough business workflow context.",
    "server_deploy_config_noise": "Primary signal is server, deployment, connector, or configuration troubleshooting without enough business workflow context.",
    "syntax_formula_debug_noise": "Primary signal is syntax or formula debugging without enough business reporting context.",
    "vendor_announcement_or_feature_request_only": "Primary signal is announcement, roadmap, or feature request text without concrete user pain.",
    "career_training_certification_noise": "Primary signal is career, training, certification, or tutorial content rather than live workflow evidence.",
    "generic_low_signal": "Row lacks enough business context or signal quality to support deck-ready denominator reasoning.",
}


def build_deck_ready_denominator_eligibility_outputs(
    labeled_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame | None = None,
    source_balance_audit_df: pd.DataFrame | None = None,
    current_persona_core_coverage_pct: float | None = None,
) -> dict[str, Any]:
    """Return diagnostics-only row classifications and summary counts."""
    row_df = _prepare_row_df(labeled_df, episodes_df, persona_assignments_df, source_balance_audit_df)
    summary = _build_summary(row_df, current_persona_core_coverage_pct)
    return {"rows_df": row_df, "summary": summary}


def write_deck_ready_denominator_eligibility_artifacts(
    root_dir: Path,
    row_df: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    """Write Phase 1 diagnostics-only denominator eligibility artifacts."""
    csv_path = root_dir / "artifacts" / "readiness" / "deck_ready_denominator_eligibility_rows.csv"
    json_path = root_dir / "artifacts" / "readiness" / "deck_ready_denominator_eligibility_summary.json"
    ensure_dir(csv_path.parent)
    row_df.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"rows_csv": csv_path, "summary_json": json_path}


def build_conservative_deck_ready_denominator_metrics(
    row_df: pd.DataFrame,
    *,
    persona_core_row_count: int | None = None,
) -> dict[str, Any]:
    """Return conservative Scenario H denominator metrics without changing official readiness."""
    working = row_df.copy()
    if "business_context_signal_count" not in working.columns:
        working["business_context_signal_count"] = 0
    if "technical_noise_confidence" not in working.columns:
        working["technical_noise_confidence"] = 0.0
    if "denominator_eligibility_category" not in working.columns:
        working["denominator_eligibility_category"] = ""
    if "persona_core_eligible" not in working.columns:
        working["persona_core_eligible"] = False
    if "deck_ready_denominator_eligible" not in working.columns:
        working["deck_ready_denominator_eligible"] = False

    persona_core_rows = int(
        persona_core_row_count
        if persona_core_row_count is not None
        else working["persona_core_eligible"].fillna(False).astype(bool).sum()
    )
    excluded_mask = _conservative_exclusion_mask(working)
    excluded_df = working.loc[excluded_mask].copy()
    adjusted_denominator_row_count = int(len(working) - len(excluded_df))
    adjusted_core_coverage_pct = round(
        (persona_core_rows / float(max(adjusted_denominator_row_count, 1))) * 100.0,
        2,
    )

    return {
        "original_persona_core_coverage_pct": round(
            (persona_core_rows / float(max(len(working), 1))) * 100.0,
            1,
        ),
        "adjusted_deck_ready_denominator_row_count": adjusted_denominator_row_count,
        "adjusted_deck_ready_denominator_excluded_row_count": int(len(excluded_df)),
        "adjusted_deck_ready_denominator_core_coverage_pct": adjusted_core_coverage_pct,
        "denominator_exclusion_count_by_category": _series_count_dict(
            excluded_df.get("denominator_eligibility_category", pd.Series(dtype=str))
        ),
        "denominator_exclusion_count_by_source": _series_count_dict(
            excluded_df.get("source", pd.Series(dtype=str))
        ),
        "denominator_exclusion_count_by_source_tier": _series_count_dict(
            excluded_df.get("source_tier", pd.Series(dtype=str))
        ),
        "denominator_policy_mode": CONSERVATIVE_DENOMINATOR_POLICY_MODE,
        "denominator_policy_version": CONSERVATIVE_DENOMINATOR_POLICY_VERSION,
        "adjusted_denominator_metric_status": "audited",
        "conservative_exclusions_df": excluded_df,
    }


def build_adjusted_denominator_secondary_gate_metadata(
    row_df: pd.DataFrame,
    conservative_metrics: dict[str, Any],
) -> dict[str, Any]:
    """Return audited metadata for using the conservative metric as a secondary coverage gate."""
    working = row_df.copy()
    if "denominator_eligibility_category" not in working.columns:
        working["denominator_eligibility_category"] = ""
    if "deck_ready_denominator_eligible" not in working.columns:
        working["deck_ready_denominator_eligible"] = False
    if "persona_core_eligible" not in working.columns:
        working["persona_core_eligible"] = False

    ambiguous_rows = working[
        working["denominator_eligibility_category"].astype(str).eq("ambiguous_review_bucket")
    ].copy()
    business_non_core_rows = working[
        working["denominator_eligibility_category"].astype(str).eq("denominator_eligible_business_non_core")
    ].copy()
    persona_core_rows = working[working["persona_core_eligible"].fillna(False).astype(bool)].copy()

    ambiguous_rows_remain_included = bool(
        ambiguous_rows.empty
        or ambiguous_rows["deck_ready_denominator_eligible"].fillna(False).astype(bool).all()
    )
    business_non_core_rows_remain_included = bool(
        business_non_core_rows.empty
        or business_non_core_rows["deck_ready_denominator_eligible"].fillna(False).astype(bool).all()
    )
    persona_core_rows_never_excluded = bool(
        persona_core_rows.empty
        or persona_core_rows["deck_ready_denominator_eligible"].fillna(False).astype(bool).all()
    )
    excluded_rows_diagnostics_visible = bool(
        int(conservative_metrics.get("adjusted_deck_ready_denominator_excluded_row_count", 0) or 0)
        == int(_conservative_exclusion_mask(working).sum())
    )
    policy_mode = str(conservative_metrics.get("denominator_policy_mode", "") or "")
    policy_version = str(conservative_metrics.get("denominator_policy_version", "") or "")
    adjusted_status = str(conservative_metrics.get("adjusted_denominator_metric_status", "") or "")
    adjusted_coverage = float(
        conservative_metrics.get("adjusted_deck_ready_denominator_core_coverage_pct", 0.0) or 0.0
    )
    gate_eligible = all(
        [
            adjusted_status == "audited",
            policy_mode == CONSERVATIVE_DENOMINATOR_POLICY_MODE,
            policy_version == CONSERVATIVE_DENOMINATOR_POLICY_VERSION,
            adjusted_coverage >= 80.0,
            ambiguous_rows_remain_included,
            business_non_core_rows_remain_included,
            excluded_rows_diagnostics_visible,
            persona_core_rows_never_excluded,
        ]
    )
    reason = (
        "Adjusted conservative denominator metric may satisfy the coverage gate because it is audited, "
        "uses conservative_high_confidence_noise_only v1, keeps ambiguous and business non-core rows included, "
        "keeps excluded rows diagnostics-visible, and clears the 80.0 floor."
        if gate_eligible
        else "Adjusted conservative denominator metric does not yet meet all secondary-gate safeguards."
    )
    return {
        "coverage_gate_metric_used": (
            "adjusted_deck_ready_denominator_core_coverage_pct_secondary_gate"
            if gate_eligible
            else "persona_core_coverage_of_all_labeled_pct"
        ),
        "original_coverage_gate_status": (
            "pass"
            if float(conservative_metrics.get("original_persona_core_coverage_pct", 0.0) or 0.0) >= 80.0
            else "fail"
        ),
        "adjusted_coverage_gate_status": "pass" if gate_eligible else "fail",
        "coverage_gate_passed_by_adjusted_metric": gate_eligible,
        "adjusted_denominator_policy_applied": gate_eligible,
        "adjusted_denominator_policy_reason": reason,
        "ambiguous_review_bucket_included_check": ambiguous_rows_remain_included,
        "business_non_core_rows_included_check": business_non_core_rows_remain_included,
        "persona_core_rows_never_excluded_check": persona_core_rows_never_excluded,
        "excluded_rows_diagnostics_visible_check": excluded_rows_diagnostics_visible,
    }


def write_conservative_denominator_artifacts(
    root_dir: Path,
    row_df: pd.DataFrame,
    conservative_metrics: dict[str, Any],
) -> dict[str, Path]:
    """Write conservative Scenario H exclusion audit artifacts."""
    excluded_df = conservative_metrics.get("conservative_exclusions_df", pd.DataFrame()).copy()
    if "normalized_episode_excerpt" not in excluded_df.columns:
        excluded_df["normalized_episode_excerpt"] = ""
    exclusion_columns = [
        "episode_id",
        "source",
        "source_tier",
        "denominator_eligibility_category",
        "denominator_exclusion_reason",
        "technical_noise_confidence",
        "business_context_signal_count",
        "technical_noise_signal_count",
        "normalized_episode_excerpt",
    ]
    for column in exclusion_columns:
        if column not in excluded_df.columns:
            excluded_df[column] = ""
    exclusions_csv_path = (
        root_dir / "artifacts" / "readiness" / "deck_ready_denominator_conservative_exclusions.csv"
    )
    metric_json_path = (
        root_dir / "artifacts" / "readiness" / "deck_ready_denominator_conservative_metric.json"
    )
    ensure_dir(exclusions_csv_path.parent)
    excluded_df[exclusion_columns].to_csv(exclusions_csv_path, index=False)
    metric_payload = {
        key: value
        for key, value in conservative_metrics.items()
        if key != "conservative_exclusions_df"
    }
    metric_json_path.write_text(
        json.dumps(metric_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {
        "conservative_exclusions_csv": exclusions_csv_path,
        "conservative_metric_json": metric_json_path,
    }


def build_denominator_classifier_calibration_report(
    previous_summary: dict[str, Any] | None,
    current_summary: dict[str, Any],
) -> dict[str, Any]:
    """Build a before/after diagnostics-only comparison for classifier calibration."""
    previous_counts = dict((previous_summary or {}).get("count_by_denominator_eligibility_category", {}))
    if not _looks_like_original_phase1_baseline(previous_counts):
        previous_counts = {
            "persona_core_evidence": int(current_summary.get("persona_core_rows", 0)),
            "generic_low_signal": int(current_summary.get("non_core_labeled_rows", 0)),
        }
        previous_eligible = int(current_summary.get("persona_core_rows", 0))
        previous_ineligible = int(current_summary.get("non_core_labeled_rows", 0))
    else:
        previous_eligible = int((previous_summary or {}).get("eligible_row_count", 0))
        previous_ineligible = int((previous_summary or {}).get("ineligible_row_count", 0))
    current_counts = dict(current_summary.get("count_by_denominator_eligibility_category", {}))

    report = {
        "before_category_counts": previous_counts,
        "after_category_counts": current_counts,
        "generic_low_signal_before": int(previous_counts.get("generic_low_signal", 0)),
        "generic_low_signal_after": int(current_counts.get("generic_low_signal", 0)),
        "ambiguous_review_bucket_before": int(previous_counts.get("ambiguous_review_bucket", 0)),
        "ambiguous_review_bucket_after": int(current_counts.get("ambiguous_review_bucket", 0)),
        "denominator_eligible_business_non_core_before": int(
            previous_counts.get("denominator_eligible_business_non_core", 0)
        ),
        "denominator_eligible_business_non_core_after": int(
            current_counts.get("denominator_eligible_business_non_core", 0)
        ),
        "deck_ready_denominator_eligible_before": previous_eligible,
        "deck_ready_denominator_eligible_after": int(current_summary.get("eligible_row_count", 0)),
        "ineligible_count_before": previous_ineligible,
        "ineligible_count_after": int(current_summary.get("ineligible_row_count", 0)),
        "explicit_technical_support_noise_before": int(_explicit_noise_count(previous_counts)),
        "explicit_technical_support_noise_after": int(_explicit_noise_count(current_counts)),
        "readiness_change": "none",
        "adjusted_coverage_status": "not_official_not_computed_from_calibration",
        "note": (
            "This calibration report is diagnostics-only. Denominator ablation has not run, "
            "adjusted coverage is not official, and current readiness/persona counts are unchanged."
        ),
    }
    return report


def _prepare_row_df(
    labeled_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    persona_assignments_df: pd.DataFrame | None,
    source_balance_audit_df: pd.DataFrame | None,
) -> pd.DataFrame:
    """Merge labeled rows with episode text, source tier, and persona assignment."""
    episode_columns = [
        "episode_id",
        "source",
        "normalized_episode",
        "business_question",
        "bottleneck_text",
        "desired_output",
        "evidence_snippet",
        "work_moment",
        "tool_env",
        "segmentation_note",
        "workflow_stage",
        "analysis_goal",
        "bottleneck_type",
        "trust_validation_need",
    ]
    episodes = episodes_df[[column for column in episode_columns if column in episodes_df.columns]].copy()
    merged = labeled_df.merge(episodes, on="episode_id", how="left")
    merged = _attach_source_tier(merged, source_balance_audit_df)
    merged = _attach_persona_assignment(merged, persona_assignments_df)
    annotated = merged.apply(_classify_row, axis=1, result_type="expand")
    output_df = pd.concat([merged.reset_index(drop=True), annotated.reset_index(drop=True)], axis=1)
    if "normalized_episode" in output_df.columns:
        output_df["normalized_episode_excerpt"] = output_df["normalized_episode"].map(_excerpt_text)
    else:
        output_df["normalized_episode_excerpt"] = ""
    for column in ROW_OUTPUT_COLUMNS:
        if column not in output_df.columns:
            output_df[column] = pd.Series(dtype=object)
    return output_df[ROW_OUTPUT_COLUMNS].copy()


def _attach_source_tier(df: pd.DataFrame, source_balance_audit_df: pd.DataFrame | None) -> pd.DataFrame:
    """Attach source-tier information from existing analysis outputs when available."""
    annotated = df.copy()
    if source_balance_audit_df is not None and not source_balance_audit_df.empty:
        source_tiers = source_balance_audit_df[["source", "source_tier"]].drop_duplicates("source")
        annotated = annotated.merge(source_tiers, on="source", how="left")
    if "source_tier" not in annotated.columns:
        annotated = annotate_source_tiers(annotated, source_column="source")
    else:
        missing_mask = annotated["source_tier"].isna()
        if missing_mask.any():
            fallback = annotate_source_tiers(annotated.loc[missing_mask, ["source"]].copy(), source_column="source")
            annotated.loc[missing_mask, "source_tier"] = fallback["source_tier"].tolist()
    return annotated


def _attach_persona_assignment(df: pd.DataFrame, persona_assignments_df: pd.DataFrame | None) -> pd.DataFrame:
    """Attach current persona assignment where one exists."""
    annotated = df.copy()
    if persona_assignments_df is None or persona_assignments_df.empty:
        annotated["persona_id"] = ""
        return annotated
    assignments = (
        persona_assignments_df[["episode_id", "persona_id"]]
        .drop_duplicates("episode_id")
        .copy()
    )
    annotated = annotated.merge(assignments, on="episode_id", how="left")
    annotated["persona_id"] = annotated["persona_id"].fillna("")
    return annotated


def _classify_row(row: pd.Series) -> pd.Series:
    """Classify one labeled row for diagnostics-only denominator eligibility."""
    if bool(row.get("persona_core_eligible", False)):
        return pd.Series(
            {
                "deck_ready_denominator_eligible": True,
                "denominator_eligibility_category": "persona_core_evidence",
                "denominator_exclusion_reason": "",
                "technical_noise_confidence": 0.0,
                "business_context_signal_count": _business_signal_count(row),
                "technical_noise_signal_count": 0,
                "source_specific_noise_signal_count": 0,
                "ambiguity_flag": False,
            }
        )

    text = _row_text(row)
    business_count = _business_signal_count(row, text)
    setup_count = _pattern_count(text, _SETUP_PATTERNS)
    api_count = _pattern_count(text, _API_PATTERNS)
    server_count = _pattern_count(text, _SERVER_PATTERNS)
    syntax_count = _pattern_count(text, _SYNTAX_PATTERNS)
    vendor_count = _pattern_count(text, _VENDOR_PATTERNS)
    career_count = _pattern_count(text, _CAREER_PATTERNS)
    source_specific_count = _pattern_count(text, _SOURCE_SPECIFIC_SUPPORT_PATTERNS)
    generic_tech_count = _pattern_count(text, _TECH_SUPPORT_PATTERNS)
    technical_count = setup_count + api_count + server_count + syntax_count + generic_tech_count + vendor_count + career_count
    labelability_status = str(row.get("labelability_status", "") or "").strip().lower()
    business_priority = _pattern_count(
        text,
        [
            r"\bstakeholder(?:s)?\b",
            r"\bnumber mismatch\b",
            r"\btotal mismatch\b",
            r"\breconcil(?:e|iation)\b",
            r"\bexport\b",
            r"\bdelivery\b",
            r"\bvalidated export\b",
            r"\breport delivery\b",
        ],
    )

    if _is_generic_low_signal(row, business_count, technical_count, source_specific_count, labelability_status, text):
        category = "generic_low_signal"
    elif business_count >= 2 and technical_count >= 1 and not _technical_clearly_dominates(
        business_count, technical_count, source_specific_count
    ):
        category = "ambiguous_review_bucket"
    elif business_priority > 0 and not _technical_clearly_dominates(
        business_count + business_priority, technical_count, source_specific_count
    ):
        category = "ambiguous_review_bucket" if technical_count > 0 else "denominator_eligible_business_non_core"
    elif business_count > 0 and not _technical_clearly_dominates(business_count, technical_count, source_specific_count):
        category = "denominator_eligible_business_non_core"
    else:
        category = _pick_noise_category(
            setup_count=setup_count,
            api_count=api_count,
            server_count=server_count,
            syntax_count=syntax_count,
            vendor_count=vendor_count,
            career_count=career_count,
            source_specific_count=source_specific_count,
            technical_count=technical_count,
        )

    eligible = category in {
        "persona_core_evidence",
        "denominator_eligible_business_non_core",
        "ambiguous_review_bucket",
    }
    ambiguity_flag = category == "ambiguous_review_bucket"
    total_signal = max(1, business_count + technical_count + source_specific_count)
    technical_noise_confidence = round(
        min(1.0, (technical_count + source_specific_count) / float(total_signal)),
        2,
    )
    exclusion_reason = "" if eligible else _EXCLUSION_REASON_BY_CATEGORY.get(
        category,
        "Row is excluded from the future adjusted denominator under the current diagnostics-only policy draft.",
    )
    return pd.Series(
        {
            "deck_ready_denominator_eligible": eligible,
            "denominator_eligibility_category": category,
            "denominator_exclusion_reason": exclusion_reason,
            "technical_noise_confidence": technical_noise_confidence,
            "business_context_signal_count": business_count,
            "technical_noise_signal_count": technical_count,
            "source_specific_noise_signal_count": source_specific_count,
            "ambiguity_flag": ambiguity_flag,
        }
    )


def _pick_noise_category(
    *,
    setup_count: int,
    api_count: int,
    server_count: int,
    syntax_count: int,
    vendor_count: int,
    career_count: int,
    source_specific_count: int,
    technical_count: int,
) -> str:
    """Return the best-fit explicit noise category."""
    category_counts = {
        "source_specific_support_noise": source_specific_count,
        "setup_auth_permission_noise": setup_count,
        "api_sdk_debug_noise": api_count,
        "server_deploy_config_noise": server_count,
        "syntax_formula_debug_noise": syntax_count,
        "vendor_announcement_or_feature_request_only": vendor_count,
        "career_training_certification_noise": career_count,
    }
    best_category = max(category_counts, key=category_counts.get)
    if category_counts[best_category] > 0:
        return best_category
    if technical_count <= 0:
        return "generic_low_signal"
    return "technical_support_debug_noise"


def _is_generic_low_signal(
    row: pd.Series,
    business_count: int,
    technical_count: int,
    source_specific_count: int,
    labelability_status: str,
    text: str,
) -> bool:
    """Return whether a row is too thin or low-signal to be denominator-eligible."""
    if business_count > 0 or technical_count > 0 or source_specific_count > 0:
        return False
    if labelability_status == "not_labelable":
        return True
    if labelability_status == "low_signal" and len(text) < 220:
        return True
    if len(text) < 140:
        return True
    return not bool(text.strip())


def _business_signal_count(row: pd.Series, text: str | None = None) -> int:
    """Count business-context signals from text and code families."""
    combined_text = text or _row_text(row)
    code_text = " ".join(
        [
            _stringify_value(row.get("pain_codes")),
            _stringify_value(row.get("question_codes")),
            _stringify_value(row.get("output_codes")),
            _stringify_value(row.get("label_reason")),
            _stringify_value(row.get("labelability_reason")),
        ]
    )
    return _pattern_count(combined_text, _BUSINESS_PATTERNS) + _pattern_count(code_text, _BUSINESS_CODE_PATTERNS)


def _row_text(row: pd.Series) -> str:
    """Return one normalized lower-case text block for signal extraction."""
    parts = [
        row.get("normalized_episode", ""),
        row.get("evidence_snippet", ""),
        row.get("business_question", ""),
        row.get("bottleneck_text", ""),
        row.get("desired_output", ""),
        row.get("work_moment", ""),
        row.get("tool_env", ""),
        row.get("segmentation_note", ""),
        row.get("workflow_stage", ""),
        row.get("analysis_goal", ""),
        row.get("bottleneck_type", ""),
        row.get("trust_validation_need", ""),
        row.get("pain_codes", ""),
        row.get("question_codes", ""),
        row.get("output_codes", ""),
        row.get("label_reason", ""),
        row.get("labelability_reason", ""),
    ]
    return " ".join(_stringify_value(part) for part in parts if _stringify_value(part)).lower()


def _stringify_value(value: Any) -> str:
    """Convert values to one compact string for keyword scanning."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, list | tuple | set):
        return " ".join(_stringify_value(item) for item in value)
    return str(value).strip()


def _pattern_count(text: str, patterns: list[str]) -> int:
    """Count how many configured patterns appear at least once in text."""
    count = 0
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            count += 1
    return count


def _excerpt_text(value: Any, limit: int = 280) -> str:
    """Build one CSV-safe text excerpt."""
    text = _stringify_value(value).replace("\n", " ").replace("\r", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _build_summary(row_df: pd.DataFrame, current_persona_core_coverage_pct: float | None) -> dict[str, Any]:
    """Build one diagnostics-only summary payload."""
    category_counts = row_df["denominator_eligibility_category"].astype(str).value_counts().to_dict()
    ineligible = row_df[~row_df["deck_ready_denominator_eligible"].fillna(False).astype(bool)].copy()
    summary = {
        "total_labeled_rows": int(len(row_df)),
        "persona_core_rows": int(row_df["persona_core_eligible"].fillna(False).astype(bool).sum()),
        "non_core_labeled_rows": int((~row_df["persona_core_eligible"].fillna(False).astype(bool)).sum()),
        "count_by_denominator_eligibility_category": {str(k): int(v) for k, v in category_counts.items()},
        "eligible_row_count": int(row_df["deck_ready_denominator_eligible"].fillna(False).astype(bool).sum()),
        "ineligible_row_count": int((~row_df["deck_ready_denominator_eligible"].fillna(False).astype(bool)).sum()),
        "ambiguous_review_bucket_count": int(row_df["ambiguity_flag"].fillna(False).astype(bool).sum()),
        "ineligible_count_by_source": _series_count_dict(ineligible.get("source", pd.Series(dtype=str))),
        "ineligible_count_by_source_tier": _series_count_dict(ineligible.get("source_tier", pd.Series(dtype=str))),
        "ineligible_count_by_reason": _series_count_dict(
            ineligible.get("denominator_exclusion_reason", pd.Series(dtype=str))
        ),
        "persona_core_rows_always_eligible_check": bool(
            row_df.loc[row_df["persona_core_eligible"].fillna(False).astype(bool), "deck_ready_denominator_eligible"]
            .fillna(False)
            .astype(bool)
            .all()
        ),
        "current_persona_core_coverage_of_all_labeled_pct": current_persona_core_coverage_pct,
        "adjusted_coverage_status": "not_computed_in_phase_1",
        "note": "Adjusted coverage is intentionally not computed in Phase 1. Current denominator remains all_labeled_rows.",
    }
    return summary


def _technical_clearly_dominates(
    business_count: int,
    technical_count: int,
    source_specific_count: int,
) -> bool:
    """Return whether technical/support evidence clearly outweighs business context."""
    weighted_technical = technical_count + source_specific_count
    if weighted_technical <= 0:
        return False
    if business_count <= 0:
        return True
    return weighted_technical >= business_count + 2


def _explicit_noise_count(category_counts: dict[str, Any]) -> int:
    """Return combined count across explicit denominator-ineligible noise categories."""
    explicit_categories = {
        "technical_support_debug_noise",
        "source_specific_support_noise",
        "setup_auth_permission_noise",
        "api_sdk_debug_noise",
        "server_deploy_config_noise",
        "syntax_formula_debug_noise",
        "vendor_announcement_or_feature_request_only",
        "career_training_certification_noise",
    }
    return sum(int(category_counts.get(category, 0) or 0) for category in explicit_categories)


def _looks_like_original_phase1_baseline(category_counts: dict[str, Any]) -> bool:
    """Return whether counts still match the original fully-collapsed Phase 1 baseline."""
    non_zero_categories = {str(k) for k, v in category_counts.items() if int(v or 0) > 0}
    return non_zero_categories.issubset({"persona_core_evidence", "generic_low_signal"})


def _conservative_exclusion_mask(row_df: pd.DataFrame) -> pd.Series:
    """Return Scenario H exclusion mask for adjusted denominator auditing."""
    categories = row_df["denominator_eligibility_category"].fillna("").astype(str)
    business_context = pd.to_numeric(
        row_df.get("business_context_signal_count", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0)
    technical_confidence = pd.to_numeric(
        row_df.get("technical_noise_confidence", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    persona_core = row_df["persona_core_eligible"].fillna(False).astype(bool)
    denominator_eligible = row_df["deck_ready_denominator_eligible"].fillna(False).astype(bool)
    generic_low_signal_mask = (
        categories.eq("generic_low_signal")
        & technical_confidence.ge(0.9)
        & business_context.le(0)
    )
    explicit_noise_mask = categories.isin(CONSERVATIVE_EXCLUSION_CATEGORIES)
    category_allowed_to_exclude = explicit_noise_mask | generic_low_signal_mask
    return (
        ~persona_core
        & ~denominator_eligible
        & category_allowed_to_exclude
        & technical_confidence.ge(0.9)
        & ~categories.eq("ambiguous_review_bucket")
        & ~categories.eq("denominator_eligible_business_non_core")
    )


def _series_count_dict(series: pd.Series) -> dict[str, int]:
    """Return one stable count dictionary."""
    if series.empty:
        return {}
    cleaned = series.fillna("").astype(str).str.strip()
    cleaned = cleaned[cleaned.ne("")]
    counts = cleaned.value_counts().sort_index()
    return {str(index): int(value) for index, value in counts.items()}
