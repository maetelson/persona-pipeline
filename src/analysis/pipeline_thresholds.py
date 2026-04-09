"""Shared threshold profiles and audit helpers for exploratory pipeline gating."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, load_yaml, read_parquet, write_parquet

AUDIT_COLUMNS = [
    "profile",
    "stage",
    "metric_name",
    "metric_value",
    "threshold_value",
    "status",
    "gate_mode",
    "message",
]


def load_threshold_profile(config_path: Path) -> tuple[str, dict[str, Any]]:
    """Load the active threshold profile from configuration."""
    config = load_yaml(config_path).get("pipeline_thresholds", {})
    active_profile = str(config.get("active_profile", "exploratory"))
    profiles = dict(config.get("profiles", {}) or {})
    if active_profile not in profiles:
        raise ValueError(f"Unknown pipeline threshold profile: {active_profile}")
    return active_profile, dict(profiles[active_profile] or {})


def evaluate_episode_thresholds(valid_df: pd.DataFrame, episodes_df: pd.DataFrame, profile: str, profile_cfg: dict[str, Any]) -> pd.DataFrame:
    """Evaluate episode-stage thresholds and return audit rows."""
    cfg = dict(profile_cfg.get("episode", {}) or {})
    gate_mode = str(profile_cfg.get("gate_mode", {}).get("episode_gate", "warn"))
    raw_posts = int(len(valid_df))
    total_episodes = int(len(episodes_df))
    avg_per_post = float(total_episodes / raw_posts) if raw_posts else 0.0
    max_per_post = int(episodes_df.groupby("raw_id").size().max()) if not episodes_df.empty else 0

    rows = [
        _audit_row(profile, "episode", "raw_post_count", raw_posts, None, "pass", gate_mode, "valid candidate posts entering episode build"),
        _bounded_low_high(
            profile,
            "episode",
            "episode_total",
            total_episodes,
            min_value=cfg.get("min_total"),
            warn_if_below=cfg.get("warn_if_below"),
            max_value=cfg.get("max_total"),
            gate_mode=gate_mode,
            exploratory_message="exploratory caution: low episode volume",
        ),
        _high_only(
            profile,
            "episode",
            "avg_episodes_per_raw_post",
            avg_per_post,
            warn_above=cfg.get("warn_if_avg_per_raw_post_above"),
            fail_above=cfg.get("max_per_raw_post"),
            gate_mode=gate_mode,
            warn_message="episode segmentation may be too fine",
            fail_message="episode count per raw post exceeds configured maximum",
        ),
        _high_only(
            profile,
            "episode",
            "max_episodes_per_raw_post",
            max_per_post,
            warn_above=cfg.get("max_per_raw_post"),
            fail_above=None,
            gate_mode=gate_mode,
            warn_message="outlier post may still be over-segmented",
            fail_message="",
        ),
    ]
    return pd.DataFrame(rows, columns=AUDIT_COLUMNS)


def evaluate_labeling_thresholds(labeled_df: pd.DataFrame, profile: str, profile_cfg: dict[str, Any]) -> pd.DataFrame:
    """Evaluate labeling-stage thresholds and return audit rows."""
    cfg = dict(profile_cfg.get("labeling", {}) or {})
    gate_mode = str(profile_cfg.get("gate_mode", {}).get("labeling_gate", "warn"))
    total_rows = int(len(labeled_df))
    unknown_ratio = float(_unknown_ratio(labeled_df))
    role_coverage = _coverage_ratio(labeled_df, "role_codes")
    question_coverage = _coverage_ratio(labeled_df, "question_codes")
    pain_coverage = _coverage_ratio(labeled_df, "pain_codes")
    output_coverage = _coverage_ratio(labeled_df, "output_codes")
    role_share = _dominant_share(labeled_df, "role_codes")
    question_share = _dominant_share(labeled_df, "question_codes")
    pain_share = _dominant_share(labeled_df, "pain_codes")

    rows = [
        _low_only(
            profile,
            "labeling",
            "labeled_row_total",
            total_rows,
            min_value=cfg.get("min_labeled_rows"),
            gate_mode=gate_mode,
            fail_message="not enough labeled rows for stable persona exploration",
        ),
        _ratio_band(
            profile,
            "labeling",
            "unknown_ratio",
            unknown_ratio,
            warn_value=cfg.get("warn_unknown_ratio"),
            fail_value=cfg.get("max_unknown_ratio"),
            gate_mode=gate_mode,
            warn_message="unknown ratio is elevated; interpret persona results cautiously",
            fail_message="persona reliability low due to high unknown ratio",
        ),
        _low_only(profile, "labeling", "role_coverage_ratio", role_coverage, cfg.get("min_role_coverage_ratio"), gate_mode, "role coverage is too sparse"),
        _low_only(profile, "labeling", "question_coverage_ratio", question_coverage, cfg.get("min_question_coverage_ratio"), gate_mode, "question coverage is too sparse"),
        _low_only(profile, "labeling", "pain_coverage_ratio", pain_coverage, cfg.get("min_pain_coverage_ratio"), gate_mode, "pain coverage is too sparse"),
        _low_only(profile, "labeling", "output_coverage_ratio", output_coverage, cfg.get("min_output_coverage_ratio"), gate_mode, "output coverage is too sparse"),
        _high_only(profile, "labeling", "role_dominant_code_share", role_share, None, cfg.get("max_single_code_share", {}).get("role"), gate_mode, "", "role labels are too concentrated"),
        _high_only(profile, "labeling", "question_dominant_code_share", question_share, None, cfg.get("max_single_code_share", {}).get("question"), gate_mode, "", "question labels are too concentrated"),
        _high_only(profile, "labeling", "pain_dominant_code_share", pain_share, None, cfg.get("max_single_code_share", {}).get("pain"), gate_mode, "", "pain labels are too concentrated"),
    ]
    return pd.DataFrame(rows, columns=AUDIT_COLUMNS)


def evaluate_cluster_thresholds(labeled_df: pd.DataFrame, profile: str, profile_cfg: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Evaluate clustering readiness and derive gating flags."""
    cfg = dict(profile_cfg.get("cluster_readiness", {}) or {})
    gate_mode = str(profile_cfg.get("gate_mode", {}).get("cluster_gate", "warn"))
    total_rows = int(len(labeled_df))
    distinct_role_codes = _distinct_code_count(labeled_df, "role_codes")
    distinct_question_codes = _distinct_code_count(labeled_df, "question_codes")
    distinct_pain_codes = _distinct_code_count(labeled_df, "pain_codes")

    rows = [
        _low_only(profile, "cluster_readiness", "labeled_rows_for_clustering", total_rows, cfg.get("min_rows_for_clustering"), gate_mode, "not enough rows for cluster exploration"),
        _low_only(profile, "cluster_readiness", "distinct_role_codes", distinct_role_codes, cfg.get("min_distinct_role_codes"), gate_mode, "role diversity is too low for persona clustering"),
        _low_only(profile, "cluster_readiness", "distinct_question_codes", distinct_question_codes, cfg.get("min_distinct_question_codes"), gate_mode, "question diversity is too low for cluster separation"),
        _low_only(profile, "cluster_readiness", "distinct_pain_codes", distinct_pain_codes, cfg.get("min_distinct_pain_codes"), gate_mode, "pain diversity is too low for cluster separation"),
    ]
    audit_df = pd.DataFrame(rows, columns=AUDIT_COLUMNS)
    worst_status = _worst_status(audit_df["status"].tolist())
    cluster_allowed = not (gate_mode == "strict" and worst_status == "fail")
    exploratory_only = profile == "exploratory" or worst_status in {"warn", "fail"}
    cluster_reliability = "high"
    if worst_status == "warn" or profile == "exploratory":
        cluster_reliability = "medium"
    if worst_status == "fail":
        cluster_reliability = "low"
    if profile == "exploratory" and worst_status in {"warn", "fail"}:
        cluster_reliability = "low"
    reason = "threshold_passed"
    if worst_status != "pass":
        reason = "threshold_below_exploratory_recommendation"
    return audit_df, {
        "cluster_allowed": cluster_allowed,
        "exploratory_only": exploratory_only,
        "cluster_reliability": cluster_reliability,
        "reason": reason,
        "gate_mode": gate_mode,
        "status": worst_status,
        "profile": profile,
    }


def upsert_threshold_audit(root_dir: Path, stage_audit_df: pd.DataFrame) -> pd.DataFrame:
    """Merge a stage audit into the shared pipeline threshold audit artifact."""
    output_path = root_dir / "data" / "analysis" / "pipeline_threshold_audit.parquet"
    csv_path = root_dir / "data" / "analysis" / "pipeline_threshold_audit.csv"
    existing = read_parquet(output_path, columns=AUDIT_COLUMNS)
    if existing.empty:
        combined = stage_audit_df.copy()
    else:
        stage_names = stage_audit_df["stage"].dropna().astype(str).unique().tolist()
        combined = pd.concat([existing[~existing["stage"].astype(str).isin(stage_names)], stage_audit_df], ignore_index=True)
    combined = combined[AUDIT_COLUMNS].copy()
    for column in ["profile", "stage", "metric_name", "threshold_value", "status", "gate_mode", "message"]:
        combined[column] = combined[column].map(lambda value: "" if value is None else str(value))
    write_parquet(combined, output_path)
    ensure_dir(csv_path.parent)
    combined.to_csv(csv_path, index=False)
    return combined


def summarize_stage_status(audit_df: pd.DataFrame, stage: str) -> str:
    """Return the worst status for a stage."""
    if audit_df.empty:
        return "pass"
    stage_df = audit_df[audit_df["stage"].astype(str) == stage]
    if stage_df.empty:
        return "pass"
    return _worst_status(stage_df["status"].tolist())


def threshold_summary_message(audit_df: pd.DataFrame, stage: str) -> str:
    """Build a readable message from failing/warning rows for one stage."""
    if audit_df.empty:
        return "no_threshold_findings"
    stage_df = audit_df[audit_df["stage"].astype(str) == stage]
    if stage_df.empty:
        return "no_threshold_findings"
    flagged = stage_df[stage_df["status"].isin(["warn", "fail"])]
    if flagged.empty:
        return "thresholds_passed"
    return " | ".join(str(message) for message in flagged["message"].tolist())


def _bounded_low_high(
    profile: str,
    stage: str,
    metric_name: str,
    metric_value: float,
    min_value: Any,
    warn_if_below: Any,
    max_value: Any,
    gate_mode: str,
    exploratory_message: str,
) -> dict[str, Any]:
    """Evaluate metrics with both lower and upper bounds."""
    status = "pass"
    threshold_value: Any = f"min={min_value};warn_if_below={warn_if_below};max={max_value}"
    message = "within configured range"
    if min_value is not None and metric_value < float(min_value):
        status = "fail"
        message = exploratory_message
    elif warn_if_below is not None and metric_value < float(warn_if_below):
        status = "warn"
        message = "below exploratory recommendation"
    elif max_value is not None and metric_value > float(max_value):
        status = "fail"
        message = "above configured upper bound"
    return _audit_row(profile, stage, metric_name, metric_value, threshold_value, status, gate_mode, message)


def _high_only(
    profile: str,
    stage: str,
    metric_name: str,
    metric_value: float,
    warn_above: Any,
    fail_above: Any,
    gate_mode: str,
    warn_message: str,
    fail_message: str,
) -> dict[str, Any]:
    """Evaluate metrics where high values are risky."""
    status = "pass"
    threshold_value: Any = fail_above if fail_above is not None else warn_above
    message = "within configured range"
    if fail_above is not None and metric_value > float(fail_above):
        status = "fail"
        message = fail_message or "value above configured maximum"
    elif warn_above is not None and metric_value > float(warn_above):
        status = "warn"
        message = warn_message or "value above warning threshold"
    return _audit_row(profile, stage, metric_name, metric_value, threshold_value, status, gate_mode, message)


def _low_only(profile: str, stage: str, metric_name: str, metric_value: float, min_value: Any, gate_mode: str, fail_message: str) -> dict[str, Any]:
    """Evaluate metrics where low values are risky."""
    status = "pass"
    message = "within configured range"
    if min_value is not None and metric_value < float(min_value):
        status = "fail"
        message = fail_message
    return _audit_row(profile, stage, metric_name, metric_value, min_value, status, gate_mode, message)


def _ratio_band(
    profile: str,
    stage: str,
    metric_name: str,
    metric_value: float,
    warn_value: Any,
    fail_value: Any,
    gate_mode: str,
    warn_message: str,
    fail_message: str,
) -> dict[str, Any]:
    """Evaluate ratios where high values are risky with warn/fail bands."""
    status = "pass"
    threshold_value: Any = f"warn={warn_value};max={fail_value}"
    message = "within configured range"
    if fail_value is not None and metric_value > float(fail_value):
        status = "fail"
        message = fail_message
    elif warn_value is not None and metric_value > float(warn_value):
        status = "warn"
        message = warn_message
    return _audit_row(profile, stage, metric_name, metric_value, threshold_value, status, gate_mode, message)


def _audit_row(profile: str, stage: str, metric_name: str, metric_value: Any, threshold_value: Any, status: str, gate_mode: str, message: str) -> dict[str, Any]:
    """Create one threshold audit row."""
    return {
        "profile": profile,
        "stage": stage,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "threshold_value": threshold_value,
        "status": status,
        "gate_mode": gate_mode,
        "message": message,
    }


def _unknown_ratio(df: pd.DataFrame) -> float:
    """Return the fraction of rows with any core family still unknown."""
    if df.empty:
        return 0.0
    mask = pd.Series(False, index=df.index)
    for column in ["role_codes", "question_codes", "pain_codes", "output_codes"]:
        if column in df.columns:
            mask = mask | df[column].fillna("").astype(str).eq("unknown")
    return float(mask.mean())


def _coverage_ratio(df: pd.DataFrame, column: str) -> float:
    """Return the ratio of rows where a family is known."""
    if df.empty or column not in df.columns:
        return 0.0
    return float((~df[column].fillna("").astype(str).eq("unknown")).mean())


def _dominant_share(df: pd.DataFrame, column: str) -> float:
    """Return the dominant exact-code share among known rows."""
    if df.empty or column not in df.columns:
        return 0.0
    series = df[column].fillna("").astype(str)
    known = series[series != "unknown"]
    if known.empty:
        return 0.0
    return float(known.value_counts(normalize=True).iloc[0])


def _distinct_code_count(df: pd.DataFrame, column: str) -> int:
    """Count distinct pipe-separated codes, excluding unknown."""
    if df.empty or column not in df.columns:
        return 0
    codes: set[str] = set()
    for value in df[column].fillna("").astype(str):
        for code in value.split("|"):
            code = code.strip()
            if code and code != "unknown":
                codes.add(code)
    return len(codes)


def _worst_status(statuses: list[str]) -> str:
    """Return the worst status from pass/warn/fail."""
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"
