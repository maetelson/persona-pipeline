"""Label episodes with rule-first logic and token-aware LLM fallback."""

from __future__ import annotations

import os
import sys
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.labeling.audit import build_label_audit, build_labeling_audit
from src.analysis.pipeline_thresholds import (
    evaluate_labeling_thresholds,
    load_threshold_profile,
    summarize_stage_status,
    threshold_summary_message,
    upsert_threshold_audit,
)
from src.labeling.labelability import build_labelability_table
from src.labeling.llm_labeler import enrich_with_llm_labels, llm_runtime_snapshot, resolve_llm_runtime
from src.labeling.quality import build_label_quality_audit, write_label_quality_outputs
from src.labeling.repair import apply_label_repairs, build_axis_label_details
from src.labeling.rule_labeler import prelabel_episodes
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger
from src.utils.run_helpers import load_dotenv
from src.utils.pipeline_schema import LABEL_CODE_COLUMNS

LOGGER = get_logger("run.label_episodes")


def main() -> None:
    """Create labeled artifacts and token-usage audit for direct or batch mode."""
    load_dotenv(ROOT / ".env")

    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    if episodes_df.empty:
        _write_empty_label_outputs(ROOT)
        LOGGER.info("No episodes to label; wrote empty labeled artifacts.")
        return
    if not episodes_df.empty and "episode_id" in episodes_df.columns:
        duplicate_count = int(episodes_df["episode_id"].astype(str).duplicated().sum())
        if duplicate_count:
            LOGGER.warning("Deduplicating %s duplicate episode_id rows before labeling.", duplicate_count)
            episodes_df = episodes_df.drop_duplicates(subset=["episode_id"], keep="first").reset_index(drop=True)
    codebook = load_yaml(ROOT / "config" / "codebook.yaml")
    labeling_policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
    previous_labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    llm_config = {
        "enabled": _env_flag("ENABLE_LLM_LABELER", "LLM_LABELER_ENABLED", default=False),
        "dry_run": _env_flag("LLM_DRY_RUN", "LLM_LABELER_DRY_RUN", "LABELING_DRY_RUN", default=False),
        "batch_enabled": _env_flag("ENABLE_BATCH_LABELING", default=False),
        "enable_escalation": _env_flag("ENABLE_LLM_ESCALATION", default=False),
        "target_unknown_only": _env_flag("LLM_TARGET_UNKNOWN_ONLY", default=True),
        "cache_enabled": _env_flag("LLM_CACHE_ENABLED", default=True),
        "min_confidence": float(os.getenv("LLM_LABELER_MIN_CONFIDENCE", "0.72")),
        "model_primary": _first_non_empty_env("LLM_MODEL_PRIMARY", "LLM_MODEL", "OPENAI_MODEL", default="gpt-5.4-mini"),
        "model_escalation": _first_non_empty_env("LLM_MODEL_ESCALATION", default="gpt-5.4-mini"),
        "max_output_tokens": int(os.getenv("MAX_LLM_OUTPUT_TOKENS", "120")),
        "prompt_cache_key": _first_non_empty_env("PROMPT_CACHE_KEY", default="persona-label-v1"),
        "prompt_cache_retention": _first_non_empty_env("PROMPT_CACHE_RETENTION", default="session"),
        "backend": _first_non_empty_env("LLM_OPENAI_BACKEND", default="http"),
        "timeout_seconds": int(os.getenv("LLM_LABELER_TIMEOUT_SECONDS", "45")),
        "cache_path": ROOT / "data" / "labeled" / "llm_response_cache.jsonl",
        "codebook": codebook,
        "policy": labeling_policy,
    }

    labelability_df = build_labelability_table(episodes_df, labeling_policy)
    labeled_df = prelabel_episodes(episodes_df, codebook)
    labeled_df = labeled_df.merge(
        labelability_df[["episode_id", "labelability_status", "labelability_score", "labelability_reason", "persona_core_eligible"]],
        on="episode_id",
        how="left",
    )
    labeled_df = _apply_low_signal_gate(labeled_df)
    write_parquet(labeled_df, ROOT / "data" / "labeled" / "labeled_episodes_rule_only.parquet")
    runtime = resolve_llm_runtime(llm_config)
    LOGGER.info("LLM runtime snapshot: %s", json.dumps(llm_runtime_snapshot(runtime), ensure_ascii=False, sort_keys=True))
    labeled_df, llm_audit_df = enrich_with_llm_labels(episodes_df, labeled_df, config=llm_config)
    labeled_df, repaired_df = apply_label_repairs(episodes_df, labeled_df, labelability_df, labeling_policy)
    details_df = build_axis_label_details(episodes_df, labeled_df, labelability_df)
    audit_df = build_label_audit(labeled_df, llm_audit_df)
    labeling_audit_df = build_labeling_audit(labeled_df, llm_audit_df)
    quality_outputs = build_label_quality_audit(
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        details_df=details_df,
        labelability_df=labelability_df,
    )

    write_parquet(labeled_df, ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    write_parquet(audit_df, ROOT / "data" / "labeled" / "label_audit.parquet")
    write_parquet(labeling_audit_df, ROOT / "data" / "labeled" / "labeling_audit.parquet")
    write_parquet(llm_audit_df, ROOT / "data" / "labeled" / "llm_label_audit.parquet")
    write_parquet(labelability_df, ROOT / "data" / "labeled" / "labelability_audit.parquet")
    label_quality_paths = write_label_quality_outputs(ROOT, quality_outputs, repaired_df, details_df)
    _write_before_after_quality_report(
        ROOT,
        previous_labeled_df,
        labeled_df,
        labelability_df,
        labeling_policy.get("audit", {}),
    )
    profile, profile_cfg = load_threshold_profile(ROOT / "config" / "pipeline_thresholds.yaml")
    threshold_df = evaluate_labeling_thresholds(labeled_df, profile, profile_cfg)
    combined_threshold_df = upsert_threshold_audit(ROOT, threshold_df)

    metrics = _metric_lookup(audit_df)
    stage_status = summarize_stage_status(combined_threshold_df, "labeling")
    LOGGER.info(
        "Labeling mode=%s, total rows=%s, rule-only rows=%s, llm-targeted rows=%s, llm-called rows=%s, llm-batch rows=%s, llm-success rows=%s, llm-failed rows=%s, avg input tokens=%s, avg output tokens=%s, total estimated tokens=%s, threshold profile=%s, threshold status=%s",
        runtime["mode"],
        len(episodes_df),
        metrics.get("rule_labeled_only_count", 0),
        metrics.get("llm_targeted_count", 0),
        metrics.get("llm_called_count", 0),
        metrics.get("llm_batch_count", 0),
        metrics.get("llm_success_count", 0),
        metrics.get("llm_failed_count", 0),
        metrics.get("avg_input_tokens", 0),
        metrics.get("avg_output_tokens", 0),
        metrics.get("usage_total_tokens_total", 0),
        profile,
        stage_status,
    )
    if stage_status in {"warn", "fail"}:
        LOGGER.warning("Labeling threshold summary: %s", threshold_summary_message(combined_threshold_df, "labeling"))
    LOGGER.info("exploratory_only thresholding does not block labeling write; gating only applies in strict profile.")
    if runtime["mode"] == "dry_run":
        LOGGER.info("API was not called because dry-run mode is enabled.")
    elif runtime["mode"] == "batch":
        LOGGER.info("Batch mode prepared requests without direct API calls in this step.")
    elif runtime["skip_reason"]:
        LOGGER.info("API was not called because %s", runtime["skip_reason"])
    else:
        LOGGER.info("Actual-run used model_primary=%s and escalation=%s", runtime["model_primary"], runtime["enable_escalation"])
    LOGGER.info("Label quality artifacts: %s", ", ".join(str(path) for path in label_quality_paths.values()))
    gate_mode = str(profile_cfg.get("gate_mode", {}).get("labeling_gate", "warn"))
    if gate_mode == "strict" and stage_status == "fail":
        raise RuntimeError("Labeling threshold failed under strict profile. See data/analysis/pipeline_threshold_audit.parquet")


def _apply_low_signal_gate(labeled_df):
    """Blank persona-driving families for low-signal rows so they do not pollute clustering."""
    if labeled_df.empty or "labelability_status" not in labeled_df.columns:
        return labeled_df
    result = labeled_df.copy()
    low_signal_mask = result["labelability_status"].fillna("").astype(str).eq("low_signal")
    if not low_signal_mask.any():
        return result
    for column in LABEL_CODE_COLUMNS:
        result.loc[low_signal_mask, column] = "unknown"
    result.loc[low_signal_mask, "label_confidence"] = 0.2
    result.loc[low_signal_mask, "label_reason"] = (
        result.loc[low_signal_mask, "label_reason"].fillna("").astype(str) + " | low_signal_input"
    ).str.strip(" |")
    result.loc[low_signal_mask, "persona_core_eligible"] = False
    return result


def _write_empty_label_outputs(root_dir: Path) -> None:
    """Write empty label artifacts with stable schemas when no episodes exist."""
    columns = [
        "episode_id",
        *LABEL_CODE_COLUMNS,
        "label_confidence",
        "label_reason",
        "rule_hit_count",
        "rule_core_known_count",
        "rule_unknown_family_count",
        "rule_coarse_match",
        "labelability_status",
        "labelability_score",
        "labelability_reason",
        "persona_core_eligible",
    ]
    labeled_df = pd.DataFrame(columns=columns)
    empty_audit_df = pd.DataFrame()
    write_parquet(labeled_df, root_dir / "data" / "labeled" / "labeled_episodes.parquet")
    write_parquet(labeled_df, root_dir / "data" / "labeled" / "labeled_episodes_rule_only.parquet")
    write_parquet(empty_audit_df, root_dir / "data" / "labeled" / "label_audit.parquet")
    write_parquet(empty_audit_df, root_dir / "data" / "labeled" / "labeling_audit.parquet")
    write_parquet(empty_audit_df, root_dir / "data" / "labeled" / "llm_label_audit.parquet")
    write_parquet(empty_audit_df, root_dir / "data" / "labeled" / "labelability_audit.parquet")


def _write_before_after_quality_report(root_dir, before_df, after_df, labelability_df, audit_config) -> None:
    """Write before/after quality comparison for the rerun."""
    before_unknown = _unknown_ratio(before_df)
    after_unknown = _unknown_ratio(after_df)
    before_core_unknown = _unknown_ratio(_persona_core_subset(before_df))
    after_core_unknown = _unknown_ratio(_persona_core_subset(after_df))
    reported_baseline_unknown = float(audit_config.get("reported_baseline_unknown_ratio", 0.0) or 0.0)
    reported_baseline_quality_flag = str(audit_config.get("reported_baseline_quality_flag", "") or "").strip()
    low_signal_rate = (
        float((labelability_df["labelability_status"] == "low_signal").mean())
        if not labelability_df.empty
        else 0.0
    )
    rows = [
        {"metric": "before_unknown_ratio", "value_numeric": before_unknown, "value_text": ""},
        {"metric": "after_unknown_ratio", "value_numeric": after_unknown, "value_text": ""},
        {"metric": "unknown_ratio_delta", "value_numeric": round(after_unknown - before_unknown, 6), "value_text": ""},
        {"metric": "reported_baseline_unknown_ratio", "value_numeric": reported_baseline_unknown, "value_text": ""},
        {"metric": "reported_baseline_to_after_delta", "value_numeric": round(after_unknown - reported_baseline_unknown, 6), "value_text": ""},
        {"metric": "before_core_unknown_ratio", "value_numeric": before_core_unknown, "value_text": ""},
        {"metric": "after_core_unknown_ratio", "value_numeric": after_core_unknown, "value_text": ""},
        {"metric": "core_unknown_ratio_delta", "value_numeric": round(after_core_unknown - before_core_unknown, 6), "value_text": ""},
        {"metric": "reported_baseline_quality_flag", "value_numeric": None, "value_text": reported_baseline_quality_flag},
        {"metric": "before_labeled_rows", "value_numeric": int(len(before_df)), "value_text": ""},
        {"metric": "after_labeled_rows", "value_numeric": int(len(after_df)), "value_text": ""},
        {"metric": "low_signal_rate", "value_numeric": round(low_signal_rate, 6), "value_text": ""},
        {"metric": "persona_core_eligible_rows", "value_numeric": int(after_df.get("persona_core_eligible", pd.Series(dtype=bool)).fillna(True).sum()), "value_text": ""},
    ]
    output_df = pd.DataFrame(rows)
    write_parquet(output_df, root_dir / "data" / "analysis" / "before_after_label_metrics.parquet")
    output_df.to_csv(root_dir / "data" / "analysis" / "before_after_label_metrics.csv", index=False)
    (root_dir / "data" / "analysis" / "before_after_label_metrics.md").write_text(
        "\n".join(
            [
                "# Before vs After Label Metrics",
                "",
                f"- before unknown ratio: `{before_unknown:.6f}`",
                f"- after unknown ratio: `{after_unknown:.6f}`",
                f"- delta: `{after_unknown - before_unknown:+.6f}`",
                f"- reported baseline unknown ratio: `{reported_baseline_unknown:.6f}`",
                f"- baseline to after delta: `{after_unknown - reported_baseline_unknown:+.6f}`",
                f"- before core-eligible unknown ratio: `{before_core_unknown:.6f}`",
                f"- after core-eligible unknown ratio: `{after_core_unknown:.6f}`",
                f"- core delta: `{after_core_unknown - before_core_unknown:+.6f}`",
                f"- reported baseline quality flag: `{reported_baseline_quality_flag or 'n/a'}`",
                f"- low-signal rate: `{low_signal_rate:.6f}`",
                f"- persona-core-eligible rows: `{int(after_df.get('persona_core_eligible', pd.Series(dtype=bool)).fillna(True).sum())}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _unknown_ratio(df) -> float:
    """Return the fraction of rows with any core family unknown."""
    if df is None or getattr(df, "empty", True):
        return 1.0
    mask = pd.Series(False, index=df.index)
    for column in ["role_codes", "question_codes", "pain_codes", "output_codes"]:
        if column in df.columns:
            mask = mask | df[column].fillna("").astype(str).eq("unknown")
    return float(mask.mean())


def _persona_core_subset(df):
    """Use persona-core-eligible rows when present."""
    if df is None or getattr(df, "empty", True) or "persona_core_eligible" not in df.columns:
        return df
    return df[df["persona_core_eligible"].fillna(True)]

def _env_flag(*keys: str, default: bool) -> bool:
    """Return the first matching true/false environment flag."""
    value = _first_non_empty_env(*keys, default=str(default).lower()).lower()
    return value == "true"


def _first_non_empty_env(*keys: str, default: str = "") -> str:
    """Return the first non-empty environment variable value."""
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


def _metric_lookup(audit_df: object) -> dict[str, float]:
    """Convert metric table to a plain lookup."""
    if audit_df is None or getattr(audit_df, "empty", True):
        return {}
    return {str(row["metric"]): row["value"] for _, row in audit_df.iterrows()}


if __name__ == "__main__":
    main()
