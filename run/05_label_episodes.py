"""Label episodes with rule-first logic and token-aware LLM fallback."""

from __future__ import annotations

import os
import sys
from pathlib import Path

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
from src.labeling.llm_labeler import enrich_with_llm_labels, resolve_llm_runtime
from src.labeling.rule_labeler import prelabel_episodes
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger
from src.utils.run_helpers import load_dotenv

LOGGER = get_logger("run.label_episodes")


def main() -> None:
    """Create labeled artifacts and token-usage audit for direct or batch mode."""
    load_dotenv(ROOT / ".env")

    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    codebook = load_yaml(ROOT / "config" / "codebook.yaml")
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
    }

    labeled_df = prelabel_episodes(episodes_df, codebook)
    write_parquet(labeled_df, ROOT / "data" / "labeled" / "labeled_episodes_rule_only.parquet")
    runtime = resolve_llm_runtime(llm_config)
    labeled_df, llm_audit_df = enrich_with_llm_labels(episodes_df, labeled_df, config=llm_config)
    audit_df = build_label_audit(labeled_df, llm_audit_df)
    labeling_audit_df = build_labeling_audit(labeled_df, llm_audit_df)

    write_parquet(labeled_df, ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    write_parquet(audit_df, ROOT / "data" / "labeled" / "label_audit.parquet")
    write_parquet(labeling_audit_df, ROOT / "data" / "labeled" / "labeling_audit.parquet")
    write_parquet(llm_audit_df, ROOT / "data" / "labeled" / "llm_label_audit.parquet")
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
    gate_mode = str(profile_cfg.get("gate_mode", {}).get("labeling_gate", "warn"))
    if gate_mode == "strict" and stage_status == "fail":
        raise RuntimeError("Labeling threshold failed under strict profile. See data/analysis/pipeline_threshold_audit.parquet")

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
