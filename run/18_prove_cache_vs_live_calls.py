"""Run a controlled experiment proving cache versus live OpenAI call behavior."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.labeling.audit import build_label_audit, build_labeling_audit, build_llm_experiment_summary
from src.labeling.labelability import build_labelability_table
from src.labeling.llm_labeler import (
    _build_prompt_payload,
    _build_target_rows,
    _cache_key_for_prompt,
    enrich_with_llm_labels,
    llm_runtime_snapshot,
    resolve_llm_runtime,
    should_send_to_llm,
)
from src.labeling.rule_labeler import prelabel_episodes
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.llm_cache import load_jsonl_cache
from src.utils.logging import get_logger
from src.utils.pipeline_schema import LABEL_CODE_COLUMNS
from src.utils.run_helpers import load_dotenv

LOGGER = get_logger("run.prove_cache_vs_live_calls")
DEBUG_MUTATION_FIELDS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
]


def main() -> None:
    """Run one or more controlled labeler scenarios and compare the results."""
    parser = _build_parser()
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    codebook = load_yaml(ROOT / "config" / "codebook.yaml")
    labeling_policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
    base_config = _base_llm_config(codebook=codebook, labeling_policy=labeling_policy)
    episodes_df = read_parquet(Path(args.input_path))
    if episodes_df.empty:
        raise RuntimeError(f"No episodes found at {args.input_path}")

    output_dir = ROOT / "data" / "analysis" / "llm_experiments" / args.audit_tag
    output_dir.mkdir(parents=True, exist_ok=True)

    preview_labeled_df = _prepare_labeled_input(episodes_df, labeling_policy, codebook)
    routing_preview_df = _build_routing_preview(episodes_df, preview_labeled_df, base_config, audit_tag=args.audit_tag)
    write_parquet(routing_preview_df, output_dir / "routing_preview.parquet")

    if args.scenario == "targeting_diagnostics" or args.targeting_diagnostics_only:
        diagnostic_df = routing_preview_df.head(args.limit).copy() if args.limit > 0 else routing_preview_df.copy()
        _print_dataframe("targeting_diagnostics", diagnostic_df[[
            "episode_id",
            "was_llm_targeted",
            "target_reason",
            "cache_source",
            "cache_key",
            "labelability_status",
            "label_confidence",
        ]])
        print(f"Wrote routing preview to {output_dir / 'routing_preview.parquet'}")
        return

    selected_input_df = _select_experiment_input(routing_preview_df, episodes_df, args.limit)
    write_parquet(selected_input_df, output_dir / "selected_input.parquet")

    scenario_rows: list[dict[str, object]] = []
    if args.scenario == "all":
        scenario_plan = [
            ("baseline_cached_run", selected_input_df.copy(), {}),
            (
                "cache_bypass_run",
                selected_input_df.copy(),
                {"disable_cache": True, "force_llm_for_targeted": True},
            ),
            (
                "fresh_rows_run",
                _make_fresh_debug_rows(selected_input_df, audit_tag=args.audit_tag),
                {},
            ),
        ]
    elif args.scenario == "custom":
        scenario_plan = [
            (
                "custom_run",
                selected_input_df.copy(),
                {
                    "disable_cache": bool(args.disable_llm_cache),
                    "force_llm_for_targeted": bool(args.force_llm_for_targeted),
                    "only_uncached": bool(args.only_uncached),
                },
            )
        ]
    else:
        scenario_lookup = {
            "baseline_cached_run": (selected_input_df.copy(), {}),
            "cache_bypass_run": (
                selected_input_df.copy(),
                {"disable_cache": True, "force_llm_for_targeted": True},
            ),
            "fresh_rows_run": (_make_fresh_debug_rows(selected_input_df, audit_tag=args.audit_tag), {}),
        }
        scenario_input_df, scenario_overrides = scenario_lookup[args.scenario]
        scenario_plan = [(args.scenario, scenario_input_df, scenario_overrides)]

    for scenario_name, scenario_input_df, scenario_overrides in scenario_plan:
        scenario_rows.append(
            _run_scenario(
                scenario_name=scenario_name,
                episodes_df=scenario_input_df,
                codebook=codebook,
                labeling_policy=labeling_policy,
                base_config=base_config,
                overrides=scenario_overrides,
                output_dir=output_dir,
                audit_tag=args.audit_tag,
            )
        )

    comparison_df = pd.DataFrame(scenario_rows)
    write_parquet(comparison_df, output_dir / "comparison.parquet")
    _print_dataframe("comparison", comparison_df)
    print(f"Artifacts written to {output_dir}")


def _build_parser() -> argparse.ArgumentParser:
    """Create CLI controls for the experiment and ad hoc diagnostics."""
    parser = argparse.ArgumentParser(description="Prove whether cache and targeting suppress live LLM calls.")
    parser.add_argument(
        "--scenario",
        choices=["all", "baseline_cached_run", "cache_bypass_run", "fresh_rows_run", "targeting_diagnostics", "custom"],
        default="all",
        help="Which scenario to run. 'all' compares baseline, cache bypass, and fresh rows side by side.",
    )
    parser.add_argument(
        "--input-path",
        default=str(ROOT / "data" / "episodes" / "episode_table.parquet"),
        help="Parquet input for the experiment sample selection.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=6,
        help="Maximum total rows per scenario. The script prefers cached targeted rows first, then non-targeted rows.",
    )
    parser.add_argument(
        "--audit-tag",
        default="prove_cache_vs_live_calls",
        help="Tag written into runtime snapshots and audit rows for this experiment.",
    )
    parser.add_argument("--disable-llm-cache", action="store_true", help="Opt-in ad hoc control for custom runs.")
    parser.add_argument("--only-uncached", action="store_true", help="Opt-in ad hoc control for custom runs.")
    parser.add_argument("--force-llm-for-targeted", action="store_true", help="Force live calls for targeted rows in custom runs.")
    parser.add_argument(
        "--targeting-diagnostics-only",
        action="store_true",
        help="Preview routing and cache diagnostics without making live calls.",
    )
    return parser


def _base_llm_config(codebook: dict[str, object], labeling_policy: dict[str, object]) -> dict[str, object]:
    """Mirror the production label-step LLM configuration defaults."""
    return {
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


def _prepare_labeled_input(
    episodes_df: pd.DataFrame,
    labeling_policy: dict[str, object],
    codebook: dict[str, object],
) -> pd.DataFrame:
    """Apply the production prelabel and low-signal gate before LLM routing."""
    labelability_df = build_labelability_table(episodes_df, labeling_policy)
    labeled_df = prelabel_episodes(episodes_df, codebook)
    labeled_df = labeled_df.merge(
        labelability_df[["episode_id", "labelability_status", "labelability_score", "labelability_reason", "persona_core_eligible"]],
        on="episode_id",
        how="left",
    )
    return _apply_low_signal_gate(labeled_df)


def _apply_low_signal_gate(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Mirror the production low-signal gate so targeting matches the main run."""
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


def _build_routing_preview(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    base_config: dict[str, object],
    audit_tag: str,
) -> pd.DataFrame:
    """Compute row-routing, cache, and targeting diagnostics without making live calls."""
    config = {**base_config, "audit_tag": f"{audit_tag}:preview", "dry_run": False, "disable_cache": False}
    runtime = resolve_llm_runtime(config)
    episode_lookup = episodes_df.drop_duplicates(subset=["episode_id"], keep="first").set_index("episode_id", drop=False)
    targeted_rows = _build_target_rows(labeled_df, runtime["threshold"], target_unknown_only=runtime["target_unknown_only"])
    cache_store = load_jsonl_cache(runtime["cache_path"]) if runtime["cache_enabled"] else {}

    rows: list[dict[str, object]] = []
    for _, row in labeled_df.iterrows():
        episode_id = str(row["episode_id"])
        should_target, target_reason = should_send_to_llm(row=row, threshold=runtime["threshold"])
        target_meta = targeted_rows.get(episode_id) if should_target else None
        record: dict[str, object] = {
            "episode_id": episode_id,
            "was_llm_targeted": bool(should_target and target_meta is not None),
            "target_reason": target_reason,
            "labelability_status": str(row.get("labelability_status", "") or ""),
            "label_confidence": float(row.get("label_confidence", 0.0) or 0.0),
            "cache_key": "",
            "cache_source": "",
            "prompt_chars": 0,
        }
        if not target_meta or episode_id not in episode_lookup.index:
            rows.append(record)
            continue
        prompt_payload = _build_prompt_payload(
            episode_row=episode_lookup.loc[episode_id],
            labeled_row=row,
            codebook=runtime["codebook"],
            target_meta=target_meta,
            policy=runtime["policy"],
        )
        cache_key = _cache_key_for_prompt(
            model=runtime["model_primary"],
            requested_families=prompt_payload["requested_families"],
            prompt=prompt_payload["prompt"],
        )
        record["cache_key"] = cache_key
        record["prompt_chars"] = len(prompt_payload["prompt"])
        record["cache_source"] = "persistent_cache" if cache_key in cache_store else "none"
        rows.append(record)
    return pd.DataFrame(rows)


def _select_experiment_input(routing_preview_df: pd.DataFrame, episodes_df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Choose a small mixed sample with cached targeted rows plus some non-targeted rows."""
    if limit <= 0:
        raise RuntimeError("--limit must be greater than zero")
    targeted_limit = max(1, min(3, limit))
    cached_targeted_df = routing_preview_df[
        routing_preview_df["was_llm_targeted"].fillna(False) & routing_preview_df["cache_source"].eq("persistent_cache")
    ].head(targeted_limit)
    if cached_targeted_df.empty:
        raise RuntimeError("Could not find any cached targeted rows for the baseline experiment sample.")
    remaining_limit = max(limit - len(cached_targeted_df), 0)
    not_targeted_df = routing_preview_df[~routing_preview_df["was_llm_targeted"].fillna(False)].head(remaining_limit)
    selected_ids = list(cached_targeted_df["episode_id"].astype(str)) + list(not_targeted_df["episode_id"].astype(str))
    selected_df = episodes_df[episodes_df["episode_id"].astype(str).isin(selected_ids)].copy()
    selected_df["_sample_order"] = pd.Categorical(selected_df["episode_id"].astype(str), categories=selected_ids, ordered=True)
    selected_df = selected_df.sort_values("_sample_order").drop(columns=["_sample_order"]).reset_index(drop=True)
    return selected_df


def _make_fresh_debug_rows(selected_input_df: pd.DataFrame, audit_tag: str) -> pd.DataFrame:
    """Clone the selected rows with unique debug markers so prompts are guaranteed uncached."""
    marker = f"debug_live::{audit_tag}::{uuid4().hex[:8]}"
    fresh_df = selected_input_df.copy()
    fresh_df["episode_id"] = fresh_df["episode_id"].astype(str).map(lambda value: f"debug_llm_experiment::{marker}::{value}")
    for field in DEBUG_MUTATION_FIELDS:
        if field in fresh_df.columns:
            fresh_df[field] = fresh_df[field].fillna("").astype(str).map(
                lambda value: f"{value}\n[debug uncached marker: {marker}]" if value else f"[debug uncached marker: {marker}]"
            )
    return fresh_df


def _run_scenario(
    scenario_name: str,
    episodes_df: pd.DataFrame,
    codebook: dict[str, object],
    labeling_policy: dict[str, object],
    base_config: dict[str, object],
    overrides: dict[str, object],
    output_dir: Path,
    audit_tag: str,
) -> dict[str, object]:
    """Run one scenario through the production labeler and persist all evidence artifacts."""
    llm_config = {**base_config, **overrides, "audit_tag": f"{audit_tag}:{scenario_name}"}
    runtime = resolve_llm_runtime(llm_config)
    LOGGER.info("Scenario %s runtime: %s", scenario_name, json.dumps(llm_runtime_snapshot(runtime), ensure_ascii=False, sort_keys=True))

    labeled_input_df = _prepare_labeled_input(episodes_df, labeling_policy, codebook)
    labeled_df, llm_audit_df = enrich_with_llm_labels(episodes_df, labeled_input_df, config=llm_config)
    audit_df = build_label_audit(labeled_df, llm_audit_df)
    labeling_audit_df = build_labeling_audit(labeled_df, llm_audit_df)
    experiment_summary_df = build_llm_experiment_summary(llm_audit_df, audit_df)

    write_parquet(episodes_df, output_dir / f"{scenario_name}_episodes.parquet")
    write_parquet(labeled_df, output_dir / f"{scenario_name}_labeled.parquet")
    write_parquet(llm_audit_df, output_dir / f"{scenario_name}_llm_label_audit.parquet")
    write_parquet(labeling_audit_df, output_dir / f"{scenario_name}_labeling_audit.parquet")
    write_parquet(audit_df, output_dir / f"{scenario_name}_label_audit.parquet")
    write_parquet(experiment_summary_df, output_dir / f"{scenario_name}_experiment_summary.parquet")

    summary_lookup = {
        str(column): experiment_summary_df.iloc[0][column]
        for column in experiment_summary_df.columns
    }
    request_ids = [
        value
        for value in llm_audit_df.get("request_id", pd.Series(dtype=str)).fillna("").astype(str).tolist()
        if value
    ]
    response_ids = [
        value
        for value in llm_audit_df.get("response_id", pd.Series(dtype=str)).fillna("").astype(str).tolist()
        if value
    ]

    return {
        "scenario": scenario_name,
        "audit_tag": str(summary_lookup.get("audit_tag", llm_config["audit_tag"])),
        "total_rows": int(summary_lookup.get("total_rows", len(episodes_df))),
        "targeted_rows": int(summary_lookup.get("targeted_rows", 0)),
        "skipped_by_targeting_count": int(summary_lookup.get("skipped_by_targeting_count", 0)),
        "cache_hit_count": int(summary_lookup.get("cache_hit_count", 0)),
        "run_reuse_count": int(summary_lookup.get("run_reuse_count", 0)),
        "served_from_cache_count": int(summary_lookup.get("served_from_cache_count", 0)),
        "uncached_targeted_count": int(summary_lookup.get("uncached_targeted_count", 0)),
        "live_call_attempt_count": int(summary_lookup.get("live_call_attempt_count", 0)),
        "live_call_success_count": int(summary_lookup.get("live_call_success_count", 0)),
        "live_call_failure_count": int(summary_lookup.get("live_call_failure_count", 0)),
        "retry_count_total": int(summary_lookup.get("retry_count_total", 0)),
        "fallback_count": int(summary_lookup.get("fallback_count", 0)),
        "usage_present_count": int(summary_lookup.get("usage_present_count", 0)),
        "usage_input_tokens_total": int(summary_lookup.get("usage_input_tokens_total", 0)),
        "usage_output_tokens_total": int(summary_lookup.get("usage_output_tokens_total", 0)),
        "usage_total_tokens_total": int(summary_lookup.get("usage_total_tokens_total", 0)),
        "percent_targeted_from_cache": float(summary_lookup.get("percent_targeted_from_cache", 0.0)),
        "percent_targeted_live": float(summary_lookup.get("percent_targeted_live", 0.0)),
        "request_id_count": int(summary_lookup.get("request_id_count", 0)),
        "response_id_count": int(summary_lookup.get("response_id_count", 0)),
        "request_ids": " | ".join(request_ids[:3]),
        "response_ids": " | ".join(response_ids[:3]),
        "api_key_masked": str(llm_runtime_snapshot(runtime).get("api_key_masked", "")),
        "api_key_project_scoped": bool(llm_runtime_snapshot(runtime).get("api_key_project_scoped", False)),
        "openai_organization": str(llm_runtime_snapshot(runtime).get("organization", "")),
        "openai_project": str(llm_runtime_snapshot(runtime).get("project", "")),
        "model_primary": str(llm_runtime_snapshot(runtime).get("model_primary", "")),
        "endpoint_used": str(llm_runtime_snapshot(runtime).get("responses_endpoint", "")),
    }


def _print_dataframe(name: str, dataframe: pd.DataFrame) -> None:
    """Print a compact dataframe for terminal-friendly experiment output."""
    print(f"\n[{name}]")
    if dataframe.empty:
        print("<empty>")
        return
    printable = dataframe.copy()
    for column in printable.columns:
        printable[column] = printable[column].map(lambda value: value if not isinstance(value, float) else round(value, 2))
    print(printable.to_string(index=False))


def _env_flag(*keys: str, default: bool = False) -> bool:
    """Read a boolean environment flag using the first non-empty key."""
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value.lower() == "true"
    return default


def _first_non_empty_env(*keys: str, default: str = "") -> str:
    """Return the first non-empty environment variable value."""
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


if __name__ == "__main__":
    main()