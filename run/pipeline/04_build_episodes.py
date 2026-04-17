"""Build episode table from valid candidates."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.episodes.builder import build_episode_outputs
from src.analysis.pipeline_thresholds import (
    evaluate_episode_thresholds,
    load_threshold_profile,
    summarize_stage_status,
    threshold_summary_message,
    upsert_threshold_audit,
)
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger
from src.utils.pipeline_schema import RAW_ID_FIELD, SOURCE_FIELD, source_row_count
from src.utils.record_access import get_raw_id, get_record_source

LOGGER = get_logger("run.build_episodes")


def main() -> None:
    """Create the episode table parquet."""
    valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    if valid_df.empty:
        valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates.parquet")
    rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
    episodes_df, debug_df, schema_df = build_episode_outputs(valid_df, rules)
    write_parquet(episodes_df, ROOT / "data" / "episodes" / "episode_table.parquet")
    write_parquet(debug_df, ROOT / "data" / "episodes" / "episode_debug.parquet")
    write_parquet(schema_df, ROOT / "data" / "episodes" / "parser_schema_diff.parquet")

    audit_df = _build_episode_audit(valid_df, episodes_df)
    write_parquet(audit_df, ROOT / "data" / "episodes" / "episode_audit.parquet")
    drop_breakdown_df = _build_episode_drop_breakdown(debug_df)
    write_parquet(drop_breakdown_df, ROOT / "data" / "episodes" / "episode_drop_breakdown.parquet")
    _write_source_before_after_sample(valid_df, debug_df, episodes_df, "hubspot_community", "hubspot_episode_before_after_sample.parquet")
    _write_source_before_after_sample(valid_df, debug_df, episodes_df, "shopify_community", "shopify_episode_before_after_sample.parquet")
    _write_shopify_quality_outputs(valid_df, debug_df, episodes_df)
    profile, profile_cfg = load_threshold_profile(ROOT / "config" / "pipeline_thresholds.yaml")
    threshold_df = evaluate_episode_thresholds(valid_df, episodes_df, profile, profile_cfg)
    combined_threshold_df = upsert_threshold_audit(ROOT, threshold_df)

    post_count = len(valid_df)
    episode_count = len(episodes_df)
    avg_per_post = round(episode_count / post_count, 2) if post_count else 0.0
    stage_status = summarize_stage_status(combined_threshold_df, "episode")
    LOGGER.info(
        "Wrote episode table: %s episodes from %s posts (avg %.2f per post); threshold profile=%s status=%s",
        episode_count,
        post_count,
        avg_per_post,
        profile,
        stage_status,
    )
    if not audit_df.empty:
        LOGGER.info("Episode audit written with source-level and per-post episode counts")
    if not drop_breakdown_df.empty:
        LOGGER.info("Episode debug written with per-row drop reasons and parser schema diff")
    if stage_status in {"warn", "fail"}:
        LOGGER.warning("Episode threshold summary: %s", threshold_summary_message(combined_threshold_df, "episode"))
    gate_mode = str(profile_cfg.get("gate_mode", {}).get("episode_gate", "warn"))
    if gate_mode == "strict" and stage_status == "fail":
        raise RuntimeError("Episode threshold failed under strict profile. See data/analysis/pipeline_threshold_audit.parquet")


def _build_episode_audit(valid_df, episodes_df):
    """Build episode count audit metrics for over-segmentation monitoring."""
    import pandas as pd

    source_rows: list[dict[str, str | int | float]] = []
    total_posts = len(valid_df)
    total_episodes = len(episodes_df)
    source_rows.append(
        {
            "audit_level": "overall_summary",
            "source": "ALL",
            "raw_id": "",
            "post_count": total_posts,
            "episode_count": total_episodes,
            "avg_episodes_per_post": round(total_episodes / total_posts, 2) if total_posts else 0.0,
        }
    )
    for source in sorted(valid_df.get(SOURCE_FIELD, pd.Series(dtype=str)).unique().tolist()):
        source_posts = source_row_count(valid_df, source)
        source_episodes = source_row_count(episodes_df, source)
        source_rows.append(
            {
                "audit_level": "source_summary",
                "source": source,
                "raw_id": "",
                "post_count": source_posts,
                "episode_count": source_episodes,
                "avg_episodes_per_post": round(source_episodes / source_posts, 2) if source_posts else 0.0,
            }
        )

    per_post_rows: list[dict[str, str | int | float]] = []
    episode_counts = (
        episodes_df.groupby([SOURCE_FIELD, RAW_ID_FIELD]).size().reset_index(name="episode_count")
        if not episodes_df.empty
        else pd.DataFrame(columns=[SOURCE_FIELD, RAW_ID_FIELD, "episode_count"])
    )
    episode_count_lookup = {
        (str(row[SOURCE_FIELD]), str(row[RAW_ID_FIELD])): int(row["episode_count"])
        for _, row in episode_counts.iterrows()
    }
    for _, row in valid_df.iterrows():
        source = get_record_source(row)
        raw_id = get_raw_id(row)
        episode_count = episode_count_lookup.get((source, raw_id), 0)
        per_post_rows.append(
            {
                "audit_level": "post_detail",
                "source": source,
                "raw_id": raw_id,
                "post_count": 1,
                "episode_count": episode_count,
                "avg_episodes_per_post": float(episode_count),
            }
        )
    return pd.DataFrame(source_rows + per_post_rows, columns=["audit_level", "source", "raw_id", "post_count", "episode_count", "avg_episodes_per_post"])


def _build_episode_drop_breakdown(debug_df):
    """Aggregate episode drop reasons by source and schema type."""
    import pandas as pd

    if debug_df.empty:
        return pd.DataFrame()
    dropped = debug_df[debug_df["episode_count"].fillna(0).astype(int) == 0].copy()
    if dropped.empty:
        return pd.DataFrame(columns=["source", "source_schema_type", "drop_reason", "drop_count"])
    return (
        dropped.groupby(["source", "source_schema_type", "drop_reason"], dropna=False)
        .size()
        .reset_index(name="drop_count")
        .sort_values(["source", "drop_count", "drop_reason"], ascending=[True, False, True])
        .reset_index(drop=True)
    )


def _write_source_before_after_sample(valid_df, debug_df, episodes_df, source: str, output_name: str) -> None:
    """Write a small before/after sample for one source episode debugging."""
    import pandas as pd

    source_valid = valid_df[valid_df[SOURCE_FIELD].astype(str) == str(source)].copy()
    if source_valid.empty:
        return
    sample = source_valid.head(20).copy()
    debug_cols = [
        "raw_id",
        "source_schema_type",
        "episode_count",
        "drop_reason",
        "drop_detail",
        "title_body_combined_used",
        "reply_like_schema",
        "passes_combined_quality",
    ]
    sample = sample.merge(debug_df[debug_cols], on="raw_id", how="left")
    sample["before_episode_count"] = 0
    actual_counts = (
        episodes_df[episodes_df[SOURCE_FIELD].astype(str) == str(source)]
        .groupby("raw_id")
        .size()
        .reset_index(name="after_episode_count")
    )
    sample = sample.merge(actual_counts, on="raw_id", how="left")
    sample["after_episode_count"] = sample["after_episode_count"].fillna(0).astype(int)
    write_parquet(sample, ROOT / "data" / "episodes" / output_name)


def _write_shopify_quality_outputs(valid_df, debug_df, episodes_df) -> None:
    """Write Shopify-specific borderline and false-negative audit outputs."""
    import pandas as pd

    source = "shopify_community"
    shop_valid = valid_df[valid_df[SOURCE_FIELD].astype(str) == source].copy()
    if shop_valid.empty:
        return
    shop_debug = debug_df[debug_df["source"].astype(str) == source].copy()
    if shop_debug.empty:
        return
    merged = shop_valid.merge(
        shop_debug[
            [
                "raw_id",
                "episode_count",
                "drop_reason",
                "drop_detail",
                "quality_score",
                "quality_bucket",
                "quality_fail_reason",
                "rescue_reason",
                "passes_combined_quality",
            ]
        ],
        on="raw_id",
        how="left",
    )
    failed = merged[merged["episode_count"].fillna(0).astype(int) == 0].copy()
    audit_sample = failed.head(100).copy()
    write_parquet(audit_sample, ROOT / "data" / "episodes" / "shopify_quality_false_negative_audit.parquet")
    audit_sample.to_csv(ROOT / "data" / "episodes" / "shopify_quality_false_negative_audit.csv", index=False)

    borderline = episodes_df[
        (episodes_df[SOURCE_FIELD].astype(str) == source) & (episodes_df.get("quality_bucket", pd.Series(dtype=str)).astype(str) == "borderline")
    ].copy()
    write_parquet(borderline, ROOT / "data" / "episodes" / "shopify_episode_borderline_review.parquet")
    borderline.to_csv(ROOT / "data" / "episodes" / "shopify_episode_borderline_review.csv", index=False)


if __name__ == "__main__":
    main()
