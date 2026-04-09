"""Build episode table from valid candidates."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.episodes.builder import build_episode_table
from src.analysis.pipeline_thresholds import (
    evaluate_episode_thresholds,
    load_threshold_profile,
    summarize_stage_status,
    threshold_summary_message,
    upsert_threshold_audit,
)
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.build_episodes")


def main() -> None:
    """Create the episode table parquet."""
    valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates.parquet")
    rules = load_yaml(ROOT / "config" / "segmentation_rules.yaml")
    episodes_df = build_episode_table(valid_df, rules)
    write_parquet(episodes_df, ROOT / "data" / "episodes" / "episode_table.parquet")

    audit_df = _build_episode_audit(valid_df, episodes_df)
    write_parquet(audit_df, ROOT / "data" / "episodes" / "episode_audit.parquet")
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
    for source in sorted(valid_df.get("source", pd.Series(dtype=str)).unique().tolist()):
        source_posts = int((valid_df["source"] == source).sum())
        source_episodes = int((episodes_df["source"] == source).sum()) if not episodes_df.empty else 0
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
        episodes_df.groupby(["source", "raw_id"]).size().reset_index(name="episode_count")
        if not episodes_df.empty
        else pd.DataFrame(columns=["source", "raw_id", "episode_count"])
    )
    for _, row in valid_df.iterrows():
        source = str(row.get("source", ""))
        raw_id = str(row.get("raw_id", ""))
        match = episode_counts[(episode_counts["source"] == source) & (episode_counts["raw_id"] == raw_id)]
        episode_count = int(match["episode_count"].iloc[0]) if not match.empty else 0
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


if __name__ == "__main__":
    main()
