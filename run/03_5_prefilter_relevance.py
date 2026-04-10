"""Apply source-aware relevance prefiltering before episode building and labeling."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.filters.relevance import (
    apply_relevance_prefilter,
    build_before_after_comparison,
    build_prefilter_summary,
    build_reddit_subreddit_summary,
    build_source_ratio_summary,
    build_stackoverflow_tag_summary,
    build_top_negative_signal_report,
)
from src.utils.io import ensure_dir, load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.prefilter_relevance")


def main() -> None:
    """Apply Reddit/Stack Overflow relevance scoring and export QA reports."""
    rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")
    valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates.parquet")
    previous_invalid_df = read_parquet(ROOT / "data" / "valid" / "invalid_candidates.parquet")
    keep_df, borderline_df, drop_df = apply_relevance_prefilter(valid_df, rules)

    ensure_dir(ROOT / "data" / "prefilter")
    ensure_dir(ROOT / "data" / "analysis")
    write_parquet(keep_df, ROOT / "data" / "prefilter" / "relevance_keep.parquet")
    write_parquet(borderline_df, ROOT / "data" / "prefilter" / "relevance_borderline.parquet")
    write_parquet(drop_df, ROOT / "data" / "prefilter" / "relevance_drop.parquet")

    candidate_df = pd.concat([keep_df, borderline_df], ignore_index=True) if (not keep_df.empty or not borderline_df.empty) else keep_df
    write_parquet(candidate_df, ROOT / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    write_parquet(borderline_df, ROOT / "data" / "valid" / "borderline_candidates.parquet")
    if not drop_df.empty:
        drop_invalid_df = drop_df.copy()
        drop_invalid_df["invalid_reason"] = "low_relevance_prefilter"
        combined_invalid_df = pd.concat([previous_invalid_df, drop_invalid_df], ignore_index=True) if not previous_invalid_df.empty else drop_invalid_df
    else:
        combined_invalid_df = previous_invalid_df
    write_parquet(combined_invalid_df, ROOT / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

    result_df = pd.concat([keep_df, borderline_df, drop_df], ignore_index=True)
    summary_df = build_prefilter_summary(keep_df, borderline_df, drop_df)
    write_parquet(summary_df, ROOT / "data" / "analysis" / "prefilter_summary_report.parquet")
    summary_df.to_csv(ROOT / "data" / "analysis" / "prefilter_summary_report.csv", index=False)

    source_ratio_df = build_source_ratio_summary(result_df)
    write_parquet(source_ratio_df, ROOT / "data" / "analysis" / "prefilter_source_ratio_report.parquet")
    source_ratio_df.to_csv(ROOT / "data" / "analysis" / "prefilter_source_ratio_report.csv", index=False)

    negative_signal_df = build_top_negative_signal_report(result_df)
    write_parquet(negative_signal_df, ROOT / "data" / "analysis" / "prefilter_top_negative_signals.parquet")
    negative_signal_df.to_csv(ROOT / "data" / "analysis" / "prefilter_top_negative_signals.csv", index=False)

    subreddit_df = build_reddit_subreddit_summary(result_df)
    write_parquet(subreddit_df, ROOT / "data" / "analysis" / "prefilter_reddit_subreddit_summary.parquet")
    subreddit_df.to_csv(ROOT / "data" / "analysis" / "prefilter_reddit_subreddit_summary.csv", index=False)

    tag_df = build_stackoverflow_tag_summary(result_df)
    write_parquet(tag_df, ROOT / "data" / "analysis" / "prefilter_stackoverflow_tag_summary.parquet")
    tag_df.to_csv(ROOT / "data" / "analysis" / "prefilter_stackoverflow_tag_summary.csv", index=False)

    _write_sample_exports(keep_df, borderline_df, drop_df)
    _write_before_after_reports(valid_df, keep_df, borderline_df, valid_df, previous_invalid_df)

    LOGGER.info("Prefilter keep=%s borderline=%s drop=%s", len(keep_df), len(borderline_df), len(drop_df))


def _write_sample_exports(keep_df: pd.DataFrame, borderline_df: pd.DataFrame, drop_df: pd.DataFrame) -> None:
    """Write sample QA row exports."""
    sample_columns = [
        "source",
        "raw_id",
        "title",
        "subreddit_or_forum",
        "final_relevance_score",
        "relevance_decision",
        "top_positive_signals",
        "top_negative_signals",
        "source_specific_reason",
    ]
    for name, frame in [("kept", keep_df), ("borderline", borderline_df), ("dropped", drop_df)]:
        sample_df = frame.head(50).copy()
        for column in sample_columns:
            if column not in sample_df.columns:
                sample_df[column] = ""
        sample_df = sample_df[sample_columns]
        write_parquet(sample_df, ROOT / "data" / "analysis" / f"prefilter_{name}_sample_rows.parquet")
        sample_df.to_csv(ROOT / "data" / "analysis" / f"prefilter_{name}_sample_rows.csv", index=False)


def _write_before_after_reports(
    all_candidate_df: pd.DataFrame,
    keep_df: pd.DataFrame,
    borderline_df: pd.DataFrame,
    previous_valid_df: pd.DataFrame,
    previous_invalid_df: pd.DataFrame,
) -> None:
    """Write before-vs-after comparison reports for Reddit and Stack Overflow."""
    rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")
    normalized_full_df = read_parquet(ROOT / "data" / "normalized" / "normalized_posts.parquet")
    for source in ["reddit", "stackoverflow"]:
        full_source_df = normalized_full_df[normalized_full_df["source"] == source].reset_index(drop=True)
        full_keep_df, full_borderline_df, _ = apply_relevance_prefilter(full_source_df, rules)
        report_map = build_before_after_comparison(
            normalized_df=full_source_df,
            keep_df=full_keep_df,
            borderline_df=full_borderline_df,
            previous_valid_df=previous_valid_df,
            previous_invalid_df=previous_invalid_df,
            source=source,
            rules=rules,
            limit=25,
        )
        for label, frame in report_map.items():
            write_parquet(frame, ROOT / "data" / "analysis" / f"{source}_{label}.parquet")
            frame.to_csv(ROOT / "data" / "analysis" / f"{source}_{label}.csv", index=False)


if __name__ == "__main__":
    main()
