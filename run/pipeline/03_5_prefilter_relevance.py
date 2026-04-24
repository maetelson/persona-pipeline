"""Apply source-aware relevance prefiltering before episode building and labeling."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
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
from src.utils.io import write_jsonl
from src.utils.logging import get_logger

LOGGER = get_logger("run.prefilter_relevance")


def main() -> None:
    """Apply Reddit/Stack Overflow relevance scoring and export QA reports."""
    args = _parse_args()
    rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")
    valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates.parquet")
    previous_invalid_df = read_parquet(ROOT / "data" / "valid" / "invalid_candidates.parquet")
    selected_sources = _normalize_selected_sources(args.source)
    selected_valid_df = _slice_selected_sources(valid_df, selected_sources)
    keep_df, borderline_df, drop_df = apply_relevance_prefilter(selected_valid_df, rules)

    merged_keep_df = _merge_source_rows(
        existing_df=read_parquet(ROOT / "data" / "prefilter" / "relevance_keep.parquet"),
        updated_df=keep_df,
        selected_sources=selected_sources,
    )
    merged_borderline_df = _merge_source_rows(
        existing_df=read_parquet(ROOT / "data" / "prefilter" / "relevance_borderline.parquet"),
        updated_df=borderline_df,
        selected_sources=selected_sources,
    )
    merged_drop_df = _merge_source_rows(
        existing_df=read_parquet(ROOT / "data" / "prefilter" / "relevance_drop.parquet"),
        updated_df=drop_df,
        selected_sources=selected_sources,
    )

    ensure_dir(ROOT / "data" / "prefilter")
    ensure_dir(ROOT / "data" / "analysis")
    write_parquet(merged_keep_df, ROOT / "data" / "prefilter" / "relevance_keep.parquet")
    write_parquet(merged_borderline_df, ROOT / "data" / "prefilter" / "relevance_borderline.parquet")
    write_parquet(merged_drop_df, ROOT / "data" / "prefilter" / "relevance_drop.parquet")

    candidate_df = _concat_frames(merged_keep_df, merged_borderline_df)
    write_parquet(candidate_df, ROOT / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    write_parquet(merged_borderline_df, ROOT / "data" / "valid" / "borderline_candidates.parquet")
    combined_invalid_df = _build_invalid_with_prefilter(
        previous_invalid_df=previous_invalid_df,
        merged_drop_df=merged_drop_df,
        selected_sources=selected_sources,
        existing_invalid_with_prefilter_df=read_parquet(ROOT / "data" / "valid" / "invalid_candidates_with_prefilter.parquet"),
    )
    write_parquet(combined_invalid_df, ROOT / "data" / "valid" / "invalid_candidates_with_prefilter.parquet")

    result_df = _concat_frames(merged_keep_df, merged_borderline_df, merged_drop_df)
    summary_df = build_prefilter_summary(merged_keep_df, merged_borderline_df, merged_drop_df)
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

    _write_sample_exports(merged_keep_df, merged_borderline_df, merged_drop_df)
    _write_false_negative_audit(merged_drop_df)
    _write_before_after_reports(
        valid_df=valid_df,
        keep_df=merged_keep_df,
        borderline_df=merged_borderline_df,
        previous_valid_df=valid_df,
        previous_invalid_df=previous_invalid_df,
        selected_sources=selected_sources,
    )

    if selected_sources:
        LOGGER.info("Prefilter source-scoped rerun for %s", ",".join(sorted(selected_sources)))
    LOGGER.info("Prefilter keep=%s borderline=%s drop=%s", len(merged_keep_df), len(merged_borderline_df), len(merged_drop_df))


def _parse_args() -> argparse.Namespace:
    """Parse optional source-scoped rerun arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        dest="source",
        action="append",
        default=[],
        help="Limit rescoring to one source id. May be repeated.",
    )
    return parser.parse_args()


def _normalize_selected_sources(raw_sources: list[str] | None) -> set[str]:
    """Return normalized source ids for source-scoped reruns."""
    return {
        str(source).strip()
        for source in raw_sources or []
        if str(source).strip()
    }


def _slice_selected_sources(df: pd.DataFrame, selected_sources: set[str]) -> pd.DataFrame:
    """Return the full frame or the selected source subset."""
    if df.empty or not selected_sources:
        return df.copy()
    return df[df["source"].astype(str).isin(selected_sources)].copy().reset_index(drop=True)


def _concat_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    """Concatenate non-empty frames while preserving schema when all are empty."""
    non_empty = [frame.copy() for frame in frames if not frame.empty]
    if non_empty:
        return pd.concat(non_empty, ignore_index=True)
    for frame in frames:
        if not frame.empty or len(frame.columns) > 0:
            return frame.iloc[0:0].copy()
    return pd.DataFrame()


def _merge_source_rows(existing_df: pd.DataFrame, updated_df: pd.DataFrame, selected_sources: set[str]) -> pd.DataFrame:
    """Replace touched-source rows while preserving untouched rows."""
    if not selected_sources:
        return updated_df.copy().reset_index(drop=True)
    existing_trimmed_df = existing_df
    if not existing_trimmed_df.empty and "source" in existing_trimmed_df.columns:
        existing_trimmed_df = existing_trimmed_df[~existing_trimmed_df["source"].astype(str).isin(selected_sources)].copy()
    return _concat_frames(existing_trimmed_df, updated_df).reset_index(drop=True)


def _build_invalid_with_prefilter(
    previous_invalid_df: pd.DataFrame,
    merged_drop_df: pd.DataFrame,
    selected_sources: set[str],
    existing_invalid_with_prefilter_df: pd.DataFrame,
) -> pd.DataFrame:
    """Rebuild invalid-with-prefilter using source-scoped replacement for prefilter drops."""
    drop_invalid_df = merged_drop_df.copy()
    if not drop_invalid_df.empty:
        drop_invalid_df["invalid_reason"] = "low_relevance_prefilter"

    if not selected_sources:
        return _concat_frames(previous_invalid_df, drop_invalid_df).reset_index(drop=True)

    preserved_existing_df = existing_invalid_with_prefilter_df.copy()
    if not preserved_existing_df.empty and {"source", "invalid_reason"}.issubset(preserved_existing_df.columns):
        mask = (
            preserved_existing_df["source"].astype(str).isin(selected_sources)
            & preserved_existing_df["invalid_reason"].fillna("").astype(str).eq("low_relevance_prefilter")
        )
        preserved_existing_df = preserved_existing_df[~mask].copy()
    return _concat_frames(preserved_existing_df, drop_invalid_df).reset_index(drop=True)


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


def _write_false_negative_audit(drop_df: pd.DataFrame) -> None:
    """Write top dropped rows per source for manual false-negative review."""
    if drop_df.empty:
        empty = pd.DataFrame(columns=["source", "raw_id", "title", "prefilter_score", "whitelist_hits", "rescue_reason", "dropped_reason"])
        write_parquet(empty, ROOT / "data" / "analysis" / "prefilter_false_negative_audit.parquet")
        write_jsonl(ROOT / "data" / "analysis" / "prefilter_false_negative_audit.jsonl", [])
        return
    audit_columns = [
        "source",
        "raw_id",
        "title",
        "prefilter_score",
        "whitelist_hits",
        "rescue_reason",
        "dropped_reason",
        "top_positive_signals",
        "top_negative_signals",
        "prefilter_reason",
        "body",
        "comments_text",
    ]
    frame = drop_df.copy()
    for column in audit_columns:
        if column not in frame.columns:
            frame[column] = ""
    ranked = frame.sort_values(["source", "prefilter_score", "raw_id"], ascending=[True, False, True])
    audit_df = ranked.groupby("source", dropna=False).head(50).reset_index(drop=True)[audit_columns]
    write_parquet(audit_df, ROOT / "data" / "analysis" / "prefilter_false_negative_audit.parquet")
    audit_df.to_csv(ROOT / "data" / "analysis" / "prefilter_false_negative_audit.csv", index=False)
    write_jsonl(ROOT / "data" / "analysis" / "prefilter_false_negative_audit.jsonl", audit_df.to_dict(orient="records"))
    shopify_audit_df = audit_df[audit_df["source"].astype(str) == "shopify_community"].reset_index(drop=True)
    write_parquet(shopify_audit_df, ROOT / "data" / "analysis" / "shopify_prefilter_audit_top50.parquet")
    shopify_audit_df.to_csv(ROOT / "data" / "analysis" / "shopify_prefilter_audit_top50.csv", index=False)
    write_jsonl(ROOT / "data" / "analysis" / "shopify_prefilter_audit_top50.jsonl", shopify_audit_df.to_dict(orient="records"))


def _write_before_after_reports(
    valid_df: pd.DataFrame,
    keep_df: pd.DataFrame,
    borderline_df: pd.DataFrame,
    previous_valid_df: pd.DataFrame,
    previous_invalid_df: pd.DataFrame,
    selected_sources: set[str],
) -> None:
    """Write before-vs-after comparison reports for Reddit and Stack Overflow."""
    rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")
    for source in ["reddit", "stackoverflow"]:
        if selected_sources and source not in selected_sources:
            LOGGER.info("Skipping %s before/after reports during source-scoped rerun.", source)
            continue
        source_valid_df = valid_df[valid_df["source"] == source].reset_index(drop=True)
        if source_valid_df.empty:
            continue
        target_previous_valid_df = previous_valid_df[previous_valid_df["source"] == source].reset_index(drop=True)
        target_previous_invalid_df = previous_invalid_df[previous_invalid_df["source"] == source].reset_index(drop=True)
        target_keep_df = keep_df[keep_df["source"] == source].reset_index(drop=True)
        target_borderline_df = borderline_df[borderline_df["source"] == source].reset_index(drop=True)
        report_map = build_before_after_comparison(
            normalized_df=source_valid_df,
            keep_df=target_keep_df,
            borderline_df=target_borderline_df,
            previous_valid_df=target_previous_valid_df,
            previous_invalid_df=target_previous_invalid_df,
            source=source,
            rules=rules,
            limit=25,
        )
        for label, frame in report_map.items():
            write_parquet(frame, ROOT / "data" / "analysis" / f"{source}_{label}.parquet")
            frame.to_csv(ROOT / "data" / "analysis" / f"{source}_{label}.csv", index=False)


if __name__ == "__main__":
    main()
