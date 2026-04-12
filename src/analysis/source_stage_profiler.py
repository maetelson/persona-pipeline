"""Stage-level source profiling from collection through labelability handoff."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import Any

import pandas as pd

from src.collectors.github_discussions_collector import GitHubDiscussionsCollector
from src.collectors.reddit_collector import RedditCollector
from src.collectors.stackoverflow_collector import StackOverflowCollector
from src.episodes.builder import build_episode_outputs
from src.filters.dedupe import split_duplicate_posts
from src.filters.invalid_filter import activate_rule_mode, apply_invalid_filter
from src.filters.relevance import apply_relevance_prefilter
from src.labeling.labelability import build_labelability_table
from src.normalizers.base import NORMALIZED_POST_COLUMNS
from src.normalizers.github_discussions_normalizer import GitHubDiscussionsNormalizer
from src.normalizers.reddit_normalizer import RedditNormalizer
from src.normalizers.stackoverflow_normalizer import StackOverflowNormalizer
from src.utils.io import ensure_dir, load_yaml, write_parquet

STAGE_ORDER = [
    "collect",
    "normalize",
    "validate",
    "dedupe",
    "prefilter",
    "episode_build",
    "labelability_handoff",
]

COLLECTOR_COMPARISON_COLUMNS = [
    "request_count",
    "request_success_count",
    "request_failure_count",
    "request_retry_count",
    "request_seconds_total",
    "search_request_count",
    "search_request_seconds",
    "comments_request_count",
    "comments_request_seconds",
    "rest_request_count",
    "rest_request_seconds",
    "graphql_request_count",
    "graphql_request_seconds",
    "backoff_sleep_seconds",
    "configured_sleep_seconds",
    "total_sleep_seconds",
    "pagination_iterations",
    "page_fetch_seconds",
    "listing_page_count",
    "average_items_per_page",
    "average_kept_items_per_page",
    "query_overlap_skip_count",
    "comment_fetch_count",
    "comment_skip_count",
    "pagination_low_yield_stop_count",
    "pagination_overlap_stop_count",
    "pagination_rolling_retention_stop_count",
    "pagination_seed_page_cap_stop_count",
    "pagination_repeated_cursor_stop_count",
    "rate_limit_header_seen_count",
    "rate_limit_remaining_min",
]

REDDIT_PIPELINE_PATH = [
    {
        "stage": "collect",
        "file_path": "run/01_collect_all.py",
        "function": "main -> RedditCollector.collect -> BaseCollector.collect_with_pagination -> RedditCollector._fetch_page -> RedditCollector._fetch_json / RedditCollector._fetch_comments / RedditCollector._build_raw_record",
        "purpose": "Expanded-query Reddit search pagination, per-post comment fetch fanout, retry/backoff handling, and raw record assembly.",
    },
    {
        "stage": "normalize",
        "file_path": "run/02_normalize_all.py",
        "function": "main -> RedditNormalizer.normalize_rows -> RedditNormalizer.normalize_row",
        "purpose": "Raw Reddit post/comment payloads into the shared normalized schema.",
    },
    {
        "stage": "validate",
        "file_path": "run/03_filter_valid.py",
        "function": "main -> apply_invalid_filter",
        "purpose": "Invalid-rule screening before dedupe.",
    },
    {
        "stage": "dedupe",
        "file_path": "run/03_filter_valid.py",
        "function": "main -> split_duplicate_posts",
        "purpose": "Source-scoped dedupe after validation.",
    },
    {
        "stage": "prefilter",
        "file_path": "run/03_5_prefilter_relevance.py",
        "function": "main -> apply_relevance_prefilter",
        "purpose": "Relevance keep/borderline/drop scoring before episode construction.",
    },
    {
        "stage": "episode_build",
        "file_path": "run/04_build_episodes.py",
        "function": "main -> build_episode_outputs",
        "purpose": "Episode segmentation over prefiltered valid candidates.",
    },
    {
        "stage": "labelability_handoff",
        "file_path": "run/05_label_episodes.py",
        "function": "main -> build_labelability_table",
        "purpose": "Deterministic labelability gate before any LLM work.",
    },
]


@dataclass(slots=True)
class SourceRuntimeArtifacts:
    """In-memory profiling outputs for one source."""

    summary_row: dict[str, Any]
    stage_rows: list[dict[str, Any]]


def profile_sources(root_dir: Path, sources: list[str]) -> dict[str, Path]:
    """Profile the requested sources and write summary artifacts."""
    analysis_dir = ensure_dir(root_dir / "data" / "analysis")
    rules = _load_rules(root_dir)
    artifacts = [profile_one_source(root_dir, source, rules) for source in sources]

    summary_df = pd.DataFrame([item.summary_row for item in artifacts])
    if not summary_df.empty:
        summary_df = summary_df.sort_values("total_pipeline_seconds", ascending=False).reset_index(drop=True)

    stage_df = pd.DataFrame([row for item in artifacts for row in item.stage_rows])
    if not stage_df.empty:
        total_lookup = {
            str(row["source"]): float(row["total_pipeline_seconds"])
            for _, row in summary_df.iterrows()
        }
        stage_df["source_total_seconds"] = stage_df["source"].map(total_lookup).fillna(0.0)
        stage_df["elapsed_share_of_total"] = stage_df.apply(
            lambda row: _safe_ratio(float(row.get("elapsed_seconds", 0.0)), float(row.get("source_total_seconds", 0.0))),
            axis=1,
        )
        stage_df["stage_order"] = stage_df["stage"].map({name: index for index, name in enumerate(STAGE_ORDER)})
        stage_df = stage_df.sort_values(["source", "stage_order"]).drop(columns=["stage_order"]).reset_index(drop=True)

    path_df = pd.DataFrame(REDDIT_PIPELINE_PATH)
    outputs = {
        "summary_csv": analysis_dir / "source_stage_profile_summary.csv",
        "summary_parquet": analysis_dir / "source_stage_profile_summary.parquet",
        "stage_csv": analysis_dir / "source_stage_profile_stages.csv",
        "stage_parquet": analysis_dir / "source_stage_profile_stages.parquet",
        "reddit_path_csv": analysis_dir / "reddit_stage_path.csv",
        "reddit_path_parquet": analysis_dir / "reddit_stage_path.parquet",
        "report_md": analysis_dir / "source_stage_profile_report.md",
    }
    summary_df.to_csv(outputs["summary_csv"], index=False)
    write_parquet(summary_df, outputs["summary_parquet"])
    stage_df.to_csv(outputs["stage_csv"], index=False)
    write_parquet(stage_df, outputs["stage_parquet"])
    path_df.to_csv(outputs["reddit_path_csv"], index=False)
    write_parquet(path_df, outputs["reddit_path_parquet"])
    outputs["report_md"].write_text(render_profile_report(summary_df, stage_df), encoding="utf-8")
    return outputs


def profile_one_source(
    root_dir: Path,
    source: str,
    rules: dict[str, Any],
) -> SourceRuntimeArtifacts:
    """Run collection through labelability handoff for one source."""
    collector = _build_collector(root_dir, source)
    normalizer = _build_normalizer(source)
    stage_rows: list[dict[str, Any]] = []

    collect_started_at = time.perf_counter()
    raw_records = collector.collect()
    collect_seconds = time.perf_counter() - collect_started_at
    raw_rows = [record.to_dict() for record in raw_records]
    collector_metrics = collector.get_profile_metrics()
    request_count = int(collector_metrics.get("request_count", 0))
    request_retry_count = int(collector_metrics.get("request_retry_count", 0))
    pagination_iterations = int(collector_metrics.get("pagination_iterations", 0))
    stage_rows.append(
        _stage_row(
            source=source,
            stage="collect",
            elapsed_seconds=collect_seconds,
            input_count=0,
            output_count=len(raw_rows),
            dropped_count=0,
            notes=(
                f"requests={request_count}; "
                f"request_seconds={float(collector_metrics.get('request_seconds_total', 0.0)):.3f}; "
                f"retries={request_retry_count}; "
                f"backoff_sleep_seconds={float(collector_metrics.get('backoff_sleep_seconds', 0.0)):.3f}; "
                f"configured_sleep_seconds={float(collector_metrics.get('configured_sleep_seconds', 0.0)):.3f}; "
                f"pagination_iterations={pagination_iterations}; "
                f"avg_items_per_page={float(collector_metrics.get('average_items_per_page', 0.0)):.2f}; "
                f"avg_kept_items_per_page={float(collector_metrics.get('average_kept_items_per_page', 0.0)):.2f}; "
                f"comment_fetches={int(collector_metrics.get('comment_fetch_count', 0))}; "
                f"comment_skips={int(collector_metrics.get('comment_skip_count', 0))}; "
                f"overlap_skips={int(collector_metrics.get('query_overlap_skip_count', 0))}; "
                f"rate_limit_headers={int(collector_metrics.get('rate_limit_header_seen_count', 0))}"
            ),
        )
    )

    normalize_started_at = time.perf_counter()
    normalized_df = normalizer.normalize_rows(raw_rows) if raw_rows else pd.DataFrame(columns=NORMALIZED_POST_COLUMNS)
    normalize_seconds = time.perf_counter() - normalize_started_at
    stage_rows.append(
        _stage_row(
            source=source,
            stage="normalize",
            elapsed_seconds=normalize_seconds,
            input_count=len(raw_rows),
            output_count=len(normalized_df),
            dropped_count=max(len(raw_rows) - len(normalized_df), 0),
            notes="normalize_rows over in-memory raw rows",
        )
    )

    validate_started_at = time.perf_counter()
    valid_before_dedupe_df, invalid_rule_df = apply_invalid_filter(normalized_df, rules["invalid_rules"])
    validate_seconds = time.perf_counter() - validate_started_at
    stage_rows.append(
        _stage_row(
            source=source,
            stage="validate",
            elapsed_seconds=validate_seconds,
            input_count=len(normalized_df),
            output_count=len(valid_before_dedupe_df),
            dropped_count=len(invalid_rule_df),
            notes=f"invalid_rule_drops={len(invalid_rule_df)}",
        )
    )

    dedupe_started_at = time.perf_counter()
    valid_df, duplicate_invalid_df = split_duplicate_posts(valid_before_dedupe_df)
    dedupe_seconds = time.perf_counter() - dedupe_started_at
    stage_rows.append(
        _stage_row(
            source=source,
            stage="dedupe",
            elapsed_seconds=dedupe_seconds,
            input_count=len(valid_before_dedupe_df),
            output_count=len(valid_df),
            dropped_count=len(duplicate_invalid_df),
            notes=f"duplicate_rows={len(duplicate_invalid_df)}",
        )
    )

    prefilter_started_at = time.perf_counter()
    kept_df, borderline_df, rejected_df = apply_relevance_prefilter(valid_df, rules["relevance_rules"])
    prefilter_seconds = time.perf_counter() - prefilter_started_at
    stage_rows.append(
        _stage_row(
            source=source,
            stage="prefilter",
            elapsed_seconds=prefilter_seconds,
            input_count=len(valid_df),
            output_count=len(kept_df),
            dropped_count=len(borderline_df) + len(rejected_df),
            notes=f"keep={len(kept_df)}; borderline={len(borderline_df)}; drop={len(rejected_df)}",
        )
    )

    episode_started_at = time.perf_counter()
    episodes_df, _, _ = build_episode_outputs(kept_df, rules["segmentation_rules"])
    episode_seconds = time.perf_counter() - episode_started_at
    stage_rows.append(
        _stage_row(
            source=source,
            stage="episode_build",
            elapsed_seconds=episode_seconds,
            input_count=len(kept_df),
            output_count=len(episodes_df),
            dropped_count=max(len(kept_df) - len(episodes_df), 0),
            notes="build_episode_outputs on prefiltered kept rows",
        )
    )

    labelability_started_at = time.perf_counter()
    labelability_df = build_labelability_table(episodes_df, rules["labeling_policy"])
    labelability_seconds = time.perf_counter() - labelability_started_at
    labelable_count = 0
    borderline_count = 0
    if not labelability_df.empty and "labelability_status" in labelability_df.columns:
        statuses = labelability_df["labelability_status"].astype(str)
        labelable_count = int((statuses == "labelable").sum())
        borderline_count = int((statuses == "borderline").sum())
    stage_rows.append(
        _stage_row(
            source=source,
            stage="labelability_handoff",
            elapsed_seconds=labelability_seconds,
            input_count=len(episodes_df),
            output_count=labelable_count + borderline_count,
            dropped_count=max(len(episodes_df) - labelable_count - borderline_count, 0),
            notes=(
                f"labelable={labelable_count}; borderline={borderline_count}; "
                f"low_signal={max(len(episodes_df) - labelable_count - borderline_count, 0)}"
            ),
        )
    )

    stage_seconds = {
        "collect_seconds": round(collect_seconds, 6),
        "normalize_seconds": round(normalize_seconds, 6),
        "validate_seconds": round(validate_seconds, 6),
        "dedupe_seconds": round(dedupe_seconds, 6),
        "prefilter_seconds": round(prefilter_seconds, 6),
        "episode_build_seconds": round(episode_seconds, 6),
        "labelability_handoff_seconds": round(labelability_seconds, 6),
    }
    total_pipeline_seconds = round(sum(stage_seconds.values()), 6)
    top_stage_name, top_stage_seconds = max(stage_seconds.items(), key=lambda item: item[1])
    summary_row = {
        "source": source,
        **stage_seconds,
        "total_pipeline_seconds": total_pipeline_seconds,
        "top_stage": top_stage_name.replace("_seconds", ""),
        "top_stage_seconds": round(top_stage_seconds, 6),
        "raw_record_count": len(raw_rows),
        "normalized_count": len(normalized_df),
        "valid_before_dedupe_count": len(valid_before_dedupe_df),
        "valid_after_dedupe_count": len(valid_df),
        "invalid_rule_count": len(invalid_rule_df),
        "duplicate_removed_count": len(duplicate_invalid_df),
        "prefilter_keep_count": len(kept_df),
        "prefilter_borderline_count": len(borderline_df),
        "prefilter_drop_count": len(rejected_df),
        "episode_count": len(episodes_df),
        "labelable_episode_count": labelable_count,
        "borderline_episode_count": borderline_count,
        "low_signal_episode_count": max(len(episodes_df) - labelable_count - borderline_count, 0),
        "prefilter_keep_ratio": _safe_ratio(len(kept_df), len(valid_df)),
        "episode_per_kept_ratio": _safe_ratio(len(episodes_df), len(kept_df)),
        "collect_seconds_per_raw_record": _safe_ratio(collect_seconds, len(raw_rows)),
        "requests_per_raw_record": _safe_ratio(request_count, len(raw_rows)),
        "request_seconds_per_raw_record": _safe_ratio(float(collector_metrics.get("request_seconds_total", 0.0)), len(raw_rows)),
        "request_seconds_share_of_collect": _safe_ratio(
            float(collector_metrics.get("request_seconds_total", 0.0)),
            collect_seconds,
        ),
        "backoff_sleep_share_of_collect": _safe_ratio(
            float(collector_metrics.get("backoff_sleep_seconds", 0.0)),
            collect_seconds,
        ),
        "configured_sleep_share_of_collect": _safe_ratio(
            float(collector_metrics.get("configured_sleep_seconds", 0.0)),
            collect_seconds,
        ),
        "total_sleep_share_of_collect": _safe_ratio(
            float(collector_metrics.get("total_sleep_seconds", 0.0)),
            collect_seconds,
        ),
        "page_fetch_seconds_share_of_collect": _safe_ratio(
            float(collector_metrics.get("page_fetch_seconds", 0.0)),
            collect_seconds,
        ),
        **collector_metrics,
    }
    return SourceRuntimeArtifacts(summary_row=summary_row, stage_rows=stage_rows)


def render_profile_report(summary_df: pd.DataFrame, stage_df: pd.DataFrame) -> str:
    """Render a markdown diagnosis from the profiling outputs."""
    lines = [
        "# Source Stage Profile Report",
        "",
        "## Exact Reddit Path",
        "",
        "| stage | file | function path | purpose |",
        "| --- | --- | --- | --- |",
    ]
    for row in REDDIT_PIPELINE_PATH:
        lines.append(
            f"| {row['stage']} | {row['file_path']} | {row['function']} | {row['purpose']} |"
        )

    lines.extend(["", "## Stage Summary", ""])
    if summary_df.empty:
        lines.append("No profiling data was produced.")
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "| source | total_s | collect_s | normalize_s | validate_s | dedupe_s | prefilter_s | episode_s | labelability_s | raw | valid | keep | episodes | top_stage |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for _, row in summary_df.iterrows():
        lines.append(
            "| {source} | {total:.3f} | {collect:.3f} | {normalize:.3f} | {validate:.3f} | {dedupe:.3f} | {prefilter:.3f} | {episode:.3f} | {labelability:.3f} | {raw} | {valid} | {keep} | {episodes} | {top_stage} |".format(
                source=row["source"],
                total=float(row.get("total_pipeline_seconds", 0.0)),
                collect=float(row.get("collect_seconds", 0.0)),
                normalize=float(row.get("normalize_seconds", 0.0)),
                validate=float(row.get("validate_seconds", 0.0)),
                dedupe=float(row.get("dedupe_seconds", 0.0)),
                prefilter=float(row.get("prefilter_seconds", 0.0)),
                episode=float(row.get("episode_build_seconds", 0.0)),
                labelability=float(row.get("labelability_handoff_seconds", 0.0)),
                raw=int(row.get("raw_record_count", 0)),
                valid=int(row.get("valid_after_dedupe_count", 0)),
                keep=int(row.get("prefilter_keep_count", 0)),
                episodes=int(row.get("episode_count", 0)),
                top_stage=row.get("top_stage", ""),
            )
        )

    lines.extend(
        [
            "",
            "## Collector Comparison",
            "",
            "| source | collect_s | request_s | search_req_s | comments_req_s | total_sleep_s | requests | retries | pages | avg_items_page | avg_kept_page | overlap_skips | comment_fetches | comment_skips | rate_headers | request_s_share | raw | valid | keep | episodes |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for _, row in summary_df.iterrows():
        lines.append(
            "| {source} | {collect:.3f} | {request:.3f} | {search_request:.3f} | {comments_request:.3f} | {total_sleep:.3f} | {requests} | {retries} | {pages} | {avg_items:.2f} | {avg_kept:.2f} | {overlap_skips} | {comment_fetches} | {comment_skips} | {rate_headers} | {request_share:.1%} | {raw} | {valid} | {keep} | {episodes} |".format(
                source=row["source"],
                collect=_float_or_zero(row.get("collect_seconds", 0.0)),
                request=_float_or_zero(row.get("request_seconds_total", 0.0)),
                search_request=_float_or_zero(row.get("search_request_seconds", 0.0)),
                comments_request=_float_or_zero(row.get("comments_request_seconds", 0.0)),
                total_sleep=_float_or_zero(row.get("total_sleep_seconds", 0.0)),
                requests=_int_or_zero(row.get("request_count", 0)),
                retries=_int_or_zero(row.get("request_retry_count", 0)),
                pages=_int_or_zero(row.get("pagination_iterations", 0)),
                avg_items=_float_or_zero(row.get("average_items_per_page", 0.0)),
                avg_kept=_float_or_zero(row.get("average_kept_items_per_page", 0.0)),
                overlap_skips=_int_or_zero(row.get("query_overlap_skip_count", 0)),
                comment_fetches=_int_or_zero(row.get("comment_fetch_count", 0)),
                comment_skips=_int_or_zero(row.get("comment_skip_count", 0)),
                rate_headers=_int_or_zero(row.get("rate_limit_header_seen_count", 0)),
                request_share=_float_or_zero(row.get("request_seconds_share_of_collect", 0.0)),
                raw=_int_or_zero(row.get("raw_record_count", 0)),
                valid=_int_or_zero(row.get("valid_after_dedupe_count", 0)),
                keep=_int_or_zero(row.get("prefilter_keep_count", 0)),
                episodes=_int_or_zero(row.get("episode_count", 0)),
            )
        )

    reddit_row = _row_for_source(summary_df, "reddit")
    if reddit_row is not None:
        lines.extend(["", "## Diagnosis", ""])
        collect_share = _safe_ratio(float(reddit_row.get("collect_seconds", 0.0)), float(reddit_row.get("total_pipeline_seconds", 0.0)))
        lines.append(
            "- Reddit hotspot: {stage} at {seconds:.3f}s, with collection taking {share:.1%} of measured pipeline time.".format(
                stage=str(reddit_row.get("top_stage", "unknown")),
                seconds=float(reddit_row.get("top_stage_seconds", 0.0)),
                share=collect_share,
            )
        )
        lines.append(
            "- Reddit collector load: requests={requests}, search_requests={search_requests}, comment_requests={comment_requests}, retries={retries}, total_sleep_seconds={sleep:.3f}, pagination_iterations={pages}, avg_items_per_page={avg_items:.2f}, avg_kept_items_per_page={avg_kept:.2f}.".format(
                requests=int(reddit_row.get("request_count", 0)),
                search_requests=int(reddit_row.get("search_request_count", 0)),
                comment_requests=int(reddit_row.get("comments_request_count", 0)),
                retries=int(reddit_row.get("request_retry_count", 0)),
                sleep=float(reddit_row.get("total_sleep_seconds", 0.0)),
                pages=int(reddit_row.get("pagination_iterations", 0)),
                avg_items=float(reddit_row.get("average_items_per_page", 0.0)),
                avg_kept=float(reddit_row.get("average_kept_items_per_page", 0.0)),
            )
        )
        lines.append(
            "- Reddit downstream yield: raw={raw}, valid_after_dedupe={valid}, prefilter_keep={keep}, episodes={episodes}, labelable_plus_borderline={usable}.".format(
                raw=int(reddit_row.get("raw_record_count", 0)),
                valid=int(reddit_row.get("valid_after_dedupe_count", 0)),
                keep=int(reddit_row.get("prefilter_keep_count", 0)),
                episodes=int(reddit_row.get("episode_count", 0)),
                usable=int(reddit_row.get("labelable_episode_count", 0)) + int(reddit_row.get("borderline_episode_count", 0)),
            )
        )
        lines.append(
            f"- Reddit prefilter retention is {float(reddit_row.get('prefilter_keep_ratio', 0.0)):.1%} after validation and dedupe, so expensive collection may be feeding a narrow downstream yield."
        )
        lines.append(
            "- Reddit collector composition: request_seconds={request_seconds:.3f}s ({request_share:.1%} of collect), search_request_seconds={search_seconds:.3f}s, comments_request_seconds={comment_seconds:.3f}s, overlap_skips={overlap_skips}, comment_fetches={comment_fetches}, comment_skips={comment_skips}, rate_limit_headers={rate_headers}.".format(
                request_seconds=_float_or_zero(reddit_row.get("request_seconds_total", 0.0)),
                request_share=_float_or_zero(reddit_row.get("request_seconds_share_of_collect", 0.0)),
                search_seconds=_float_or_zero(reddit_row.get("search_request_seconds", 0.0)),
                comment_seconds=_float_or_zero(reddit_row.get("comments_request_seconds", 0.0)),
                overlap_skips=_int_or_zero(reddit_row.get("query_overlap_skip_count", 0)),
                comment_fetches=_int_or_zero(reddit_row.get("comment_fetch_count", 0)),
                comment_skips=_int_or_zero(reddit_row.get("comment_skip_count", 0)),
                rate_headers=_int_or_zero(reddit_row.get("rate_limit_header_seen_count", 0)),
            )
        )
        lines.append(
            "- Reddit pagination controls: low_yield_stops={low_yield_stops}, overlap_stops={overlap_stops}, rolling_retention_stops={rolling_stops}, seed_page_cap_stops={seed_cap_stops}, repeated_cursor_stops={cursor_stops}, rate_limit_remaining_min={remaining:.2f}.".format(
                low_yield_stops=_int_or_zero(reddit_row.get("pagination_low_yield_stop_count", 0)),
                overlap_stops=_int_or_zero(reddit_row.get("pagination_overlap_stop_count", 0)),
                rolling_stops=_int_or_zero(reddit_row.get("pagination_rolling_retention_stop_count", 0)),
                seed_cap_stops=_int_or_zero(reddit_row.get("pagination_seed_page_cap_stop_count", 0)),
                cursor_stops=_int_or_zero(reddit_row.get("pagination_repeated_cursor_stop_count", 0)),
                remaining=_float_or_zero(reddit_row.get("rate_limit_remaining_min", 0.0)),
            )
        )

        comparison_rows = summary_df[summary_df["source"].astype(str) != "reddit"].head(2)
        for _, comparison in comparison_rows.iterrows():
            lines.append(
                "- Versus {source}: Reddit collect_s is {collect_ratio:.2f}x, request_count is {request_ratio:.2f}x, requests_per_raw_record is {req_per_raw_ratio:.2f}x, and prefilter_keep_ratio is {keep_ratio_delta:.1f} points lower.".format(
                    source=comparison["source"],
                    collect_ratio=_ratio_text(
                        float(reddit_row.get("collect_seconds", 0.0)),
                        float(comparison.get("collect_seconds", 0.0)),
                    ),
                    request_ratio=_ratio_text(
                        float(reddit_row.get("request_count", 0.0)),
                        float(comparison.get("request_count", 0.0)),
                    ),
                    req_per_raw_ratio=_ratio_text(
                        float(reddit_row.get("requests_per_raw_record", 0.0)),
                        float(comparison.get("requests_per_raw_record", 0.0)),
                    ),
                    keep_ratio_delta=(
                        float(reddit_row.get("prefilter_keep_ratio", 0.0))
                        - float(comparison.get("prefilter_keep_ratio", 0.0))
                    )
                    * 100.0,
                )
            )

    lines.extend(["", "## Comparison Notes", ""])
    for _, row in summary_df.iterrows():
        lines.append(
            "- {source}: collect_seconds_per_raw_record={per_record:.4f}, requests_per_raw_record={requests_per_record:.4f}, prefilter_keep_ratio={keep_ratio:.1%}.".format(
                source=row["source"],
                per_record=float(row.get("collect_seconds_per_raw_record", 0.0)),
                requests_per_record=float(row.get("requests_per_raw_record", 0.0)),
                keep_ratio=float(row.get("prefilter_keep_ratio", 0.0)),
            )
        )

    if not stage_df.empty:
        lines.extend(["", "## Stage Detail", "", "| source | stage | elapsed_s | share_of_source | input | output | dropped | notes |", "| --- | --- | --- | --- | --- | --- | --- | --- |"])
        for _, row in stage_df.iterrows():
            lines.append(
                "| {source} | {stage} | {elapsed:.3f} | {share:.1%} | {input_count} | {output_count} | {dropped_count} | {notes} |".format(
                    source=row["source"],
                    stage=row["stage"],
                    elapsed=float(row.get("elapsed_seconds", 0.0)),
                    share=float(row.get("elapsed_share_of_total", 0.0)),
                    input_count=int(row.get("input_count", 0)),
                    output_count=int(row.get("output_count", 0)),
                    dropped_count=int(row.get("dropped_count", 0)),
                    notes=str(row.get("notes", "")),
                )
            )

    lines.extend(
        [
            "",
            "## Reproducible Command",
            "",
            "```powershell",
            "python run/17_profile_sources.py --sources reddit stackoverflow github_discussions",
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_rules(root_dir: Path) -> dict[str, Any]:
    """Load and prepare pipeline rules once for the profiling run."""
    invalid_rules = load_yaml(root_dir / "config" / "invalid_rules.yaml")
    filter_mode = os.getenv("VALID_FILTER_MODE", str(invalid_rules.get("default_mode", "analysis")))
    return {
        "invalid_rules": activate_rule_mode(invalid_rules, mode=filter_mode),
        "relevance_rules": load_yaml(root_dir / "config" / "relevance_rules.yaml"),
        "segmentation_rules": load_yaml(root_dir / "config" / "segmentation_rules.yaml"),
        "labeling_policy": load_yaml(root_dir / "config" / "labeling_policy.yaml"),
    }


def _build_collector(root_dir: Path, source: str):
    """Instantiate the requested live collector."""
    data_dir = root_dir / "data"
    if source == "reddit":
        return RedditCollector(config=load_yaml(root_dir / "config" / "sources" / "reddit.yaml"), data_dir=data_dir)
    if source == "stackoverflow":
        return StackOverflowCollector(config=load_yaml(root_dir / "config" / "sources" / "stackoverflow.yaml"), data_dir=data_dir)
    if source == "github_discussions":
        return GitHubDiscussionsCollector(
            config=load_yaml(root_dir / "config" / "sources" / "github_discussions.yaml"),
            data_dir=data_dir,
        )
    raise ValueError(f"Unsupported profiling source: {source}")


def _build_normalizer(source: str):
    """Instantiate the requested source normalizer."""
    if source == "reddit":
        return RedditNormalizer()
    if source == "stackoverflow":
        return StackOverflowNormalizer()
    if source == "github_discussions":
        return GitHubDiscussionsNormalizer()
    raise ValueError(f"Unsupported profiling source: {source}")


def _stage_row(
    source: str,
    stage: str,
    elapsed_seconds: float,
    input_count: int,
    output_count: int,
    dropped_count: int,
    notes: str,
) -> dict[str, Any]:
    """Build one stage timing row."""
    return {
        "source": source,
        "stage": stage,
        "elapsed_seconds": round(float(elapsed_seconds), 6),
        "input_count": int(input_count),
        "output_count": int(output_count),
        "dropped_count": int(dropped_count),
        "notes": notes,
    }


def _row_for_source(summary_df: pd.DataFrame, source: str) -> pd.Series | None:
    """Return the summary row for one source when present."""
    if summary_df.empty or "source" not in summary_df.columns:
        return None
    matches = summary_df[summary_df["source"].astype(str) == source]
    if matches.empty:
        return None
    return matches.iloc[0]


def _safe_ratio(numerator: float, denominator: float) -> float:
    """Return a rounded ratio while guarding zero denominators."""
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _ratio_text(numerator: float, denominator: float) -> float:
    """Return a stable x-ratio for human-readable comparison text."""
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 2)


def _float_or_zero(value: Any) -> float:
    """Return a finite float for report rendering."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(numeric):
        return 0.0
    return numeric


def _int_or_zero(value: Any) -> int:
    """Return an integer-like value while treating NaN as zero."""
    return int(round(_float_or_zero(value)))