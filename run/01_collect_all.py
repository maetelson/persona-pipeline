"""Collect raw records from all enabled sources into JSONL."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.analysis.raw_audit import (
    ERROR_AUDIT_COLUMNS,
    PAGE_AUDIT_COLUMNS,
    RAW_AUDIT_COLUMNS,
    SUMMARY_COLUMNS,
    build_error_summary_df,
    build_low_yield_query_audit,
    build_raw_audit_df,
    build_raw_query_window_matrix,
    build_summary_df,
)
from src.collectors.discourse_collector import DiscourseCollector
from src.collectors.github_discussions_collector import GitHubDiscussionsCollector
from src.collectors.hackernews_collector import HackerNewsCollector
from src.collectors.reddit_collector import RedditCollector
from src.collectors.stackoverflow_collector import StackOverflowCollector
from src.collectors.youtube_collector import YouTubeCollector
from src.utils.io import load_yaml, write_parquet
from src.utils.logging import get_logger
from src.utils.run_helpers import load_dotenv, parse_csv_env_set

LOGGER = get_logger("run.collect_all")

COLLECTOR_REGISTRY = {
    "reddit": (ROOT / "config" / "sources" / "reddit.yaml", RedditCollector),
    "stackoverflow": (ROOT / "config" / "sources" / "stackoverflow.yaml", StackOverflowCollector),
    "github_discussions": (ROOT / "config" / "sources" / "github_discussions.yaml", GitHubDiscussionsCollector),
    "discourse": (ROOT / "config" / "sources" / "discourse.yaml", DiscourseCollector),
    "hackernews": (ROOT / "config" / "sources" / "hackernews.yaml", HackerNewsCollector),
    "youtube": (ROOT / "config" / "sources" / "youtube.yaml", YouTubeCollector),
}

def main() -> None:
    """Collect raw JSONL for all enabled sources and write raw count audits."""
    load_dotenv(ROOT / ".env")
    source_filter = parse_csv_env_set("COLLECT_SOURCE_FILTER")
    source_rows: list[dict[str, str | int]] = []
    page_rows: list[dict[str, str | int | float]] = []
    error_rows: list[dict[str, str | int | bool]] = []
    for source_name, (config_path, collector_cls) in COLLECTOR_REGISTRY.items():
        if source_filter and source_name not in source_filter:
            LOGGER.info("Skipping source outside COLLECT_SOURCE_FILTER: %s", source_name)
            continue
        config = load_yaml(config_path)
        if not config.get("enabled", True):
            LOGGER.info("Skipping disabled source: %s", source_name)
            continue

        collector = collector_cls(config=config, data_dir=ROOT / "data")
        try:
            records = collector.collect()
            output_path = collector.save(records)
            LOGGER.info("Collected %s raw records for %s -> %s", len(records), source_name, output_path)
            source_rows.append(
                {
                    "source": source_name,
                    "raw_record_count": len(records),
                    "raw_path": str(output_path),
                    "collector_mode": "stub" if config.get("use_stub", True) else "custom",
                    "status": "ok",
                    "error_message": "",
                    "page_error_count": len(collector.error_stats),
                }
            )
            page_rows.extend(collector.collection_stats)
            error_rows.extend(collector.error_stats)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Collector failed for source: %s", source_name)
            source_rows.append(
                {
                    "source": source_name,
                    "raw_record_count": 0,
                    "raw_path": "",
                    "collector_mode": "stub" if config.get("use_stub", True) else "custom",
                    "status": "error",
                    "error_message": str(exc),
                    "page_error_count": len(collector.error_stats),
                }
            )
            page_rows.extend(collector.collection_stats)
            error_rows.extend(collector.error_stats)

    raw_audit_df = build_raw_audit_df(source_rows)
    write_parquet(raw_audit_df, ROOT / "data" / "analysis" / "raw_audit.parquet")

    page_audit_df = pd.DataFrame(page_rows, columns=PAGE_AUDIT_COLUMNS)
    write_parquet(page_audit_df, ROOT / "data" / "analysis" / "raw_page_audit.parquet")

    error_audit_df = pd.DataFrame(error_rows, columns=ERROR_AUDIT_COLUMNS)
    write_parquet(error_audit_df, ROOT / "data" / "analysis" / "raw_error_audit.parquet")
    error_summary_df = build_error_summary_df(error_audit_df)
    write_parquet(error_summary_df, ROOT / "data" / "analysis" / "raw_error_summary.parquet")

    summary_df = build_summary_df(page_audit_df)
    write_parquet(summary_df, ROOT / "data" / "analysis" / "raw_query_window_summary.parquet")

    matrix_df = build_raw_query_window_matrix(page_audit_df)
    write_parquet(matrix_df, ROOT / "data" / "analysis" / "raw_query_window_matrix.parquet")

    low_yield_df = build_low_yield_query_audit(matrix_df, low_yield_threshold=1)
    write_parquet(low_yield_df, ROOT / "data" / "analysis" / "raw_low_yield_queries.parquet")


if __name__ == "__main__":
    main()
