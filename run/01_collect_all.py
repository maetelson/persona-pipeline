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
from src.collectors.business_community_collector import BusinessCommunityCollector
from src.collectors.github_discussions_collector import GitHubDiscussionsCollector
from src.collectors.hackernews_collector import HackerNewsCollector
from src.collectors.reddit_collector import RedditCollector
from src.collectors.reddit_public_collector import RedditPublicCollector
from src.collectors.stackoverflow_collector import StackOverflowCollector
from src.collectors.youtube_collector import YouTubeCollector
from src.utils.io import load_yaml, write_parquet
from src.utils.logging import get_logger
from src.utils.run_helpers import load_dotenv, parse_csv_env_set
from src.utils.source_registry import load_source_definitions

LOGGER = get_logger("run.collect_all")

COLLECTOR_REGISTRY: dict[str, tuple[Path, object]] = {
    "reddit": (ROOT / "config" / "sources" / "reddit.yaml", RedditCollector),
    "stackoverflow": (ROOT / "config" / "sources" / "stackoverflow.yaml", StackOverflowCollector),
    "github_discussions": (ROOT / "config" / "sources" / "github_discussions.yaml", GitHubDiscussionsCollector),
    "discourse": (ROOT / "config" / "sources" / "discourse.yaml", DiscourseCollector),
    "hackernews": (ROOT / "config" / "sources" / "hackernews.yaml", HackerNewsCollector),
    "youtube": (ROOT / "config" / "sources" / "youtube.yaml", YouTubeCollector),
}


def _extend_registry_with_source_groups() -> dict[str, tuple[Path, object]]:
    """Add config-driven source-group collectors to the legacy registry."""
    registry = dict(COLLECTOR_REGISTRY)
    collector_map = {
        "business_communities": BusinessCommunityCollector,
        "reddit": RedditPublicCollector,
    }
    for definition in load_source_definitions(ROOT, include_disabled=True):
        collector_cls = collector_map.get(definition.collector_kind)
        if collector_cls is None:
            continue
        registry[definition.source_id] = (
            definition.config_path,
            lambda config, source_id=definition.source_id, cls=collector_cls: cls(source_id, config=config, data_dir=ROOT / "data"),
        )
    return registry

def main() -> None:
    """Collect raw JSONL for all enabled sources and write raw count audits."""
    load_dotenv(ROOT / ".env")
    source_filter = parse_csv_env_set("COLLECT_SOURCE_FILTER")
    source_rows: list[dict[str, str | int]] = []
    page_rows: list[dict[str, str | int | float]] = []
    error_rows: list[dict[str, str | int | bool]] = []
    for source_name, (config_path, collector_cls) in sorted(_extend_registry_with_source_groups().items(), key=_collection_order_key):
        if source_filter and source_name not in source_filter:
            LOGGER.info("Skipping source outside COLLECT_SOURCE_FILTER: %s", source_name)
            continue
        config = load_yaml(config_path)
        if not config.get("enabled", True):
            LOGGER.info("Skipping disabled source: %s", source_name)
            continue

        collector = collector_cls(config=config, data_dir=ROOT / "data") if isinstance(collector_cls, type) else collector_cls(config)
        try:
            records = collector.collect()
            output_path = collector.save(records)
            LOGGER.info("Collected %s raw records for %s -> %s", len(records), source_name, output_path)
            source_rows.append(
                {
                    "source": source_name,
                    "raw_record_count": len(records),
                    "raw_path": str(output_path),
                    "collector_mode": "stub" if config.get("use_stub", False) else "custom",
                    "status": "ok",
                    "error_message": "",
                    "page_error_count": len(collector.error_stats),
                }
            )
            page_rows.extend(collector.collection_stats)
            error_rows.extend(collector.error_stats)
            business_health = getattr(collector, "business_health", None)
            if business_health:
                _write_business_collection_health([business_health])
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Collector failed for source: %s", source_name)
            source_rows.append(
                {
                    "source": source_name,
                    "raw_record_count": 0,
                    "raw_path": "",
                    "collector_mode": "stub" if config.get("use_stub", False) else "custom",
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


def _collection_order_key(item: tuple[str, tuple[Path, object]]) -> tuple[int, str]:
    """Order slow Reddit sources last while keeping other sources deterministic."""
    source_name, (config_path, _) = item
    config = load_yaml(config_path)
    collector_kind = str(config.get("collector_kind", "")).strip().lower()
    source_group = str(config.get("source_group", "")).strip().lower()
    is_reddit = source_name == "reddit" or collector_kind == "reddit" or source_group == "reddit"
    return (1 if is_reddit else 0, source_name)


def _write_business_collection_health(rows: list[dict[str, object]]) -> None:
    """Append business community collection diagnostics."""
    path = ROOT / "data" / "analysis" / "business_community_collection_health.csv"
    existing = pd.read_csv(path) if path.exists() else pd.DataFrame()
    updated = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    if not updated.empty and "source_id" in updated.columns:
        updated = updated.drop_duplicates("source_id", keep="last").sort_values("source_id").reset_index(drop=True)
    updated.to_csv(path, index=False)
    write_parquet(updated, ROOT / "data" / "analysis" / "business_community_collection_health.parquet")


if __name__ == "__main__":
    main()
