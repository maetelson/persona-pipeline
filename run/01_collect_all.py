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
from src.collectors.google_ads_help_community_collector import GoogleAdsHelpCommunityCollector
from src.collectors.reddit_collector import RedditCollector
from src.collectors.reddit_public_collector import RedditPublicCollector
from src.collectors.stackoverflow_collector import StackOverflowCollector
from src.utils.io import load_yaml, write_parquet
from src.utils.logging import get_logger
from src.utils.run_helpers import load_dotenv, parse_csv_env_set
from src.utils.source_registry import load_source_definitions

LOGGER = get_logger("run.collect_all")
MIN_RAW_EXEMPT_SOURCES = {"google_ads_community", "google_ads_help_community", "reddit", "stackoverflow"}


class LowRawVolumeError(RuntimeError):
    """Raised when a source finishes but does not meet the raw-volume floor."""

COLLECTOR_REGISTRY: dict[str, tuple[Path, object]] = {
    "reddit": (ROOT / "config" / "sources" / "reddit.yaml", RedditCollector),
    "stackoverflow": (ROOT / "config" / "sources" / "stackoverflow.yaml", StackOverflowCollector),
    "github_discussions": (ROOT / "config" / "sources" / "github_discussions.yaml", GitHubDiscussionsCollector),
    "discourse": (ROOT / "config" / "sources" / "discourse.yaml", DiscourseCollector),
}


def _extend_registry_with_source_groups() -> dict[str, tuple[Path, object]]:
    """Add config-driven source-group collectors to the legacy registry."""
    registry = dict(COLLECTOR_REGISTRY)
    definitions = load_source_definitions(ROOT, include_disabled=True)
    collector_map = {
        "business_communities": BusinessCommunityCollector,
        "discourse": DiscourseCollector,
        "google_ads_help_community": GoogleAdsHelpCommunityCollector,
        "reddit": RedditPublicCollector,
    }
    for definition in definitions:
        collector_cls = collector_map.get(definition.collector_kind)
        if collector_cls is None:
            continue
        if definition.collector_kind in {"discourse", "google_ads_help_community"}:
            registry[definition.source_id] = (
                definition.config_path,
                lambda config, source_id=definition.source_id, cls=collector_cls, kind=definition.collector_kind: cls(
                    config=config,
                    data_dir=ROOT / "data",
                    source_name=source_id,
                )
                if kind == "discourse"
                else cls(config=config, data_dir=ROOT / "data"),
            )
            continue
        registry[definition.source_id] = (
            definition.config_path,
            lambda config, source_id=definition.source_id, cls=collector_cls: cls(source_id, config=config, data_dir=ROOT / "data"),
        )
    if any(definition.collector_kind == "discourse" and definition.source_id != "discourse" for definition in definitions):
        registry.pop("discourse", None)
    return registry

def main() -> None:
    """Collect raw JSONL for all enabled sources and write raw count audits."""
    load_dotenv(ROOT / ".env")
    source_filter = parse_csv_env_set("COLLECT_SOURCE_FILTER")
    low_raw_warn_threshold = _int_env("COLLECT_MIN_RAW_RECORDS_WARN", 600)
    fail_fast_on_low_raw = _bool_env("COLLECT_FAIL_FAST_ON_LOW_RAW", True)
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

        source_raw_threshold = _source_raw_threshold(config, low_raw_warn_threshold)
        collector = collector_cls(config=config, data_dir=ROOT / "data") if isinstance(collector_cls, type) else collector_cls(config)
        try:
            records = collector.collect()
            output_path = collector.save(records)
            LOGGER.info("Collected %s raw records for %s -> %s", len(records), source_name, output_path)
            volume_status = _emit_source_raw_count(source_name, len(records), "ok", source_raw_threshold)
            source_rows.append(
                {
                    "source": source_name,
                    "raw_record_count": len(records),
                    "raw_path": str(output_path),
                    "collector_mode": "stub" if config.get("use_stub", False) else "custom",
                    "status": "ok",
                    "error_message": "",
                    "page_error_count": len(collector.error_stats),
                    "raw_threshold": source_raw_threshold,
                }
            )
            page_rows.extend(collector.collection_stats)
            error_rows.extend(collector.error_stats)
            business_health = getattr(collector, "business_health", None)
            if business_health:
                _write_business_collection_health([business_health])
            if _should_fail_low_raw(source_name, volume_status, fail_fast_on_low_raw):
                _write_collection_audits(source_rows, page_rows, error_rows)
                _raise_low_raw(source_name, len(records), source_raw_threshold)
        except LowRawVolumeError:
            raise
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Collector failed for source: %s", source_name)
            volume_status = _emit_source_raw_count(source_name, 0, "error", source_raw_threshold)
            source_rows.append(
                {
                    "source": source_name,
                    "raw_record_count": 0,
                    "raw_path": "",
                    "collector_mode": "stub" if config.get("use_stub", False) else "custom",
                    "status": "error",
                    "error_message": str(exc),
                    "page_error_count": len(collector.error_stats),
                    "raw_threshold": source_raw_threshold,
                }
            )
            page_rows.extend(collector.collection_stats)
            error_rows.extend(collector.error_stats)
            business_health = getattr(collector, "business_health", None)
            if business_health:
                _write_business_collection_health([business_health])
            if fail_fast_on_low_raw:
                _write_collection_audits(source_rows, page_rows, error_rows)
                raise RuntimeError(f"Collector failed for {source_name}; stopping collection.") from exc

    _write_collection_audits(source_rows, page_rows, error_rows)
    _emit_collection_summary(source_rows, low_raw_warn_threshold)


def _collection_order_key(item: tuple[str, tuple[Path, object]]) -> tuple[int, str]:
    """Order slow Reddit sources last while keeping other sources deterministic."""
    source_name, (config_path, _) = item
    config = load_yaml(config_path)
    collector_kind = str(config.get("collector_kind", "")).strip().lower()
    source_group = str(config.get("source_group", "")).strip().lower()
    is_reddit = source_name == "reddit" or collector_kind == "reddit" or source_group == "reddit"
    return (1 if is_reddit else 0, source_name)


def _emit_source_raw_count(source_name: str, raw_count: int, status: str, low_raw_warn_threshold: int) -> str:
    """Print a source-level raw count line immediately after collection."""
    volume_status = _volume_status(raw_count, status, low_raw_warn_threshold)
    message = f"RAW_COUNT source={source_name} count={raw_count} collection_status={status} volume_status={volume_status}"
    print(message, flush=True)
    if volume_status != "ok":
        LOGGER.warning(
            "Low or failed raw collection: source=%s raw_count=%s threshold=%s status=%s",
            source_name,
            raw_count,
            low_raw_warn_threshold,
            status,
        )
    return volume_status


def _emit_collection_summary(source_rows: list[dict[str, str | int]], low_raw_warn_threshold: int) -> None:
    """Print a compact final raw-count summary for all attempted sources."""
    print("RAW_COUNT_SUMMARY", flush=True)
    for row in source_rows:
        source = str(row.get("source", ""))
        count = int(row.get("raw_record_count", 0) or 0)
        status = str(row.get("status", ""))
        raw_threshold = row.get("raw_threshold")
        threshold = low_raw_warn_threshold if raw_threshold is None else int(raw_threshold)
        volume_status = _volume_status(count, status, threshold)
        print(
            f"RAW_COUNT_SUMMARY source={source} count={count} collection_status={status} volume_status={volume_status}",
            flush=True,
        )


def _volume_status(raw_count: int, collection_status: str, low_raw_warn_threshold: int) -> str:
    """Return the volume status independently from collector success."""
    if collection_status != "ok":
        return "failed"
    if raw_count <= low_raw_warn_threshold:
        return "low_raw"
    return "ok"


def _should_fail_low_raw(source_name: str, volume_status: str, fail_fast_on_low_raw: bool) -> bool:
    """Return whether a source should fail the collection run for low raw volume."""
    return fail_fast_on_low_raw and volume_status != "ok" and not _is_min_raw_exempt(source_name)


def _raise_low_raw(
    source_name: str,
    raw_count: int,
    low_raw_warn_threshold: int,
) -> None:
    """Stop collection when a non-exempt source fails the minimum raw-volume gate."""
    raise LowRawVolumeError(
        f"Raw collection volume gate failed for {source_name}: "
        f"raw_count={raw_count}, required>{low_raw_warn_threshold}. "
        "Increase source discovery/query coverage before continuing."
    )


def _int_env(name: str, default: int) -> int:
    """Read an integer environment variable with a safe fallback."""
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        LOGGER.warning("Invalid integer env %s=%r; using default %s", name, raw_value, default)
        return default


def _source_raw_threshold(config: dict[str, object], default: int) -> int:
    """Return the source-specific raw count floor when configured."""
    value = config.get("min_raw_records_warn", default)
    try:
        return int(value)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid min_raw_records_warn=%r; using default %s", value, default)
        return default


def _bool_env(name: str, default: bool) -> bool:
    """Read a boolean environment variable with a safe fallback."""
    raw_value = os.getenv(name, "").strip().lower()
    if not raw_value:
        return default
    return raw_value in {"1", "true", "yes", "y", "on"}


def _is_min_raw_exempt(source_name: str) -> bool:
    """Return whether a source is exempt from the non-core raw-volume gate."""
    return source_name in MIN_RAW_EXEMPT_SOURCES or source_name.startswith("reddit_")


def _write_collection_audits(
    source_rows: list[dict[str, str | int]],
    page_rows: list[dict[str, str | int | float]],
    error_rows: list[dict[str, str | int | bool]],
) -> None:
    """Write raw collection audits, including partial results before fail-fast exits."""
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
