"""Collector for official BI communities via public feeds and manual snapshots."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from src.collectors.base import BaseCollector, RawRecord
from src.collectors.official_community_parser import parse_feed_entries, parse_official_community_html
from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.logging import get_logger
from src.utils.text import make_hash_id

LOGGER = get_logger("collectors.official_community")


class OfficialCommunityCollector(BaseCollector):
    """Collect threads from official product communities."""

    source_type = "forum"

    def __init__(self, source_name: str, config: dict[str, Any], data_dir: Path) -> None:
        self.source_name = source_name
        super().__init__(config=config, data_dir=data_dir)

    def collect(self) -> list[RawRecord]:
        """Collect live feed items plus manual snapshots when present."""
        rows: list[RawRecord] = []
        rows.extend(self._collect_manual_html())
        rows.extend(self._collect_feed_rows())
        rows.extend(self._collect_thread_pages())
        if rows:
            LOGGER.info("Collected %s official community rows for %s", len(rows), self.source_name)
            return rows
        return [self._blocked_record()]

    def _collect_manual_html(self) -> list[RawRecord]:
        """Collect manual HTML snapshots for the official community."""
        manual_dir = self.root_dir / str(self.config.get("manual_input_dir", f"data/manual_ingest/{self.source_name}"))
        if not manual_dir.exists():
            return []
        rows: list[RawRecord] = []
        for file_path in sorted(path for path in manual_dir.iterdir() if path.is_file() and path.suffix.lower() in {".html", ".htm"}):
            parsed_rows = parse_official_community_html(
                file_path.read_text(encoding="utf-8"),
                forum_name=str(self.config.get("forum_name", self.source_name)),
                product_or_tool=str(self.config.get("product_or_tool", "")),
                canonical_url="",
                raw_file_path=str(file_path),
            )
            rows.extend(self._build_records(parsed_rows, crawl_method="manual_import", crawl_status="ok_manual_import"))
        return rows

    def _collect_feed_rows(self) -> list[RawRecord]:
        """Collect public feed entries when configured and robots-safe."""
        user_agent = os.getenv("PUBLIC_WEB_USER_AGENT", "persona-pipeline/0.1")
        rows: list[RawRecord] = []
        for feed_url in self.config.get("feed_urls", []) or []:
            allowed, _ = check_robots_allowed(feed_url, user_agent=user_agent)
            if not allowed:
                continue
            response = fetch_text(feed_url, user_agent=user_agent)
            if not response.ok:
                continue
            parsed_rows = parse_feed_entries(response.body_text)
            for parsed in parsed_rows:
                parsed["product_or_tool"] = str(self.config.get("product_or_tool", ""))
                parsed["subreddit_or_forum"] = str(self.config.get("forum_name", self.source_name))
            rows.extend(self._build_records(parsed_rows, crawl_method="public_feed", crawl_status="ok"))
        return rows

    def _collect_thread_pages(self) -> list[RawRecord]:
        """Collect directly from configured public HTML thread or board URLs."""
        user_agent = os.getenv("PUBLIC_WEB_USER_AGENT", "persona-pipeline/0.1")
        rows: list[RawRecord] = []
        for thread_url in self.config.get("thread_urls", []) or []:
            allowed, _ = check_robots_allowed(thread_url, user_agent=user_agent)
            if not allowed:
                continue
            response = fetch_text(thread_url, user_agent=user_agent)
            if not response.ok:
                continue
            parsed_rows = parse_official_community_html(
                response.body_text,
                forum_name=str(self.config.get("forum_name", self.source_name)),
                product_or_tool=str(self.config.get("product_or_tool", "")),
                canonical_url=thread_url,
            )
            rows.extend(self._build_records(parsed_rows, crawl_method="public_html", crawl_status="ok"))
        return rows

    def _build_records(self, parsed_rows: list[dict[str, Any]], crawl_method: str, crawl_status: str) -> list[RawRecord]:
        """Convert parsed community rows into raw records."""
        rows: list[RawRecord] = []
        for index, parsed in enumerate(parsed_rows, start=1):
            title = str(parsed.get("title", "") or "")
            body_text = str(parsed.get("body_text", "") or "")
            canonical_url = str(parsed.get("canonical_url", "") or "")
            raw_source_id = str(parsed.get("raw_source_id", "") or f"{self.source_name}-{index}")
            rows.append(
                RawRecord(
                    source=self.source_name,
                    source_group=str(self.config.get("source_group", "official_communities")),
                    source_name=str(self.config.get("source_name", self.source_name)),
                    source_type=str(parsed.get("source_type", "thread")),
                    raw_id=raw_source_id,
                    raw_source_id=raw_source_id,
                    url=canonical_url,
                    canonical_url=canonical_url,
                    title=title,
                    body=body_text,
                    body_text=body_text,
                    comments_text="",
                    created_at=str(parsed.get("created_at", "") or ""),
                    fetched_at=str(parsed.get("retrieved_at", parsed.get("created_at", "")) or ""),
                    retrieved_at=str(parsed.get("retrieved_at", parsed.get("created_at", "")) or ""),
                    query_seed=str(self.config.get("forum_name", self.source_name)),
                    author_hint=str(parsed.get("author_name", "") or ""),
                    author_name=str(parsed.get("author_name", "") or ""),
                    product_or_tool=str(parsed.get("product_or_tool", "") or self.config.get("product_or_tool", "")),
                    subreddit_or_forum=str(parsed.get("subreddit_or_forum", "") or self.config.get("forum_name", self.source_name)),
                    thread_title=str(parsed.get("thread_title", "") or title),
                    parent_context=str(parsed.get("parent_context", "") or ""),
                    workflow_hint=str(parsed.get("workflow_hint", "") or ""),
                    pain_point_hint=str(parsed.get("pain_point_hint", "") or ""),
                    output_need_hint=str(parsed.get("output_need_hint", "") or ""),
                    crawl_method=crawl_method,
                    crawl_status=crawl_status,
                    manual_import_flag=(crawl_method == "manual_import"),
                    raw_file_path=str(parsed.get("raw_file_path", "") or ""),
                    parse_version=str(parsed.get("parse_version", "official_community_v1")),
                    hash_id=make_hash_id(self.source_name, raw_source_id, canonical_url, title, body_text),
                    source_meta=dict(parsed.get("source_meta", {}) or {}),
                )
            )
        return rows

    def _blocked_record(self) -> RawRecord:
        """Return a non-fatal status row for coverage reporting."""
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "official_communities")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="status",
            raw_id=f"{self.source_name}-blocked",
            raw_source_id=f"{self.source_name}-blocked",
            url="",
            canonical_url="",
            title=f"{self.source_name} requires manual snapshots",
            body="No public feed rows were available and no manual snapshots were found.",
            body_text="No public feed rows were available and no manual snapshots were found.",
            comments_text="",
            created_at="",
            fetched_at="",
            retrieved_at="",
            query_seed=str(self.config.get("forum_name", self.source_name)),
            crawl_method="status",
            crawl_status="blocked_or_manual_required",
            manual_import_flag=False,
            raw_file_path="",
            parse_version="official_community_v1",
            hash_id=make_hash_id(self.source_name, "blocked"),
            source_meta={"manual_input_dir": str(self.config.get("manual_input_dir", ""))},
        )
