"""Safe two-lane collector for review sites with manual-ingest fallback."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from src.collectors.base import BaseCollector, RawRecord
from src.collectors.review_site_parser import parse_review_html, parse_review_snapshot
from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.logging import get_logger
from src.utils.text import make_hash_id

LOGGER = get_logger("collectors.review_site")


class ReviewSiteCollector(BaseCollector):
    """Collect reviews via safe direct crawl or manual snapshots."""

    source_type = "review"

    def __init__(self, source_name: str, config: dict[str, Any], data_dir: Path) -> None:
        self.source_name = source_name
        super().__init__(config=config, data_dir=data_dir)

    def collect(self) -> list[RawRecord]:
        """Collect review-site rows without failing the pipeline on blocked crawl."""
        records: list[RawRecord] = []
        records.extend(self._collect_manual_rows())
        records.extend(self._collect_direct_rows())
        if records:
            return records
        return [self._blocked_status_record()]

    def _collect_manual_rows(self) -> list[RawRecord]:
        """Collect rows from manual snapshot files when present."""
        manual_dir = self.root_dir / str(self.config.get("manual_input_dir", f"data/manual_ingest/{self.source_name}"))
        if not manual_dir.exists():
            return []
        rows: list[RawRecord] = []
        for file_path in sorted(path for path in manual_dir.iterdir() if path.is_file()):
            parsed_rows = parse_review_snapshot(
                file_path=file_path,
                source_name=str(self.config.get("source_name", self.source_name)),
                product_or_tool=str(self.config.get("product_or_tool", "")),
            )
            for index, parsed in enumerate(parsed_rows, start=1):
                rows.append(self._build_review_record(parsed, row_index=index, manual_import_flag=True))
        if rows:
            LOGGER.info("Loaded %s manual review rows for %s", len(rows), self.source_name)
        return rows

    def _collect_direct_rows(self) -> list[RawRecord]:
        """Collect review rows only when direct crawl is explicitly allowed and robots-safe."""
        if not self.config.get("direct_crawl_enabled", False):
            return []
        user_agent = os.getenv("PUBLIC_WEB_USER_AGENT", "persona-pipeline/0.1")
        rows: list[RawRecord] = []
        for url in self.config.get("direct_urls", []) or []:
            allowed, reason = check_robots_allowed(url, user_agent=user_agent)
            if not allowed:
                self.error_stats.append(
                    {"source": self.source_name, "page_no": 1, "error_stage": "robots", "error_type": "robots", "error_code": "", "error_message": reason, "is_retryable": False}
                )
                continue
            response = fetch_text(url, user_agent=user_agent)
            if not response.ok:
                self.error_stats.append(
                    {"source": self.source_name, "page_no": 1, "error_stage": "fetch", "error_type": response.crawl_status, "error_code": str(response.status_code), "error_message": response.error_message, "is_retryable": False}
                )
                continue
            parsed_rows = parse_review_html(
                response.body_text,
                source_name=str(self.config.get("source_name", self.source_name)),
                canonical_url=url,
                raw_file_path="",
                product_or_tool=str(self.config.get("product_or_tool", "")),
            )
            for index, parsed in enumerate(parsed_rows, start=1):
                rows.append(self._build_review_record(parsed, row_index=index, manual_import_flag=False))
        if rows:
            LOGGER.info("Collected %s direct review rows for %s", len(rows), self.source_name)
        return rows

    def _build_review_record(self, parsed: dict[str, Any], row_index: int, manual_import_flag: bool) -> RawRecord:
        """Build one raw review record from parsed fields."""
        title = str(parsed.get("title", "") or "")
        body_text = str(parsed.get("body_text", "") or "")
        canonical_url = str(parsed.get("canonical_url", "") or "")
        raw_source_id = str(parsed.get("raw_source_id", "") or f"{self.source_name}-{row_index}")
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "review_sites")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type=str(parsed.get("source_type", self.source_type)),
            raw_id=raw_source_id,
            raw_source_id=raw_source_id,
            url=canonical_url,
            canonical_url=canonical_url,
            title=title,
            body=body_text,
            body_text=body_text,
            comments_text="",
            created_at=str(parsed.get("created_at", "") or ""),
            fetched_at=str(parsed.get("retrieved_at", "") or ""),
            retrieved_at=str(parsed.get("retrieved_at", "") or ""),
            query_seed="manual_review_snapshot" if manual_import_flag else "public_review_page",
            author_hint=str(parsed.get("author_name", "") or ""),
            author_name=str(parsed.get("author_name", "") or ""),
            product_or_tool=str(parsed.get("product_or_tool", "") or self.config.get("product_or_tool", "")),
            subreddit_or_forum="",
            thread_title=title,
            parent_context=str(parsed.get("parent_context", "") or ""),
            role_hint=str(parsed.get("role_hint", "") or ""),
            company_size_hint=str(parsed.get("company_size_hint", "") or ""),
            industry_hint=str(parsed.get("industry_hint", "") or ""),
            workflow_hint=str(parsed.get("workflow_hint", "") or ""),
            pain_point_hint=str(parsed.get("pain_point_hint", "") or ""),
            output_need_hint=str(parsed.get("output_need_hint", "") or ""),
            crawl_method="manual_import" if manual_import_flag else "direct_html",
            crawl_status="ok_manual_import" if manual_import_flag else "ok",
            manual_import_flag=manual_import_flag,
            raw_file_path=str(parsed.get("raw_file_path", "") or ""),
            parse_version=str(parsed.get("parse_version", "review_v1")),
            hash_id=make_hash_id(self.source_name, raw_source_id, canonical_url, title, body_text),
            source_meta=dict(parsed.get("source_meta", {}) or {}),
        )

    def _blocked_status_record(self) -> RawRecord:
        """Return a non-fatal status row when crawl is blocked or manual ingest is needed."""
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "review_sites")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="status",
            raw_id=f"{self.source_name}-blocked",
            raw_source_id=f"{self.source_name}-blocked",
            url="",
            canonical_url="",
            title=f"{self.source_name} manual ingest required",
            body="Public-safe direct crawl is unavailable or produced no safely accessible rows.",
            body_text="Public-safe direct crawl is unavailable or produced no safely accessible rows.",
            comments_text="",
            created_at="",
            fetched_at="",
            retrieved_at="",
            query_seed="blocked_or_manual_required",
            crawl_method="status",
            crawl_status="blocked_or_manual_required",
            manual_import_flag=False,
            raw_file_path="",
            parse_version="review_v1",
            hash_id=make_hash_id(self.source_name, "blocked_or_manual_required"),
            source_meta={"manual_input_dir": str(self.config.get("manual_input_dir", ""))},
        )
