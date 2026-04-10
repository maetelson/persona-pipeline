"""Public business-community collector for merchant and marketing forums."""

from __future__ import annotations

import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.collectors.base import BaseCollector, RawRecord
from src.collectors.business_community_parser import (
    ThreadLink,
    discover_rss_thread_links,
    discover_thread_links,
    parse_thread_page,
)
from src.utils.dates import utc_now_iso
from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.logging import get_logger
from src.utils.seed_bank import load_seed_bank, resolve_seed_queries
from src.utils.text import make_hash_id

LOGGER = get_logger("collectors.business_community")


class BusinessCommunityCollector(BaseCollector):
    """Collect public thread pages from non-developer business communities."""

    source_type = "business_community"

    def __init__(self, source_name: str, config: dict[str, Any], data_dir: Path) -> None:
        self.source_name = source_name
        super().__init__(config=config, data_dir=data_dir)
        self.business_health: dict[str, int | str] = {
            "source_id": self.source_name,
            "discovered_thread_count": 0,
            "fetched_thread_count": 0,
            "parse_success_count": 0,
            "parse_error_count": 0,
        }

    def collect(self) -> list[RawRecord]:
        """Discover, fetch, and parse public community threads."""
        discovered = self._discover_threads()
        records: list[RawRecord] = []
        seen_urls: set[str] = set()
        max_threads = int(os.getenv("BUSINESS_COMMUNITY_MAX_THREADS", self.config.get("max_threads_per_run", 20)))
        sleep_seconds = float(self.config.get("sleep_seconds", 0.5))
        user_agent = self._user_agent()

        for link in discovered:
            if link.url in seen_urls:
                continue
            seen_urls.add(link.url)
            if len(seen_urls) > max_threads:
                break
            if not self._robots_allowed(link.url, user_agent):
                continue
            response = fetch_text(link.url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
            if not response.ok:
                self._record_error(link.url, "fetch", str(response.status_code), response.error_message or response.crawl_status)
                continue
            self.business_health["fetched_thread_count"] = int(self.business_health["fetched_thread_count"]) + 1
            try:
                parsed = parse_thread_page(
                    response.body_text,
                    url=link.url,
                    platform=str(self.config.get("platform", "")),
                    fallback=link,
                    product_or_tool=str(self.config.get("product_or_tool", "")),
                )
            except Exception as exc:  # noqa: BLE001
                self.business_health["parse_error_count"] = int(self.business_health["parse_error_count"]) + 1
                self._record_error(link.url, "parse", "", str(exc))
                continue
            if parsed.parse_status == "parse_empty" or (
                parsed.parse_status == "ok_listing_only" and bool(self.config.get("drop_listing_only", False))
            ):
                self.business_health["parse_error_count"] = int(self.business_health["parse_error_count"]) + 1
                self._record_error(link.url, "parse", "", f"parsed thread status was {parsed.parse_status}")
                continue
            self.business_health["parse_success_count"] = int(self.business_health["parse_success_count"]) + 1
            records.append(self._build_record(parsed, link))
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        self._record_collection_summary(len(records))
        LOGGER.info("Collected %s business community rows for %s", len(records), self.source_name)
        return records

    def _discover_threads(self) -> list[ThreadLink]:
        """Discover candidate thread URLs from configured public listing pages."""
        platform = str(self.config.get("platform", ""))
        user_agent = self._user_agent()
        max_per_url = int(os.getenv("BUSINESS_COMMUNITY_MAX_DISCOVERY_PER_URL", self.config.get("max_discovered_threads_per_url", 20)))
        seed_terms = self._seed_terms()
        discovered: dict[str, ThreadLink] = {}
        for row in self.config.get("discovery_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url:
                continue
            if not self._robots_allowed(url, user_agent):
                continue
            response = fetch_text(url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
            if not response.ok:
                self._record_error(url, "discovery_fetch", str(response.status_code), response.error_message or response.crawl_status)
                continue
            links = discover_thread_links(response.body_text, base_url=url, platform=platform, board=board)
            accepted = 0
            for link in links:
                if link.url in discovered:
                    continue
                if not self._accept_discovered_link(link, seed_terms):
                    continue
                discovered[link.url] = link
                accepted += 1
                if accepted >= max_per_url:
                    break
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": "thread_discovery",
                    "query_text": url,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": 1,
                    "page_raw_count": accepted,
                    "page_raw_count_before_dedupe": len(links),
                    "duplicate_count": max(0, len(links) - accepted),
                    "duplicate_ratio": round((max(0, len(links) - accepted) / max(len(links), 1)), 4),
                    "stop_reason": "ok" if links else "no_thread_links",
                }
            )
        for row in self.config.get("rss_discovery_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url:
                continue
            if not self._robots_allowed(url, user_agent):
                continue
            response = fetch_text(url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
            if not response.ok:
                self._record_error(url, "rss_fetch", str(response.status_code), response.error_message or response.crawl_status)
                continue
            links = discover_rss_thread_links(response.body_text, base_url=url, platform=platform, board=board)
            accepted = 0
            for link in links:
                if link.url in discovered:
                    continue
                if not self._accept_discovered_link(link, seed_terms):
                    continue
                discovered[link.url] = link
                accepted += 1
                if accepted >= max_per_url:
                    break
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": "rss_discovery",
                    "query_text": url,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": 1,
                    "page_raw_count": accepted,
                    "page_raw_count_before_dedupe": len(links),
                    "duplicate_count": max(0, len(links) - accepted),
                    "duplicate_ratio": round((max(0, len(links) - accepted) / max(len(links), 1)), 4),
                    "stop_reason": "ok" if links else "no_thread_links",
                }
            )
        self.business_health["discovered_thread_count"] = len(discovered)
        return list(discovered.values())

    def _build_record(self, parsed, link: ThreadLink) -> RawRecord:
        """Build a raw record preserving source identity and public metadata."""
        fetched_at = utc_now_iso()
        created_at = _safe_iso_datetime(parsed.published_at) or fetched_at
        source_meta = dict(parsed.source_meta)
        source_meta.update(
            {
                "discovered_url": link.url,
                "listing_title": link.title,
                "board": parsed.board or link.board,
                "reply_count": parsed.reply_count,
                "parse_status": parsed.parse_status,
            }
        )
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "business_communities")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="thread",
            raw_id=parsed.raw_id,
            raw_source_id=parsed.raw_id,
            url=parsed.canonical_url,
            canonical_url=parsed.canonical_url,
            title=parsed.title,
            body=parsed.body_text,
            body_text=parsed.body_text,
            comments_text="",
            created_at=created_at,
            fetched_at=fetched_at,
            retrieved_at=fetched_at,
            query_seed=link.title,
            query_id="thread_discovery",
            query_text=link.title,
            author_hint=parsed.author_name,
            author_name=parsed.author_name,
            product_or_tool=str(self.config.get("product_or_tool", "")),
            subreddit_or_forum=parsed.board or link.board,
            thread_title=parsed.title,
            crawl_method="public_html",
            crawl_status=parsed.parse_status,
            parse_version="business_community_v1",
            hash_id=make_hash_id(self.source_name, parsed.raw_id, parsed.canonical_url),
            source_meta=source_meta,
        )

    def _seed_terms(self) -> list[str]:
        """Return compact source seed terms for optional listing-title filtering."""
        if not bool(self.config.get("filter_discovery_by_seed", False)):
            return []
        seeds = resolve_seed_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )
        if bool(self.config.get("include_candidate_seed_pool_for_discovery", False)):
            seed_bank = load_seed_bank(
                self.root_dir,
                source_group=str(self.config.get("source_group", "")),
                source_id=self.source_name,
            )
            if seed_bank is not None:
                seeds = list(dict.fromkeys([*seeds, *seed_bank.candidate_seed_pool]))
        terms: list[str] = []
        for seed in seeds:
            for token in str(seed).lower().split():
                if len(token) >= 4 and token not in terms:
                    terms.append(token)
        return terms

    def _matches_seed_terms(self, title: str, seed_terms: list[str]) -> bool:
        """Return whether a discovered title overlaps configured seed terms."""
        lowered = title.lower()
        minimum_matches = int(self.config.get("min_seed_term_matches", 1) or 1)
        return sum(1 for term in seed_terms if term in lowered) >= minimum_matches

    def _accept_discovered_link(self, link: ThreadLink, seed_terms: list[str]) -> bool:
        """Return whether a discovered thread is worth fetching."""
        if self._matches_excluded_discovery_pattern(link):
            return False
        return not seed_terms or self._matches_seed_terms(link.title, seed_terms)

    def _matches_excluded_discovery_pattern(self, link: ThreadLink) -> bool:
        """Return whether a listing item is known boilerplate or off-scope."""
        title = link.title.lower()
        url = link.url.lower()
        for pattern in self.config.get("discovery_exclude_title_patterns", []) or []:
            if re.search(str(pattern), title, flags=re.IGNORECASE):
                return True
        for pattern in self.config.get("discovery_exclude_url_patterns", []) or []:
            if re.search(str(pattern), url, flags=re.IGNORECASE):
                return True
        return False

    def _robots_allowed(self, url: str, user_agent: str) -> bool:
        """Honor robots.txt unless explicitly disabled in source config."""
        if not bool(self.config.get("check_robots", True)):
            return True
        allowed, reason = check_robots_allowed(url, user_agent=user_agent)
        if allowed:
            return True
        self._record_error(url, "robots", "", reason)
        return False

    def _record_error(self, url: str, stage: str, code: str, message: str) -> None:
        """Record collection errors without emitting placeholder rows."""
        self.error_stats.append(
            {
                "source": self.source_name,
                "query_id": "thread_discovery",
                "query_text": url,
                "window_id": "",
                "window_start": "",
                "window_end": "",
                "page_no": 1,
                "error_stage": stage,
                "error_type": stage,
                "error_code": code,
                "error_message": message,
                "is_retryable": self._is_retryable_error(code, message),
            }
        )

    def _record_collection_summary(self, record_count: int) -> None:
        """Record a stable source-level page audit summary."""
        self.collection_stats.append(
            {
                "source": self.source_name,
                "query_id": "thread_fetch",
                "query_text": "public_thread_pages",
                "window_id": "",
                "window_start": "",
                "window_end": "",
                "page_no": 1,
                "page_raw_count": record_count,
                "page_raw_count_before_dedupe": int(self.business_health["fetched_thread_count"]),
                "duplicate_count": max(0, int(self.business_health["discovered_thread_count"]) - int(self.business_health["fetched_thread_count"])),
                "duplicate_ratio": 0.0,
                "stop_reason": "ok" if record_count else "empty_results",
            }
        )

    def _user_agent(self) -> str:
        """Return a browser-like declared user agent for public HTML pages."""
        return os.getenv(
            "PUBLIC_WEB_USER_AGENT",
            "Mozilla/5.0 (compatible; persona-pipeline/0.1; public research)",
        )


def _safe_iso_datetime(value: str) -> str:
    """Return an ISO datetime only when the public page value is parseable."""
    if not value:
        return ""
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat()
