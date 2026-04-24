"""Public business-community collector for merchant and marketing forums."""

from __future__ import annotations

import os
import json
import re
import time
from datetime import UTC, datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from src.collectors.base import BaseCollector, RawRecord
from src.collectors.business_community_parser import (
    ThreadLink,
    discover_rss_thread_links,
    discover_sitemap_index_urls,
    discover_sitemap_thread_links,
    discover_thread_links,
    parse_thread_page,
)
from src.utils.dates import utc_now_iso
from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.logging import get_logger
from src.utils.seed_bank import DiscoveryQuery, build_discovery_queries, load_seed_bank, resolve_seed_queries
from src.utils.text import make_hash_id

LOGGER = get_logger("collectors.business_community")


class BusinessCommunityCollector(BaseCollector):
    """Collect public thread pages from non-developer business communities."""

    source_type = "business_community"

    def __init__(self, source_name: str, config: dict[str, Any], data_dir: Path) -> None:
        self.source_name = source_name
        super().__init__(config=config, data_dir=data_dir)
        self._robots_cache: dict[str, bool] = {}
        self._network_retry_sleep_seconds = 0.0
        self.business_health: dict[str, int | str] = {
            "source_id": self.source_name,
            "inventory_thread_count": 0,
            "discovered_thread_count": 0,
            "fetched_thread_count": 0,
            "parse_success_count": 0,
            "parse_error_count": 0,
            "discovery_excluded_count": 0,
            "discovery_seed_filtered_count": 0,
            "discovery_duplicate_count": 0,
        }
        self.discovery_audit_rows: list[dict[str, int | str]] = []

    def collect(self) -> list[RawRecord]:
        """Discover, fetch, and parse public community threads."""
        if bool((self.config.get("api_discovery", {}) or {}).get("enabled", False)):
            records = self._collect_api_records()
            if records:
                self._record_collection_summary(len(records))
                LOGGER.info("Collected %s business community API rows for %s", len(records), self.source_name)
                return records

        discovered = self._discover_threads()
        if not discovered and self._discovery_network_failed():
            raise RuntimeError(
                f"{self.source_name} discovery failed because remote community fetches hit a network/DNS outage. "
                "This is not a trustworthy zero-yield result; retry collection after connectivity recovers."
            )
        self._warn_on_low_discovery(discovered)
        records: list[RawRecord] = []
        seen_urls: set[str] = set()
        max_threads = _optional_positive_int(
            os.getenv("BUSINESS_COMMUNITY_MAX_THREADS"),
            self.config.get("max_threads_per_run"),
        )
        sleep_seconds = float(self.config.get("sleep_seconds", 0.5))
        user_agent = self._user_agent()

        for link in discovered:
            if link.url in seen_urls:
                continue
            seen_urls.add(link.url)
            if max_threads is not None and len(seen_urls) > max_threads:
                break
            if not self._robots_allowed(link.url, user_agent):
                continue
            response = self._fetch_with_retries(link.url, user_agent=user_agent, stage="fetch")
            if not response.ok:
                fallback_record = self._build_listing_fallback_record(link, response.status_code)
                if fallback_record is not None:
                    self.business_health["fetched_thread_count"] = int(self.business_health["fetched_thread_count"]) + 1
                    self.business_health["parse_success_count"] = int(self.business_health["parse_success_count"]) + 1
                    records.append(fallback_record)
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
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

    def _build_listing_fallback_record(self, link: ThreadLink, status_code: int) -> RawRecord | None:
        """Build a record from listing metadata when thread pages are intentionally unreachable."""
        if not bool(self.config.get("allow_listing_fallback_on_fetch_error", False)):
            return None
        allowed_codes = {
            int(code)
            for code in (self.config.get("listing_fallback_status_codes", [403]) or [403])
            if str(code).strip()
        }
        if int(status_code or 0) not in allowed_codes:
            return None
        fetched_at = utc_now_iso()
        body_text = (link.snippet or link.title or "").strip()
        if not body_text:
            return None
        raw_id = make_hash_id(self.source_name, link.url, link.title)
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "business_communities")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="thread_listing_fallback",
            raw_id=raw_id,
            raw_source_id=raw_id,
            url=link.url,
            canonical_url=link.url,
            title=link.title,
            body=body_text,
            body_text=body_text,
            comments_text="",
            created_at=_safe_iso_datetime(link.activity_date) or fetched_at,
            fetched_at=fetched_at,
            retrieved_at=fetched_at,
            query_seed=link.title,
            query_id="thread_discovery_listing_fallback",
            query_text=link.title,
            author_hint="",
            author_name="",
            product_or_tool=str(self.config.get("product_or_tool", "")),
            subreddit_or_forum=link.board,
            thread_title=link.title,
            crawl_method="public_listing_fallback",
            crawl_status="listing_fallback",
            parse_version="business_community_listing_fallback_v1",
            hash_id=make_hash_id(self.source_name, raw_id, link.url),
            source_meta={
                "platform": self.config.get("platform", ""),
                "product_or_tool": self.config.get("product_or_tool", ""),
                "board": link.board,
                "reply_count": link.reply_count,
                "listing_snippet": link.snippet,
                "listing_activity_date": link.activity_date,
                "fetch_status_code": int(status_code or 0),
                "fallback_reason": "thread_fetch_unavailable",
            },
        )

    def _collect_api_records(self) -> list[RawRecord]:
        """Collect records from public community APIs when listing pages are capped."""
        api_cfg = dict(self.config.get("api_discovery", {}) or {})
        api_kind = str(api_cfg.get("kind", "")).strip().lower()
        if api_kind != "khoros_search":
            return []
        records = self._collect_khoros_search_records(api_cfg)
        self.business_health["discovered_thread_count"] = len(records)
        self.business_health["fetched_thread_count"] = len(records)
        self.business_health["parse_success_count"] = len(records)
        self._record_seed_discovery_audit(
            discovery_queries=self._discovery_queries(),
            links=[],
            records=records,
        )
        return records

    def _collect_khoros_search_records(self, api_cfg: dict[str, Any]) -> list[RawRecord]:
        """Collect public Khoros message rows through the unauthenticated search API."""
        base_url = str(api_cfg.get("base_url", "")).rstrip("/")
        board_ids = [str(board).strip() for board in api_cfg.get("board_ids", []) if str(board).strip()]
        where_clause = str(api_cfg.get("where_clause", "") or "").strip()
        if not base_url or not board_ids:
            return []
        max_items = _optional_positive_int(api_cfg.get("max_items_per_board"))
        page_size = min(max(int(api_cfg.get("page_size", 100)), 1), 100)
        top_level_only = bool(api_cfg.get("top_level_only", False))
        user_agent = self._user_agent()
        discovery_queries = self._discovery_queries()
        records: list[RawRecord] = []
        seen_ids: set[str] = set()
        for board_id in board_ids:
            fetched_for_board = 0
            offset = 0
            while True:
                if max_items is None:
                    limit = page_size
                else:
                    remaining = max_items - offset
                    if remaining <= 0:
                        break
                    limit = min(page_size, remaining)
                query = f"SELECT * FROM messages WHERE board.id='{board_id}'"
                if where_clause:
                    query = f"{query} {where_clause.strip()}"
                query = f"{query} LIMIT {limit} OFFSET {offset}"
                url = f"{base_url}/api/2.0/search?q={quote(query)}"
                response = self._fetch_with_retries(url, user_agent=user_agent, stage="api_fetch")
                if not response.ok:
                    break
                items = _khoros_items(response.body_text)
                accepted = 0
                seed_filtered = 0
                excluded = 0
                for item in items:
                    if top_level_only and int(item.get("depth", 0) or 0) > 0:
                        continue
                    raw_id = str(item.get("id", "") or "").strip()
                    if not raw_id or raw_id in seen_ids:
                        continue
                    title = _clean_khoros_title(str(item.get("subject", "") or ""))
                    url = str(item.get("view_href", "") or "")
                    conversation = item.get("conversation", {}) if isinstance(item.get("conversation"), dict) else {}
                    if str(conversation.get("view_href", "") or "").strip():
                        url = str(conversation.get("view_href", "") or "").strip()
                    link = ThreadLink(url=url, title=title, board=board_id)
                    decision = self._discovery_decision(link, {}, discovery_queries)
                    if decision == "excluded":
                        excluded += 1
                        continue
                    if decision == "seed_filtered":
                        seed_filtered += 1
                        continue
                    seen_ids.add(raw_id)
                    records.append(self._build_khoros_record(item, board_id=board_id))
                    accepted += 1
                fetched_for_board += accepted
                self.collection_stats.append(
                    {
                        "source": self.source_name,
                        "query_id": "khoros_api_search",
                        "query_text": board_id,
                        "window_id": "",
                        "window_start": "",
                        "window_end": "",
                        "page_no": int(offset / page_size) + 1,
                        "page_raw_count": accepted,
                        "page_raw_count_before_dedupe": len(items),
                        "duplicate_count": max(0, len(items) - accepted - seed_filtered - excluded),
                        "duplicate_ratio": round((max(0, len(items) - accepted - seed_filtered - excluded) / max(len(items), 1)), 4),
                        "stop_reason": "ok" if items else "empty_results",
                        "seed_filtered_count": seed_filtered,
                        "excluded_count": excluded,
                    }
                )
                if len(items) < limit:
                    break
                offset += limit
            board_status = "ok" if fetched_for_board else "empty_results"
            log = LOGGER.info if fetched_for_board else LOGGER.warning
            log(
                "Khoros API discovery for %s board=%s accepted=%s status=%s",
                self.source_name,
                board_id,
                fetched_for_board,
                board_status,
            )
        max_threads = _optional_positive_int(
            os.getenv("BUSINESS_COMMUNITY_MAX_THREADS"),
            self.config.get("max_threads_per_run"),
        )
        if max_threads is None:
            return records
        return records[:max_threads]

    def _build_khoros_record(self, item: dict[str, Any], board_id: str) -> RawRecord:
        """Build a raw record from one public Khoros API message item."""
        fetched_at = utc_now_iso()
        url = str(item.get("view_href", "") or "")
        conversation = item.get("conversation", {}) if isinstance(item.get("conversation"), dict) else {}
        if str(conversation.get("view_href", "") or "").strip():
            url = str(conversation.get("view_href", "") or "").strip()
        title = _clean_khoros_title(str(item.get("subject", "") or ""))
        body_text = _strip_html(str(item.get("body", "") or ""))
        created_at = _safe_iso_datetime(str(item.get("post_time", "") or "")) or fetched_at
        author = item.get("author", {}) if isinstance(item.get("author"), dict) else {}
        raw_id = str(item.get("id", "") or make_hash_id(self.source_name, url, title))
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "business_communities")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="community_message",
            raw_id=raw_id,
            raw_source_id=raw_id,
            url=url,
            canonical_url=url,
            title=title,
            body=body_text,
            body_text=body_text,
            comments_text="",
            created_at=created_at,
            fetched_at=fetched_at,
            retrieved_at=fetched_at,
            query_seed=board_id,
            query_id="khoros_api_search",
            query_text=board_id,
            author_hint=str(author.get("login", "") or author.get("id", "") or ""),
            author_name=str(author.get("login", "") or ""),
            product_or_tool=str(self.config.get("product_or_tool", "")),
            subreddit_or_forum=board_id,
            thread_title=title,
            crawl_method="public_khoros_api",
            crawl_status="ok",
            parse_version="business_community_khoros_api_v1",
            hash_id=make_hash_id(self.source_name, raw_id, url),
            source_meta={
                "platform": self.config.get("platform", ""),
                "product_or_tool": self.config.get("product_or_tool", ""),
                "board_id": board_id,
                "api_item": item,
            },
        )

    def _discover_threads(self) -> list[ThreadLink]:
        """Discover candidate thread URLs from configured public listing pages."""
        platform = str(self.config.get("platform", ""))
        user_agent = self._user_agent()
        max_per_url = _optional_positive_int(
            os.getenv("BUSINESS_COMMUNITY_MAX_DISCOVERY_PER_URL"),
            self.config.get("max_discovered_threads_per_url"),
        )
        discovery_queries = self._discovery_queries()
        discovered: dict[str, ThreadLink] = {}
        stop_on_http_404 = bool(self.config.get("stop_on_http_404_for_discovery", False))
        max_consecutive_empty = int(self.config.get("max_consecutive_empty_discovery_pages", 0) or 0)
        max_consecutive_duplicate_only = int(self.config.get("max_consecutive_duplicate_only_pages", 0) or 0)
        max_consecutive_seed_filtered_only = int(self.config.get("max_consecutive_seed_filtered_only_pages", 0) or 0)
        discovery_sleep_seconds = float(self.config.get("discovery_sleep_seconds", 0.0) or 0.0)
        listing_state: dict[str, dict[str, int | bool | str]] = {}
        for row in self._expanded_discovery_url_rows():
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            listing_root = str(row.get("listing_root", "") if isinstance(row, dict) else url).strip() or url
            if not url:
                continue
            state = listing_state.setdefault(
                listing_root,
                {
                    "stopped": False,
                    "consecutive_empty": 0,
                    "consecutive_duplicate_only": 0,
                    "consecutive_seed_filtered_only": 0,
                    "stop_reason": "",
                },
            )
            if bool(state.get("stopped", False)):
                continue
            if not self._robots_allowed(url, user_agent):
                continue
            response = self._fetch_with_retries(url, user_agent=user_agent, stage="discovery_fetch")
            if not response.ok and stop_on_http_404 and int(response.status_code) == 404:
                state["stopped"] = True
                state["stop_reason"] = "http_404_stop"
                continue
            if not response.ok:
                continue
            links = discover_thread_links(response.body_text, base_url=url, platform=platform, board=board)
            audit = self._ingest_discovery_links(
                discovered=discovered,
                links=links,
                discovery_queries=discovery_queries,
                channel="listing",
                surface_url=url,
                board=board,
                max_per_url=max_per_url,
            )
            accepted = int(audit["accepted"])
            if not links:
                state["consecutive_empty"] = int(state["consecutive_empty"]) + 1
                state["consecutive_duplicate_only"] = 0
                state["consecutive_seed_filtered_only"] = 0
            elif accepted == 0 and int(audit["duplicate_count"]) == len(links):
                state["consecutive_duplicate_only"] = int(state["consecutive_duplicate_only"]) + 1
                state["consecutive_empty"] = 0
                state["consecutive_seed_filtered_only"] = 0
            elif accepted == 0 and int(audit["seed_filtered_count"]) == len(links):
                state["consecutive_seed_filtered_only"] = int(state["consecutive_seed_filtered_only"]) + 1
                state["consecutive_empty"] = 0
                state["consecutive_duplicate_only"] = 0
            else:
                state["consecutive_empty"] = 0
                state["consecutive_duplicate_only"] = 0
                state["consecutive_seed_filtered_only"] = 0
            if max_consecutive_empty > 0 and int(state["consecutive_empty"]) >= max_consecutive_empty:
                state["stopped"] = True
                state["stop_reason"] = "consecutive_zero_accept_stop"
            if (
                max_consecutive_duplicate_only > 0
                and int(state["consecutive_duplicate_only"]) >= max_consecutive_duplicate_only
            ):
                state["stopped"] = True
                state["stop_reason"] = "consecutive_duplicate_only_stop"
            if (
                max_consecutive_seed_filtered_only > 0
                and int(state["consecutive_seed_filtered_only"]) >= max_consecutive_seed_filtered_only
            ):
                state["stopped"] = True
                state["stop_reason"] = "seed_exhaustion_stop"
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": "thread_discovery",
                    "query_text": url,
                    "seed_used": "",
                    "expanded_query": "",
                    "discovered_url_count": accepted,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": 1,
                    "page_raw_count": accepted,
                    "page_raw_count_before_dedupe": len(links),
                    "duplicate_count": max(0, len(links) - accepted),
                    "duplicate_ratio": round((max(0, len(links) - accepted) / max(len(links), 1)), 4),
                    "stop_reason": str(state.get("stop_reason", "") or ("ok" if links else "no_thread_links")),
                }
            )
            if discovery_sleep_seconds > 0:
                time.sleep(discovery_sleep_seconds)
        for row in self.config.get("rss_discovery_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url:
                continue
            if not self._robots_allowed(url, user_agent):
                continue
            response = self._fetch_with_retries(url, user_agent=user_agent, stage="rss_fetch")
            if not response.ok:
                continue
            links = discover_rss_thread_links(response.body_text, base_url=url, platform=platform, board=board)
            audit = self._ingest_discovery_links(
                discovered=discovered,
                links=links,
                discovery_queries=discovery_queries,
                channel="rss",
                surface_url=url,
                board=board,
                max_per_url=max_per_url,
            )
            accepted = int(audit["accepted"])
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": "rss_discovery",
                    "query_text": url,
                    "seed_used": "",
                    "expanded_query": "",
                    "discovered_url_count": accepted,
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
        for row in self.config.get("sitemap_discovery_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url:
                continue
            if not self._robots_allowed(url, user_agent):
                continue
            response = self._fetch_with_retries(url, user_agent=user_agent, stage="sitemap_fetch")
            if not response.ok:
                continue
            links = discover_sitemap_thread_links(response.body_text, base_url=url, platform=platform, board=board)
            audit = self._ingest_discovery_links(
                discovered=discovered,
                links=links,
                discovery_queries=discovery_queries,
                channel="sitemap",
                surface_url=url,
                board=board,
                max_per_url=max_per_url,
            )
            accepted = int(audit["accepted"])
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": "sitemap_discovery",
                    "query_text": url,
                    "seed_used": "",
                    "expanded_query": "",
                    "discovered_url_count": accepted,
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
        sitemap_rows = self._expand_sitemap_index_rows(user_agent)
        sitemap_stop_on_404 = bool(self.config.get("stop_on_http_404_for_sitemap_index", stop_on_http_404))
        max_zero_accept_sitemaps = int(self.config.get("max_consecutive_zero_accept_sitemaps", 0) or 0)
        max_duplicate_only_sitemaps = int(self.config.get("max_consecutive_duplicate_only_sitemaps", 0) or 0)
        max_seed_filtered_only_sitemaps = int(self.config.get("max_consecutive_seed_filtered_only_sitemaps", 0) or 0)
        sitemap_state = {
            "stopped": False,
            "consecutive_zero_accept": 0,
            "consecutive_duplicate_only": 0,
            "consecutive_seed_filtered_only": 0,
            "stop_reason": "",
        }
        for row in sitemap_rows:
            url = str(row.get("url", "")).strip()
            board = str(row.get("board", "")).strip()
            if bool(sitemap_state["stopped"]):
                continue
            if not url or not self._robots_allowed(url, user_agent):
                continue
            response = fetch_text(url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
            if not response.ok:
                self._record_error(url, "sitemap_fetch", str(response.status_code), response.error_message or response.crawl_status)
                if sitemap_stop_on_404 and int(response.status_code) == 404:
                    sitemap_state["stopped"] = True
                    sitemap_state["stop_reason"] = "http_404_stop"
                continue
            links = discover_sitemap_thread_links(response.body_text, base_url=url, platform=platform, board=board)
            audit = self._ingest_discovery_links(
                discovered=discovered,
                links=links,
                discovery_queries=discovery_queries,
                channel="sitemap_index",
                surface_url=url,
                board=board,
                max_per_url=max_per_url,
            )
            accepted = int(audit["accepted"])
            if not links or accepted == 0:
                sitemap_state["consecutive_zero_accept"] = int(sitemap_state["consecutive_zero_accept"]) + 1
            else:
                sitemap_state["consecutive_zero_accept"] = 0
            if links and accepted == 0 and int(audit["duplicate_count"]) == len(links):
                sitemap_state["consecutive_duplicate_only"] = int(sitemap_state["consecutive_duplicate_only"]) + 1
            else:
                sitemap_state["consecutive_duplicate_only"] = 0
            if links and accepted == 0 and int(audit["seed_filtered_count"]) == len(links):
                sitemap_state["consecutive_seed_filtered_only"] = int(sitemap_state["consecutive_seed_filtered_only"]) + 1
            else:
                sitemap_state["consecutive_seed_filtered_only"] = 0
            if max_zero_accept_sitemaps > 0 and int(sitemap_state["consecutive_zero_accept"]) >= max_zero_accept_sitemaps:
                sitemap_state["stopped"] = True
                sitemap_state["stop_reason"] = "consecutive_zero_accept_stop"
            if (
                max_duplicate_only_sitemaps > 0
                and int(sitemap_state["consecutive_duplicate_only"]) >= max_duplicate_only_sitemaps
            ):
                sitemap_state["stopped"] = True
                sitemap_state["stop_reason"] = "consecutive_duplicate_only_stop"
            if (
                max_seed_filtered_only_sitemaps > 0
                and int(sitemap_state["consecutive_seed_filtered_only"]) >= max_seed_filtered_only_sitemaps
            ):
                sitemap_state["stopped"] = True
                sitemap_state["stop_reason"] = "seed_exhaustion_stop"
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": "sitemap_index_discovery",
                    "query_text": url,
                    "seed_used": "",
                    "expanded_query": "",
                    "discovered_url_count": accepted,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": 1,
                    "page_raw_count": accepted,
                    "page_raw_count_before_dedupe": len(links),
                    "duplicate_count": max(0, len(links) - accepted),
                    "duplicate_ratio": round((max(0, len(links) - accepted) / max(len(links), 1)), 4),
                    "stop_reason": str(sitemap_state["stop_reason"] or ("ok" if links else "no_thread_links")),
                }
            )
        self.business_health["discovered_thread_count"] = len(discovered)
        discovered_links = list(discovered.values())
        self._record_seed_discovery_audit(discovery_queries=discovery_queries, links=discovered_links)
        return discovered_links

    def _ingest_discovery_links(
        self,
        *,
        discovered: dict[str, ThreadLink],
        links: list[ThreadLink],
        discovery_queries: list[DiscoveryQuery],
        channel: str,
        surface_url: str,
        board: str,
        max_per_url: int | None,
    ) -> dict[str, int]:
        """Ingest one discovery batch and persist audit-friendly decision counts."""
        accepted = 0
        duplicate_count = 0
        excluded_count = 0
        seed_filtered_count = 0
        for link in links:
            decision = self._discovery_decision(link, discovered, discovery_queries)
            if decision == "duplicate":
                duplicate_count += 1
                continue
            if decision == "excluded":
                excluded_count += 1
                continue
            if decision == "seed_filtered":
                seed_filtered_count += 1
                continue
            discovered[link.url] = link
            accepted += 1
            if max_per_url is not None and accepted >= max_per_url:
                break

        self.business_health["inventory_thread_count"] = int(self.business_health["inventory_thread_count"]) + len(links)
        self.business_health["discovery_duplicate_count"] = int(self.business_health["discovery_duplicate_count"]) + duplicate_count
        self.business_health["discovery_excluded_count"] = int(self.business_health["discovery_excluded_count"]) + excluded_count
        self.business_health["discovery_seed_filtered_count"] = (
            int(self.business_health["discovery_seed_filtered_count"]) + seed_filtered_count
        )
        self.discovery_audit_rows.append(
            {
                "source_id": self.source_name,
                "channel": channel,
                "surface_url": surface_url,
                "board": board,
                "inventory_thread_count": len(links),
                "accepted_thread_count": accepted,
                "seed_filtered_count": seed_filtered_count,
                "excluded_count": excluded_count,
                "duplicate_count": duplicate_count,
            }
        )
        return {
            "accepted": accepted,
            "duplicate_count": duplicate_count,
            "excluded_count": excluded_count,
            "seed_filtered_count": seed_filtered_count,
        }

    def _expand_sitemap_index_rows(self, user_agent: str) -> list[dict[str, str]]:
        """Return child sitemap rows discovered from configured sitemap index pages."""
        rows: list[dict[str, str]] = []
        include_patterns = [str(pattern) for pattern in (self.config.get("sitemap_index_include_patterns", []) or []) if str(pattern).strip()]
        exclude_patterns = [str(pattern) for pattern in (self.config.get("sitemap_index_exclude_patterns", []) or []) if str(pattern).strip()]
        for row in self.config.get("sitemap_index_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url or not self._robots_allowed(url, user_agent):
                continue
            response = self._fetch_with_retries(url, user_agent=user_agent, stage="sitemap_index_fetch")
            if not response.ok:
                continue
            for child_url in discover_sitemap_index_urls(response.body_text):
                lowered = child_url.lower()
                if include_patterns and not any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in include_patterns):
                    continue
                if exclude_patterns and any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in exclude_patterns):
                    continue
                rows.append({"url": child_url, "board": board})
        deduped: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in rows:
            url = str(row.get("url", "")).strip()
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append({"url": url, "board": str(row.get("board", "")).strip()})
        return deduped

    def _expanded_discovery_url_rows(self) -> list[dict[str, str]]:
        """Expand listing URLs across simple public pagination patterns."""
        source_page_count = int(os.getenv("BUSINESS_COMMUNITY_DISCOVERY_PAGE_COUNT", self.config.get("discovery_page_count", 1)))
        rows: list[dict[str, str]] = []
        for row in self.config.get("discovery_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url:
                continue
            row_page_count = source_page_count
            if isinstance(row, dict) and row.get("page_count") is not None:
                try:
                    row_page_count = int(row.get("page_count", source_page_count))
                except (TypeError, ValueError):
                    row_page_count = source_page_count
            row_page_count = max(row_page_count, 1)
            rows.append({"url": url, "board": board, "page_no": "1", "listing_root": url})
            for page_no in range(2, row_page_count + 1):
                rows.append(
                    {
                        "url": _paginate_listing_url(url, str(self.config.get("platform", "")), page_no),
                        "board": board,
                        "page_no": str(page_no),
                        "listing_root": url,
                    }
                )
        return rows

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
        """Return compact token terms from source-specific discovery queries."""
        discovery_queries = self._discovery_queries()
        terms: list[str] = []
        for query in discovery_queries:
            for token in query.token_terms:
                if token not in terms:
                    terms.append(token)
        if terms:
            return terms
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

    def _discovery_queries(self) -> list[DiscoveryQuery]:
        """Return source-specific discovery queries built from the local seed bank."""
        return build_discovery_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )

    def _matches_seed_terms(self, title: str, seed_terms: list[str]) -> bool:
        """Return whether a discovered title overlaps configured seed terms."""
        lowered = title.lower()
        minimum_matches = int(self.config.get("min_seed_term_matches", 1) or 1)
        return sum(1 for term in seed_terms if term in lowered) >= minimum_matches

    def _accept_discovered_link(self, link: ThreadLink, discovery_queries: list[DiscoveryQuery]) -> bool:
        """Return whether a discovered thread is worth fetching."""
        return self._discovery_decision(link, {}, discovery_queries) == "accepted"

    def _discovery_decision(
        self,
        link: ThreadLink,
        discovered: dict[str, ThreadLink],
        discovery_queries: list[DiscoveryQuery],
    ) -> str:
        """Return the audit-friendly decision for one discovered thread."""
        if link.url in discovered:
            return "duplicate"
        if self._matches_excluded_discovery_pattern(link):
            return "excluded"
        if not bool(self.config.get("filter_discovery_by_seed", False)):
            return "accepted"
        if not discovery_queries:
            return "accepted"
        if any(self._matches_discovery_query(link.title, query) for query in discovery_queries):
            return "accepted"
        return "seed_filtered"

    def _matches_discovery_query(self, title: str, query: DiscoveryQuery) -> bool:
        """Return whether a discovered title matches one expanded source query."""
        lowered = title.lower()
        if query.expanded_query and query.expanded_query in lowered:
            return True
        token_hits = sum(1 for term in query.token_terms if term in lowered)
        minimum_matches = int(self.config.get("min_seed_term_matches", 1) or 1)
        if token_hits >= minimum_matches:
            return True
        return False

    def _record_seed_discovery_audit(
        self,
        discovery_queries: list[DiscoveryQuery],
        links: list[ThreadLink],
        records: list[RawRecord] | None = None,
    ) -> None:
        """Record source-seed discovery yield so low-recall seeds are visible immediately."""
        if not discovery_queries:
            return
        titles = [link.title for link in links]
        if records:
            titles.extend(str(record.title) for record in records if str(record.title).strip())
        if not titles:
            return
        for index, query in enumerate(discovery_queries, start=1):
            discovered_count = sum(1 for title in titles if self._matches_discovery_query(title, query))
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": f"seed_discovery_{index:03d}",
                    "query_text": query.expanded_query,
                    "seed_used": query.seed_used,
                    "expanded_query": query.expanded_query,
                    "discovered_url_count": discovered_count,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": 1,
                    "page_raw_count": discovered_count,
                    "page_raw_count_before_dedupe": len(titles),
                    "duplicate_count": 0,
                    "duplicate_ratio": 0.0,
                    "stop_reason": "seed_match_audit",
                }
            )

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
        cache_key = _robots_cache_key(url)
        if cache_key in self._robots_cache:
            return self._robots_cache[cache_key]
        allowed, reason = check_robots_allowed(url, user_agent=user_agent)
        self._robots_cache[cache_key] = allowed
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

    def _warn_on_low_discovery(self, discovered: list[ThreadLink]) -> None:
        """Warn when discovery volume is low, but keep collecting what is publicly available."""
        fail_fast = os.getenv("COLLECT_FAIL_FAST_ON_LOW_RAW", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
        threshold = int(self.config.get("min_raw_records_warn", os.getenv("COLLECT_MIN_RAW_RECORDS_WARN", "600")))
        if threshold <= 0:
            return
        discovered_count = len(discovered)
        if discovered_count > threshold:
            return
        LOGGER.warning(
            "%s discovered only %s unique public thread URLs against threshold>%s; continuing with available public threads%s.",
            self.source_name,
            discovered_count,
            threshold,
            " (fail-fast disabled for low-discovery business community sources)" if fail_fast else "",
        )

    def _fetch_with_retries(self, url: str, user_agent: str, stage: str):
        """Fetch one public community URL with retries for transient network failures."""
        timeout_seconds = int(self.config.get("timeout_seconds", 20))
        max_attempts = int(self.config.get("fetch_retry_attempts", 4) or 4)
        retryable_statuses = {0, 429, 500, 502, 503, 504}
        last_response = None
        for attempt in range(1, max_attempts + 1):
            response = fetch_text(url, user_agent=user_agent, timeout_seconds=timeout_seconds)
            last_response = response
            if response.ok:
                return response
            retryable = response.status_code in retryable_statuses or response.crawl_status in {"network_error", "network_timeout"}
            if not retryable or attempt >= max_attempts:
                self._record_error(url, stage, str(response.status_code), response.error_message or response.crawl_status)
                return response
            wait_seconds = self._retry_sleep_seconds(attempt, response.status_code, response.crawl_status)
            LOGGER.warning(
                "%s received %s for %s (attempt %s/%s); sleeping %.1fs before retry.",
                self.source_name,
                response.error_message or response.crawl_status or str(response.status_code),
                url,
                attempt,
                max_attempts,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            self._network_retry_sleep_seconds += wait_seconds
        return last_response

    def _retry_sleep_seconds(self, attempt: int, status_code: int, crawl_status: str) -> float:
        """Return bounded backoff seconds for transient public fetch failures."""
        if status_code == 429:
            base_seconds = float(self.config.get("rate_limit_retry_base_seconds", 15.0) or 15.0)
            max_seconds = float(self.config.get("rate_limit_retry_max_seconds", 90.0) or 90.0)
        elif crawl_status in {"network_error", "network_timeout"} or status_code == 0:
            base_seconds = float(self.config.get("network_retry_base_seconds", 5.0) or 5.0)
            max_seconds = float(self.config.get("network_retry_max_seconds", 30.0) or 30.0)
        else:
            base_seconds = float(self.config.get("fetch_retry_base_seconds", 3.0) or 3.0)
            max_seconds = float(self.config.get("fetch_retry_max_seconds", 20.0) or 20.0)
        return min(base_seconds * attempt, max_seconds)

    def _discovery_network_failed(self) -> bool:
        """Return whether discovery failed because all remote fetches hit network/DNS errors."""
        discovery_errors = [
            row
            for row in self.error_stats
            if str(row.get("error_stage", "")) in {"api_fetch", "discovery_fetch", "rss_fetch", "sitemap_fetch", "sitemap_index_fetch"}
        ]
        if not discovery_errors:
            return False
        return all(str(row.get("error_code", "")).strip() in {"0", ""} for row in discovery_errors)


def _optional_positive_int(*values: Any) -> int | None:
    """Return the first positive integer from the provided values, else None for no cap."""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in {"none", "null", "unlimited", "inf", "infinite"}:
            return None
        try:
            parsed = int(text)
        except (TypeError, ValueError):
            continue
        if parsed <= 0:
            return None
        return parsed
    return None


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


def _khoros_items(body_text: str) -> list[dict[str, Any]]:
    """Return message items from a Khoros search response."""
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return []
    items = payload.get("data", {}).get("items", []) if isinstance(payload, dict) else []
    return [item for item in items if isinstance(item, dict)]


def _clean_khoros_title(value: str) -> str:
    """Normalize Khoros reply prefixes while preserving the public subject."""
    return re.sub(r"^\s*Re:\s*", "", unescape(str(value or "")), flags=re.IGNORECASE).strip()


def _strip_html(value: str) -> str:
    """Convert simple HTML bodies from public APIs into plain raw text."""
    text = unescape(str(value or ""))
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"</(?:p|div|li|ul|ol|blockquote)>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _paginate_listing_url(url: str, platform: str, page_no: int) -> str:
    """Return a best-effort paginated listing URL for supported community platforms."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if platform == "hubspot":
        path = re.sub(r"/page/\d+$", "", path)
        return urlunparse((parsed.scheme, parsed.netloc, f"{path}/page/{page_no}", "", parsed.query, ""))
    if platform == "amplitude":
        if re.search(r"/p\d+$", path):
            path = re.sub(r"/p\d+$", f"/p{page_no}", path)
            return urlunparse((parsed.scheme, parsed.netloc, path, "", parsed.query, ""))
        return urlunparse((parsed.scheme, parsed.netloc, f"{path}/p{page_no}", "", parsed.query, ""))
    if platform == "domo":
        if re.search(r"/p\d+$", path):
            path = re.sub(r"/p\d+$", f"/p{page_no}", path)
            return urlunparse((parsed.scheme, parsed.netloc, path, "", parsed.query, ""))
        return urlunparse((parsed.scheme, parsed.netloc, f"{path}/p{page_no}", "", parsed.query, ""))
    query = dict(parse_qsl(parsed.query, keep_blank_values=False))
    query["page"] = str(page_no)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query), ""))


def _robots_cache_key(url: str) -> str:
    """Group repeated robots checks by stable public path family."""
    parsed = urlparse(url)
    path = parsed.path
    if "/google-ads/thread/" in path:
        path = "/google-ads/thread"
    elif "/merchants/thread/" in path:
        path = "/merchants/thread"
    elif "/analytics/thread/" in path:
        path = "/analytics/thread"
    elif "/looker-studio/thread/" in path:
        path = "/looker-studio/thread"
    elif path.endswith("/threads"):
        path = path.rsplit("/", 1)[0] + "/threads"
    elif path.endswith("/community"):
        path = path.rsplit("/", 1)[0] + "/community"
    return f"{parsed.scheme}://{parsed.netloc}{path}"
