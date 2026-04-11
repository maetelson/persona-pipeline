"""Google Ads Help Community collector using public Help Community pages."""

from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from src.collectors.base import BaseCollector, RawRecord
from src.collectors.business_community_parser import ThreadLink, discover_thread_links, parse_thread_page
from src.utils.dates import utc_now_iso
from src.utils.http_fetch import check_robots_allowed, fetch_text
from src.utils.logging import get_logger
from src.utils.seed_bank import DiscoveryQuery, build_discovery_queries
from src.utils.text import make_hash_id

LOGGER = get_logger("collectors.google_ads_help_community")


class GoogleAdsHelpCommunityCollector(BaseCollector):
    """Collect public threads from the Google Ads Help Community."""

    source_name = "google_ads_help_community"
    source_type = "help_community"

    def __init__(self, config: dict[str, Any], data_dir: Path) -> None:
        super().__init__(config=config, data_dir=data_dir)
        self.business_health: dict[str, int | str] = {
            "source_id": self.source_name,
            "discovered_listing_count": 0,
            "discovered_thread_count": 0,
            "fetched_thread_count": 0,
            "parse_success_count": 0,
            "parse_error_count": 0,
            "discovery_mode": "public_help_community_html",
        }
        self._robots_cache: dict[str, bool] = {}

    def collect(self) -> list[RawRecord]:
        """Discover public Help Community threads, fetch pages, and preserve raw rows."""
        user_agent = self._user_agent()
        listing_rows = self._discover_listing_rows(user_agent)
        discovered = self._discover_threads(listing_rows, user_agent)
        self._fail_fast_on_low_discovery(discovered)
        records = self._fetch_thread_records(discovered, user_agent)
        self._record_collection_summary(len(records))
        LOGGER.info("Collected %s Google Ads Help Community rows", len(records))
        return records

    def _discover_listing_rows(self, user_agent: str) -> list[dict[str, str]]:
        """Return configured and auto-discovered Help Community listing pages."""
        rows: dict[str, dict[str, str]] = {}
        for row in self.config.get("discovery_urls", []) or []:
            self._add_listing_row(rows, row)

        home_rows = list(self.config.get("home_urls", []) or [])
        for row in home_rows:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url or not self._robots_allowed(url, user_agent):
                continue
            response = fetch_text(url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
            if not response.ok:
                self._record_error(url, "home_fetch", str(response.status_code), response.error_message or response.crawl_status)
                continue
            self._add_listing_row(rows, {"url": url, "board": board or "Google Ads Help Community"})
            for listing_url, listing_board in self._extract_category_listing_rows(response.body_text, base_url=url).items():
                self._add_listing_row(rows, {"url": listing_url, "board": listing_board})

        for category, board in (self.config.get("category_boards", {}) or {}).items():
            self._add_listing_row(
                rows,
                {
                    "url": f"https://support.google.com/google-ads/threads?hl=en&thread_filter=(category:{category})",
                    "board": str(board),
                },
            )

        self.business_health["discovered_listing_count"] = len(rows)
        return list(rows.values())

    def _discover_threads(self, listing_rows: list[dict[str, str]], user_agent: str) -> list[ThreadLink]:
        """Extract thread links from each public Help Community listing page."""
        platform = str(self.config.get("platform", "google_support"))
        max_per_url = int(os.getenv("GOOGLE_ADS_HELP_MAX_DISCOVERY_PER_URL", self.config.get("max_discovered_threads_per_url", 100)))
        discovery_queries = self._discovery_queries()
        discovered: dict[str, ThreadLink] = {}
        for row in listing_rows:
            url = str(row.get("url", "")).strip()
            board = str(row.get("board", "")).strip()
            if not url or not self._robots_allowed(url, user_agent):
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
                if not self._accept_discovered_link(link, discovery_queries):
                    continue
                discovered[link.url] = link
                accepted += 1
                if accepted >= max_per_url:
                    break
            self._record_page_stat(
                query_id="help_thread_discovery",
                query_text=url,
                raw_count=accepted,
                before_dedupe=len(links),
                stop_reason="ok" if links else "no_thread_links",
            )

        self.business_health["discovered_thread_count"] = len(discovered)
        discovered_links = list(discovered.values())
        self._record_seed_discovery_audit(discovery_queries, discovered_links)
        return discovered_links

    def _fetch_thread_records(self, discovered: list[ThreadLink], user_agent: str) -> list[RawRecord]:
        """Fetch discovered thread pages and parse them into raw records."""
        max_threads = int(os.getenv("GOOGLE_ADS_HELP_MAX_THREADS", self.config.get("max_threads_per_run", 1200)))
        max_workers = int(os.getenv("GOOGLE_ADS_HELP_MAX_WORKERS", self.config.get("max_fetch_workers", 4)))
        seen_urls: set[str] = set()
        selected_links: list[ThreadLink] = []
        for link in discovered:
            if link.url in seen_urls:
                continue
            seen_urls.add(link.url)
            if len(seen_urls) > max_threads:
                break
            selected_links.append(link)

        if max_workers <= 1:
            records: list[RawRecord] = []
            for link in selected_links:
                record = self._fetch_one_thread(link, user_agent)
                if record is not None:
                    records.append(record)
            return records

        records_by_url: dict[str, RawRecord] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._fetch_one_thread, link, user_agent): link for link in selected_links}
            for future in as_completed(futures):
                link = futures[future]
                try:
                    record = future.result()
                except Exception as exc:  # noqa: BLE001
                    self._record_error(link.url, "thread_fetch", "", str(exc))
                    continue
                if record is not None:
                    records_by_url[link.url] = record
        return [records_by_url[link.url] for link in selected_links if link.url in records_by_url]

    def _fetch_one_thread(self, link: ThreadLink, user_agent: str) -> RawRecord | None:
        """Fetch and parse one public thread page."""
        sleep_seconds = float(self.config.get("sleep_seconds", 0.1))
        if not self._robots_allowed(link.url, user_agent):
            return None
        response = fetch_text(link.url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
        if not response.ok:
            self._record_error(link.url, "thread_fetch", str(response.status_code), response.error_message or response.crawl_status)
            return None
        self.business_health["fetched_thread_count"] = int(self.business_health["fetched_thread_count"]) + 1
        try:
            parsed = parse_thread_page(
                response.body_text,
                url=link.url,
                platform=str(self.config.get("platform", "google_support")),
                fallback=link,
                product_or_tool=str(self.config.get("product_or_tool", "Google Ads")),
            )
        except Exception as exc:  # noqa: BLE001
            self.business_health["parse_error_count"] = int(self.business_health["parse_error_count"]) + 1
            self._record_error(link.url, "parse", "", str(exc))
            return None
        if parsed.parse_status == "parse_empty":
            self.business_health["parse_error_count"] = int(self.business_health["parse_error_count"]) + 1
            self._record_error(link.url, "parse", "", "parsed thread status was parse_empty")
            return None
        self.business_health["parse_success_count"] = int(self.business_health["parse_success_count"]) + 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        return self._build_record(parsed, link)

    def _build_record(self, parsed, link: ThreadLink) -> RawRecord:
        """Build one raw record using the shared business-community schema."""
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
                "collector": "google_ads_help_community",
            }
        )
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "business_communities")),
            source_name=str(self.config.get("source_name", "Google Ads Help Community")),
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
            query_id="help_thread_discovery",
            query_text=link.title,
            author_hint=parsed.author_name,
            author_name=parsed.author_name,
            product_or_tool=str(self.config.get("product_or_tool", "Google Ads")),
            subreddit_or_forum=parsed.board or link.board,
            thread_title=parsed.title,
            crawl_method="public_help_community_html",
            crawl_status=parsed.parse_status,
            parse_version="google_ads_help_community_v1",
            hash_id=make_hash_id(self.source_name, parsed.raw_id, parsed.canonical_url),
            source_meta=source_meta,
        )

    def _extract_category_listing_rows(self, html: str, base_url: str) -> dict[str, str]:
        """Extract category listing URLs exposed in the server-rendered page."""
        category_boards = {str(key): str(value) for key, value in (self.config.get("category_boards", {}) or {}).items()}
        categories = set(re.findall(r"category:([a-z0-9_]+)", html))
        listing_rows: dict[str, str] = {}
        for category in categories:
            board = category_boards.get(category, category.replace("_", " ").title())
            listing_rows[f"https://support.google.com/google-ads/threads?hl=en&thread_filter=(category:{category})"] = board

        for href in re.findall(r"href=[\"']([^\"']*thread_filter=[^\"']+)[\"']", html):
            url = urljoin(base_url, unescape(href).replace("&amp;", "&"))
            if "/google-ads/threads" not in url:
                continue
            board = self._board_from_url(url, category_boards)
            listing_rows[url] = board

        for thread_filter in self.config.get("additional_thread_filters", []) or []:
            value = str(thread_filter).strip()
            if not value:
                continue
            filter_value = value if value.startswith("(") else f"({value})"
            listing_rows[f"https://support.google.com/google-ads/threads?hl=en&thread_filter={filter_value}"] = filter_value
        return listing_rows

    def _board_from_url(self, url: str, category_boards: dict[str, str]) -> str:
        """Return a readable board label for a Help Community listing URL."""
        match = re.search(r"category:([a-z0-9_]+)", url)
        if not match:
            return "Google Ads Help Community"
        category = match.group(1)
        return category_boards.get(category, category.replace("_", " ").title())

    def _add_listing_row(self, rows: dict[str, dict[str, str]], row: object) -> None:
        """Add a listing row to the ordered dedupe map."""
        url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
        board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
        if not url:
            return
        rows.setdefault(url, {"url": url, "board": board or "Google Ads Help Community"})

    def _record_page_stat(self, query_id: str, query_text: str, raw_count: int, before_dedupe: int, stop_reason: str) -> None:
        """Record one listing discovery audit row."""
        duplicate_count = max(0, before_dedupe - raw_count)
        self.collection_stats.append(
            {
                "source": self.source_name,
                "query_id": query_id,
                "query_text": query_text,
                "seed_used": "",
                "expanded_query": "",
                "discovered_url_count": raw_count,
                "window_id": "",
                "window_start": "",
                "window_end": "",
                "page_no": 1,
                "page_raw_count": raw_count,
                "page_raw_count_before_dedupe": before_dedupe,
                "duplicate_count": duplicate_count,
                "duplicate_ratio": round(duplicate_count / max(before_dedupe, 1), 4),
                "stop_reason": stop_reason,
            }
        )

    def _record_collection_summary(self, record_count: int) -> None:
        """Record a source-level fetch summary for raw audits."""
        self.collection_stats.append(
            {
                "source": self.source_name,
                "query_id": "help_thread_fetch",
                "query_text": "public_help_community_thread_pages",
                "seed_used": "",
                "expanded_query": "",
                "discovered_url_count": record_count,
                "window_id": "",
                "window_start": "",
                "window_end": "",
                "page_no": 1,
                "page_raw_count": record_count,
                "page_raw_count_before_dedupe": int(self.business_health["fetched_thread_count"]),
                "duplicate_count": max(
                    0,
                    int(self.business_health["discovered_thread_count"]) - int(self.business_health["fetched_thread_count"]),
                ),
                "duplicate_ratio": 0.0,
                "stop_reason": "ok" if record_count else "empty_results",
            }
        )

    def _fail_fast_on_low_discovery(self, discovered: list[ThreadLink]) -> None:
        """Abort when public listing discovery cannot satisfy this source's raw floor."""
        fail_fast = os.getenv("COLLECT_FAIL_FAST_ON_LOW_RAW", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
        if not fail_fast:
            return
        threshold = int(self.config.get("min_raw_records_warn", os.getenv("COLLECT_MIN_RAW_RECORDS_WARN", "600")))
        discovered_count = len(discovered)
        if discovered_count > threshold:
            return
        self._record_collection_summary(0)
        raise RuntimeError(
            f"{self.source_name} discovered only {discovered_count} unique public Help Community thread URLs; "
            f"required>{threshold}. Public category/listing HTML did not expose the claimed 1000+ rows. "
            "Use an allowed bulk export/API or add more public listing surfaces before continuing."
        )

    def _record_error(self, url: str, stage: str, code: str, message: str) -> None:
        """Record collection errors in the shared raw error audit format."""
        self.error_stats.append(
            {
                "source": self.source_name,
                "query_id": "help_thread_discovery",
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

    def _robots_allowed(self, url: str, user_agent: str) -> bool:
        """Honor robots.txt for public Help Community pages."""
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

    def _discovery_queries(self) -> list[DiscoveryQuery]:
        """Return source-specific discovery queries for listing-title filtering and audit."""
        return build_discovery_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )

    def _accept_discovered_link(self, link: ThreadLink, discovery_queries: list[DiscoveryQuery]) -> bool:
        """Return whether a discovered link matches the active source-specific seed bank."""
        if not bool(self.config.get("filter_discovery_by_seed", False)):
            return True
        if not discovery_queries:
            return True
        return any(self._matches_discovery_query(link.title, query) for query in discovery_queries)

    def _matches_discovery_query(self, title: str, query: DiscoveryQuery) -> bool:
        """Return whether a title aligns with one expanded query."""
        lowered = title.lower()
        if query.expanded_query and query.expanded_query in lowered:
            return True
        min_matches = int(self.config.get("min_seed_term_matches", 1) or 1)
        token_hits = sum(1 for term in query.token_terms if term in lowered)
        return token_hits >= min_matches

    def _record_seed_discovery_audit(self, discovery_queries: list[DiscoveryQuery], links: list[ThreadLink]) -> None:
        """Record how many discovered URLs each source-specific query retrieved."""
        if not discovery_queries or not links:
            return
        for index, query in enumerate(discovery_queries, start=1):
            discovered_count = sum(1 for link in links if self._matches_discovery_query(link.title, query))
            self.collection_stats.append(
                {
                    "source": self.source_name,
                    "query_id": f"help_seed_discovery_{index:03d}",
                    "query_text": query.expanded_query,
                    "seed_used": query.seed_used,
                    "expanded_query": query.expanded_query,
                    "discovered_url_count": discovered_count,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": 1,
                    "page_raw_count": discovered_count,
                    "page_raw_count_before_dedupe": len(links),
                    "duplicate_count": 0,
                    "duplicate_ratio": 0.0,
                    "stop_reason": "seed_match_audit",
                }
            )

    def _user_agent(self) -> str:
        """Return a declared browser-like user agent."""
        return os.getenv(
            "PUBLIC_WEB_USER_AGENT",
            "Mozilla/5.0 (compatible; persona-pipeline/0.1; public research)",
        )


def _safe_iso_datetime(value: str) -> str:
    """Return a UTC ISO datetime only when the page value is parseable."""
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


def _robots_cache_key(url: str) -> str:
    """Group Google Support robot checks by stable public path family."""
    parsed = urlparse(url)
    path = parsed.path
    if "/google-ads/thread/" in path:
        path = "/google-ads/thread"
    elif path.endswith("/threads"):
        path = "/google-ads/threads"
    elif path.endswith("/community"):
        path = "/google-ads/community"
    return f"{parsed.scheme}://{parsed.netloc}{path}"
