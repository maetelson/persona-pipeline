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
        self.business_health: dict[str, int | str] = {
            "source_id": self.source_name,
            "discovered_thread_count": 0,
            "fetched_thread_count": 0,
            "parse_success_count": 0,
            "parse_error_count": 0,
        }

    def collect(self) -> list[RawRecord]:
        """Discover, fetch, and parse public community threads."""
        if bool((self.config.get("api_discovery", {}) or {}).get("enabled", False)):
            records = self._collect_api_records()
            if records:
                self._record_collection_summary(len(records))
                LOGGER.info("Collected %s business community API rows for %s", len(records), self.source_name)
                return records

        discovered = self._discover_threads()
        self._fail_fast_on_low_discovery(discovered)
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
        if not base_url or not board_ids:
            return []
        max_items = int(api_cfg.get("max_items_per_board", 200))
        page_size = min(max(int(api_cfg.get("page_size", 100)), 1), 100)
        user_agent = self._user_agent()
        records: list[RawRecord] = []
        seen_ids: set[str] = set()
        for board_id in board_ids:
            fetched_for_board = 0
            for offset in range(0, max_items, page_size):
                limit = min(page_size, max_items - offset)
                query = f"SELECT * FROM messages WHERE board.id='{board_id}' LIMIT {limit} OFFSET {offset}"
                url = f"{base_url}/api/2.0/search?q={quote(query)}"
                response = fetch_text(url, user_agent=user_agent, timeout_seconds=int(self.config.get("timeout_seconds", 20)))
                if not response.ok:
                    self._record_error(url, "api_fetch", str(response.status_code), response.error_message or response.crawl_status)
                    break
                items = _khoros_items(response.body_text)
                accepted = 0
                for item in items:
                    raw_id = str(item.get("id", "") or "").strip()
                    if not raw_id or raw_id in seen_ids:
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
                        "duplicate_count": max(0, len(items) - accepted),
                        "duplicate_ratio": round((max(0, len(items) - accepted) / max(len(items), 1)), 4),
                        "stop_reason": "ok" if items else "empty_results",
                    }
                )
                if len(items) < limit:
                    break
            LOGGER.info("Khoros API discovery for %s board=%s accepted=%s", self.source_name, board_id, fetched_for_board)
        return records[: int(self.config.get("max_threads_per_run", len(records)))]

    def _build_khoros_record(self, item: dict[str, Any], board_id: str) -> RawRecord:
        """Build a raw record from one public Khoros API message item."""
        fetched_at = utc_now_iso()
        url = str(item.get("view_href", "") or "")
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
        max_per_url = int(os.getenv("BUSINESS_COMMUNITY_MAX_DISCOVERY_PER_URL", self.config.get("max_discovered_threads_per_url", 20)))
        discovery_queries = self._discovery_queries()
        discovered: dict[str, ThreadLink] = {}
        for row in self._expanded_discovery_url_rows():
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
                if not self._accept_discovered_link(link, discovery_queries):
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
                if not self._accept_discovered_link(link, discovery_queries):
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
        self.business_health["discovered_thread_count"] = len(discovered)
        discovered_links = list(discovered.values())
        self._record_seed_discovery_audit(discovery_queries=discovery_queries, links=discovered_links)
        return discovered_links

    def _expanded_discovery_url_rows(self) -> list[dict[str, str]]:
        """Expand listing URLs across simple public pagination patterns."""
        page_count = int(os.getenv("BUSINESS_COMMUNITY_DISCOVERY_PAGE_COUNT", self.config.get("discovery_page_count", 1)))
        rows: list[dict[str, str]] = []
        for row in self.config.get("discovery_urls", []) or []:
            url = str(row.get("url", "") if isinstance(row, dict) else row).strip()
            board = str(row.get("board", "") if isinstance(row, dict) else "").strip()
            if not url:
                continue
            rows.append({"url": url, "board": board})
            for page_no in range(2, max(page_count, 1) + 1):
                rows.append({"url": _paginate_listing_url(url, str(self.config.get("platform", "")), page_no), "board": board})
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
        if self._matches_excluded_discovery_pattern(link):
            return False
        if not bool(self.config.get("filter_discovery_by_seed", False)):
            return True
        if not discovery_queries:
            return True
        return any(self._matches_discovery_query(link.title, query) for query in discovery_queries)

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

    def _fail_fast_on_low_discovery(self, discovered: list[ThreadLink]) -> None:
        """Abort before thread fetch when listing discovery cannot satisfy the raw-volume gate."""
        fail_fast = os.getenv("COLLECT_FAIL_FAST_ON_LOW_RAW", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
        if not fail_fast:
            return
        threshold = int(self.config.get("min_raw_records_warn", os.getenv("COLLECT_MIN_RAW_RECORDS_WARN", "600")))
        if threshold <= 0:
            return
        discovered_count = len(discovered)
        if discovered_count > threshold:
            return
        self._record_collection_summary(0)
        raise RuntimeError(
            f"{self.source_name} discovered only {discovered_count} unique public thread URLs; "
            f"minimum raw volume requires more than {threshold}. "
            "The configured public listing/category pages do not expose enough unique threads."
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
    elif path.endswith("/threads"):
        path = path.rsplit("/", 1)[0] + "/threads"
    elif path.endswith("/community"):
        path = path.rsplit("/", 1)[0] + "/community"
    return f"{parsed.scheme}://{parsed.netloc}{path}"
