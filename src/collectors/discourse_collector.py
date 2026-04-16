"""Public Discourse JSON collector for forum-style discussion sources."""

from __future__ import annotations

from datetime import UTC, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin
from urllib.request import Request, urlopen

from src.collectors.base import BaseCollector, RawRecord
from src.utils.dates import utc_now_iso
from src.utils.seed_bank import DiscoveryQuery, build_discovery_queries, resolve_seed_queries
from src.utils.text import clean_text, combine_text, make_hash_id


class DiscourseCollector(BaseCollector):
    """Collect public Discourse topics through the JSON endpoints."""

    source_name = "discourse"
    source_type = "forum"

    def __init__(self, config: dict[str, Any], data_dir: Path, source_name: str | None = None) -> None:
        self.source_name = source_name or str(config.get("source_id", config.get("source", "discourse")))
        super().__init__(config=config, data_dir=data_dir)

    def collect(self) -> list[RawRecord]:
        """Collect topic-level raw records from latest pages and search queries."""
        if bool(self.config.get("use_stub", False)):
            return [self.build_stub_record()]

        max_topics = _optional_positive_int(
            os.getenv("DISCOURSE_MAX_TOPICS"),
            self.config.get("max_topics_per_run"),
        )
        topic_refs = self._discover_topic_refs(max_topics=max_topics)
        seen_ids: set[int] = set()
        selected_refs: list[dict[str, Any]] = []
        for ref in topic_refs:
            topic_id = int(ref.get("id", 0) or 0)
            if not topic_id or topic_id in seen_ids:
                continue
            seen_ids.add(topic_id)
            if max_topics is not None and len(selected_refs) >= max_topics:
                break
            selected_refs.append(ref)

        records = self._fetch_topic_records(selected_refs)
        self._record_fetch_summary(len(records), len(selected_refs))
        return records

    def _fetch_topic_records(self, topic_refs: list[dict[str, Any]]) -> list[RawRecord]:
        """Fetch topic details, using bounded parallelism for larger Discourse sources."""
        max_workers = int(os.getenv("DISCOURSE_MAX_WORKERS", self.config.get("max_fetch_workers", 8)))
        sleep_seconds = float(self.config.get("sleep_seconds", 0.1))
        if max_workers <= 1:
            records: list[RawRecord] = []
            for ref in topic_refs:
                record = self._fetch_one_topic_record(ref)
                if record is not None:
                    records.append(record)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
            return records

        records_by_id: dict[int, RawRecord] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._fetch_one_topic_record, ref): ref for ref in topic_refs}
            for future in as_completed(futures):
                ref = futures[future]
                topic_id = int(ref.get("id", 0) or 0)
                try:
                    record = future.result()
                except Exception as exc:  # noqa: BLE001
                    self.error_stats.append(
                        {
                            "source": self.source_name,
                            "query_id": "topic_fetch",
                            "query_text": str(topic_id),
                            "window_id": "",
                            "window_start": "",
                            "window_end": "",
                            "page_no": 1,
                            "error_stage": "topic_fetch",
                            "error_type": type(exc).__name__,
                            "error_code": "",
                            "error_message": str(exc),
                            "is_retryable": self._is_retryable_error("", str(exc)),
                        }
                    )
                    continue
                if record is not None:
                    records_by_id[topic_id] = record
        return [records_by_id[int(ref["id"])] for ref in topic_refs if int(ref.get("id", 0) or 0) in records_by_id]

    def _fetch_one_topic_record(self, ref: dict[str, Any]) -> RawRecord | None:
        """Fetch and build one topic record."""
        topic_id = int(ref.get("id", 0) or 0)
        if not topic_id:
            return None
        topic = self._fetch_topic(topic_id, str(ref.get("slug", "")))
        if not topic:
            if bool(self.config.get("allow_listing_fallback", True)):
                return self._build_listing_fallback_record(ref, reason="topic_fetch_failed")
            return None
        return self._build_record(topic=topic, discovery_ref=ref)

    def _discover_topic_refs(self, max_topics: int | None) -> list[dict[str, Any]]:
        """Discover topic references from latest pages and source query seeds."""
        discovered: dict[int, dict[str, Any]] = {}
        for ref in self._latest_topic_refs():
            topic_id = int(ref.get("id", 0) or 0)
            if topic_id:
                discovered.setdefault(topic_id, ref)
            if max_topics is not None and len(discovered) >= max_topics:
                return list(discovered.values())
        for ref in self._search_topic_refs():
            topic_id = int(ref.get("id", 0) or 0)
            if topic_id:
                discovered.setdefault(topic_id, ref)
            if max_topics is not None and len(discovered) >= max_topics:
                break
        return list(discovered.values())

    def _latest_topic_refs(self) -> list[dict[str, Any]]:
        """Fetch topic references from `/latest.json` pages."""
        refs: list[dict[str, Any]] = []
        max_pages = _optional_positive_int(
            os.getenv("DISCOURSE_LATEST_PAGES"),
            self.config.get("max_latest_pages"),
        )
        page_limit = max_pages if max_pages is not None else 1000000
        for page_no in range(page_limit):
            url = self._api_url(f"/latest.json?page={page_no}")
            payload = self._fetch_json(url, query_id="latest", query_text="latest", page_no=page_no + 1)
            topics = payload.get("topic_list", {}).get("topics", []) if payload else []
            for topic in topics:
                ref = dict(topic)
                ref["discovery_query_id"] = "latest"
                ref["discovery_query_text"] = "latest"
                refs.append(ref)
            self._record_page_stat("latest", "latest", page_no + 1, len(topics), len(topics), "ok" if topics else "empty_results")
            if not topics:
                break
        return refs

    def _search_topic_refs(self) -> list[dict[str, Any]]:
        """Fetch topic references from `/search.json` for configured seeds."""
        refs: list[dict[str, Any]] = []
        max_pages = _optional_positive_int(
            os.getenv("DISCOURSE_SEARCH_PAGES"),
            self.config.get("max_search_pages"),
        )
        page_limit = max_pages if max_pages is not None else 1000000
        for query_index, query in enumerate(self._discovery_queries(), start=1):
            query_id = f"discourse_search_{query_index:03d}"
            for page_no in range(page_limit):
                url = self._api_url(f"/search.json?q={quote_plus(query.expanded_query)}&page={page_no}")
                payload = self._fetch_json(url, query_id=query_id, query_text=query.expanded_query, page_no=page_no + 1)
                topics = payload.get("topics", []) if payload else []
                for topic in topics:
                    ref = dict(topic)
                    ref["discovery_query_id"] = query_id
                    ref["discovery_query_text"] = query.expanded_query
                    ref["discovery_seed_used"] = query.seed_used
                    refs.append(ref)
                self._record_page_stat(
                    query_id,
                    query.expanded_query,
                    page_no + 1,
                    len(topics),
                    len(topics),
                    "ok" if topics else "empty_results",
                    seed_used=query.seed_used,
                    expanded_query=query.expanded_query,
                )
                if not topics:
                    break
        return refs

    def _fetch_topic(self, topic_id: int, slug: str) -> dict[str, Any]:
        """Fetch one topic JSON payload."""
        slug_part = slug or str(topic_id)
        url = self._api_url(f"/t/{slug_part}/{topic_id}.json")
        return self._fetch_json(url, query_id="topic_fetch", query_text=str(topic_id), page_no=1)

    def _build_record(self, topic: dict[str, Any], discovery_ref: dict[str, Any]) -> RawRecord | None:
        """Build a topic-level raw record from one Discourse topic payload."""
        topic_id = str(topic.get("id", "") or "")
        title = clean_text(topic.get("title") or discovery_ref.get("title"))
        posts = list(topic.get("post_stream", {}).get("posts", []) or [])
        visible_posts = [post for post in posts if not post.get("hidden") and not post.get("deleted_at")]
        if not topic_id or not title or not visible_posts:
            return None

        first_post = visible_posts[0]
        body = _html_to_text(str(first_post.get("cooked", "")))
        comments = combine_text(*[_html_to_text(str(post.get("cooked", ""))) for post in visible_posts[1:]])
        fetched_at = utc_now_iso()
        created_at = _safe_iso_datetime(str(topic.get("created_at") or first_post.get("created_at") or "")) or fetched_at
        slug = str(topic.get("slug") or discovery_ref.get("slug") or topic_id)
        url = self._public_topic_url(slug, topic_id)
        query_text = str(discovery_ref.get("discovery_query_text", "latest"))
        query_id = str(discovery_ref.get("discovery_query_id", "latest"))
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "discourse")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="thread",
            raw_id=topic_id,
            raw_source_id=topic_id,
            url=url,
            canonical_url=url,
            title=title,
            body=body,
            body_text=body,
            comments_text=comments,
            created_at=created_at,
            fetched_at=fetched_at,
            retrieved_at=fetched_at,
            query_seed=query_text,
            query_id=query_id,
            query_text=query_text,
            author_hint=clean_text(first_post.get("username") or first_post.get("display_username")),
            author_name=clean_text(first_post.get("username") or first_post.get("display_username")),
            product_or_tool=str(self.config.get("product_or_tool", "")),
            subreddit_or_forum=str(self.config.get("forum_name", self.config.get("source_name", self.source_name))),
            thread_title=title,
            crawl_method="discourse_json_api",
            crawl_status="ok",
            parse_version="discourse_json_v1",
            hash_id=make_hash_id(self.source_name, topic_id, url),
            source_meta={
                "raw_topic": topic,
                "discovery_ref": discovery_ref,
                "discovery_seed_used": discovery_ref.get("discovery_seed_used", ""),
                "post_count_collected": len(visible_posts),
                "reply_count": topic.get("reply_count", discovery_ref.get("reply_count")),
                "views": topic.get("views", discovery_ref.get("views")),
                "category_id": topic.get("category_id", discovery_ref.get("category_id")),
            },
        )

    def _build_listing_fallback_record(self, ref: dict[str, Any], reason: str) -> RawRecord | None:
        """Build a raw record from topic-list metadata when topic detail is rate-limited."""
        topic_id = str(ref.get("id", "") or "")
        title = clean_text(ref.get("title") or ref.get("fancy_title"))
        if not topic_id or not title:
            return None
        body = _html_to_text(str(ref.get("excerpt", "")))
        fetched_at = utc_now_iso()
        created_at = _safe_iso_datetime(str(ref.get("created_at") or ref.get("last_posted_at") or "")) or fetched_at
        slug = str(ref.get("slug") or topic_id)
        url = self._public_topic_url(slug, topic_id)
        query_text = str(ref.get("discovery_query_text", "latest"))
        query_id = str(ref.get("discovery_query_id", "latest"))
        return RawRecord(
            source=self.source_name,
            source_group=str(self.config.get("source_group", "discourse")),
            source_name=str(self.config.get("source_name", self.source_name)),
            source_type="thread",
            raw_id=topic_id,
            raw_source_id=topic_id,
            url=url,
            canonical_url=url,
            title=title,
            body=body,
            body_text=body,
            comments_text="",
            created_at=created_at,
            fetched_at=fetched_at,
            retrieved_at=fetched_at,
            query_seed=query_text,
            query_id=query_id,
            query_text=query_text,
            author_hint=clean_text(ref.get("last_poster_username")),
            author_name=clean_text(ref.get("last_poster_username")),
            product_or_tool=str(self.config.get("product_or_tool", "")),
            subreddit_or_forum=str(self.config.get("forum_name", self.config.get("source_name", self.source_name))),
            thread_title=title,
            crawl_method="discourse_json_api",
            crawl_status=f"listing_fallback:{reason}",
            parse_version="discourse_json_v1",
            hash_id=make_hash_id(self.source_name, topic_id, url),
            source_meta={
                "raw_topic_ref": ref,
                "fallback_reason": reason,
                "discovery_seed_used": ref.get("discovery_seed_used", ""),
                "reply_count": ref.get("reply_count"),
                "views": ref.get("views"),
                "category_id": ref.get("category_id"),
            },
        )

    def _query_seeds(self) -> list[str]:
        """Return configured search seeds with blanks removed."""
        raw_seeds = self.config.get("query_seeds") or resolve_seed_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )
        return [str(seed).strip() for seed in raw_seeds if str(seed).strip()]

    def _discovery_queries(self) -> list[DiscoveryQuery]:
        """Return source-only discovery queries for Discourse search expansion."""
        return build_discovery_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )

    def _api_url(self, path: str) -> str:
        """Build a full Discourse API URL."""
        base_url = str(self.config.get("base_url", "")).rstrip("/")
        if not base_url:
            raise ValueError(f"{self.source_name} requires base_url in source config")
        return urljoin(base_url + "/", path.lstrip("/"))

    def _public_topic_url(self, slug: str, topic_id: str) -> str:
        """Build a user-facing topic URL."""
        base_url = str(self.config.get("base_url", "")).rstrip("/")
        return f"{base_url}/t/{slug}/{topic_id}"

    def _fetch_json(self, url: str, query_id: str, query_text: str, page_no: int) -> dict[str, Any]:
        """Fetch JSON while recording retryable errors in raw audit format."""
        request = Request(
            url,
            headers={
                "User-Agent": self._user_agent(),
                "Accept": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=int(self.config.get("timeout_seconds", 20))) as response:
                payload = response.read().decode("utf-8", errors="replace")
                return json.loads(payload)
        except Exception as exc:  # noqa: BLE001
            self.error_stats.append(
                {
                    "source": self.source_name,
                    "query_id": query_id,
                    "query_text": query_text,
                    "window_id": "",
                    "window_start": "",
                    "window_end": "",
                    "page_no": page_no,
                    "error_stage": "fetch_json",
                    "error_type": type(exc).__name__,
                    "error_code": "",
                    "error_message": str(exc),
                    "is_retryable": self._is_retryable_error("", str(exc)),
                }
            )
            return {}

    def _record_page_stat(
        self,
        query_id: str,
        query_text: str,
        page_no: int,
        raw_count: int,
        before_dedupe: int,
        stop_reason: str,
        seed_used: str = "",
        expanded_query: str = "",
    ) -> None:
        """Record one Discourse discovery page audit row."""
        self.collection_stats.append(
            {
                "source": self.source_name,
                "query_id": query_id,
                "query_text": query_text,
                "seed_used": seed_used,
                "expanded_query": expanded_query or query_text,
                "discovered_url_count": raw_count,
                "window_id": "",
                "window_start": "",
                "window_end": "",
                "page_no": page_no,
                "page_raw_count": raw_count,
                "page_raw_count_before_dedupe": before_dedupe,
                "duplicate_count": 0,
                "duplicate_ratio": 0.0,
                "stop_reason": stop_reason,
            }
        )

    def _record_fetch_summary(self, record_count: int, attempted_count: int) -> None:
        """Record a source-level topic fetch summary."""
        self.collection_stats.append(
            {
                "source": self.source_name,
                "query_id": "topic_fetch",
                "query_text": "discourse_topic_json",
                "seed_used": "",
                "expanded_query": "",
                "discovered_url_count": record_count,
                "window_id": "",
                "window_start": "",
                "window_end": "",
                "page_no": 1,
                "page_raw_count": record_count,
                "page_raw_count_before_dedupe": attempted_count,
                "duplicate_count": max(0, attempted_count - record_count),
                "duplicate_ratio": 0.0,
                "stop_reason": "ok" if record_count else "empty_results",
            }
        )

    def _user_agent(self) -> str:
        """Return a declared user agent for public forum JSON requests."""
        return os.getenv(
            "PUBLIC_WEB_USER_AGENT",
            "Mozilla/5.0 (compatible; persona-pipeline/0.1; public research)",
        )


class _CookedHTMLParser(HTMLParser):
    """Small HTML-to-text parser for Discourse cooked post bodies."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        text = clean_text(data)
        if text:
            self.parts.append(text)


def _html_to_text(value: str) -> str:
    """Convert Discourse cooked HTML to compact text."""
    parser = _CookedHTMLParser()
    parser.feed(value or "")
    return clean_text(" ".join(parser.parts))


def _safe_iso_datetime(value: str) -> str:
    """Return an ISO datetime only when the Discourse value is parseable."""
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


def _optional_positive_int(*values: object) -> int | None:
    """Return the first positive integer from provided values, else None for no cap."""
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
