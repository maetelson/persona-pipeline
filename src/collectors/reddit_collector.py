"""Reddit collector with query × time window × pagination batching."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.collectors.base import BaseCollector, PageResult, QuerySeedTask, RawRecord, TimeSlice

class RedditRateLimitError(RuntimeError):
    """Raised when Reddit responds with a rate limit and collection should back off."""

    def __init__(self, wait_seconds: float, url: str) -> None:
        self.wait_seconds = wait_seconds
        self.url = url
        super().__init__(f"Reddit rate limited request for URL: {url}")


class RedditCollector(BaseCollector):
    """Collect Reddit search results and preserve raw payloads in JSONL."""

    source_name = "reddit"
    source_type = "forum"

    def __init__(self, config: dict[str, Any], data_dir) -> None:
        super().__init__(config=config, data_dir=data_dir)
        self._selected_query_variants: dict[str, str] = {}

    def collect(self) -> list[RawRecord]:
        if self.config.get("use_stub", False):
            return [self.build_stub_record()]
        if not self.get_query_seed_tasks():
            raise ValueError("Reddit collector requires at least one query seed or expanded query map task.")
        return self.collect_with_pagination(self._fetch_page)

    def get_collection_time_slices(self) -> list[TimeSlice]:
        """Use one combined 5-year window for Reddit collection.

        Reddit's public search endpoint does not reliably support exact historical
        slicing for multi-year windows. We therefore collect against one combined
        window and rely on the downstream created_at filter as the conservative
        gate before valid filtering.
        """
        start_at, end_at = self.get_time_window_bounds()
        return [
            TimeSlice(
                window_id="recent_5y_combined",
                start_at=start_at,
                end_at=end_at,
                label=f"{start_at.date().isoformat()}__{end_at.date().isoformat()}",
            )
        ]

    def _fetch_page(self, query_task: QuerySeedTask, time_slice: TimeSlice, page_no: int) -> PageResult:
        """Fetch one Reddit result page for a query/time-window pair."""
        search_limit = int(os.getenv("REDDIT_SEARCH_LIMIT", self.config.get("max_posts_per_seed", 10)))
        selected_variant = self._selected_query_variants.get(self._variant_key(query_task, time_slice))
        variant_candidates = [selected_variant] if selected_variant else self._build_query_variants(query_task.query_text)
        payload: dict[str, Any] = {}
        children: list[dict[str, Any]] = []
        search_url = ""
        used_variant = query_task.query_text
        after = self._page_after_cursor(page_no, query_task, time_slice)

        for variant_text in variant_candidates:
            params = self._build_search_params(
                query_task=query_task,
                time_slice=time_slice,
                limit=search_limit,
                after=after,
                search_text=variant_text,
            )
            search_url = f"https://www.reddit.com/search.json?{urlencode(params)}"
            try:
                payload = self._fetch_json(search_url)
            except RedditRateLimitError as exc:
                return PageResult(
                    records=[],
                    has_more=False,
                    rate_limit_wait_seconds=exc.wait_seconds,
                    stop_reason="rate_limit_wait",
                )
            except RuntimeError as exc:
                if "HTTP 429" in str(exc) or "Too Many Requests" in str(exc):
                    return PageResult(
                        records=[],
                        has_more=False,
                        rate_limit_wait_seconds=float(self.config.get("rate_limit_wait_seconds", 20)),
                        stop_reason="rate_limit_wait",
                    )
                raise
            children = payload.get("data", {}).get("children", [])
            used_variant = variant_text
            if children or selected_variant or page_no > 1:
                break

        if children:
            self._selected_query_variants[self._variant_key(query_task, time_slice)] = used_variant

        records: list[RawRecord] = []
        for item_index, child in enumerate(children, start=1):
            post = child.get("data", {})
            comments = self._fetch_comments(str(post.get("id", "")))
            records.append(
                self._build_raw_record(
                    query_task=query_task,
                    time_slice=time_slice,
                    page_no=page_no,
                    item_index=item_index,
                    search_url=search_url,
                    search_query_variant=used_variant,
                    post=post,
                    comments=comments,
                )
            )

        after_token = payload.get("data", {}).get("after")
        if after_token:
            self._remember_page_after_cursor(query_task, time_slice, page_no + 1, str(after_token))
        return PageResult(records=records, has_more=bool(after_token))

    def _build_search_params(
        self,
        query_task: QuerySeedTask,
        time_slice: TimeSlice,
        limit: int,
        after: str | None,
        search_text: str,
    ) -> dict[str, Any]:
        """Build a Reddit search request using plain search.

        Exact multi-year timestamp slicing on the public Reddit endpoint has
        proven too brittle in practice and led to systematic zero-result pages.
        The collector therefore uses plain search here and preserves the combined
        window metadata so that `02.5_filter_time_window.py` can enforce the
        actual created_at gate afterwards.
        """
        params: dict[str, Any] = {
            "q": search_text,
            "sort": self.config.get("search_sort", "new"),
            "t": "all",
            "limit": limit,
            "raw_json": 1,
            "restrict_sr": 0,
            "include_over_18": "on",
        }
        if after:
            params["after"] = after
        return params

    def _build_raw_record(
        self,
        query_task: QuerySeedTask,
        time_slice: TimeSlice,
        page_no: int,
        item_index: int,
        search_url: str,
        search_query_variant: str,
        post: dict[str, Any],
        comments: list[dict[str, Any]],
    ) -> RawRecord:
        """Map one Reddit post plus comments into the raw storage model."""
        permalink = post.get("permalink", "")
        post_id = str(post.get("id", ""))
        title = str(post.get("title", "") or "")
        body = str(post.get("selftext", "") or "")
        comments_text = self._flatten_comment_bodies(comments)

        return RawRecord(
            source=self.source_name,
            source_type=self.source_type,
            raw_id=post_id,
            url=f"https://www.reddit.com{permalink}" if permalink else "",
            title=title,
            body=body,
            comments_text=comments_text,
            created_at=self._to_iso(post.get("created_utc")),
            fetched_at=self._to_iso(time.time()),
            query_seed=query_task.query_text,
            window_id=time_slice.window_id,
            window_start=time_slice.start_at.isoformat(),
            window_end=time_slice.end_at.isoformat(),
            query_id=query_task.query_id,
            query_text=query_task.query_text,
            page_no=page_no,
            author_hint=str(post.get("author", "") or ""),
            source_meta={
                "collector_mode": "live",
                "query_id": query_task.query_id,
                "query_text": query_task.query_text,
                "query_priority": query_task.priority,
                "expected_signal_type": query_task.expected_signal_type,
                "query_axes_used": query_task.axes_used,
                "window_id": time_slice.window_id,
                "window_label": time_slice.label,
                "window_start": time_slice.start_at.isoformat(),
                "window_end": time_slice.end_at.isoformat(),
                "time_filter_mode": "post_filter_only",
                "page_no": page_no,
                "item_index": item_index,
                "search_url": search_url,
                "search_query_variant": search_query_variant,
                "subreddit": post.get("subreddit"),
                "subreddit_name_prefixed": post.get("subreddit_name_prefixed"),
                "num_comments": post.get("num_comments"),
                "score": post.get("score"),
                "permalink": permalink,
                "raw_post": post,
                "raw_comments": comments,
            },
        )

    def _fetch_comments(self, post_id: str) -> list[dict[str, Any]]:
        """Fetch a limited comment tree for one Reddit post."""
        if not post_id:
            return []

        comment_limit = int(os.getenv("REDDIT_COMMENT_LIMIT", self.config.get("comment_limit_per_post", 20)))
        comments_url = f"https://www.reddit.com/comments/{post_id}.json?limit={comment_limit}&raw_json=1"
        try:
            payload = self._fetch_json(comments_url)
        except (RuntimeError, RedditRateLimitError):
            return []

        if not isinstance(payload, list) or len(payload) < 2:
            return []
        listing = payload[1].get("data", {}).get("children", [])
        return self._extract_comment_nodes(listing)

    def _extract_comment_nodes(self, children: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Flatten Reddit comment nodes while preserving raw comment payloads."""
        extracted: list[dict[str, Any]] = []
        for child in children:
            if child.get("kind") != "t1":
                continue
            data = child.get("data", {})
            extracted.append(data)
            replies = data.get("replies")
            if isinstance(replies, dict):
                nested = replies.get("data", {}).get("children", [])
                extracted.extend(self._extract_comment_nodes(nested))
        return extracted

    def _flatten_comment_bodies(self, comments: list[dict[str, Any]]) -> str:
        """Combine comment bodies into one raw text field without filtering."""
        bodies = [str(comment.get("body", "") or "").strip() for comment in comments]
        return "\n\n".join(body for body in bodies if body)

    def _fetch_json(self, url: str) -> Any:
        """Fetch JSON with a configurable user agent and basic 429 backoff handling."""
        user_agent = os.getenv("REDDIT_USER_AGENT", "").strip()
        if not user_agent:
            raise RuntimeError(
                "REDDIT_USER_AGENT is required for live Reddit collection. "
                "Set it in your environment or switch config/sources/reddit.yaml to use_stub: true."
            )

        max_retries = int(self.config.get("max_rate_limit_retries", 3))
        default_wait = float(self.config.get("rate_limit_wait_seconds", 20))

        for attempt in range(max_retries + 1):
            request = Request(
                url,
                headers={
                    "User-Agent": user_agent,
                    "Accept": "application/json",
                },
            )
            try:
                with urlopen(request, timeout=30) as response:
                    return json.load(response)
            except HTTPError as exc:
                if exc.code == 429:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    wait_seconds = float(retry_after) if retry_after else default_wait * (attempt + 1)
                    if attempt < max_retries:
                        time.sleep(wait_seconds)
                        continue
                    raise RedditRateLimitError(wait_seconds=wait_seconds, url=url) from exc
                raise RuntimeError(f"Reddit request failed with HTTP {exc.code} for URL: {url}") from exc
            except URLError as exc:
                raise RuntimeError(f"Reddit request failed due to network error for URL: {url}") from exc
        raise RedditRateLimitError(wait_seconds=default_wait, url=url)

    def _to_iso(self, timestamp: Any) -> str:
        """Convert a Unix timestamp into ISO 8601 UTC format."""
        if timestamp in (None, ""):
            return ""
        return datetime.fromtimestamp(float(timestamp), tz=UTC).replace(microsecond=0).isoformat()

    def _build_query_variants(self, query_text: str) -> list[str]:
        """Build progressively broader Reddit search phrases for the same seed.

        Reddit search recall collapses quickly when we keep role anchors and long
        business wording together with cloudsearch timestamps. We therefore try
        the original query first, then shorter pain-oriented variants.
        """
        normalized = query_text.strip().lower()
        variants: list[str] = []

        def add(candidate: str) -> None:
            cleaned = " ".join(candidate.split()).strip()
            if len(cleaned) < 4:
                return
            if cleaned not in variants:
                variants.append(cleaned)

        add(normalized)

        role_patterns = [
            r"\bperformance marketer\b",
            r"\bgrowth marketer\b",
            r"\bcrm marketer\b",
            r"\bbusiness analyst\b",
            r"\banalyst\b",
            r"\bmanager\b",
            r"\bbusiness user\b",
            r"\bstakeholders?\b",
            r"\bleadership\b",
        ]
        stripped = normalized
        for pattern in role_patterns:
            stripped = re.sub(pattern, " ", stripped)
        add(stripped)

        pain_patterns = [
            r"(conversion drop)",
            r"(retention down)",
            r"(numbers do not match)",
            r"(numbers mismatch)",
            r"(metric mismatch)",
            r"(numbers differ)",
            r"(dashboard trust)",
            r"(not trusted)",
            r"(spreadsheet validation)",
            r"(validate numbers)",
            r"(reconcile numbers)",
            r"(source of truth)",
            r"(segment comparison)",
            r"(pivot table)",
            r"(manual reporting)",
            r"(spreadsheet bottleneck)",
            r"(root cause)",
            r"(filter logic mismatch)",
            r"(ad hoc report)",
            r"(want excel)",
        ]
        for pattern in pain_patterns:
            match = re.search(pattern, stripped)
            if match:
                add(match.group(1))

        tokens = [token for token in re.split(r"\s+", stripped) if token and token not in {"for", "to", "from", "the"}]
        if len(tokens) >= 3:
            add(" ".join(tokens[:3]))
        if len(tokens) >= 2:
            add(" ".join(tokens[:2]))

        return variants or [normalized]

    def _variant_key(self, query_task: QuerySeedTask, time_slice: TimeSlice) -> str:
        """Return the cache key for the chosen search variant within one time slice."""
        return f"{query_task.query_id}::{time_slice.window_id}"

    def _page_after_cursor(self, page_no: int, query_task: QuerySeedTask, time_slice: TimeSlice) -> str | None:
        """Return the Reddit after cursor for a specific page if known."""
        if page_no == 1:
            return None
        cursor_key = f"{query_task.query_id}::{time_slice.window_id}::{page_no}"
        return getattr(self, "_after_cursors", {}).get(cursor_key)

    def _remember_page_after_cursor(
        self,
        query_task: QuerySeedTask,
        time_slice: TimeSlice,
        page_no: int,
        after_token: str,
    ) -> None:
        """Store the Reddit after cursor for the next page."""
        if not hasattr(self, "_after_cursors"):
            self._after_cursors: dict[str, str] = {}
        cursor_key = f"{query_task.query_id}::{time_slice.window_id}::{page_no}"
        self._after_cursors[cursor_key] = after_token
