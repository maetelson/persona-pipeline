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


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Parse a numeric-like value into float without raising."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_subreddit_name(value: Any) -> str:
    """Normalize a subreddit or user-prefixed token for policy checks."""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.startswith(("r/", "u/")):
        return text
    return f"r/{text}"


def _has_any_term(text: str, terms: list[str]) -> bool:
    """Return whether any configured term appears in a string."""
    lowered = str(text or "").lower()
    return any(str(term).strip().lower() in lowered for term in terms if str(term).strip())


def _build_reddit_search_text(search_text: str, negative_keywords: list[str] | None = None) -> str:
    """Append source-specific negative keywords to a Reddit search phrase."""
    cleaned = " ".join(str(search_text or "").split()).strip()
    negatives: list[str] = []
    for item in negative_keywords or []:
        token = str(item or "").strip().lower()
        if token and token not in negatives:
            negatives.append(token)
    suffix = " ".join(f'-"{token}"' if " " in token else f"-{token}" for token in negatives)
    return f"{cleaned} {suffix}".strip() if suffix else cleaned


def _resolve_seed_page_cap(query_text: str, config: dict[str, Any]) -> int:
    """Return the page cap for one Reddit seed from source config."""
    seed_caps = config.get("per_seed_page_caps", {}) or {}
    normalized_query = str(query_text or "").strip().lower()
    for key, value in seed_caps.items():
        if str(key or "").strip().lower() == normalized_query:
            return max(int(_safe_float(value, 0.0)), 1)
    default_cap = config.get("default_per_seed_page_cap", config.get("max_pages_per_query", 2))
    return max(int(_safe_float(default_cap, 2.0)), 1)


def _should_collect_reddit_post(post: dict[str, Any], config: dict[str, Any]) -> tuple[bool, str]:
    """Return whether a Reddit search hit is worth comment hydration and raw persistence."""
    subreddit = _normalize_subreddit_name(post.get("subreddit_name_prefixed") or post.get("subreddit"))
    preferred_subreddits = {
        _normalize_subreddit_name(value)
        for value in config.get("preferred_subreddits", []) or []
        if _normalize_subreddit_name(value)
    }
    deny_patterns = [str(pattern).strip() for pattern in config.get("deny_subreddit_patterns", []) or [] if str(pattern).strip()]
    for pattern in deny_patterns:
        if re.search(pattern, subreddit, flags=re.IGNORECASE):
            return False, "deny_subreddit"

    title = str(post.get("title", "") or "")
    body = str(post.get("selftext", "") or "")
    combined = f"{title} {body} {subreddit}".lower()
    if _has_any_term(combined, [str(item) for item in config.get("precollector_negative_keywords", []) or []]):
        return False, "negative_keyword"

    if preferred_subreddits and subreddit in preferred_subreddits:
        return True, "preferred_subreddit"

    required_terms = [str(item) for item in config.get("precollector_required_signal_terms", []) or []]
    workflow_terms = [str(item) for item in config.get("precollector_workflow_terms", []) or []]
    problem_terms = [str(item) for item in config.get("precollector_problem_terms", []) or []]
    if required_terms and not _has_any_term(combined, required_terms):
        return False, "missing_required_signal"
    if workflow_terms and problem_terms and not (_has_any_term(combined, workflow_terms) and _has_any_term(combined, problem_terms)):
        return False, "weak_problem_workflow_signal"
    return True, "text_signal_match"


def _should_fetch_reddit_comments(
    post: dict[str, Any],
    config: dict[str, Any],
    expanded_on_page: int,
    expanded_on_query: int,
) -> tuple[bool, str]:
    """Return whether comment hydration is worth an extra request for this post."""
    mode = str(config.get("comment_expansion_mode", "conditional") or "conditional").strip().lower()
    if mode in {"none", "off", "disabled"}:
        return False, "disabled"

    num_comments = int(_safe_float(post.get("num_comments"), 0.0))
    if num_comments <= 0:
        return False, "no_comments"

    max_per_page = int(config.get("comment_expand_max_posts_per_page", 3) or 3)
    if expanded_on_page >= max_per_page:
        return False, "page_limit"

    max_per_query = int(config.get("comment_expand_max_posts_per_query", 8) or 8)
    if expanded_on_query >= max_per_query:
        return False, "query_limit"

    if mode == "all":
        return True, "enabled"

    body = str(post.get("selftext", "") or "").strip()
    body_threshold = int(config.get("comment_expand_body_char_threshold", 220) or 220)
    is_self = bool(post.get("is_self", True))
    if is_self and len(body) >= body_threshold:
        return False, "sufficient_body"
    return True, "sparse_body"

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
        self._after_cursors: dict[str, str] = {}
        self._seen_post_ids: set[str] = set()
        self._query_state: dict[str, dict[str, Any]] = {}
        self.profile_metrics.setdefault("precollector_scanned_post_count", 0)
        self.profile_metrics.setdefault("precollector_kept_post_count", 0)
        self.profile_metrics.setdefault("precollector_skipped_post_count", 0)
        self.profile_metrics.setdefault("precollector_skipped_deny_subreddit_count", 0)
        self.profile_metrics.setdefault("precollector_skipped_negative_keyword_count", 0)
        self.profile_metrics.setdefault("precollector_skipped_missing_required_signal_count", 0)
        self.profile_metrics.setdefault("precollector_skipped_weak_problem_workflow_signal_count", 0)
        self.profile_metrics.setdefault("query_overlap_skip_count", 0)
        self.profile_metrics.setdefault("listing_page_count", 0)
        self.profile_metrics.setdefault("listing_items_total", 0)
        self.profile_metrics.setdefault("listing_items_kept_total", 0)
        self.profile_metrics.setdefault("listing_overlap_items_total", 0)
        self.profile_metrics.setdefault("listing_policy_skip_items_total", 0)
        self.profile_metrics.setdefault("comment_request_candidate_count", 0)
        self.profile_metrics.setdefault("comment_fetch_count", 0)
        self.profile_metrics.setdefault("comment_skip_count", 0)
        self.profile_metrics.setdefault("comment_skip_disabled_count", 0)
        self.profile_metrics.setdefault("comment_skip_no_comments_count", 0)
        self.profile_metrics.setdefault("comment_skip_page_limit_count", 0)
        self.profile_metrics.setdefault("comment_skip_query_limit_count", 0)
        self.profile_metrics.setdefault("comment_skip_sufficient_body_count", 0)
        self.profile_metrics.setdefault("pagination_low_yield_stop_count", 0)
        self.profile_metrics.setdefault("pagination_overlap_stop_count", 0)
        self.profile_metrics.setdefault("pagination_rolling_retention_stop_count", 0)
        self.profile_metrics.setdefault("pagination_seed_page_cap_stop_count", 0)
        self.profile_metrics.setdefault("pagination_repeated_cursor_stop_count", 0)
        self.profile_metrics.setdefault("rate_limit_header_seen_count", 0)
        self.profile_metrics.setdefault("retry_after_header_seen_count", 0)
        self.profile_metrics.setdefault("rate_limit_remaining_min", 0.0)
        self.profile_metrics.setdefault("rate_limit_remaining_last", 0.0)
        self.profile_metrics.setdefault("rate_limit_used_last", 0.0)
        self.profile_metrics.setdefault("rate_limit_reset_seconds_last", 0.0)
        self.profile_metrics.setdefault("rate_limit_reset_seconds_max", 0.0)

    def collect(self) -> list[RawRecord]:
        if self.config.get("use_stub", False):
            return [self.build_stub_record()]
        if not self.get_query_seed_tasks():
            raise ValueError("Reddit collector requires at least one query seed or expanded query map task.")
        self._selected_query_variants = {}
        self._after_cursors = {}
        self._seen_post_ids = set()
        self._query_state = {}
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
        query_key = self._variant_key(query_task, time_slice)

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
                payload = self._fetch_json(search_url, request_kind="search")
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
            self._selected_query_variants[query_key] = used_variant

        records: list[RawRecord] = []
        page_overlap_count = 0
        page_policy_skip_count = 0
        page_comment_fetch_count = 0
        page_comment_skip_count = 0
        page_kept_count = 0
        for item_index, child in enumerate(children, start=1):
            post = child.get("data", {})
            self._record_precollector_scan()
            post_id = str(post.get("id", "") or "").strip()
            if post_id and post_id in self._seen_post_ids:
                page_overlap_count += 1
                self._record_overlap_skip()
                continue
            should_collect, reason = _should_collect_reddit_post(post, self.config)
            if not should_collect:
                page_policy_skip_count += 1
                self._record_precollector_skip(reason)
                continue
            self._record_precollector_accept()
            page_kept_count += 1
            if post_id:
                self._seen_post_ids.add(post_id)
            comments = []
            self.profile_metrics["comment_request_candidate_count"] = int(
                self.profile_metrics.get("comment_request_candidate_count", 0)
            ) + 1
            query_comment_fetches = int(self._query_state_for(query_key).get("comment_fetch_count", 0))
            should_fetch_comments, comment_reason = _should_fetch_reddit_comments(
                post,
                self.config,
                expanded_on_page=page_comment_fetch_count,
                expanded_on_query=query_comment_fetches,
            )
            if should_fetch_comments:
                comments = self._fetch_comments(str(post.get("id", "")))
                page_comment_fetch_count += 1
                self._record_comment_fetch(query_key)
            else:
                page_comment_skip_count += 1
                self._record_comment_skip(comment_reason)
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
        repeated_cursor = False
        seed_page_cap = _resolve_seed_page_cap(query_task.query_text, self.config)
        if after_token:
            repeated_cursor = self._has_seen_after_cursor(query_key, str(after_token))
            if not repeated_cursor:
                self._remember_page_after_cursor(query_task, time_slice, page_no + 1, str(after_token))
                self._query_state_for(query_key).setdefault("seen_after_tokens", set()).add(str(after_token))

        stop_reason = ""
        if repeated_cursor:
            stop_reason = "repeated_cursor"
            self.profile_metrics["pagination_repeated_cursor_stop_count"] = int(
                self.profile_metrics.get("pagination_repeated_cursor_stop_count", 0)
            ) + 1
        elif bool(after_token) and page_no >= seed_page_cap:
            stop_reason = "seed_page_cap"
            self.profile_metrics["pagination_seed_page_cap_stop_count"] = int(
                self.profile_metrics.get("pagination_seed_page_cap_stop_count", 0)
            ) + 1
        else:
            stop_reason = self._update_pagination_state(
                query_key=query_key,
                page_no=page_no,
                items_on_page=len(children),
                kept_count=page_kept_count,
                overlap_count=page_overlap_count,
                has_more=bool(after_token),
            )

        self._record_listing_page(
            items_on_page=len(children),
            kept_count=page_kept_count,
            overlap_count=page_overlap_count,
            policy_skip_count=page_policy_skip_count,
            comment_fetch_count=page_comment_fetch_count,
            comment_skip_count=page_comment_skip_count,
        )
        return PageResult(
            records=records,
            has_more=bool(after_token) and not bool(stop_reason),
            stop_reason=stop_reason,
            metrics={
                "page_listing_count": len(children),
                "page_kept_count": page_kept_count,
                "page_overlap_count": page_overlap_count,
                "page_policy_skip_count": page_policy_skip_count,
                "page_comment_fetch_count": page_comment_fetch_count,
                "page_comment_skip_count": page_comment_skip_count,
                "seed_page_cap": seed_page_cap,
                "page_fill_ratio": round(
                    (len(children) / max(int(os.getenv("REDDIT_SEARCH_LIMIT", self.config.get("max_posts_per_seed", 10))), 1)),
                    4,
                ),
                "page_kept_ratio": round((page_kept_count / len(children)) if children else 0.0, 4),
            },
        )

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
            "q": _build_reddit_search_text(
                search_text,
                [str(item) for item in self.config.get("search_negative_keywords", []) or []],
            ),
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

    def _record_precollector_scan(self) -> None:
        """Track one Reddit search hit inspected before comment hydration."""
        self.profile_metrics["precollector_scanned_post_count"] = int(
            self.profile_metrics.get("precollector_scanned_post_count", 0)
        ) + 1

    def _record_precollector_accept(self) -> None:
        """Track one Reddit search hit accepted for downstream processing."""
        self.profile_metrics["precollector_kept_post_count"] = int(
            self.profile_metrics.get("precollector_kept_post_count", 0)
        ) + 1

    def _record_precollector_skip(self, reason: str) -> None:
        """Track one Reddit search hit skipped by collector-side policy."""
        self.profile_metrics["precollector_skipped_post_count"] = int(
            self.profile_metrics.get("precollector_skipped_post_count", 0)
        ) + 1
        metric_key = {
            "deny_subreddit": "precollector_skipped_deny_subreddit_count",
            "negative_keyword": "precollector_skipped_negative_keyword_count",
            "missing_required_signal": "precollector_skipped_missing_required_signal_count",
            "weak_problem_workflow_signal": "precollector_skipped_weak_problem_workflow_signal_count",
        }.get(str(reason), "")
        if metric_key:
            self.profile_metrics[metric_key] = int(self.profile_metrics.get(metric_key, 0)) + 1

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
            payload = self._fetch_json(comments_url, request_kind="comments")
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

    def _fetch_json(self, url: str, request_kind: str = "generic") -> Any:
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
            request_started_at = time.perf_counter()
            try:
                with urlopen(request, timeout=30) as response:
                    self._record_rate_limit_headers(response.headers)
                    payload = json.load(response)
                self.record_request(request_kind, time.perf_counter() - request_started_at, success=True)
                return payload
            except HTTPError as exc:
                self._record_rate_limit_headers(exc.headers)
                self.record_request(request_kind, time.perf_counter() - request_started_at, success=False)
                if exc.code == 429:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    wait_seconds = float(retry_after) if retry_after else default_wait * (attempt + 1)
                    if attempt < max_retries:
                        self.record_request_retry()
                        self.record_backoff_sleep(wait_seconds)
                        time.sleep(wait_seconds)
                        continue
                    raise RedditRateLimitError(wait_seconds=wait_seconds, url=url) from exc
                raise RuntimeError(f"Reddit request failed with HTTP {exc.code} for URL: {url}") from exc
            except URLError as exc:
                self.record_request(request_kind, time.perf_counter() - request_started_at, success=False)
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

    def _query_state_for(self, query_key: str) -> dict[str, Any]:
        """Return mutable pagination state for one query/window pair."""
        if query_key not in self._query_state:
            self._query_state[query_key] = {
                "comment_fetch_count": 0,
                "consecutive_low_yield_pages": 0,
                "consecutive_overlap_pages": 0,
                "rolling_items_seen": 0,
                "rolling_items_kept": 0,
                "seen_after_tokens": set(),
            }
        return self._query_state[query_key]

    def _has_seen_after_cursor(self, query_key: str, after_token: str) -> bool:
        """Return whether a pagination cursor has already been seen for this query."""
        return str(after_token) in self._query_state_for(query_key).get("seen_after_tokens", set())

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

    def _update_pagination_state(
        self,
        query_key: str,
        page_no: int,
        items_on_page: int,
        kept_count: int,
        overlap_count: int,
        has_more: bool,
    ) -> str:
        """Return an early stop reason when marginal page yield collapses."""
        if not has_more or items_on_page <= 0:
            return ""
        state = self._query_state_for(query_key)
        overlap_ratio_stop = float(self.config.get("early_stop_overlap_ratio", 0.6))
        low_yield_ratio_stop = float(self.config.get("early_stop_max_kept_ratio", 0.15))
        low_yield_page_limit = int(self.config.get("early_stop_low_yield_pages", 2) or 2)
        overlap_page_limit = int(self.config.get("early_stop_overlap_pages", 1) or 1)
        min_kept_per_page = int(self.config.get("early_stop_min_kept_per_page", 1) or 1)
        rolling_retention_threshold = float(self.config.get("minimum_rolling_retention_threshold", 0.0) or 0.0)
        rolling_retention_min_pages = int(self.config.get("rolling_retention_min_pages", 2) or 2)

        kept_ratio = kept_count / items_on_page
        overlap_ratio = overlap_count / items_on_page
        low_yield = kept_count < min_kept_per_page or kept_ratio <= low_yield_ratio_stop
        overlap_heavy = overlap_ratio >= overlap_ratio_stop
        state["rolling_items_seen"] = int(state.get("rolling_items_seen", 0)) + int(items_on_page)
        state["rolling_items_kept"] = int(state.get("rolling_items_kept", 0)) + int(kept_count)
        rolling_retention = state["rolling_items_kept"] / max(int(state.get("rolling_items_seen", 0)), 1)

        state["consecutive_low_yield_pages"] = int(state.get("consecutive_low_yield_pages", 0)) + 1 if low_yield else 0
        state["consecutive_overlap_pages"] = int(state.get("consecutive_overlap_pages", 0)) + 1 if overlap_heavy else 0

        if page_no > 1 and int(state.get("consecutive_overlap_pages", 0)) >= overlap_page_limit:
            self.profile_metrics["pagination_overlap_stop_count"] = int(
                self.profile_metrics.get("pagination_overlap_stop_count", 0)
            ) + 1
            return "overlap_heavy_page"
        if page_no > 1 and int(state.get("consecutive_low_yield_pages", 0)) >= low_yield_page_limit:
            self.profile_metrics["pagination_low_yield_stop_count"] = int(
                self.profile_metrics.get("pagination_low_yield_stop_count", 0)
            ) + 1
            return "low_yield_page"
        if rolling_retention_threshold > 0 and page_no >= rolling_retention_min_pages and rolling_retention < rolling_retention_threshold:
            self.profile_metrics["pagination_rolling_retention_stop_count"] = int(
                self.profile_metrics.get("pagination_rolling_retention_stop_count", 0)
            ) + 1
            return "rolling_retention_below_threshold"
        return ""

    def _record_overlap_skip(self) -> None:
        """Track one Reddit post skipped due to cross-query overlap."""
        self.profile_metrics["query_overlap_skip_count"] = int(
            self.profile_metrics.get("query_overlap_skip_count", 0)
        ) + 1

    def _record_comment_fetch(self, query_key: str) -> None:
        """Track one comment hydration request."""
        self.profile_metrics["comment_fetch_count"] = int(self.profile_metrics.get("comment_fetch_count", 0)) + 1
        state = self._query_state_for(query_key)
        state["comment_fetch_count"] = int(state.get("comment_fetch_count", 0)) + 1

    def _record_comment_skip(self, reason: str) -> None:
        """Track one post that skipped comment hydration."""
        self.profile_metrics["comment_skip_count"] = int(self.profile_metrics.get("comment_skip_count", 0)) + 1
        metric_key = {
            "disabled": "comment_skip_disabled_count",
            "no_comments": "comment_skip_no_comments_count",
            "page_limit": "comment_skip_page_limit_count",
            "query_limit": "comment_skip_query_limit_count",
            "sufficient_body": "comment_skip_sufficient_body_count",
        }.get(str(reason), "")
        if metric_key:
            self.profile_metrics[metric_key] = int(self.profile_metrics.get(metric_key, 0)) + 1

    def _record_listing_page(
        self,
        items_on_page: int,
        kept_count: int,
        overlap_count: int,
        policy_skip_count: int,
        comment_fetch_count: int,
        comment_skip_count: int,
    ) -> None:
        """Track per-page listing efficiency for Reddit search."""
        self.profile_metrics["listing_page_count"] = int(self.profile_metrics.get("listing_page_count", 0)) + 1
        self.profile_metrics["listing_items_total"] = int(self.profile_metrics.get("listing_items_total", 0)) + int(items_on_page)
        self.profile_metrics["listing_items_kept_total"] = int(self.profile_metrics.get("listing_items_kept_total", 0)) + int(kept_count)
        self.profile_metrics["listing_overlap_items_total"] = int(self.profile_metrics.get("listing_overlap_items_total", 0)) + int(overlap_count)
        self.profile_metrics["listing_policy_skip_items_total"] = int(self.profile_metrics.get("listing_policy_skip_items_total", 0)) + int(policy_skip_count)
        self.profile_metrics["comment_fetch_count"] = int(self.profile_metrics.get("comment_fetch_count", 0))
        self.profile_metrics["comment_skip_count"] = int(self.profile_metrics.get("comment_skip_count", 0))

    def _record_rate_limit_headers(self, headers: Any) -> None:
        """Track Reddit rate-limit headers when the endpoint exposes them."""
        if not headers:
            return
        remaining = _safe_float(headers.get("x-ratelimit-remaining"), -1.0)
        used = _safe_float(headers.get("x-ratelimit-used"), -1.0)
        reset_seconds = _safe_float(headers.get("x-ratelimit-reset"), -1.0)
        retry_after = headers.get("Retry-After")
        if remaining >= 0 or used >= 0 or reset_seconds >= 0:
            self.profile_metrics["rate_limit_header_seen_count"] = int(
                self.profile_metrics.get("rate_limit_header_seen_count", 0)
            ) + 1
            if remaining >= 0:
                current_min = _safe_float(self.profile_metrics.get("rate_limit_remaining_min"), -1.0)
                self.profile_metrics["rate_limit_remaining_min"] = remaining if current_min < 0 else min(current_min, remaining)
                self.profile_metrics["rate_limit_remaining_last"] = remaining
            if used >= 0:
                self.profile_metrics["rate_limit_used_last"] = used
            if reset_seconds >= 0:
                self.profile_metrics["rate_limit_reset_seconds_last"] = reset_seconds
                self.profile_metrics["rate_limit_reset_seconds_max"] = max(
                    _safe_float(self.profile_metrics.get("rate_limit_reset_seconds_max"), 0.0),
                    reset_seconds,
                )
        if retry_after not in (None, ""):
            self.profile_metrics["retry_after_header_seen_count"] = int(
                self.profile_metrics.get("retry_after_header_seen_count", 0)
            ) + 1

    def get_profile_metrics(self) -> dict[str, Any]:
        """Return collector metrics with Reddit-specific derived efficiency fields."""
        metrics = super().get_profile_metrics()
        listing_page_count = int(metrics.get("listing_page_count", 0))
        listing_items_total = int(metrics.get("listing_items_total", 0))
        listing_items_kept_total = int(metrics.get("listing_items_kept_total", 0))
        metrics["average_items_per_page"] = round((listing_items_total / listing_page_count), 6) if listing_page_count else 0.0
        metrics["average_kept_items_per_page"] = round((listing_items_kept_total / listing_page_count), 6) if listing_page_count else 0.0
        metrics["total_sleep_seconds"] = round(
            float(metrics.get("backoff_sleep_seconds", 0.0)) + float(metrics.get("configured_sleep_seconds", 0.0)),
            6,
        )
        metrics["configured_listing_limit"] = int(os.getenv("REDDIT_SEARCH_LIMIT", self.config.get("max_posts_per_seed", 10)))
        metrics["configured_comment_limit"] = int(os.getenv("REDDIT_COMMENT_LIMIT", self.config.get("comment_limit_per_post", 20)))
        metrics["comment_expansion_mode"] = str(self.config.get("comment_expansion_mode", "conditional") or "conditional")
        metrics["minimum_rolling_retention_threshold"] = float(self.config.get("minimum_rolling_retention_threshold", 0.0) or 0.0)
        metrics["default_per_seed_page_cap"] = _resolve_seed_page_cap("", self.config)
        metrics["rate_limit_remaining_min"] = max(float(metrics.get("rate_limit_remaining_min", 0.0)), 0.0)
        return metrics
