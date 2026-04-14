"""GitHub issues/discussions collector with query +ù time window +ù pagination batching."""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.collectors.base import BaseCollector, PageResult, QuerySeedTask, RawRecord, TimeSlice


class GitHubDiscussionsCollector(BaseCollector):
    """Collect GitHub issues and discussions while preserving raw payload shape."""

    source_name = "github_discussions"
    source_type = "discussion"

    def collect(self) -> list[RawRecord]:
        if self.config.get("use_stub", False):
            return [self.build_stub_record()]

        repositories = self.config.get("repositories") or []
        if not self.get_query_seed_tasks():
            raise ValueError("GitHub collector requires at least one query seed or expanded query task.")
        if not repositories:
            raise ValueError(
                "GitHub collector requires at least one repository in config/sources/github_discussions.yaml"
            )
        return self.collect_with_pagination(self._fetch_page)

    def _fetch_page(self, query_task: QuerySeedTask, time_slice: TimeSlice, page_no: int) -> PageResult:
        """Fetch one GitHub page across configured repositories."""
        repositories = self.config.get("repositories") or []
        records: list[RawRecord] = []
        any_has_more = False

        for repo_index, repository in enumerate(repositories, start=1):
            owner, repo = repository.split("/", maxsplit=1)

            try:
                issues, issues_has_more = self._fetch_issues_page(query_task.query_text, repository, time_slice, page_no)
                any_has_more = any_has_more or issues_has_more
                for item_index, issue in enumerate(issues, start=1):
                    records.append(
                        self._build_issue_record(
                            query_task=query_task,
                            repository=repository,
                            time_slice=time_slice,
                            repo_index=repo_index,
                            page_no=page_no,
                            item_index=item_index,
                            issue=issue,
                            issue_comments=self._fetch_issue_comments(issue),
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                self._record_partial_error(query_task, time_slice, page_no, "issues_page", exc, repository)

            token = os.getenv("GITHUB_TOKEN", "").strip()
            discussions_enabled = str(os.getenv("GITHUB_ENABLE_DISCUSSIONS", str(self.config.get("enable_discussions", False)))).lower() == "true"
            if token and discussions_enabled:
                try:
                    discussions, discussions_has_more = self._fetch_discussions_page(
                        owner,
                        repo,
                        query_task.query_text,
                        time_slice,
                        page_no,
                    )
                    any_has_more = any_has_more or discussions_has_more
                    for item_index, discussion in enumerate(discussions, start=1):
                        records.append(
                            self._build_discussion_record(
                                query_task=query_task,
                                repository=repository,
                                time_slice=time_slice,
                                repo_index=repo_index,
                                page_no=page_no,
                                item_index=repo_index * 1000 + item_index,
                                discussion=discussion,
                            )
                        )
                except Exception as exc:  # noqa: BLE001
                    self._record_partial_error(query_task, time_slice, page_no, "discussions_page", exc, repository)

        stop_reason = "no_more_results" if not records and not any_has_more else ""
        return PageResult(records=records, has_more=any_has_more, stop_reason=stop_reason)

    def _fetch_issues_page(
        self,
        query_seed: str,
        repository: str,
        time_slice: TimeSlice,
        page_no: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch one GitHub issues page for one repository/query/window."""
        per_query = int(os.getenv("GITHUB_ISSUES_PER_QUERY", self.config.get("max_items_per_seed", 5)))
        query = (
            f"repo:{repository} is:issue {self._issue_search_terms(query_seed)} "
            f'created:{time_slice.start_at.date().isoformat()}..{time_slice.end_at.date().isoformat()}'
        )
        params = {
            "q": query,
            "sort": "updated",
            "order": "desc",
            "per_page": per_query,
            "page": page_no,
        }
        payload = self._fetch_rest_json("https://api.github.com/search/issues", params=params)
        page_items = payload.get("items", [])
        return page_items, len(page_items) >= per_query

    def _fetch_issue_comments(self, issue: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch comments for one issue."""
        comments_url = issue.get("comments_url")
        if not comments_url or int(issue.get("comments", 0) or 0) == 0:
            return []

        per_page = int(os.getenv("GITHUB_COMMENTS_PER_ITEM", self.config.get("max_comments_per_item", 10)))
        if per_page <= 0:
            return []
        params = {"per_page": per_page}
        payload = self._fetch_rest_json(str(comments_url), params=params)
        if isinstance(payload, list):
            return payload[:per_page]
        return []

    def _fetch_discussions_page(
        self,
        owner: str,
        repo: str,
        query_seed: str,
        time_slice: TimeSlice,
        page_no: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch one GitHub discussions page and filter it by window/query seed."""
        per_query = int(os.getenv("GITHUB_ISSUES_PER_QUERY", self.config.get("max_items_per_seed", 5)))
        per_comment = int(os.getenv("GITHUB_COMMENTS_PER_ITEM", self.config.get("max_comments_per_item", 10)))
        per_reply = int(
            os.getenv("GITHUB_DISCUSSION_REPLIES_PER_COMMENT", self.config.get("max_replies_per_comment", 5))
        )
        query = """
        query($owner: String!, $repo: String!, $first: Int!, $commentFirst: Int!, $replyFirst: Int!, $after: String) {
          repository(owner: $owner, name: $repo) {
            discussions(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
              pageInfo {
                endCursor
                hasNextPage
              }
              nodes {
                id
                number
                title
                body
                url
                createdAt
                updatedAt
                category { name slug }
                author { login }
                comments(first: $commentFirst) {
                  nodes {
                    id
                    body
                    createdAt
                    author { login }
                    replies(first: $replyFirst) {
                      nodes {
                        id
                        body
                        createdAt
                        author { login }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        filtered: list[dict[str, Any]] = []
        query_tokens = self._query_tokens(query_seed)
        after = self._discussion_after_cursor(query_seed, owner, repo, time_slice, page_no)
        payload = self._fetch_graphql_json(
            query=query,
            variables={
                "owner": owner,
                "repo": repo,
                "first": per_query,
                "commentFirst": per_comment,
                "replyFirst": per_reply,
                "after": after,
            },
        )
        discussions_data = payload.get("data", {}).get("repository", {}).get("discussions", {}) or {}
        discussions = discussions_data.get("nodes", []) or []
        for discussion in discussions:
            created_at = self._parse_iso(discussion.get("createdAt"))
            if created_at is None or created_at < time_slice.start_at or created_at > time_slice.end_at:
                continue
            haystack = " ".join(
                [
                    str(discussion.get("title", "") or ""),
                    str(discussion.get("body", "") or ""),
                    " ".join(str(comment.get("body", "") or "") for comment in discussion.get("comments", {}).get("nodes", [])),
                ]
            ).lower()
            overlap = sum(1 for token in query_tokens if token in haystack)
            required_overlap = 1 if len(query_tokens) <= 2 else 2
            if overlap >= required_overlap:
                filtered.append(discussion)

        page_info = discussions_data.get("pageInfo", {}) or {}
        end_cursor = page_info.get("endCursor")
        if end_cursor:
            self._remember_discussion_after_cursor(query_seed, owner, repo, time_slice, page_no + 1, str(end_cursor))
        return filtered, bool(page_info.get("hasNextPage"))

    def _build_issue_record(
        self,
        query_task: QuerySeedTask,
        repository: str,
        time_slice: TimeSlice,
        repo_index: int,
        page_no: int,
        item_index: int,
        issue: dict[str, Any],
        issue_comments: list[dict[str, Any]],
    ) -> RawRecord:
        """Create a raw record for a GitHub issue thread."""
        body = str(issue.get("body", "") or "")
        comments_text = "\n\n".join(
            str(comment.get("body", "") or "").strip()
            for comment in issue_comments
            if str(comment.get("body", "") or "").strip()
        )
        return RawRecord(
            source=self.source_name,
            source_type="github_issue",
            raw_id=str(issue.get("id", "")),
            url=str(issue.get("html_url", "") or ""),
            title=str(issue.get("title", "") or ""),
            body=body,
            comments_text=comments_text,
            created_at=str(issue.get("created_at", "") or ""),
            fetched_at=self._to_iso(time.time()),
            query_seed=query_task.query_text,
            query_id=query_task.query_id,
            query_text=query_task.query_text,
            window_id=time_slice.window_id,
            window_start=time_slice.start_at.isoformat(),
            window_end=time_slice.end_at.isoformat(),
            page_no=page_no,
            author_hint=str((issue.get("user") or {}).get("login", "") or ""),
            source_meta={
                "collector_mode": "live",
                "content_kind": "issue",
                "query_id": query_task.query_id,
                "query_text": query_task.query_text,
                "query_priority": query_task.priority,
                "expected_signal_type": query_task.expected_signal_type,
                "query_axes_used": query_task.axes_used,
                "window_id": time_slice.window_id,
                "window_label": time_slice.label,
                "window_start": time_slice.start_at.isoformat(),
                "window_end": time_slice.end_at.isoformat(),
                "page_no": page_no,
                "repo_index": repo_index,
                "item_index": item_index,
                "repository": repository,
                "raw_issue": issue,
                "raw_issue_comments": issue_comments,
            },
        )

    def _build_discussion_record(
        self,
        query_task: QuerySeedTask,
        repository: str,
        time_slice: TimeSlice,
        repo_index: int,
        page_no: int,
        item_index: int,
        discussion: dict[str, Any],
    ) -> RawRecord:
        """Create a raw record for a GitHub discussion thread."""
        comments = discussion.get("comments", {}).get("nodes", []) or []
        comment_bodies = []
        reply_map: dict[str, list[dict[str, Any]]] = {}
        for comment in comments:
            body = str(comment.get("body", "") or "").strip()
            if body:
                comment_bodies.append(body)
            reply_nodes = comment.get("replies", {}).get("nodes", []) or []
            reply_map[str(comment.get("id", ""))] = reply_nodes
            for reply in reply_nodes:
                reply_body = str(reply.get("body", "") or "").strip()
                if reply_body:
                    comment_bodies.append(reply_body)

        return RawRecord(
            source=self.source_name,
            source_type="github_discussion",
            raw_id=str(discussion.get("id", "")),
            url=str(discussion.get("url", "") or ""),
            title=str(discussion.get("title", "") or ""),
            body=str(discussion.get("body", "") or ""),
            comments_text="\n\n".join(comment_bodies),
            created_at=str(discussion.get("createdAt", "") or ""),
            fetched_at=self._to_iso(time.time()),
            query_seed=query_task.query_text,
            query_id=query_task.query_id,
            query_text=query_task.query_text,
            window_id=time_slice.window_id,
            window_start=time_slice.start_at.isoformat(),
            window_end=time_slice.end_at.isoformat(),
            page_no=page_no,
            author_hint=str((discussion.get("author") or {}).get("login", "") or ""),
            source_meta={
                "collector_mode": "live",
                "content_kind": "discussion",
                "query_id": query_task.query_id,
                "query_text": query_task.query_text,
                "query_priority": query_task.priority,
                "expected_signal_type": query_task.expected_signal_type,
                "query_axes_used": query_task.axes_used,
                "window_id": time_slice.window_id,
                "window_label": time_slice.label,
                "window_start": time_slice.start_at.isoformat(),
                "window_end": time_slice.end_at.isoformat(),
                "page_no": page_no,
                "repo_index": repo_index,
                "item_index": item_index,
                "repository": repository,
                "raw_discussion": {
                    key: value
                    for key, value in discussion.items()
                    if key not in {"comments"}
                },
                "raw_discussion_comments": comments,
                "raw_discussion_replies": reply_map,
            },
        )

    def _fetch_rest_json(self, base_url: str, params: dict[str, Any] | None = None) -> Any:
        """Fetch JSON from the GitHub REST API."""
        url = base_url if not params else f"{base_url}?{urlencode(params)}"
        max_retries = int(os.getenv("GITHUB_REQUEST_RETRIES", "2"))
        for attempt in range(max_retries + 1):
            request = Request(url, headers=self._headers())
            try:
                with urlopen(request, timeout=30) as response:
                    return json.load(response)
            except HTTPError as exc:
                raise RuntimeError(f"GitHub REST request failed with HTTP {exc.code} for URL: {url}") from exc
            except URLError as exc:
                if attempt < max_retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise RuntimeError(f"GitHub REST request failed due to network error for URL: {url}") from exc
        raise RuntimeError(f"GitHub REST request failed after retries for URL: {url}")

    def _fetch_graphql_json(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Fetch JSON from the GitHub GraphQL API."""
        token = os.getenv("GITHUB_TOKEN", "").strip()
        if not token:
            raise RuntimeError("GITHUB_TOKEN is required for GitHub Discussions GraphQL collection.")

        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        max_retries = int(os.getenv("GITHUB_REQUEST_RETRIES", "2"))
        for attempt in range(max_retries + 1):
            request = Request("https://api.github.com/graphql", headers=self._headers(), data=body, method="POST")
            try:
                with urlopen(request, timeout=30) as response:
                    payload = json.load(response)
                break
            except HTTPError as exc:
                raise RuntimeError("GitHub GraphQL request failed.") from exc
            except URLError as exc:
                if attempt < max_retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise RuntimeError("GitHub GraphQL network request failed.") from exc
        else:
            raise RuntimeError("GitHub GraphQL request failed after retries.")
        if payload.get("errors"):
            raise RuntimeError(f"GitHub GraphQL returned errors: {payload['errors']}")
        return payload

    def _headers(self) -> dict[str, str]:
        """Build GitHub API headers with optional token."""
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "persona-pipeline/0.1",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = os.getenv("GITHUB_TOKEN", "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _issue_search_terms(self, query_seed: str) -> str:
        """Reduce long complaint phrasing to a few search-worthy tokens for GitHub."""
        tokens = self._query_tokens(query_seed)
        preferred_terms = [
            "dashboard",
            "report",
            "reporting",
            "excel",
            "export",
            "metric",
            "metrics",
            "kpi",
            "data",
            "analytics",
            "reconcile",
            "segment",
            "drill",
        ]
        for term in preferred_terms:
            if term in tokens:
                return term
        return tokens[0] if tokens else str(query_seed).strip()

    def _query_tokens(self, query_seed: str) -> list[str]:
        """Extract compact non-trivial tokens from a query seed."""
        stopwords = {
            "need",
            "better",
            "support",
            "for",
            "the",
            "and",
            "with",
            "from",
            "this",
            "that",
            "why",
            "how",
            "cannot",
            "cant",
            "users",
            "user",
        }
        tokens = [
            token
            for token in query_seed.lower().replace("/", " ").replace("-", " ").split()
            if len(token) >= 3 and token not in stopwords
        ]
        return list(dict.fromkeys(tokens))

    def _record_partial_error(
        self,
        query_task: QuerySeedTask,
        time_slice: TimeSlice,
        page_no: int,
        error_stage: str,
        exc: Exception,
        repository: str,
    ) -> None:
        """Record per-repository errors without aborting the whole query/window page."""
        error_code = self._extract_error_code(exc)
        self.error_stats.append(
            {
                "source": self.source_name,
                "query_id": query_task.query_id,
                "query_text": query_task.query_text,
                "window_id": time_slice.window_id,
                "window_start": time_slice.start_at.isoformat(),
                "window_end": time_slice.end_at.isoformat(),
                "page_no": page_no,
                "error_stage": error_stage,
                "error_type": type(exc).__name__,
                "error_code": error_code,
                "error_message": f"{repository}: {exc}",
                "is_retryable": self._is_retryable_error(error_code, str(exc)),
            }
        )

    def _parse_iso(self, value: str | None) -> datetime | None:
        """Parse an ISO timestamp into UTC."""
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

    def _to_iso(self, timestamp: Any) -> str:
        """Convert a Unix timestamp into ISO 8601 UTC format."""
        if timestamp in (None, ""):
            return ""
        return datetime.fromtimestamp(float(timestamp), tz=UTC).replace(microsecond=0).isoformat()

    def _discussion_after_cursor(
        self,
        query_text: str,
        owner: str,
        repo: str,
        time_slice: TimeSlice,
        page_no: int,
    ) -> str | None:
        """Return the discussion pagination cursor for a target page if known."""
        if page_no == 1:
            return None
        key = f"{query_text}::{owner}/{repo}::{time_slice.window_id}::{page_no}"
        return getattr(self, "_discussion_cursors", {}).get(key)

    def _remember_discussion_after_cursor(
        self,
        query_text: str,
        owner: str,
        repo: str,
        time_slice: TimeSlice,
        page_no: int,
        cursor: str,
    ) -> None:
        """Store the next GitHub discussion pagination cursor."""
        if not hasattr(self, "_discussion_cursors"):
            self._discussion_cursors: dict[str, str] = {}
        key = f"{query_text}::{owner}/{repo}::{time_slice.window_id}::{page_no}"
        self._discussion_cursors[key] = cursor
