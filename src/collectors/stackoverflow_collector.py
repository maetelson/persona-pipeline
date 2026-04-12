"""Stack Overflow collector with query × time window × pagination batching."""

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


class StackOverflowCollector(BaseCollector):
    """Collect Stack Overflow questions plus answers/comments as raw JSONL."""

    source_name = "stackoverflow"
    source_type = "qa"

    def collect(self) -> list[RawRecord]:
        if self.config.get("use_stub", False):
            return [self.build_stub_record()]
        if not self.get_query_seed_tasks():
            raise ValueError("Stack Overflow collector requires at least one query seed or expanded query task.")
        return self.collect_with_pagination(self._fetch_page)

    def _fetch_page(self, query_task: QuerySeedTask, time_slice: TimeSlice, page_no: int) -> PageResult:
        """Fetch one Stack Overflow result page for a query/time-window pair."""
        payload = self._fetch_questions_page(query_task.query_text, time_slice, page_no)
        question_items = payload.get("items", [])
        question_ids = [str(item["question_id"]) for item in question_items if item.get("question_id")]

        answers_by_question = self._fetch_answers_by_question(question_ids)
        question_comments = self._fetch_comments_for_posts(question_ids, post_type="questions")

        answer_ids = [
            str(answer.get("answer_id"))
            for answers in answers_by_question.values()
            for answer in answers
            if answer.get("answer_id")
        ]
        answer_comments = self._fetch_comments_for_posts(answer_ids, post_type="answers")

        records: list[RawRecord] = []
        for item_index, question in enumerate(question_items, start=1):
            question_id = str(question.get("question_id", ""))
            answers = answers_by_question.get(question_id, [])
            question_level_comments = question_comments.get(question_id, [])
            answer_comments_map = {
                str(answer.get("answer_id")): answer_comments.get(str(answer.get("answer_id")), [])
                for answer in answers
                if answer.get("answer_id")
            }
            records.append(
                self._build_raw_record(
                    query_task=query_task,
                    time_slice=time_slice,
                    page_no=page_no,
                    item_index=item_index,
                    question=question,
                    question_comments=question_level_comments,
                    answers=answers,
                    answer_comments=answer_comments_map,
                )
            )
        if payload.get("backoff"):
            wait_seconds = float(payload["backoff"])
        else:
            wait_seconds = 0.0
        return PageResult(records=records, has_more=bool(payload.get("has_more")), rate_limit_wait_seconds=wait_seconds)

    def _build_raw_record(
        self,
        query_task: QuerySeedTask,
        time_slice: TimeSlice,
        page_no: int,
        item_index: int,
        question: dict[str, Any],
        question_comments: list[dict[str, Any]],
        answers: list[dict[str, Any]],
        answer_comments: dict[str, list[dict[str, Any]]],
    ) -> RawRecord:
        """Map one question thread into the raw storage model."""
        question_id = str(question.get("question_id", ""))
        title = str(question.get("title", "") or "")
        body = str(question.get("body_markdown") or question.get("body", "") or "")
        comments_text = self._flatten_comments(question_comments, answers, answer_comments)

        return RawRecord(
            source=self.source_name,
            source_type=self.source_type,
            raw_id=question_id,
            url=str(question.get("link", "") or ""),
            title=title,
            body=body,
            comments_text=comments_text,
            created_at=self._to_iso(question.get("creation_date")),
            fetched_at=self._to_iso(time.time()),
            query_seed=query_task.query_text,
            query_id=query_task.query_id,
            query_text=query_task.query_text,
            window_id=time_slice.window_id,
            window_start=time_slice.start_at.isoformat(),
            window_end=time_slice.end_at.isoformat(),
            page_no=page_no,
            author_hint=self._owner_display_name(question.get("owner")),
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
                "page_no": page_no,
                "item_index": item_index,
                "site": self.config.get("site", "stackoverflow"),
                "search_params": {
                    "query_seed": query_task.query_text,
                    "site": self.config.get("site", "stackoverflow"),
                    "page_no": page_no,
                },
                "raw_question": question,
                "raw_question_comments": question_comments,
                "raw_answers": answers,
                "raw_answer_comments": answer_comments,
            },
        )

    def _fetch_questions_page(self, query_seed: str, time_slice: TimeSlice, page_no: int) -> dict[str, Any]:
        """Fetch a single Stack Overflow search page within one exact time slice."""
        pagesize = int(os.getenv("STACKOVERFLOW_PAGE_SIZE", self.config.get("pagesize", 10)))
        params = {
            "site": self.config.get("site", "stackoverflow"),
            "q": query_seed,
            "fromdate": int(time_slice.start_at.timestamp()),
            "todate": int(time_slice.end_at.timestamp()),
            "pagesize": pagesize,
            "page": page_no,
            "order": self.config.get("search_order", "desc"),
            "sort": self.config.get("search_sort", "relevance"),
            "filter": "withbody",
        }
        return self._fetch_json("https://api.stackexchange.com/2.3/search/advanced", params, request_kind="search")

    def _fetch_answers_by_question(self, question_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Fetch answers for question IDs and group them by question."""
        if not question_ids:
            return {}
        grouped: dict[str, list[dict[str, Any]]] = {question_id: [] for question_id in question_ids}
        max_answers = int(os.getenv("STACKOVERFLOW_MAX_ANSWERS", self.config.get("max_answers_per_question", 5)))
        for chunk in self._chunk_ids(question_ids, chunk_size=20):
            endpoint = f"https://api.stackexchange.com/2.3/questions/{';'.join(chunk)}/answers"
            params = {
                "site": self.config.get("site", "stackoverflow"),
                "pagesize": min(100, max_answers * max(len(chunk), 1)),
                "filter": "withbody",
                "sort": "votes",
                "order": "desc",
            }
            payload = self._fetch_json(endpoint, params, request_kind="answers")
            for answer in payload.get("items", []):
                parent_id = str(answer.get("question_id", ""))
                grouped.setdefault(parent_id, []).append(answer)
        return {question_id: answers[:max_answers] for question_id, answers in grouped.items()}

    def _fetch_comments_for_posts(self, post_ids: list[str], post_type: str) -> dict[str, list[dict[str, Any]]]:
        """Fetch comments for question or answer posts."""
        if not post_ids:
            return {}
        grouped: dict[str, list[dict[str, Any]]] = {post_id: [] for post_id in post_ids}
        max_comments = int(os.getenv("STACKOVERFLOW_MAX_COMMENTS", self.config.get("max_comments_per_post", 10)))
        for chunk in self._chunk_ids(post_ids, chunk_size=10):
            endpoint = f"https://api.stackexchange.com/2.3/{post_type}/{';'.join(chunk)}/comments"
            params = {
                "site": self.config.get("site", "stackoverflow"),
                "pagesize": min(100, max_comments * max(len(chunk), 1)),
                "filter": "withbody",
                "sort": "votes",
                "order": "desc",
            }
            payload = self._fetch_json(endpoint, params, request_kind="comments")
            for comment in payload.get("items", []):
                post_id = str(comment.get("post_id", ""))
                grouped.setdefault(post_id, []).append(comment)
        return {post_id: comments[:max_comments] for post_id, comments in grouped.items()}

    def _flatten_comments(
        self,
        question_comments: list[dict[str, Any]],
        answers: list[dict[str, Any]],
        answer_comments: dict[str, list[dict[str, Any]]],
    ) -> str:
        """Combine question comments, answers, and answer comments into one text field."""
        blocks: list[str] = []
        for comment in question_comments:
            body = str(comment.get("body_markdown") or comment.get("body", "") or "").strip()
            if body:
                blocks.append(f"[question_comment] {body}")
        for answer in answers:
            answer_body = str(answer.get("body_markdown") or answer.get("body", "") or "").strip()
            if answer_body:
                blocks.append(f"[answer] {answer_body}")
            for comment in answer_comments.get(str(answer.get("answer_id", "")), []):
                comment_body = str(comment.get("body_markdown") or comment.get("body", "") or "").strip()
                if comment_body:
                    blocks.append(f"[answer_comment] {comment_body}")
        return "\n\n".join(blocks)

    def _fetch_json(self, base_url: str, params: dict[str, Any], request_kind: str = "rest") -> dict[str, Any]:
        """Fetch JSON from the Stack Exchange API."""
        request_params = dict(params)
        request_params["site"] = self.config.get("site", "stackoverflow")
        key = os.getenv("STACKEXCHANGE_KEY", "").strip()
        if key:
            request_params["key"] = key

        url = f"{base_url}?{urlencode(request_params)}"
        max_retries = int(os.getenv("STACKOVERFLOW_REQUEST_RETRIES", "2"))
        for attempt in range(max_retries + 1):
            request = Request(url, headers={"Accept": "application/json", "User-Agent": "persona-pipeline/0.1"})
            request_started_at = time.perf_counter()
            try:
                with urlopen(request, timeout=30) as response:
                    payload = json.load(response)
                self.record_request(request_kind, time.perf_counter() - request_started_at, success=True)
                return payload
            except HTTPError as exc:
                self.record_request(request_kind, time.perf_counter() - request_started_at, success=False)
                raise RuntimeError(f"Stack Overflow request failed with HTTP {exc.code} for URL: {url}") from exc
            except URLError as exc:
                self.record_request(request_kind, time.perf_counter() - request_started_at, success=False)
                if attempt < max_retries:
                    retry_sleep_seconds = 1.5 * (attempt + 1)
                    self.record_request_retry()
                    self.record_backoff_sleep(retry_sleep_seconds)
                    time.sleep(retry_sleep_seconds)
                    continue
                raise RuntimeError(f"Stack Overflow request failed due to network error for URL: {url}") from exc
        raise RuntimeError(f"Stack Overflow request failed after retries for URL: {url}")

    def _owner_display_name(self, owner: Any) -> str:
        """Extract a display name from a Stack Exchange owner object."""
        if not isinstance(owner, dict):
            return ""
        return str(owner.get("display_name", "") or "")

    def _chunk_ids(self, values: list[str], chunk_size: int) -> list[list[str]]:
        """Split IDs into API-safe chunks to avoid oversized pagesize requests."""
        return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]

    def _to_iso(self, timestamp: Any) -> str:
        """Convert a Unix timestamp into ISO 8601 UTC format."""
        if timestamp in (None, ""):
            return ""
        return datetime.fromtimestamp(float(timestamp), tz=UTC).replace(microsecond=0).isoformat()
