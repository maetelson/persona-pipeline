"""Base interface and shared raw record model for collectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
import os
from pathlib import Path
import re
import time
from typing import Any, Callable

from src.utils.dates import build_relative_time_window, build_time_slices, utc_now_iso
from src.utils.io import ensure_dir, load_yaml, write_jsonl
from src.utils.seed_bank import resolve_seed_queries


@dataclass(slots=True)
class RawRecord:
    """Raw source record stored before any filtering or segmentation."""

    source: str
    source_type: str
    raw_id: str
    url: str
    title: str
    body: str
    comments_text: str
    created_at: str
    fetched_at: str
    query_seed: str
    query_id: str = ""
    query_text: str = ""
    window_id: str = ""
    window_start: str = ""
    window_end: str = ""
    page_no: int = 1
    author_hint: str = ""
    source_group: str = ""
    source_name: str = ""
    raw_source_id: str = ""
    canonical_url: str = ""
    body_text: str = ""
    author_name: str = ""
    retrieved_at: str = ""
    product_or_tool: str = ""
    subreddit_or_forum: str = ""
    thread_title: str = ""
    parent_context: str = ""
    role_hint: str = ""
    company_size_hint: str = ""
    industry_hint: str = ""
    workflow_hint: str = ""
    pain_point_hint: str = ""
    output_need_hint: str = ""
    crawl_method: str = ""
    crawl_status: str = ""
    manual_import_flag: bool = False
    raw_file_path: str = ""
    parse_version: str = "v1"
    hash_id: str = ""
    source_meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the raw record to a plain dictionary."""
        return asdict(self)


@dataclass(slots=True)
class QuerySeedTask:
    """One expanded query seed resolved for a specific source."""

    query_id: str
    query_text: str
    axes_used: list[str]
    source_applicability: list[str]
    priority: str
    expected_signal_type: str


@dataclass(slots=True)
class TimeSlice:
    """One collection slice across the configured time window."""

    window_id: str
    start_at: datetime
    end_at: datetime
    label: str


@dataclass(slots=True)
class PageResult:
    """One fetched pagination unit for a query/time-window task."""

    records: list[RawRecord]
    has_more: bool
    rate_limit_wait_seconds: float = 0.0
    stop_reason: str = ""


class BaseCollector(ABC):
    """Shared collector contract for source-specific raw collection."""

    source_name: str
    source_type: str

    def __init__(self, config: dict[str, Any], data_dir: Path) -> None:
        self.config = config
        self.root_dir = data_dir.parent
        self.data_dir = ensure_dir(data_dir / "raw" / self.source_name)
        self.time_window = load_yaml(self.root_dir / "config" / "time_window.yaml")
        self.query_map = load_yaml(self.root_dir / "config" / "query_map.yaml")
        self.collection_stats: list[dict[str, Any]] = []
        self.error_stats: list[dict[str, Any]] = []

    @abstractmethod
    def collect(self) -> list[RawRecord]:
        """Collect raw records from the source."""

    def save(self, records: list[RawRecord]) -> Path:
        """Persist collected records as JSONL."""
        output_path = self.data_dir / "raw.jsonl"
        write_jsonl(output_path, [record.to_dict() for record in records])
        return output_path

    def build_stub_record(self, index: int = 1) -> RawRecord:
        """Create a default raw stub for local skeleton runs."""
        query_seeds = self.config.get("query_seeds") or resolve_seed_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )
        seed = (query_seeds or ["stub query"])[0]
        slug = self.source_name.replace("_", "-")
        time_slices = self.build_time_slices()
        time_slice = time_slices[0] if time_slices else None
        return RawRecord(
            source=self.source_name,
            source_type=self.source_type,
            raw_id=f"{self.source_name}-{index}",
            url=f"https://example.com/{slug}/{index}",
            title=f"{self.source_name} stub post about analyst bottlenecks",
            body=(
                "I manually export dashboard data every week, clean it in spreadsheets, "
                "and copy the same metrics into a stakeholder report."
            ),
            comments_text=(
                "We also spend time checking mismatched numbers and rebuilding the same charts."
            ),
            created_at=utc_now_iso(),
            fetched_at=utc_now_iso(),
            query_seed=seed,
            query_id=f"{self.source_name.upper()}_STUB_001",
            query_text=seed,
            window_id=time_slice.window_id if time_slice else "",
            window_start=time_slice.start_at.isoformat() if time_slice else "",
            window_end=time_slice.end_at.isoformat() if time_slice else "",
            page_no=1,
            author_hint="anonymous_practitioner",
            source_group=self.config.get("source_group", ""),
            source_name=self.config.get("source_name", self.source_name),
            raw_source_id=f"{self.source_name}-{index}",
            canonical_url=f"https://example.com/{slug}/{index}",
            body_text=(
                "I manually export dashboard data every week, clean it in spreadsheets, "
                "and copy the same metrics into a stakeholder report."
            ),
            author_name="anonymous_practitioner",
            retrieved_at=utc_now_iso(),
            product_or_tool=self.config.get("product_or_tool", ""),
            subreddit_or_forum=self.config.get("subreddit_or_forum", ""),
            thread_title=f"{self.source_name} stub post about analyst bottlenecks",
            parent_context=self.config.get("parent_context", ""),
            role_hint="business analyst",
            company_size_hint="",
            industry_hint="",
            workflow_hint="manual reporting",
            pain_point_hint="dashboard trust and spreadsheet rework",
            output_need_hint="stakeholder report",
            crawl_method="stub",
            crawl_status="stubbed",
            manual_import_flag=bool(self.config.get("manual_import_flag", False)),
            raw_file_path="",
            parse_version="v1",
            hash_id=f"{self.source_name}-{index}",
            source_meta={
                "collector_mode": "stub",
                "query_id": f"{self.source_name.upper()}_STUB_001",
                "query_text": seed,
                "window_id": time_slice.window_id if time_slice else "",
                "window_start": time_slice.start_at.isoformat() if time_slice else "",
                "window_end": time_slice.end_at.isoformat() if time_slice else "",
                "page_no": 1,
            },
        )

    def get_time_window_bounds(self) -> tuple[datetime, datetime]:
        """Return the configured collection time window in UTC."""
        return build_relative_time_window(self.time_window)

    def get_query_seed_tasks(self) -> list[QuerySeedTask]:
        """Resolve source-specific query tasks from the expanded query map."""
        query_mode = str(self.config.get("query_mode", "expanded_map") or "expanded_map")
        if query_mode == "source_config":
            return self._fallback_query_seed_tasks()

        allowed_priorities = {
            str(priority).strip().lower()
            for priority in self.config.get("allowed_query_priorities", ["high", "medium"])
        }
        raw_queries = self.query_map.get("expanded_queries", []) or []
        tasks: list[QuerySeedTask] = []
        for row in raw_queries:
            source_applicability = [str(source) for source in row.get("source_applicability", [])]
            priority = str(row.get("priority", "medium")).lower()
            if self.source_name not in source_applicability:
                continue
            if allowed_priorities and priority not in allowed_priorities:
                continue
            tasks.append(
                QuerySeedTask(
                    query_id=str(row.get("query_id", "")),
                    query_text=str(row.get("query_text", "")).strip(),
                    axes_used=[str(value) for value in row.get("axes_used", [])],
                    source_applicability=source_applicability,
                    priority=priority,
                    expected_signal_type=str(row.get("expected_signal_type", "unknown")),
                )
            )

        if not tasks:
            return self._fallback_query_seed_tasks()

        max_queries = self.config.get("max_queries_per_run")
        max_queries_override = os.getenv("COLLECT_MAX_QUERIES_PER_SOURCE", "").strip()
        if max_queries_override:
            max_queries = int(max_queries_override)
        if max_queries not in (None, ""):
            tasks = tasks[: int(max_queries)]
        return tasks

    def build_time_slices(self) -> list[TimeSlice]:
        """Split the configured time window into source-level collection slices."""
        slices = build_time_slices(self.time_window, source_name=self.source_name)
        time_slices: list[TimeSlice] = []
        for row in slices:
            slice_start = datetime.fromisoformat(str(row["window_start"]).replace("Z", "+00:00"))
            slice_end = datetime.fromisoformat(str(row["window_end"]).replace("Z", "+00:00"))
            label = f"{slice_start.date().isoformat()}__{slice_end.date().isoformat()}"
            time_slices.append(
                TimeSlice(
                    window_id=str(row["window_id"]),
                    start_at=slice_start,
                    end_at=slice_end,
                    label=label,
                )
            )
        return time_slices

    def get_collection_time_slices(self) -> list[TimeSlice]:
        """Return the time slices actually used during collection.

        Sources with weak or missing native date filters may override this and
        collect against a combined window, while downstream time filtering
        remains the conservative gate.
        """
        return self.build_time_slices()

    def collect_with_pagination(
        self,
        page_fetcher: Callable[[QuerySeedTask, TimeSlice, int], PageResult],
    ) -> list[RawRecord]:
        """Run the shared query × window × page collection loop.

        Stop conditions:
        - no more results
        - duplicate-heavy page
        - max pages per query
        - source-reported rate limit wait
        """
        records: list[RawRecord] = []
        seen_ids: set[tuple[str, str, str, str]] = set()
        duplicate_ratio_stop = float(self.config.get("duplicate_ratio_stop", 0.75))
        max_pages = int(self.config.get("max_pages_per_query", 2))
        max_pages_override = os.getenv("COLLECT_MAX_PAGES_PER_QUERY", "").strip()
        if max_pages_override:
            max_pages = int(max_pages_override)
        sleep_seconds = float(self.config.get("sleep_seconds", 0.0))

        for query_task in self.get_query_seed_tasks():
            for time_slice in self.get_collection_time_slices():
                stop_reason = ""
                for page_no in range(1, max_pages + 1):
                    try:
                        page_result = page_fetcher(query_task, time_slice, page_no)
                    except Exception as exc:  # noqa: BLE001
                        error_code = self._extract_error_code(exc)
                        error_message = str(exc)
                        stop_reason = "page_fetch_error"
                        self.collection_stats.append(
                            {
                                "source": self.source_name,
                                "query_id": query_task.query_id,
                                "query_text": query_task.query_text,
                                "window_id": time_slice.window_id,
                                "window_start": time_slice.start_at.isoformat(),
                                "window_end": time_slice.end_at.isoformat(),
                                "page_no": page_no,
                                "page_raw_count": 0,
                                "page_raw_count_before_dedupe": 0,
                                "duplicate_count": 0,
                                "duplicate_ratio": 0.0,
                                "stop_reason": stop_reason,
                            }
                        )
                        self.error_stats.append(
                            {
                                "source": self.source_name,
                                "query_id": query_task.query_id,
                                "query_text": query_task.query_text,
                                "window_id": time_slice.window_id,
                                "window_start": time_slice.start_at.isoformat(),
                                "window_end": time_slice.end_at.isoformat(),
                                "page_no": page_no,
                                "error_stage": "page_fetch",
                                "error_type": type(exc).__name__,
                                "error_code": error_code,
                                "error_message": error_message,
                                "is_retryable": self._is_retryable_error(error_code, error_message),
                            }
                        )
                        break
                    page_records = page_result.records
                    unique_page_records: list[RawRecord] = []
                    duplicate_count = 0

                    for record in page_records:
                        dedupe_token = (record.source, record.query_id, record.window_id, record.raw_id)
                        if dedupe_token in seen_ids:
                            duplicate_count += 1
                            continue
                        seen_ids.add(dedupe_token)
                        unique_page_records.append(record)

                    page_count = len(page_records)
                    duplicate_ratio = (duplicate_count / page_count) if page_count else 0.0
                    stop_reason = page_result.stop_reason
                    if page_count == 0:
                        stop_reason = stop_reason or "no_more_results"
                    elif duplicate_ratio >= duplicate_ratio_stop:
                        stop_reason = "duplicate_heavy_page"
                    elif not page_result.has_more:
                        stop_reason = stop_reason or "no_more_results"
                    elif page_no >= max_pages:
                        stop_reason = "max_pages_per_query"

                    records.extend(unique_page_records)
                    self.collection_stats.append(
                        {
                            "source": self.source_name,
                            "query_id": query_task.query_id,
                            "query_text": query_task.query_text,
                            "window_id": time_slice.window_id,
                            "window_start": time_slice.start_at.isoformat(),
                            "window_end": time_slice.end_at.isoformat(),
                            "page_no": page_no,
                            "page_raw_count": len(unique_page_records),
                            "page_raw_count_before_dedupe": page_count,
                            "duplicate_count": duplicate_count,
                            "duplicate_ratio": round(duplicate_ratio, 4),
                            "stop_reason": stop_reason,
                        }
                    )

                    if page_result.rate_limit_wait_seconds > 0:
                        time.sleep(page_result.rate_limit_wait_seconds)
                    elif sleep_seconds > 0 and not stop_reason:
                        time.sleep(sleep_seconds)

                    if stop_reason:
                        break

                if not stop_reason:
                    self.collection_stats.append(
                        {
                            "source": self.source_name,
                            "query_id": query_task.query_id,
                            "query_text": query_task.query_text,
                            "window_id": time_slice.window_id,
                            "window_start": time_slice.start_at.isoformat(),
                            "window_end": time_slice.end_at.isoformat(),
                            "page_no": max_pages,
                            "page_raw_count": 0,
                            "page_raw_count_before_dedupe": 0,
                            "duplicate_count": 0,
                            "duplicate_ratio": 0.0,
                            "stop_reason": "max_pages_per_query",
                        }
                    )
        return records

    def _extract_error_code(self, exc: Exception) -> str:
        """Extract an HTTP-like error code from an exception message when present."""
        message = str(exc)
        match = re.search(r"\bHTTP\s+(\d{3})\b", message)
        if match:
            return match.group(1)
        return ""

    def _is_retryable_error(self, error_code: str, error_message: str) -> bool:
        """Return whether the error looks transient enough to retry later."""
        if error_code in {"408", "409", "425", "429", "500", "502", "503", "504"}:
            return True
        lowered = error_message.lower()
        return any(token in lowered for token in ["timeout", "temporar", "rate limit", "too many requests"])

    def _fallback_query_seed_tasks(self) -> list[QuerySeedTask]:
        """Build query tasks from legacy source config seeds."""
        query_seeds = self.config.get("query_seeds") or resolve_seed_queries(
            self.root_dir,
            config=self.config,
            source_id=self.source_name,
            source_group=str(self.config.get("source_group", "")),
        )
        return [
            QuerySeedTask(
                query_id=f"{self.source_name.upper()}_LEGACY_{index:03d}",
                query_text=str(seed),
                axes_used=[],
                source_applicability=[self.source_name],
                priority="high",
                expected_signal_type="legacy_seed",
            )
            for index, seed in enumerate(query_seeds, start=1)
            if str(seed).strip()
        ]
