"""Base normalizer and shared normalized post schema."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from src.utils.record_access import serialize_source_meta
from src.utils.text import clean_text, combine_text, make_dedupe_key

NORMALIZED_POST_COLUMNS = [
    "source",
    "source_group",
    "source_name",
    "source_type",
    "raw_id",
    "raw_source_id",
    "url",
    "canonical_url",
    "query_seed",
    "title",
    "body",
    "body_text",
    "comments_text",
    "raw_text",
    "created_at",
    "fetched_at",
    "retrieved_at",
    "author_name",
    "product_or_tool",
    "subreddit_or_forum",
    "thread_title",
    "parent_context",
    "role_hint",
    "company_size_hint",
    "industry_hint",
    "workflow_hint",
    "pain_point_hint",
    "output_need_hint",
    "dev_heavy_score",
    "biz_user_score",
    "relevance_score",
    "language",
    "crawl_method",
    "crawl_status",
    "manual_import_flag",
    "raw_file_path",
    "parse_version",
    "hash_id",
    "author_hint",
    "source_meta",
    "dedupe_key",
    "text_len",
]


@dataclass(slots=True)
class NormalizedPost:
    """Common schema for normalized posts across all sources."""

    source: str
    source_group: str
    source_name: str
    source_type: str
    raw_id: str
    raw_source_id: str
    url: str
    canonical_url: str
    query_seed: str
    title: str
    body: str
    body_text: str
    comments_text: str
    raw_text: str
    created_at: str
    fetched_at: str
    retrieved_at: str
    author_name: str = ""
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
    dev_heavy_score: float = 0.0
    biz_user_score: float = 0.0
    relevance_score: float = 0.0
    language: str = "en"
    crawl_method: str = ""
    crawl_status: str = ""
    manual_import_flag: bool = False
    raw_file_path: str = ""
    parse_version: str = "v1"
    hash_id: str = ""
    author_hint: str = ""
    source_meta: dict[str, Any] = field(default_factory=dict)
    dedupe_key: str = ""
    text_len: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dataframe-friendly dictionary."""
        return asdict(self)


class BaseNormalizer(ABC):
    """Base normalizer converting raw rows to normalized post rows."""

    @abstractmethod
    def normalize_row(self, row: dict[str, Any]) -> NormalizedPost:
        """Normalize a single raw row."""

    def normalize_rows(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        """Normalize all rows into the shared dataframe schema."""
        normalized = [self.normalize_row(row).to_dict() for row in rows]
        return pd.DataFrame(normalized, columns=NORMALIZED_POST_COLUMNS)


class PassThroughNormalizer(BaseNormalizer):
    """Default normalizer for stub and initial source support."""

    def normalize_row(self, row: dict[str, Any]) -> NormalizedPost:
        source_meta = dict(row.get("source_meta", {}) or {})
        title = clean_text(row.get("title"))
        body = clean_text(row.get("body"))
        comments = clean_text(row.get("comments_text"))
        raw_text = combine_text(title, body, comments)
        return NormalizedPost(
            source=row["source"],
            source_group=row.get("source_group", ""),
            source_name=row.get("source_name", row.get("source", "")),
            source_type=row.get("source_type", "unknown"),
            raw_id=str(row["raw_id"]),
            raw_source_id=str(row.get("raw_source_id", row.get("raw_id", ""))),
            url=row.get("url", ""),
            canonical_url=row.get("canonical_url", row.get("url", "")),
            query_seed=row.get("query_seed", ""),
            title=title,
            body=body,
            body_text=clean_text(row.get("body_text") or body),
            comments_text=comments,
            raw_text=raw_text,
            created_at=row.get("created_at", ""),
            fetched_at=row.get("fetched_at", ""),
            retrieved_at=row.get("retrieved_at", row.get("fetched_at", "")),
            author_name=row.get("author_name", row.get("author_hint", "")),
            product_or_tool=row.get("product_or_tool", ""),
            subreddit_or_forum=row.get("subreddit_or_forum", ""),
            thread_title=row.get("thread_title", title),
            parent_context=row.get("parent_context", ""),
            role_hint=row.get("role_hint", ""),
            company_size_hint=row.get("company_size_hint", ""),
            industry_hint=row.get("industry_hint", ""),
            workflow_hint=row.get("workflow_hint", ""),
            pain_point_hint=row.get("pain_point_hint", ""),
            output_need_hint=row.get("output_need_hint", ""),
            dev_heavy_score=float(row.get("dev_heavy_score", 0.0) or 0.0),
            biz_user_score=float(row.get("biz_user_score", 0.0) or 0.0),
            relevance_score=float(row.get("relevance_score", 0.0) or 0.0),
            language=row.get("language", "en"),
            crawl_method=row.get("crawl_method", source_meta.get("crawl_method", "")),
            crawl_status=row.get("crawl_status", source_meta.get("crawl_status", "")),
            manual_import_flag=bool(row.get("manual_import_flag", False)),
            raw_file_path=row.get("raw_file_path", ""),
            parse_version=row.get("parse_version", "v1"),
            hash_id=row.get("hash_id", ""),
            author_hint=row.get("author_hint", ""),
            source_meta=serialize_source_meta(
                source_meta,
                window_id=row.get("window_id", ""),
                window_start=row.get("window_start", ""),
                window_end=row.get("window_end", ""),
            ),
            dedupe_key=make_dedupe_key(row.get("source", ""), title, body, comments),
            text_len=len(raw_text),
        )
