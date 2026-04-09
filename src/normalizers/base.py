"""Base normalizer and shared normalized post schema."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from src.utils.text import clean_text, combine_text, make_dedupe_key

NORMALIZED_POST_COLUMNS = [
    "source",
    "source_type",
    "raw_id",
    "url",
    "query_seed",
    "title",
    "body",
    "comments_text",
    "raw_text",
    "created_at",
    "fetched_at",
    "language",
    "author_hint",
    "source_meta",
    "dedupe_key",
    "text_len",
]


@dataclass(slots=True)
class NormalizedPost:
    """Common schema for normalized posts across all sources."""

    source: str
    source_type: str
    raw_id: str
    url: str
    query_seed: str
    title: str
    body: str
    comments_text: str
    raw_text: str
    created_at: str
    fetched_at: str
    language: str = "en"
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
        title = clean_text(row.get("title"))
        body = clean_text(row.get("body"))
        comments = clean_text(row.get("comments_text"))
        raw_text = combine_text(title, body, comments)
        return NormalizedPost(
            source=row["source"],
            source_type=row.get("source_type", "unknown"),
            raw_id=str(row["raw_id"]),
            url=row.get("url", ""),
            query_seed=row.get("query_seed", ""),
            title=title,
            body=body,
            comments_text=comments,
            raw_text=raw_text,
            created_at=row.get("created_at", ""),
            fetched_at=row.get("fetched_at", ""),
            language=row.get("language", "en"),
            author_hint=row.get("author_hint", ""),
            source_meta={
                "json": json.dumps(
                    {
                        **dict(row.get("source_meta", {}) or {}),
                        "window_id": row.get("window_id", ""),
                        "window_start": row.get("window_start", ""),
                        "window_end": row.get("window_end", ""),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            },
            dedupe_key=make_dedupe_key(row.get("source", ""), title, body, comments),
            text_len=len(raw_text),
        )
