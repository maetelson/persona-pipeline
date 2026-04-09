"""Shared record access helpers for dict/Series-like pipeline rows."""

from __future__ import annotations

from typing import Any, Iterable

from src.utils.pipeline_schema import LABEL_CODE_COLUMNS, split_pipe_codes


TEXT_FIELDS = [
    "normalized_episode",
    "evidence_snippet",
    "business_question",
    "bottleneck_text",
    "workaround_text",
    "desired_output",
    "title",
    "body",
    "comments_text",
    "raw_text",
]

DEMO_FIELDS = [
    "role_codes",
    "role_clue",
]


def get_record_value(record: Any, field: str, default: object = "") -> object:
    """Read one field from a dict-like or Series-like record."""
    if record is None:
        return default
    if hasattr(record, "get"):
        try:
            return record.get(field, default)
        except Exception:  # noqa: BLE001
            return default
    return getattr(record, field, default)


def get_record_id(record: Any) -> str:
    """Return the best available stable identifier for one record."""
    for field in ["episode_id", "raw_id", "id"]:
        value = str(get_record_value(record, field, "") or "").strip()
        if value:
            return value
    return ""


def get_record_source(record: Any) -> str:
    """Return record source when present."""
    return str(get_record_value(record, "source", "") or "").strip()


def get_record_text(record: Any, fields: Iterable[str] | None = None) -> str:
    """Return one normalized text blob from the requested fields."""
    selected_fields = list(fields or TEXT_FIELDS)
    parts = []
    for field in selected_fields:
        value = str(get_record_value(record, field, "") or "").strip()
        if value:
            parts.append(value)
    return " ".join(parts).strip()


def get_record_codes(record: Any, columns: Iterable[str] | None = None) -> dict[str, list[str]]:
    """Return split code lists for all requested label-family columns."""
    selected = list(columns or LABEL_CODE_COLUMNS)
    return {
        column: split_pipe_codes(get_record_value(record, column, ""))
        for column in selected
    }


def get_record_demo(record: Any) -> list[str]:
    """Return role/demo-style hints from the record."""
    values: list[str] = []
    for field in DEMO_FIELDS:
        raw = get_record_value(record, field, "")
        if field.endswith("_codes"):
            values.extend(split_pipe_codes(raw))
        else:
            text = str(raw or "").strip()
            if text:
                values.append(text)
    return values


def is_valid_record(record: Any, required_fields: Iterable[str] | None = None) -> bool:
    """Return whether a record has the minimum required identifiers."""
    required = list(required_fields or ["episode_id"])
    return all(str(get_record_value(record, field, "") or "").strip() for field in required)
