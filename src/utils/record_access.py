"""Shared record access helpers for dict/Series-like pipeline rows."""

from __future__ import annotations

import json
from typing import Any, Iterable

from src.utils.pipeline_schema import (
    LABEL_CODE_COLUMNS,
    RECORD_ID_FIELDS,
    RECORD_SOURCE_TEXT_FIELDS,
    RECORD_TEXT_FIELDS,
    SOURCE_META_JSON_KEY,
    parse_json_dict,
    split_pipe_codes,
)

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
    for field in RECORD_ID_FIELDS:
        value = str(get_record_value(record, field, "") or "").strip()
        if value:
            return value
    return ""


def get_record_source(record: Any) -> str:
    """Return record source when present."""
    return str(get_record_value(record, "source", "") or "").strip()


def get_record_text(record: Any, fields: Iterable[str] | None = None) -> str:
    """Return one normalized text blob from the requested fields."""
    selected_fields = list(fields or RECORD_TEXT_FIELDS)
    parts = []
    for field in selected_fields:
        value = str(get_record_value(record, field, "") or "").strip()
        if value:
            parts.append(value)
    return " ".join(parts).strip()


def get_record_source_text(record: Any) -> str:
    """Return source-facing text fields used by relevance and forum parsers."""
    return get_record_text(record, fields=RECORD_SOURCE_TEXT_FIELDS)


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


def get_record_source_meta(record: Any) -> dict[str, Any]:
    """Return parsed source metadata for normalized rows."""
    return parse_json_dict(get_record_value(record, "source_meta", {}), nested_json_key=SOURCE_META_JSON_KEY)


def get_record_tags(record: Any) -> list[str]:
    """Return normalized source tags when present."""
    source_meta = get_record_source_meta(record)
    raw_question = source_meta.get("raw_question", {}) if isinstance(source_meta.get("raw_question"), dict) else {}
    tags = raw_question.get("tags", source_meta.get("tags", []))
    if not isinstance(tags, list):
        return []
    return [str(tag).strip().lower() for tag in tags if str(tag).strip()]


def serialize_source_meta(payload: dict[str, Any] | None, **extra_fields: Any) -> dict[str, str]:
    """Serialize source metadata into the normalized storage contract."""
    merged = {**dict(payload or {}), **{key: value for key, value in extra_fields.items() if value not in {"", None}}}
    return {
        SOURCE_META_JSON_KEY: json.dumps(
            merged,
            ensure_ascii=False,
            sort_keys=True,
        )
    }


def is_valid_record(record: Any, required_fields: Iterable[str] | None = None) -> bool:
    """Return whether a record has the minimum required identifiers."""
    required = list(required_fields or ["episode_id"])
    return all(str(get_record_value(record, field, "") or "").strip() for field in required)
