"""Shared helpers for compact labeling payloads."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
from src.utils.pipeline_schema import LABEL_CODE_COLUMNS

FIELD_LIMITS = {
    "normalized_episode": 560,
    "evidence_snippet": 180,
    "role_clue": 80,
    "work_moment": 80,
    "business_question": 110,
    "tool_env": 80,
    "bottleneck_text": 120,
    "workaround_text": 90,
    "desired_output": 80,
}

LABEL_COLUMNS = LABEL_CODE_COLUMNS
EPISODE_FIELD_ALIASES = {
    "normalized_episode": "ep",
    "evidence_snippet": "ev",
    "role_clue": "rc",
    "work_moment": "wm",
    "business_question": "bq",
    "tool_env": "te",
    "bottleneck_text": "bt",
    "workaround_text": "wa",
    "desired_output": "do",
}
LABEL_FAMILY_ALIASES = {
    "role_codes": "r",
    "moment_codes": "m",
    "question_codes": "q",
    "pain_codes": "p",
    "env_codes": "e",
    "workaround_codes": "w",
    "output_codes": "o",
    "fit_code": "f",
    "confidence": "cf",
    "reason": "rs",
}
LABEL_FAMILY_ALIAS_REVERSE = {alias: key for key, alias in LABEL_FAMILY_ALIASES.items()}


def compact_json(value: Any) -> str:
    """Serialize JSON with a stable compact format."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def truncate_text(text: str, limit: int) -> str:
    """Trim long text while keeping both head and tail context."""
    normalized = " ".join(text.split())
    if len(normalized) <= limit:
        return normalized
    head = int(limit * 0.7)
    tail = max(limit - head - 5, 0)
    return f"{normalized[:head]} ... {normalized[-tail:]}" if tail else normalized[:limit]


def build_episode_payload(episode_row: pd.Series) -> dict[str, str]:
    """Extract and truncate only the fields needed for LLM labeling."""
    return {
        field: truncate_text(str(episode_row.get(field, "") or ""), FIELD_LIMITS[field])
        for field in FIELD_LIMITS
    }


def build_compact_episode_payload(episode_row: pd.Series) -> dict[str, str]:
    """Extract only non-empty episode fields and rename them to short aliases."""
    payload: dict[str, str] = {}
    for field, limit in FIELD_LIMITS.items():
        value = truncate_text(str(episode_row.get(field, "") or ""), limit)
        if value:
            payload[EPISODE_FIELD_ALIASES.get(field, field)] = value
    return payload


def extract_rule_labels(labeled_row: pd.Series, requested_families: list[str]) -> dict[str, str]:
    """Extract the current rule labels for only the requested families."""
    return {
        family: str(labeled_row.get(family, "unknown") or "unknown")
        for family in requested_families
        if family in LABEL_COLUMNS
    }


def extract_compact_rule_labels(labeled_row: pd.Series, requested_families: list[str]) -> dict[str, str]:
    """Extract current rule labels and rename them to short aliases."""
    labels = extract_rule_labels(labeled_row, requested_families)
    return {
        LABEL_FAMILY_ALIASES.get(family, family): value
        for family, value in labels.items()
        if value
    }


def build_compact_label_schema(requested_families: list[str]) -> dict[str, object]:
    """Build the smallest response schema needed for one labeling request."""
    schema: dict[str, object] = {}
    for family in requested_families:
        alias = LABEL_FAMILY_ALIASES.get(family)
        if not alias:
            continue
        schema[alias] = "CODE" if family == "fit_code" else ["CODE"]
    schema[LABEL_FAMILY_ALIASES["confidence"]] = 0.0
    schema[LABEL_FAMILY_ALIASES["reason"]] = "short phrase"
    return schema


def expand_compact_label_suggestion(suggestion: dict[str, Any]) -> dict[str, Any]:
    """Expand compact response aliases back into the full labeling schema."""
    expanded: dict[str, Any] = {}
    for key, value in dict(suggestion or {}).items():
        expanded[LABEL_FAMILY_ALIAS_REVERSE.get(str(key), str(key))] = value
    return expanded
