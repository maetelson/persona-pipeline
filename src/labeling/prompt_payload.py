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


def extract_rule_labels(labeled_row: pd.Series, requested_families: list[str]) -> dict[str, str]:
    """Extract the current rule labels for only the requested families."""
    return {
        family: str(labeled_row.get(family, "unknown") or "unknown")
        for family in requested_families
        if family in LABEL_COLUMNS
    }
