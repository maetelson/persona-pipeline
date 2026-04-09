"""Shared cache and response helpers for token-aware LLM calls."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def build_prompt_cache_key(model: str, prompt: str, namespace: str = "") -> str:
    """Build a stable cache key from the actual prompt payload, not row identity."""
    raw = f"{namespace}||{model}||{prompt}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def load_jsonl_cache(path: Path) -> dict[str, dict[str, Any]]:
    """Load a simple JSONL cache file keyed by cache_key."""
    if not path.exists():
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        key = str(payload.get("cache_key", "") or "")
        if not key:
            continue
        cache[key] = dict(payload.get("suggestion", {}) or {})
    return cache


def append_jsonl_cache(path: Path, cache_key: str, suggestion: dict[str, Any]) -> None:
    """Append one cache record to a JSONL cache file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"cache_key": cache_key, "suggestion": suggestion}, ensure_ascii=False) + "\n")


def parse_responses_json(raw_response: dict[str, Any]) -> dict[str, Any]:
    """Extract a JSON object from an OpenAI Responses API payload."""
    if isinstance(raw_response.get("output_text"), str) and raw_response["output_text"].strip():
        return json.loads(raw_response["output_text"])
    output_items = raw_response.get("output", []) or []
    text_chunks: list[str] = []
    for item in output_items:
        for content in item.get("content", []) or []:
            if isinstance(content.get("text"), str):
                text_chunks.append(content["text"])
    if not text_chunks:
        raise ValueError("No text content returned by Responses API")
    return json.loads("\n".join(text_chunks))


def extract_responses_usage(raw_response: dict[str, Any]) -> dict[str, int]:
    """Extract token usage fields from a Responses API payload."""
    usage = raw_response.get("usage", {}) or {}
    input_tokens = int(usage.get("input_tokens", 0) or 0)
    output_tokens = int(usage.get("output_tokens", 0) or 0)
    total_tokens = int(usage.get("total_tokens", input_tokens + output_tokens) or 0)
    return {
        "usage_input_tokens": input_tokens,
        "usage_output_tokens": output_tokens,
        "usage_total_tokens": total_tokens,
    }
