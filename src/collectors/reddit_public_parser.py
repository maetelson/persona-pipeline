"""Helpers for parsing public Reddit listing and comments payloads."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def parse_reddit_listing_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return Reddit post payloads from a subreddit listing response."""
    children = payload.get("data", {}).get("children", [])
    return [dict(child.get("data", {}) or {}) for child in children if child.get("kind") == "t3"]


def parse_reddit_comment_payload(payload: Any) -> list[dict[str, Any]]:
    """Return top-level comment payloads from a Reddit comments response."""
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    listing = payload[1].get("data", {}).get("children", [])
    comments: list[dict[str, Any]] = []
    for child in listing:
        if child.get("kind") != "t1":
            continue
        data = dict(child.get("data", {}) or {})
        comments.append(data)
    return comments


def reddit_timestamp_to_iso(timestamp: Any) -> str:
    """Convert Reddit UTC seconds to ISO 8601."""
    if timestamp in (None, ""):
        return ""
    return datetime.fromtimestamp(float(timestamp), tz=UTC).replace(microsecond=0).isoformat()
