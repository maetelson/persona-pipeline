"""Text normalization helpers."""

from __future__ import annotations

import hashlib
import re


def clean_text(value: str | None) -> str:
    """Return normalized single-spaced text."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def combine_text(*parts: str | None) -> str:
    """Join non-empty text parts with double newlines."""
    cleaned = [clean_text(part) for part in parts if clean_text(part)]
    return "\n\n".join(cleaned)


def make_dedupe_key(*parts: str | None) -> str:
    """Create a stable dedupe key from key text fragments."""
    combined = "||".join(clean_text(part).lower() for part in parts if part)
    return hashlib.sha1(combined.encode("utf-8")).hexdigest()
