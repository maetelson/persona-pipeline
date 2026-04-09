"""Hacker News normalizer skeleton."""

from __future__ import annotations

from src.normalizers.base import PassThroughNormalizer


class HackerNewsNormalizer(PassThroughNormalizer):
    """Hacker News-specific extension point for raw parsing."""
