"""Discourse normalizer skeleton."""

from __future__ import annotations

from src.normalizers.base import PassThroughNormalizer


class DiscourseNormalizer(PassThroughNormalizer):
    """Discourse-specific extension point for raw parsing."""
