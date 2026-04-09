"""YouTube normalizer skeleton."""

from __future__ import annotations

from src.normalizers.base import PassThroughNormalizer


class YouTubeNormalizer(PassThroughNormalizer):
    """YouTube-specific extension point for raw parsing."""
