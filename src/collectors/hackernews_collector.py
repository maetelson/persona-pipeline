"""Hacker News collector skeleton."""

from __future__ import annotations

from src.collectors.base import BaseCollector, RawRecord


class HackerNewsCollector(BaseCollector):
    """Stub-ready Hacker News collector that only emits raw records."""

    source_name = "hackernews"
    source_type = "forum"

    def collect(self) -> list[RawRecord]:
        return [self.build_stub_record()]
