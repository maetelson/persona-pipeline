"""Discourse collector skeleton."""

from __future__ import annotations

from src.collectors.base import BaseCollector, RawRecord


class DiscourseCollector(BaseCollector):
    """Stub-ready Discourse collector that only emits raw records."""

    source_name = "discourse"
    source_type = "forum"

    def collect(self) -> list[RawRecord]:
        return [self.build_stub_record()]
