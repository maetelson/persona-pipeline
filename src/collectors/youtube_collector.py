"""YouTube collector skeleton."""

from __future__ import annotations

from src.collectors.base import BaseCollector, RawRecord


class YouTubeCollector(BaseCollector):
    """Stub-ready YouTube collector that only emits raw records."""

    source_name = "youtube"
    source_type = "video"

    def collect(self) -> list[RawRecord]:
        return [self.build_stub_record()]
