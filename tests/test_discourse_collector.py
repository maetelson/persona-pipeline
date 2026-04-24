"""Tests for Discourse collector category filtering behavior."""

from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import patch

from src.collectors.discourse_collector import DiscourseCollector

ROOT = Path(__file__).resolve().parents[1]


class DiscourseCollectorTests(unittest.TestCase):
    """Verify source-specific Discourse category filters are honored."""

    def test_discover_topic_refs_filters_to_allowed_category_ids(self) -> None:
        collector = DiscourseCollector(
            config={
                "source_id": "google_developer_forums",
                "source_name": "Google Developer Forums",
                "source_group": "discourse",
                "base_url": "https://discuss.google.dev",
                "allowed_category_ids": [97, 213],
            },
            data_dir=ROOT / "data",
            source_name="google_developer_forums",
        )

        with (
            patch.object(
                collector,
                "_latest_topic_refs",
                return_value=[
                    {"id": 1, "title": "allowed latest", "category_id": 97},
                    {"id": 2, "title": "filtered latest", "category_id": 2},
                ],
            ),
            patch.object(
                collector,
                "_search_topic_refs",
                return_value=[
                    {"id": 3, "title": "allowed search", "category_id": 213},
                    {"id": 4, "title": "filtered search", "category_id": 111},
                ],
            ),
        ):
            refs = collector._discover_topic_refs(max_topics=None)

        self.assertEqual([int(ref["id"]) for ref in refs], [1, 3])

    def test_allowed_category_slugs_resolve_via_site_json(self) -> None:
        collector = DiscourseCollector(
            config={
                "source_id": "google_developer_forums",
                "source_name": "Google Developer Forums",
                "source_group": "discourse",
                "base_url": "https://discuss.google.dev",
                "allowed_category_slugs": ["looker-forum", "data-studio-qa"],
            },
            data_dir=ROOT / "data",
            source_name="google_developer_forums",
        )

        with (
            patch.object(
                collector,
                "_fetch_json",
                return_value={
                    "categories": [
                        {"id": 213, "slug": "looker-forum"},
                        {"id": 214, "slug": "data-studio-qa"},
                        {"id": 97, "slug": "cloud-data-analytics"},
                    ]
                },
            ),
            patch.object(
                collector,
                "_latest_topic_refs",
                return_value=[
                    {"id": 11, "title": "looker", "category_id": 213},
                    {"id": 12, "title": "cloud", "category_id": 97},
                ],
            ),
            patch.object(collector, "_search_topic_refs", return_value=[]),
        ):
            refs = collector._discover_topic_refs(max_topics=None)

        self.assertEqual([int(ref["id"]) for ref in refs], [11])


if __name__ == "__main__":
    unittest.main()
