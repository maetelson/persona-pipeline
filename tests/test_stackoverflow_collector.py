"""Tests for Stack Overflow pain-family query construction."""

from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import patch

from src.collectors.base import TimeSlice
from src.collectors.stackoverflow_collector import StackOverflowCollector

ROOT = Path(__file__).resolve().parents[1]


class StackOverflowCollectorTests(unittest.TestCase):
    """Verify structured pain-family query behavior for Stack Overflow."""

    def _collector(self) -> StackOverflowCollector:
        return StackOverflowCollector(
            config={
                "source_group": "existing_forums",
                "source_name": "stackoverflow",
                "query_mode": "pain_family_search",
                "seed_bank_path": "config/seeds/existing_forums/stackoverflow.yaml",
                "site": "stackoverflow",
                "pagesize": 20,
                "search_sort": "relevance",
                "search_order": "desc",
            },
            data_dir=ROOT / "data",
        )

    def test_pain_family_search_builds_tag_scoped_tasks(self) -> None:
        collector = self._collector()
        tasks = collector.get_query_seed_tasks()
        self.assertEqual(len(tasks), 130)
        self.assertTrue(all(task.search_params.get("tagged") for task in tasks))
        self.assertTrue(all(task.search_params.get("q") for task in tasks))
        self.assertTrue(all(task.query_text.startswith(("reconciliation_mismatch__", "metric_definition_conflict__", "export_report_integrity__", "segmentation_breakdown_confusion__", "attribution_source_of_truth__")) for task in tasks))

    def test_fetch_questions_page_uses_structured_search_params(self) -> None:
        collector = self._collector()
        task = collector.get_query_seed_tasks()[0]
        time_slice = TimeSlice(
            window_id="w1",
            start_at=collector.build_time_slices()[0].start_at,
            end_at=collector.build_time_slices()[0].end_at,
            label="test",
        )
        captured: dict[str, object] = {}

        def fake_fetch_json(base_url: str, params: dict[str, object], request_kind: str = "rest") -> dict[str, object]:
            captured["base_url"] = base_url
            captured["params"] = params
            captured["request_kind"] = request_kind
            return {"items": [], "has_more": False}

        with patch.object(collector, "_fetch_json", side_effect=fake_fetch_json):
            collector._fetch_questions_page(task, time_slice, 1)

        params = dict(captured["params"])
        self.assertEqual(captured["base_url"], "https://api.stackexchange.com/2.3/search/advanced")
        self.assertEqual(captured["request_kind"], "search")
        self.assertIn("q", params)
        self.assertIn("tagged", params)
        self.assertNotIn("nottagged", params)
        self.assertNotIn("title_terms", params)
        self.assertNotIn("body_terms", params)


if __name__ == "__main__":
    unittest.main()
