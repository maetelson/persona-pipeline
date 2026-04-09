"""Tests for source-aware Reddit and Stack Overflow relevance scoring."""

from __future__ import annotations

import json
from pathlib import Path
import unittest

import pandas as pd

from src.filters.relevance import apply_relevance_prefilter
from src.utils.io import load_yaml

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def _load_fixture(name: str) -> dict[str, object]:
    """Load one JSON fixture row."""
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class RelevancePrefilterTests(unittest.TestCase):
    """Verify lexical, pattern, and source-aware scoring rules."""

    def setUp(self) -> None:
        self.rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")

    def test_reddit_relevant_reporting_post_is_kept(self) -> None:
        frame = pd.DataFrame([_load_fixture("reddit_relevant_excel_reporting.json")])
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(keep_df), 1)
        self.assertEqual(len(borderline_df), 0)
        self.assertEqual(len(drop_df), 0)
        self.assertGreater(float(keep_df.iloc[0]["excel_rework_score"]), 0.0)
        self.assertIn("export_excel", keep_df.iloc[0]["top_positive_signals"])

    def test_reddit_dashboard_trust_pattern_is_detected(self) -> None:
        frame = pd.DataFrame([_load_fixture("reddit_relevant_dashboard_trust.json")])
        keep_df, _, _ = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(keep_df), 1)
        self.assertGreater(float(keep_df.iloc[0]["dashboard_trust_score"]), 0.0)
        self.assertGreater(float(keep_df.iloc[0]["segmentation_breakdown_score"]), 0.0)

    def test_reddit_programming_noise_is_dropped(self) -> None:
        frame = pd.DataFrame([_load_fixture("reddit_irrelevant_programming.json")])
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)
        self.assertGreater(float(drop_df.iloc[0]["implementation_only_score"]), 0.0)
        self.assertIn("subreddit:r/learnpython", drop_df.iloc[0]["top_negative_signals"])

    def test_stackoverflow_relevant_power_bi_mismatch_is_kept(self) -> None:
        frame = pd.DataFrame([_load_fixture("stackoverflow_relevant_reporting_mismatch.json")])
        keep_df, _, _ = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(keep_df), 1)
        self.assertGreater(float(keep_df.iloc[0]["metric_definition_score"]), 0.0)
        self.assertGreater(float(keep_df.iloc[0]["bi_tool_score"]), 0.0)

    def test_stackoverflow_borderline_dax_without_business_context_is_borderline(self) -> None:
        frame = pd.DataFrame([_load_fixture("stackoverflow_borderline_dax_unclear.json")])
        _, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(borderline_df), 1)
        self.assertEqual(len(drop_df), 0)

    def test_stackoverflow_debugging_noise_is_dropped(self) -> None:
        frame = pd.DataFrame([_load_fixture("stackoverflow_irrelevant_debugging.json")])
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)
        self.assertGreater(float(drop_df.iloc[0]["dev_heavy_score"]), 0.0)
        self.assertGreater(float(drop_df.iloc[0]["infra_noise_score"]), 0.0)

    def test_technical_but_relevant_recovery_rule_keeps_reporting_context(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-recovery",
                    "title": "DAX measure issue in board report",
                    "body": "This Power BI measure mismatch affects our monthly report because finance definition differs from leadership dashboard definition.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"raw_question\": {\"tags\": [\"power-bi\", \"dax\"], \"is_answered\": false}}"},
                }
            ]
        )
        keep_df, _, _ = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(keep_df), 1)
        self.assertIn("technical_but_relevant_recovery", keep_df.iloc[0]["top_positive_signals"])

    def test_technical_only_drop_rule_rejects_when_negative_dominates(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-tech-only",
                    "title": "Docker oauth webdriver package error",
                    "body": "pip install failed with selenium stack trace in playwright browser automation setup.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"raw_question\": {\"tags\": [\"selenium\", \"docker\", \"oauth-2.0\"], \"is_answered\": false}}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(drop_df.iloc[0]["relevance_decision"], "drop")


if __name__ == "__main__":
    unittest.main()
