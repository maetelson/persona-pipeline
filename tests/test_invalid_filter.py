"""Tests for source-aware invalid filtering."""

from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from src.filters.invalid_filter import activate_rule_mode, apply_invalid_filter
from src.analysis.persona_service import _stabilize_generic_persona_context
from src.utils.io import load_yaml


class InvalidFilterTests(unittest.TestCase):
    """Verify source-specific signal overrides do not leak across sources."""

    ROOT = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        rules = load_yaml(self.ROOT / "config" / "invalid_rules.yaml")
        self.rules = activate_rule_mode(rules, mode="analysis")

    def test_mixpanel_source_of_truth_row_is_not_invalid_for_missing_pain(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "title": "Difference between funnel and insights",
                    "body": "Mixpanel dashboard says one thing and export says another, which report should I trust for benchmark reviews?",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_klaviyo_segment_export_row_is_not_invalid_for_missing_pain(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "title": "Segment export to Google Sheets is missing profiles",
                    "body": "The segment export no longer matches profile counts and we use this for weekly reporting and manual spreadsheet review.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_mixpanel_timezone_export_row_is_not_invalid_for_missing_pain(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "title": "Timezone mismatch between export and dashboard",
                    "body": "The Mixpanel exported data and dashboard totals do not match because timezone mismatch changes the weekly report we share.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_klaviyo_skipped_report_row_is_not_invalid_for_missing_pain(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "title": "Skipped report reason missing in export",
                    "body": "Our custom report in Google Sheets only says message was skipped, and we need the skip reason before weekly reporting signoff.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_klaviyo_overview_dashboard_ga4_row_is_not_invalid_for_missing_pain(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "title": "Overview dashboard does not match GA4 revenue",
                    "body": "Our Klaviyo overview dashboard and bulk export do not match Google Analytics revenue totals, and we compare segments before weekly reporting signoff.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_generic_persona_context_is_stabilized_by_bottleneck(self) -> None:
        context, job, output = _stabilize_generic_persona_context(
            functional_context="analytics_workflow_execution",
            recurring_job_to_be_done="move_analysis_work_to_a_shareable_output",
            expected_output_artifact="shareable_analysis_output",
            workflow="",
            goal="",
            bottleneck="data_quality",
            trust="numbers_do_not_reconcile_or_feel_safe_to_share",
            tool_mode="",
        )
        self.assertEqual(context, "metric_validation_and_signoff")
        self.assertEqual(job, "validate_numbers_before_sharing_or_acting")
        self.assertEqual(output, "validated_metric_pack_before_distribution")


if __name__ == "__main__":
    unittest.main()
