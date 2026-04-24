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

    def test_adobe_workspace_debugger_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Adobe Analytics data visible in Debugger but not in Workspace for one user",
                    "body": "I can see the data firing correctly in the Adobe Analytics Debugger, but the same data does not appear in Workspace.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 190,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_cja_not_showing_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "CJA Browser Name Value is Not Showing up",
                    "body": "Within CJA we are not seeing any value assigned to Browser Name even though the Datastream is configured correctly.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 170,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_entries_higher_than_visits_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "How Can Entries Be Higher Than Visits?",
                    "body": "Adobe Analytics is showing entries higher than visits for these items and I need to understand why the numbers differ.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 170,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_operational_question_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "What happened to freeform table columns?",
                    "body": "I used to be able to include multiple metrics as columns in an Analysis Workspace freeform table, and now the resize behavior changed. Is there a way to restore it?",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 220,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_no_longer_workspace_behavior_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Adobe Report Builder dependent data block",
                    "body": "Are Dependent Data Blocks no longer available in the new Adobe Report Builder? I can only find brief info for Legacy Report Builder.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 190,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_greyed_out_result_interpretation_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Adobe Classification Import overwrite checkbox greyed out",
                    "body": "When we upload the classification file in Adobe Analytics, the overwrite option is greyed out and we need to understand what happens next.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 200,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_ecid_edge_network_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "ECID generation on first-time load in AEP Edge Network API",
                    "body": "I am working on a server-side Adobe Target implementation using the AEP Edge Network API and need to understand how ECID capture works on the first interaction.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 220,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_not_populating_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Adobe OOTB Referrer not populating",
                    "body": "Our Adobe Analytics referrer dimension is not populating correctly for cross-domain traffic and we need to understand why.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 170,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_not_adding_up_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Freeform Table grand total is not adding up correctly",
                    "body": "The Adobe Analytics freeform table grand total is not adding up correctly compared with the dimension values in the report.",
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

    def test_adobe_classification_performance_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Classification Sets Performance When Using Multiple Report Suites",
                    "body": "With the migration to Classification Sets in Adobe Analytics, I need to understand performance optimization when several report suites depend on the same classification data.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 220,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_rolling_date_label_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "CJA Rolling Date Labels",
                    "body": "We are reusing a CJA dashboard with rolling date ranges and need to understand how the rolling date labels update for daily reporting.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 190,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_internal_traffic_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Marking Traffic as internal Traffic",
                    "body": "We need to filter out internal traffic in CJA and want to confirm the best way when processing rules are no longer available in Analytics.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 200,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_significance_reading_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "How do I read A4T confidence levels for my ab tests?",
                    "body": "I am trying to standardize how we read Adobe Analytics significance and confidence levels for experiment reporting, and I need to confirm the right way.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 210,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_dynamic_reporting_role_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Dynamic Reporting of Logins per User Role in Adobe Analytics",
                    "body": "We need an Adobe Analytics report for logins per user role and percentage of users in each role, and currently can only achieve this with a calculated metric workaround.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 210,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_community_member_announcement_stays_invalid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Celebrating Jennifer Dungan, 2025 Community Member of the Year for Adobe Analytics!",
                    "body": "We are excited to celebrate a long-time contributor and highlight her impact across the Adobe Analytics community.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)

    def test_adobe_different_page_numbers_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "How to have numbers of one page",
                    "body": "I checked one URL in two different Adobe Analytics reports and the numbers were very different, so I need to know the right way to read the page data.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 190,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_google_scheduled_reports_disappeared_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "google_developer_forums",
                    "title": "Scheduled Reports have disappeared and those left don't work properly",
                    "body": "In our Looker Studio report most scheduled reports disappeared and the one left just spins forever, so we need help understanding this data corruption issue.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 220,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_google_pivot_export_limitation_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "google_developer_forums",
                    "title": "Can we export the pivot table data into an excel file?",
                    "body": "I can export a normal Looker Studio table but not a pivot table, and I need to know whether this is a limitation because the reporting export is blocked.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 210,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_google_welcome_announcement_stays_invalid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "google_developer_forums",
                    "title": "Welcome to the Looker Studio Community!",
                    "body": "Hello and welcome to the Looker Studio Community forum. This is your resource for asking questions and contributing your knowledge.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 170,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)

    def test_adobe_freeform_blank_rows_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Why am I seeing blank rows in a freeform table instead of zeroes?",
                    "body": "In Adobe Analytics freeform table I am seeing blank rows instead of zeroes and need to understand whether this is expected behavior or a data issue.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 190,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_data_warehouse_mismatch_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Difference seen between Data Warehouse Report and Analysis Workspace Reports",
                    "body": "When we extract data from Data Warehouse the values are not matching Analysis Workspace, and some eVar values seem to persist unexpectedly.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 210,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(invalid_df), 0)

    def test_adobe_exam_reschedule_row_stays_invalid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Unable to reschedule AD0-E207 exam",
                    "body": "My laptop had issues connecting to the portal and I need help rescheduling the certification exam.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 150,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)
        self.assertIn("missing_business_signal", invalid_df.iloc[0]["invalid_reason"])

    def test_adobe_how_to_implement_row_stays_invalid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "how to implement Adobe Analytics on a React app",
                    "body": "I would like to know how to implement Adobe Analytics in a React application and need setup guidance.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 150,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)
        self.assertIn("missing_pain_signal", invalid_df.iloc[0]["invalid_reason"])

    def test_adobe_training_instance_row_stays_invalid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "Is there a test instance for learners?",
                    "body": "I am taking training for the Business Practitioner certificate and want a test instance so I can practice what I learn before starting a new job.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)
        self.assertIn("missing_business_signal", invalid_df.iloc[0]["invalid_reason"])

    def test_adobe_backend_api_row_stays_invalid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "title": "How do I call analytics api from backend javascript server application",
                    "body": "I am trying to call the Adobe Analytics API from a backend javascript server application and need implementation guidance.",
                    "comments_text": "",
                    "raw_text": "",
                    "text_len": 180,
                    "language": "en",
                }
            ]
        )
        valid_df, invalid_df = apply_invalid_filter(frame, self.rules)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(invalid_df), 1)
        self.assertIn("missing_pain_signal", invalid_df.iloc[0]["invalid_reason"])

    def test_domo_filter_card_row_is_rescued_into_valid(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "domo_community_forum",
                    "title": "Drop Down Filter Card - default selection and not allow user to clear filter",
                    "body": "I want the filter card to always have something selected and not allow the user to clear the filter.",
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
