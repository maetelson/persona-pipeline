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

    def test_stackoverflow_personal_learning_export_post_is_dropped(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-personal",
                    "title": "Export outlook email body to excel",
                    "body": "I can copy it manually and paste it into a new workbook, but I'm trying to do this to sharpen my Python skills.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"raw_question\": {\"tags\": [\"python\", \"excel\"], \"is_answered\": false}}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)

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

    def test_github_discussion_issue_template_noise_is_dropped_without_workflow_context(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "github_discussions",
                    "raw_id": "gh-noise",
                    "title": "Bug report: expected behavior vs actual behavior",
                    "body": "Version 1.2.3. Steps to reproduce. Console error after plugin installation.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"repository\": \"apache/superset\"}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)
        self.assertIn("github_issue_template_noise", drop_df.iloc[0]["top_negative_signals"])

    def test_github_discussion_reporting_context_survives_downweight(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "github_discussions",
                    "raw_id": "gh-reporting",
                    "title": "Dashboard numbers do not match source data in scheduled report export",
                    "body": "Leadership does not trust the dashboard because we still reconcile the export before sending the weekly reporting pack.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"repository\": \"metabase/metabase\"}"},
                }
            ]
        )
        keep_df, borderline_df, _ = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertIn("github_discussions_workflow_context", scored.iloc[0]["source_specific_reason"])

    def test_source_whitelist_rescues_metabase_borderline_drop(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "metabase_discussions",
                    "raw_id": "mb-whitelist",
                    "title": "Dashboard filter issue",
                    "body": "The dashboard filter is not working and the wrong numbers appear after export csv.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        _, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(borderline_df), 1)
        self.assertIn("metabase_dashboard_filter", borderline_df.iloc[0]["whitelist_hits"])
        self.assertIn("rescued_by_source_whitelist", borderline_df.iloc[0]["rescue_reason"])

    def test_shopify_source_whitelist_rescues_reporting_conversion_row(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "shopify_community",
                    "raw_id": "shopify-whitelist",
                    "title": "Shopify analytics report mismatch",
                    "body": "Weekly sales and conversion dropped after checkout tracking stopped matching dashboard numbers in csv export.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertIn("shopify_reporting_metrics_combo", scored.iloc[0]["whitelist_hits"])
        self.assertGreater(float(scored.iloc[0]["prefilter_score"]), 0.0)

    def test_shopify_reconciliation_rescue_keeps_cross_source_mismatch(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "shopify_community",
                    "raw_id": "shopify-reconcile",
                    "title": "GA4 and Shopify payout report are off",
                    "body": "Before sending finance numbers we double check because Shopify payout settlement is not matching GA4 and Google Ads revenue totals.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertIn("shopify_finance_reconciliation", scored.iloc[0]["whitelist_hits"])
        self.assertIn("shopify_cross_source_mismatch", scored.iloc[0]["whitelist_hits"])

    def test_klaviyo_setup_only_post_is_dropped(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-noise",
                    "title": "Best practices for welcome flow subject lines",
                    "body": "Looking for ideas on popup copy, welcome series setup, and subject line best practices for a new list.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)

    def test_klaviyo_reporting_mismatch_survives_prefilter(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-reporting",
                    "title": "Attributed revenue no longer matches segment report",
                    "body": "Our benchmark report and CSV export show different attributed revenue totals and we need to reconcile what changed before sending numbers.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_klaviyo_source_of_truth_reporting_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-source-truth",
                    "title": "Weekly reporting source of truth changed",
                    "body": "Our weekly reporting export excel workflow no longer matches segment count totals and we need to reconcile what changed before sending numbers.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertEqual(len(scored), 1)
        self.assertIn("klaviyo", scored.iloc[0]["source_specific_reason"])

    def test_mixpanel_api_setup_post_is_dropped(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-noise",
                    "title": "API issue with SDK instrumentation",
                    "body": "How do I send events with the mobile SDK and webhook API for a new implementation?",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)

    def test_qlik_export_mismatch_row_reaches_borderline(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "raw_id": "qlik-export",
                    "title": "Export to Excel board report wrong totals",
                    "body": "Our board report export to excel is not correct and the totals do not add up before distribution.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_google_developer_forums_metric_mismatch_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "google_developer_forums",
                    "raw_id": "google-mismatch",
                    "title": "Scorecard total doesn't match Bar chart total",
                    "body": "In Looker Studio our scorecard and bar chart show different active users for the same time range and the client is asking which number to trust.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertEqual(len(scored), 1)
        self.assertIn("google_bi_metric_mismatch", scored.iloc[0]["whitelist_hits"])

    def test_google_developer_forums_blend_data_operational_bug_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "google_developer_forums",
                    "raw_id": "google-blend-null",
                    "title": "Blended Tables Returning Null Values Where No Data",
                    "body": "In Looker Studio our blend data pivot table now returns null values and the summary row is incorrect, so the reporting view is not usable.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_adobe_workspace_debugger_gap_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "raw_id": "adobe-debugger-gap",
                    "title": "Adobe Analytics data visible in Debugger but not in Workspace",
                    "body": "I can see the data firing in Adobe Analytics Debugger, but the same data does not appear in Workspace for this validation workflow.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertEqual(len(scored), 1)
        self.assertIn("adobe_debugger_workspace_gap", scored.iloc[0]["whitelist_hits"])

    def test_adobe_cja_export_metric_gap_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "adobe_analytics_community",
                    "raw_id": "adobe-cja-export",
                    "title": "Add Calculated Metrics to CJA Full Table Export",
                    "body": "Our Customer Journey Analytics full table export is missing calculated metrics, which makes the csv download unreliable for reporting validation.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_domo_card_chart_issue_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "domo_community_forum",
                    "raw_id": "domo-chart-issue",
                    "title": "Why is Domo forcing daily data onto hourly chart?",
                    "body": "This Domo chart is forcing an hourly chart even though the data is daily, and the card is not usable for reporting.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        scored = keep_df if not keep_df.empty else borderline_df
        self.assertEqual(len(scored), 1)
        self.assertIn("domo_card_workflow_friction", scored.iloc[0]["whitelist_hits"])

    def test_klaviyo_export_integrity_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-export-integrity",
                    "title": "Segment export to Google Sheets is missing profiles",
                    "body": "Our custom report export to Google Sheets is missing profile rows and the segment export no longer matches attributed revenue totals.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_klaviyo_skipped_report_reason_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-skip-reason",
                    "title": "Skipped report reason is not detailed enough",
                    "body": "We export weekly reporting into Excel and need the skip reason because the message was skipped but the report only says skipped.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_klaviyo_ga4_overview_dashboard_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-ga4-overview",
                    "title": "Klaviyo overview dashboard does not match GA4 revenue",
                    "body": "Our overview dashboard and bulk export no longer match GA4 revenue attribution totals, and we need to compare segments before weekly reporting signoff.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_klaviyo_ga4_metric_compare_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "klaviyo_community",
                    "raw_id": "klaviyo-ga4-metric-compare",
                    "title": "How does your Klaviyo Active on Site Metric compare to Google Analytics Users Metric?",
                    "body": "Our weekly reporting keeps showing different numbers between Klaviyo active on site and Google Analytics users, and we need to reconcile the discrepancy before signoff.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_mixpanel_export_discrepancy_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-export-trust",
                    "title": "Duplicate events in export compared to reports",
                    "body": "The dashboard says one thing, the exported CSV says another, and we need a source of truth before our weekly reporting review.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_mixpanel_timestamp_export_integrity_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-timestamp-integrity",
                    "title": "Timestamp timezone differences in exported data vs dashboard",
                    "body": "Our Mixpanel dashboard and exported data do not match because timezone differences change the reporting totals we share each week.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_mixpanel_raw_usage_export_gap_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-raw-usage",
                    "title": "Raw usage data does not match exported report",
                    "body": "Our raw activity feed and exported report do not match because duplicate event rows change the dashboard totals we share weekly.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_qlik_export_total_line_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "raw_id": "qlik-export-total-line",
                    "title": "Export table to Excel not exporting the total line",
                    "body": "The board report export to Excel changes the total line and wrong values are exported for ad hoc reporting.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_qlik_export_error_row_reaches_borderline(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "raw_id": "qlik-hypercube-export",
                    "title": "Error in Qlik sense excel export like The hypercube results are too large",
                    "body": "When we export to Excel from a Qlik Sense table the hypercube results are too large and the report export fails for analysts.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_qlik_pivot_reconciliation_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "raw_id": "qlik-reconcile",
                    "title": "Pivot tables do not reconcile in board report",
                    "body": "We compare the data in two pivot tables for a board report and the summary and detail do not reconcile, even though the numbers look correct in Excel.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_mixpanel_reporting_trust_post_survives_prefilter(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-trust",
                    "title": "Dashboard says one thing export says another",
                    "body": "Our funnel insights and CSV export are not matching, and we need a source of truth before sharing the report.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_mixpanel_reporting_mismatch_can_rescue_borderline_row(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "mixpanel_community",
                    "raw_id": "mixpanel-borderline",
                    "title": "Which report should I trust",
                    "body": "The dashboard says one thing, the CSV export says another, and we are trying to figure out what changed in funnel reporting.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_qlik_generic_chart_help_without_reporting_pain_is_dropped(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "qlik_community",
                    "raw_id": "qlik-generic-chart",
                    "title": "Straight table expression and sort order help",
                    "body": "How do I hide a column, change chart label formatting, and control sort order in a straight table dimension expression?",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)

    def test_shopify_checkout_tracking_validation_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "shopify_community",
                    "raw_id": "shopify-checkout-validation",
                    "title": "Shop Pay checkout changed and GA4 no longer matches orders",
                    "body": "Before sending weekly reporting we validate payouts and checkout conversions because GA4 and Google Ads no longer match Shopify orders after Shop Pay changed the checkout flow.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_github_ratio_semantics_row_is_rescued(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "github_discussions",
                    "raw_id": "github-ratio-semantics",
                    "title": "Share of total metric denominator is wrong in dashboard reporting",
                    "body": "Our ratio metric and denominator logic are producing wrong totals, and the team cannot explain which dashboard number is the source of truth before sharing reporting.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"repository\": \"dbt-labs/metricflow\"}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_sisense_infra_post_is_dropped(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "sisense_community",
                    "raw_id": "sisense-noise",
                    "title": "JWT token not working after upgrade",
                    "body": "Our Kubernetes deployment needs Auth0 and SSO troubleshooting after install.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)

    def test_sisense_dashboard_trust_post_survives_prefilter(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "sisense_community",
                    "raw_id": "sisense-trust",
                    "title": "Widget value does not match table",
                    "body": "The dashboard export and widget totals are not matching the source data, and we are replacing SSRS with live detail reporting.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_stackoverflow_bi_reconciliation_post_survives_prefilter(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-bi-trust",
                    "title": "Power BI wrong total after date filter",
                    "body": "Our dashboard total is not matching the SQL Server query result, and we need a source of truth before sending the report to stakeholders.",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"raw_question\": {\"tags\": [\"powerbi\", \"sql-server\"], \"is_answered\": true}}"},
                }
            ]
        )
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 0)
        self.assertEqual(len(keep_df) + len(borderline_df), 1)

    def test_stackoverflow_generic_export_help_is_dropped(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "source": "stackoverflow",
                    "raw_id": "so-generic-export",
                    "title": "Strapi export csv with React frontend",
                    "body": "How do I export CSV from a React app with a Strapi API and JavaScript table component?",
                    "comments_text": "",
                    "raw_text": "",
                    "source_meta": {"json": "{\"raw_question\": {\"tags\": [\"reactjs\", \"javascript\", \"strapi\"], \"is_answered\": false}}"},
                }
            ]
        )
        _, _, drop_df = apply_relevance_prefilter(frame, self.rules)
        self.assertEqual(len(drop_df), 1)

    def test_prefilter_is_idempotent_for_scored_rows(self) -> None:
        frame = pd.DataFrame([_load_fixture("stackoverflow_relevant_reporting_mismatch.json")])
        keep_df, borderline_df, drop_df = apply_relevance_prefilter(frame, self.rules)
        scored_df = pd.concat([keep_df, borderline_df, drop_df], ignore_index=True)
        keep_again, borderline_again, drop_again = apply_relevance_prefilter(scored_df, self.rules)
        rescored_df = pd.concat([keep_again, borderline_again, drop_again], ignore_index=True)
        self.assertEqual(len(rescored_df), len(scored_df))
        self.assertFalse(rescored_df.columns.duplicated().any())


if __name__ == "__main__":
    unittest.main()
