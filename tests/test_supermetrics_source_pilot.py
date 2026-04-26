"""Tests for the bounded Supermetrics Community source pilot."""

from __future__ import annotations

import unittest

from src.analysis.supermetrics_source_pilot import (
    BASE_URL,
    DetailParseResult,
    PilotRow,
    build_pilot_row,
    build_summary,
    estimate_labelable,
    estimate_persona_core_candidate,
    estimate_valid_candidate,
    extract_listing_urls,
    extract_thread_urls,
    normalize_url,
    parse_thread_detail_html,
    score_signals,
)


LISTING_HTML = """
<html>
  <body>
    <a href="/ask-the-community-43/reporting-data-blend-101">Thread A</a>
    <a href="/ask-the-community-43/reporting-data-blend-101">Thread A duplicate</a>
    <a href="/ask-the-community-43/dashboard-mismatch-102">Thread B</a>
    <a href="/ask-the-community-43?page=2">Next page</a>
  </body>
</html>
"""

DETAIL_HTML = """
<html>
  <head>
    <title>Dashboard mismatch and blended reporting issue | Supermetrics Community</title>
    <script type="application/ld+json">
      {"keywords":"Looker Studio, Blending, Reporting"}
    </script>
  </head>
  <body>
    <div>Ask the Community</div>
    <h1>Dashboard mismatch and blended reporting issue</h1>
    <div>September 17, 2025</div>
    <div>3 replies</div>
    <div>Best answer by Colin</div>
    <div>EdnaAltenwerth Newbie</div>
    <p>We need trustworthy reporting, attribution reconciliation, and client-ready dashboards.</p>
    <p>Current setup is manual and we still export to spreadsheets for QA.</p>
  </body>
</html>
"""


class SupermetricsSourcePilotTests(unittest.TestCase):
    """Verify HTML parsing and pilot scoring utilities."""

    def test_extract_thread_urls_dedupes_listing_links(self) -> None:
        urls = extract_thread_urls(LISTING_HTML, BASE_URL)
        self.assertEqual(
            urls,
            [
                "https://community.supermetrics.com/ask-the-community-43/reporting-data-blend-101",
                "https://community.supermetrics.com/ask-the-community-43/dashboard-mismatch-102",
            ],
        )

    def test_extract_listing_urls_keeps_pagination(self) -> None:
        urls = extract_listing_urls(LISTING_HTML, BASE_URL)
        self.assertIn("https://community.supermetrics.com/ask-the-community-43?page=2", urls)

    def test_parse_thread_detail_html_extracts_core_fields(self) -> None:
        parsed = parse_thread_detail_html(
            "https://community.supermetrics.com/ask-the-community-43/dashboard-mismatch-and-blended-reporting-102",
            DETAIL_HTML,
        )
        self.assertEqual(parsed.raw_id, "dashboard-mismatch-and-blended-reporting-102")
        self.assertEqual(parsed.category, "Ask the Community")
        self.assertEqual(parsed.author, "EdnaAltenwerth")
        self.assertEqual(parsed.reply_count, 3)
        self.assertTrue(parsed.accepted_solution)
        self.assertIn("Looker Studio", parsed.tags)

    def test_signal_scoring_detects_reporting_and_noise(self) -> None:
        scores = score_signals(
            "Dashboard mismatch and blended reporting issue",
            "We still export to spreadsheets and reconcile attribution for client dashboards.",
            "Ask the Community",
            ["Looker Studio", "Reporting"],
        )
        self.assertEqual(scores["reporting_pain_signal"], 1)
        self.assertEqual(scores["attribution_or_blended_data_signal"], 1)
        self.assertEqual(scores["export_or_spreadsheet_signal"], 1)
        self.assertEqual(scores["validation_or_reconciliation_signal"], 1)

    def test_build_pilot_row_estimates_persona_fit(self) -> None:
        parsed = DetailParseResult(
            raw_id="dashboard-mismatch-102",
            title="Dashboard mismatch and blended reporting issue",
            body_or_excerpt="We still export to spreadsheets and reconcile attribution for client dashboards.",
            category="Ask the Community",
            tags=["Looker Studio", "Reporting"],
            author="Edna",
            created_at="September 17, 2025",
            reply_count=3,
            accepted_solution=True,
        )
        row = build_pilot_row(parsed, "https://community.supermetrics.com/thread", "ok", "2026-04-26T00:00:00+00:00")
        self.assertGreaterEqual(row.persona_01_fit, 1)
        self.assertGreaterEqual(row.persona_04_fit, 1)
        self.assertTrue(estimate_valid_candidate(row))
        self.assertTrue(estimate_persona_core_candidate(row))
        self.assertTrue(estimate_labelable(row))

    def test_build_summary_counts_403_and_gate_fail(self) -> None:
        ok_row = PilotRow(
            source="supermetrics_community",
            raw_id="thread-1",
            url="https://community.supermetrics.com/thread-1",
            title="Reporting mismatch thread",
            body_or_excerpt="Dashboard mismatch and spreadsheets for client reporting.",
            category="Ask the Community",
            tags="Reporting",
            author="A",
            created_at="2025-09-17",
            reply_count=1,
            accepted_solution=False,
            fetch_status="ok",
            fetch_method="public_html",
            fetched_at="2026-04-26T00:00:00+00:00",
            reporting_pain_signal=1,
            dashboard_or_metric_signal=1,
            attribution_or_blended_data_signal=0,
            export_or_spreadsheet_signal=1,
            validation_or_reconciliation_signal=1,
            stakeholder_or_delivery_context=1,
            setup_support_noise=0,
            connector_setup_noise=0,
            api_developer_noise=0,
            training_certification_noise=0,
            hiring_career_noise=0,
            vendor_announcement_noise=0,
            persona_01_fit=2,
            persona_02_fit=0,
            persona_03_fit=1,
            persona_04_fit=2,
            persona_05_fit=0,
        )
        blocked_row = PilotRow(
            source="supermetrics_community",
            raw_id="thread-2",
            url="https://community.supermetrics.com/thread-2",
            title="",
            body_or_excerpt="",
            category="",
            tags="",
            author="",
            created_at="",
            reply_count=None,
            accepted_solution=None,
            fetch_status="http_error:403",
            fetch_method="public_html",
            fetched_at="2026-04-26T00:00:00+00:00",
            reporting_pain_signal=0,
            dashboard_or_metric_signal=0,
            attribution_or_blended_data_signal=0,
            export_or_spreadsheet_signal=0,
            validation_or_reconciliation_signal=0,
            stakeholder_or_delivery_context=0,
            setup_support_noise=0,
            connector_setup_noise=0,
            api_developer_noise=0,
            training_certification_noise=0,
            hiring_career_noise=0,
            vendor_announcement_noise=0,
            persona_01_fit=0,
            persona_02_fit=0,
            persona_03_fit=0,
            persona_04_fit=0,
            persona_05_fit=0,
        )
        summary = build_summary(
            rows=[ok_row, blocked_row],
            listing_pages_attempted=2,
            listing_pages_succeeded=1,
            listing_forbidden_403_count=0,
            listing_not_found_404_count=1,
            listing_timeout_error_count=0,
            thread_urls_discovered=2,
            detail_attempts=2,
            forbidden_403_count=1,
            not_found_404_count=0,
            timeout_error_count=0,
            duplicate_url_count=0,
            stable_pagination_seen=False,
        )
        self.assertEqual(summary["forbidden_403_count"], 1)
        self.assertEqual(summary["onboarding_gate_result"], "fail")
        self.assertIn(summary["recommendation"], {"expand_pilot_with_different_category", "pause_source_expansion"})

    def test_normalize_url_preserves_public_path(self) -> None:
        normalized = normalize_url("https://community.supermetrics.com/ask-the-community-43/reporting-data-blend-101/")
        self.assertEqual(normalized, "https://community.supermetrics.com/ask-the-community-43/reporting-data-blend-101")


if __name__ == "__main__":
    unittest.main()
