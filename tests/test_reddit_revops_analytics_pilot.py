"""Tests for the bounded Reddit RevOps / analytics pilot."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from src.analysis.reddit_revops_analytics_pilot import (
    RedditPilotRow,
    build_pilot_row,
    build_summary,
    estimate_labelable,
    estimate_persona_core_candidate,
    estimate_valid_candidate,
    fit_is_meaningful,
    is_pilot_output_path,
    load_pilot_seed_config,
    score_persona_fit,
    score_signals,
)


class RedditRevopsAnalyticsPilotTests(unittest.TestCase):
    """Verify pilot seed loading, scoring, and output isolation."""

    def test_seed_config_loads(self) -> None:
        root = Path(__file__).resolve().parents[1]
        config = load_pilot_seed_config(root)
        self.assertEqual(config["bundle_name"], "reddit_revops_analytics_bundle")
        self.assertTrue(config["pilot_only"])
        self.assertIn("revops", config["target_subreddits"])

    def test_reporting_and_reconciliation_signals_are_detected(self) -> None:
        scores = score_signals(
            "Dashboard numbers don't match CRM",
            "We still export to spreadsheets every week to reconcile funnel reporting for leadership.",
            "",
            "revops",
        )
        self.assertEqual(scores["reporting_pain_signal"], 1)
        self.assertEqual(scores["dashboard_or_metric_signal"], 1)
        self.assertEqual(scores["CRM_or_salesops_reporting_signal"], 1)
        self.assertEqual(scores["validation_or_reconciliation_signal"], 1)
        self.assertEqual(scores["manual_spreadsheet_work_signal"], 1)

    def test_career_and_job_noise_is_detected(self) -> None:
        scores = score_signals(
            "Analytics career advice",
            "How do I improve my resume and negotiate salary for a new job?",
            "",
            "dataanalysis",
        )
        self.assertEqual(scores["career_training_noise"], 1)
        self.assertEqual(scores["job_salary_resume_noise"], 1)

    def test_persona_fit_scoring_identifies_persona_01_and_04(self) -> None:
        fits = score_persona_fit(
            score_signals(
                "Weekly sales dashboard mismatch",
                "We export from Salesforce to Excel every Monday because leadership does not trust the dashboard totals.",
                "",
                "salesops",
            ),
            "Weekly sales dashboard mismatch",
            "We export from Salesforce to Excel every Monday because leadership does not trust the dashboard totals.",
            "",
        )
        self.assertTrue(fit_is_meaningful(fits["persona_01_fit"]))
        self.assertTrue(fit_is_meaningful(fits["persona_04_fit"]))

    def test_build_pilot_row_estimates_validity_and_labelability(self) -> None:
        post = {
            "id": "abc123",
            "title": "Dashboard numbers do not match CRM export",
            "selftext": "Every weekly report requires spreadsheet reconciliation for stakeholder reviews.",
            "created_utc": 1714089600,
            "score": 12,
            "num_comments": 4,
            "permalink": "/r/revops/comments/abc123/dashboard_numbers_do_not_match_crm_export/",
        }
        row = build_pilot_row(
            post=post,
            subreddit="revops",
            comment_excerpt="Same issue here. We reconcile Salesforce totals in Excel before every exec review.",
            fetched_at="2026-04-26T00:00:00+00:00",
            fetch_method="reddit_public_json",
        )
        self.assertTrue(estimate_valid_candidate(row))
        self.assertTrue(estimate_persona_core_candidate(row))
        self.assertTrue(estimate_labelable(row))

    def test_output_path_guard_keeps_writes_under_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            allowed = root / "artifacts" / "source_pilots" / "pilot.json"
            blocked = root / "data" / "raw" / "pilot.json"
            self.assertTrue(is_pilot_output_path(allowed, root))
            self.assertFalse(is_pilot_output_path(blocked, root))

    def test_summary_reports_pass_for_high_quality_rows(self) -> None:
        row = RedditPilotRow(
            source="reddit_revops_analytics_bundle",
            subreddit="revops",
            raw_id="abc123",
            url="https://www.reddit.com/r/revops/comments/abc123/example/",
            title="Dashboard numbers do not match CRM export",
            body_or_excerpt="Every weekly report requires spreadsheet reconciliation for stakeholder reviews.",
            comment_excerpt="We do the same before leadership meetings.",
            created_at="2026-01-01T00:00:00+00:00",
            score=10,
            num_comments=4,
            permalink="/r/revops/comments/abc123/example/",
            fetch_method="reddit_public_json",
            fetched_at="2026-04-26T00:00:00+00:00",
            reporting_pain_signal=1,
            dashboard_or_metric_signal=1,
            stakeholder_reporting_signal=1,
            attribution_or_funnel_signal=0,
            CRM_or_salesops_reporting_signal=1,
            manual_spreadsheet_work_signal=1,
            validation_or_reconciliation_signal=1,
            recurring_report_delivery_signal=1,
            career_training_noise=0,
            job_salary_resume_noise=0,
            generic_chatter_noise=0,
            pure_coding_debug_noise=0,
            self_promotion_noise=0,
            vendor_marketing_noise=0,
            homework_noise=0,
            tool_recommendation_noise=0,
            persona_01_fit="strong",
            persona_02_fit="medium",
            persona_03_fit="weak",
            persona_04_fit="strong",
            persona_05_fit="weak",
        )
        summary = build_summary(
            rows=[row] * 320,
            total_fetched_posts=1000,
            total_fetched_comments=120,
            subreddit_post_counts={"revops": 250, "salesops": 250, "dataanalysis": 250, "analyticsengineering": 250},
            exhausted_subreddits=[],
            request_error_count=0,
            fetched_posts_target=1000,
        )
        self.assertEqual(summary["onboarding_gate_result"], "pass")
        self.assertIn(summary["decision"], {"promote_to_active_source_bundle", "expand_pilot_sample"})


if __name__ == "__main__":
    unittest.main()
