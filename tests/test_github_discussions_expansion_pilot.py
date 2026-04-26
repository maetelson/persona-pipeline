"""Tests for the bounded GitHub Discussions expansion pilot."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from src.analysis.github_discussions_expansion_pilot import (
    GithubDiscussionPilotRow,
    build_pilot_row,
    build_summary,
    estimate_labelable,
    estimate_persona_core_candidate,
    estimate_valid_candidate,
    extract_discussion_urls,
    extract_listing_urls,
    fit_is_meaningful,
    is_pilot_output_path,
    load_pilot_seed_config,
    parse_discussion_detail_html,
    score_persona_fit,
    score_signals,
)


LISTING_HTML = """
<html>
  <body>
    <a href="/apache/superset/discussions/39652">One</a>
    <a href="/apache/superset/discussions/39652">Duplicate</a>
    <a href="/apache/superset/discussions/39651">Two</a>
    <a href="/apache/superset/discussions?page=2">Next</a>
  </body>
</html>
"""

DETAIL_HTML = """
<html>
  <head>
    <title>Dashboard numbers wrong · apache superset · Discussions · GitHub</title>
  </head>
  <body>
    <div>
      <span>TCP404</span>
      started this conversation in
      <a href="/apache/superset/discussions/categories/ideas">Ideas</a>
    </div>
    <a href="#issue-123" class="Link--secondary js-timestamp"><relative-time datetime="2026-04-20T00:22:53Z">Apr 20, 2026</relative-time></a>
    <a href="#issuecomment-456" class="Link--secondary js-timestamp"><relative-time datetime="2026-04-23T00:22:53Z">Apr 23, 2026</relative-time></a>
    <a href="/apache/superset/labels/reporting">reporting</a>
    <table>
      <tbody class="d-block js-translation-source">
        <tr class="d-block">
          <td class="d-block color-fg-default comment-body markdown-body js-comment-body">
            <p>I need better drill down by segment because dashboard numbers are wrong and stakeholders cannot trust the report.</p>
            <p>We export to Excel every week to reconcile totals.</p>
          </td>
        </tr>
        <tr class="d-block">
          <td class="d-block color-fg-default comment-body markdown-body js-comment-body">
            <p>Same issue here. We still rely on spreadsheets for exec reviews.</p>
          </td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


class GithubDiscussionsExpansionPilotTests(unittest.TestCase):
    """Verify pilot config, parsing, scoring, and output isolation."""

    def test_pilot_config_loads_and_is_pilot_only(self) -> None:
        root = Path(__file__).resolve().parents[1]
        config = load_pilot_seed_config(root)
        self.assertEqual(config["bundle_name"], "github_discussions_expansion_bi_tools")
        self.assertTrue(config["pilot_only"])
        self.assertIn("apache/superset", config["target_repos"])

    def test_narrow_pilot_config_excludes_noisy_repos(self) -> None:
        root = Path(__file__).resolve().parents[1]
        config = load_pilot_seed_config(
            root,
            Path("config") / "seeds" / "github_discussions" / "github_discussions_narrow_pilot.yaml",
        )
        self.assertEqual(config["bundle_name"], "github_discussions_narrow_bi_tools")
        self.assertNotIn("apache/superset", config["target_repos"])
        self.assertIn("apache/superset", config["excluded_repos"])
        self.assertIn("plausible/analytics", config["excluded_repos"])

    def test_lightdash_single_repo_config_loads(self) -> None:
        root = Path(__file__).resolve().parents[1]
        config = load_pilot_seed_config(
            root,
            Path("config") / "seeds" / "github_discussions" / "github_discussions_lightdash_pilot.yaml",
        )
        self.assertEqual(config["bundle_name"], "github_discussions_lightdash_single_repo_pilot")
        self.assertEqual(config["target_repos"], ["lightdash/lightdash"])
        self.assertTrue(config["pilot_only"])

    def test_extract_discussion_urls_dedupes(self) -> None:
        urls = extract_discussion_urls(LISTING_HTML, "apache/superset")
        self.assertEqual(
            urls,
            [
                "https://github.com/apache/superset/discussions/39652",
                "https://github.com/apache/superset/discussions/39651",
            ],
        )

    def test_extract_listing_urls_keeps_pagination(self) -> None:
        urls = extract_listing_urls(LISTING_HTML, "apache/superset")
        self.assertIn("https://github.com/apache/superset/discussions?page=2", urls)

    def test_parse_detail_html_extracts_core_fields(self) -> None:
        parsed = parse_discussion_detail_html(
            "https://github.com/apache/superset/discussions/39652",
            "apache/superset",
            DETAIL_HTML,
            comment_cap=10,
        )
        self.assertEqual(parsed.raw_id, "39652")
        self.assertEqual(parsed.category, "Ideas")
        self.assertEqual(parsed.author, "TCP404")
        self.assertEqual(parsed.comment_count, 1)
        self.assertIn("reporting", parsed.labels)

    def test_scoring_detects_reporting_and_noise(self) -> None:
        scores = score_signals(
            "Dashboard numbers wrong",
            "We export to Excel and reconcile totals for stakeholder reviews.",
            "Ideas",
            ["reporting"],
            "Same issue here.",
        )
        self.assertEqual(scores["dashboard_reporting_pain"], 1)
        self.assertEqual(scores["export_or_spreadsheet_workaround"], 1)
        self.assertEqual(scores["data_trust_or_reconciliation_issue"], 1)
        noise_scores = score_signals(
            "Deployment issue",
            "Docker install and OAuth permission setup are broken in CI/CD.",
            "Q&A",
            [],
            "",
        )
        self.assertEqual(noise_scores["install_deploy_debug_noise"], 1)
        self.assertEqual(noise_scores["auth_permission_setup_noise"], 1)
        self.assertEqual(noise_scores["ci_cd_infrastructure_issue"], 1)

    def test_persona_fit_scoring_works(self) -> None:
        fits = score_persona_fit(
            score_signals(
                "Dashboard numbers wrong",
                "We export to Excel and reconcile totals for stakeholder reviews.",
                "Ideas",
                ["reporting"],
                "Same issue here.",
            ),
            "Dashboard numbers wrong",
            "We export to Excel and reconcile totals for stakeholder reviews.",
            "Same issue here.",
        )
        self.assertTrue(fit_is_meaningful(fits["persona_01_fit"]))
        self.assertTrue(fit_is_meaningful(fits["persona_04_fit"]))

    def test_build_pilot_row_estimates_validity(self) -> None:
        parsed = parse_discussion_detail_html(
            "https://github.com/apache/superset/discussions/39652",
            "apache/superset",
            DETAIL_HTML,
            comment_cap=10,
        )
        row = build_pilot_row(parsed, "apache/superset", "https://github.com/apache/superset/discussions/39652", "public_html", "2026-04-26T00:00:00+00:00")
        self.assertTrue(estimate_valid_candidate(row))
        self.assertTrue(estimate_persona_core_candidate(row))
        self.assertTrue(estimate_labelable(row))
        self.assertEqual(row.fetch_status, "ok")

    def test_output_path_guard_stays_outside_production(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            allowed = root / "artifacts" / "source_pilots" / "pilot.json"
            blocked = root / "data" / "raw" / "pilot.json"
            self.assertTrue(is_pilot_output_path(allowed, root))
            self.assertFalse(is_pilot_output_path(blocked, root))

    def test_summary_can_pass_for_high_quality_rows(self) -> None:
        row = GithubDiscussionPilotRow(
            source="github_discussions_expansion_bi_tools",
            repo="apache/superset",
            raw_id="39652",
            url="https://github.com/apache/superset/discussions/39652",
            title="Dashboard numbers wrong",
            body_or_excerpt="We export to Excel and reconcile totals for stakeholder reviews.",
            category="Ideas",
            labels="reporting",
            author="TCP404",
            created_at="2026-04-20T00:22:53Z",
            updated_at="2026-04-23T00:22:53Z",
            comment_count=1,
            comment_excerpt="Same issue here.",
            fetch_status="ok",
            fetch_method="public_html",
            fetched_at="2026-04-26T00:00:00+00:00",
            dashboard_reporting_pain=1,
            metric_definition_confusion=1,
            semantic_layer_or_model_confusion_tied_to_reporting=0,
            stakeholder_reporting_need=1,
            export_or_spreadsheet_workaround=1,
            data_trust_or_reconciliation_issue=1,
            workflow_limitation_in_bi_reporting_tool=1,
            root_cause_or_explanation_handoff=0,
            install_deploy_debug_noise=0,
            auth_permission_setup_noise=0,
            generic_feature_request_without_user_pain=0,
            api_library_coding_issue=0,
            ci_cd_infrastructure_issue=0,
            beginner_tutorial_help=0,
            release_announcement=0,
            maintainer_internal_discussion_without_user_pain=0,
            persona_01_fit="strong",
            persona_02_fit="medium",
            persona_03_fit="strong",
            persona_04_fit="strong",
            persona_05_fit="medium",
        )
        summary = build_summary(
            rows=[row] * 320,
            total_discovered_discussions=360,
            total_fetched_discussions=320,
            total_fetched_comments=160,
            unavailable_discussion_count=5,
            request_error_count=0,
        )
        self.assertEqual(summary["onboarding_gate_result"], "pass")
        self.assertIn(
            summary["decision"],
            {
                "promote_to_active_github_discussions_bundle",
                "expand_repo_sample",
                "promote_lightdash_to_active_source_candidate",
            },
        )
        self.assertIn("fetched_comments", summary["per_repo_summary"]["apache/superset"])
        self.assertIn("expected_source_tier", summary["per_repo_summary"]["apache/superset"])


if __name__ == "__main__":
    unittest.main()
