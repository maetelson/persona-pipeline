"""Diagnostics-only tests for Phase 1 source-slice classification fields."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.source_slice_classification import build_source_slice_classification_outputs


ROOT_DIR = Path(__file__).resolve().parents[1]


class SourceSliceClassificationTests(unittest.TestCase):
    """Validate diagnostics-only source-slice policy fields and invariants."""

    def _build_outputs(
        self,
        *,
        labeled_rows: list[dict[str, object]],
        episode_rows: list[dict[str, object]],
        denominator_rows: list[dict[str, object]],
        source_tier_rows: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        labeled_df = pd.DataFrame(labeled_rows)
        episodes_df = pd.DataFrame(episode_rows)
        denominator_df = pd.DataFrame(denominator_rows)
        source_balance_audit_df = pd.DataFrame(
            source_tier_rows
            or [
                {"source": "google_developer_forums", "source_tier": "supporting_validation_source"},
                {"source": "adobe_analytics_community", "source_tier": "supporting_validation_source"},
                {"source": "domo_community_forum", "source_tier": "exploratory_edge_source"},
                {"source": "klaviyo_community", "source_tier": "excluded_from_deck_ready_core"},
                {"source": "reddit", "source_tier": "supporting_validation_source"},
                {"source": "stackoverflow", "source_tier": "supporting_validation_source"},
            ]
        )
        overview_df = pd.DataFrame(
            [
                {"metric": "effective_balanced_source_count", "value": 5.89},
                {"metric": "weak_source_cost_center_count", "value": 4},
                {"metric": "persona_readiness_state", "value": "reviewable_but_not_deck_ready"},
                {"metric": "final_usable_persona_count", "value": 3},
            ]
        )
        quality_checks_df = pd.DataFrame(
            [
                {"metric": "core_readiness_weak_source_cost_center_count", "value": 3},
            ]
        )
        return build_source_slice_classification_outputs(
            labeled_df=labeled_df,
            episodes_df=episodes_df,
            denominator_rows_df=denominator_df,
            source_balance_audit_df=source_balance_audit_df,
            overview_df=overview_df,
            quality_checks_df=quality_checks_df,
        )

    def test_google_evidence_producing_slices_are_detected(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {"episode_id": "ep-share", "persona_core_eligible": False},
                {"episode_id": "ep-filters", "persona_core_eligible": False},
            ],
            episode_rows=[
                {
                    "episode_id": "ep-share",
                    "source": "google_developer_forums",
                    "normalized_episode": "Can't share report because permission flow blocks stakeholder delivery in new tab.",
                    "url": "https://discuss.google.dev/share-report",
                },
                {
                    "episode_id": "ep-filters",
                    "source": "google_developer_forums",
                    "normalized_episode": "Blend data filter logic breaks dashboard report totals.",
                    "url": "https://discuss.google.dev/filter-logic",
                },
            ],
            denominator_rows=[
                {"episode_id": "ep-share", "denominator_eligibility_category": "denominator_eligible_business_non_core", "deck_ready_denominator_eligible": True, "technical_noise_confidence": 0.2},
                {"episode_id": "ep-filters", "denominator_eligibility_category": "denominator_eligible_business_non_core", "deck_ready_denominator_eligible": True, "technical_noise_confidence": 0.2},
            ],
        )
        rows = outputs["rows_df"].set_index("episode_id")
        self.assertEqual(str(rows.loc["ep-share", "source_slice_name"]), "sharing_permissions_delivery")
        self.assertEqual(str(rows.loc["ep-share", "source_slice_category"]), "evidence_producing_slice")
        self.assertEqual(str(rows.loc["ep-filters", "source_slice_name"]), "report_logic_and_filters")
        self.assertEqual(str(rows.loc["ep-filters", "source_slice_category"]), "evidence_producing_slice")

    def test_google_support_tail_remains_visible_as_debt(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-google-debt", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-google-debt",
                    "source": "google_developer_forums",
                    "normalized_episode": "OAuth token setup keeps failing during service account configuration.",
                    "url": "https://discuss.google.dev/oauth-setup",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-google-debt",
                    "denominator_eligibility_category": "setup_auth_permission_noise",
                    "deck_ready_denominator_eligible": False,
                    "technical_noise_confidence": 0.96,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "report_delivery_ui")
        self.assertEqual(str(row["source_slice_category"]), "mixed_evidence_slice")
        self.assertEqual(str(row["source_slice_quarantine_status"]), "active_diagnostic")
        self.assertEqual(str(row["refined_source_slice_name"]), "google_auth_query_formula_support")
        self.assertEqual(str(row["refined_source_slice_category"]), "debt_producing_slice")

    def test_adobe_metric_reconciliation_is_evidence_producing(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-adobe", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-adobe",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "Average time on site numbers do not reconcile across workspace reports.",
                    "url": "https://experienceleaguecommunities.adobe.com/reconcile",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-adobe",
                    "denominator_eligibility_category": "denominator_eligible_business_non_core",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.15,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "metric_reconciliation")
        self.assertEqual(str(row["source_slice_category"]), "evidence_producing_slice")
        self.assertEqual(str(row["refined_source_slice_name"]), "metric_reconciliation")

    def test_adobe_mixed_slices_are_not_over_promoted(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-adobe-mixed", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-adobe-mixed",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "Workspace is slow and reporting panels need troubleshooting.",
                    "url": "https://experienceleaguecommunities.adobe.com/workspace-slow",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-adobe-mixed",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.55,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "workspace_reporting")
        self.assertEqual(str(row["source_slice_category"]), "mixed_evidence_slice")
        self.assertFalse(bool(row["source_slice_deck_ready_balance_eligible"]))
        self.assertTrue(bool(row["source_slice_weak_debt_eligible"]))
        self.assertIn(
            str(row["refined_source_slice_name"]),
            {"adobe_workspace_business_reporting", "adobe_workspace_ambiguous"},
        )

    def test_domo_debt_producing_slices_do_not_disappear(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-domo", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-domo",
                    "source": "domo_community_forum",
                    "normalized_episode": "Beast mode formula and Magic ETL setup keep failing.",
                    "url": "https://community-forums.domo.com/beast-mode",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-domo",
                    "denominator_eligibility_category": "source_specific_support_noise",
                    "deck_ready_denominator_eligible": False,
                    "technical_noise_confidence": 0.97,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_category"]), "debt_producing_slice")
        self.assertTrue(bool(row["source_slice_weak_debt_eligible"]))
        self.assertIn("Debt-producing", str(row["source_slice_quarantine_reason"]))

    def test_klaviyo_remains_excluded_from_core_even_with_good_slices(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-klaviyo", "persona_core_eligible": True}],
            episode_rows=[
                {
                    "episode_id": "ep-klaviyo",
                    "source": "klaviyo_community",
                    "normalized_episode": "Revenue reporting does not match dashboard exports.",
                    "url": "https://community.klaviyo.com/revenue",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-klaviyo",
                    "denominator_eligibility_category": "persona_core_evidence",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.1,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_tier"]), "excluded_from_deck_ready_core")
        self.assertEqual(str(row["source_slice_category"]), "evidence_producing_slice")
        self.assertFalse(bool(row["source_slice_deck_ready_balance_eligible"]))
        self.assertIn("source-tier policy still excludes", str(row["source_slice_quarantine_reason"]))

    def test_non_weak_denom_eligible_business_rows_become_evidence_producing(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-reddit", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-reddit",
                    "source": "reddit",
                    "normalized_episode": "Weekly reporting still needs manual export cleanup for leadership.",
                    "url": "https://reddit.com/r/reporting",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-reddit",
                    "denominator_eligibility_category": "denominator_eligible_business_non_core",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.2,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_category"]), "evidence_producing_slice")
        self.assertTrue(bool(row["source_slice_deck_ready_balance_eligible"]))

    def test_high_confidence_technical_noise_becomes_debt_producing(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-tech", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-tech",
                    "source": "stackoverflow",
                    "normalized_episode": "DAX syntax error in calculated column.",
                    "url": "https://stackoverflow.com/q/123",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-tech",
                    "denominator_eligibility_category": "syntax_formula_debug_noise",
                    "deck_ready_denominator_eligible": False,
                    "technical_noise_confidence": 0.95,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_category"]), "debt_producing_slice")
        self.assertFalse(bool(row["source_slice_deck_ready_balance_eligible"]))

    def test_ambiguous_rows_become_mixed_evidence_slice(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-amb", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-amb",
                    "source": "reddit",
                    "normalized_episode": "Dashboard export mismatch with some technical debugging context.",
                    "url": "https://reddit.com/r/amb",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-amb",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.52,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_category"]), "mixed_evidence_slice")

    def test_google_report_mismatch_routes_to_refined_delivery_slice(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-google-business", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-google-business",
                    "source": "google_developer_forums",
                    "normalized_episode": "Looker Studio dashboard does not match Google Ads totals and the scheduled stakeholder report is wrong.",
                    "url": "https://discuss.google.dev/report-mismatch",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-google-business",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.35,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "report_delivery_ui")
        self.assertEqual(str(row["refined_source_slice_name"]), "google_delivery_mismatch_missing_data")
        self.assertIn(str(row["refined_source_slice_category"]), {"evidence_producing_slice", "mixed_evidence_slice"})

    def test_google_oauth_setup_routes_to_refined_support_slice(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-google-tech", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-google-tech",
                    "source": "google_developer_forums",
                    "normalized_episode": "Looker Studio OAuth permission and property setup issue blocks connector access.",
                    "url": "https://discuss.google.dev/oauth-property",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-google-tech",
                    "denominator_eligibility_category": "setup_auth_permission_noise",
                    "deck_ready_denominator_eligible": False,
                    "technical_noise_confidence": 0.96,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "report_delivery_ui")
        self.assertEqual(str(row["refined_source_slice_name"]), "google_auth_query_formula_support")
        self.assertEqual(str(row["refined_source_slice_category"]), "debt_producing_slice")

    def test_google_mixed_reporting_and_setup_stays_mixed(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-google-mixed", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-google-mixed",
                    "source": "google_developer_forums",
                    "normalized_episode": "Scheduled report delivery mismatch appears after connector setup and query parameter changes.",
                    "url": "https://discuss.google.dev/report-setup-mixed",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-google-mixed",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.55,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["refined_source_slice_name"]), "google_report_delivery_mixed_uncertain")
        self.assertEqual(str(row["refined_source_slice_category"]), "mixed_evidence_slice")

    def test_adobe_workspace_business_reporting_routes_correctly(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-adobe-workspace", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-adobe-workspace",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "Workspace report delivery is slow and metric comparison is wrong for stakeholder reporting.",
                    "url": "https://experienceleague.adobe.com/workspace-report",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-adobe-workspace",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.3,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["refined_source_slice_name"]), "adobe_workspace_business_reporting")
        self.assertIn(str(row["refined_source_slice_category"]), {"evidence_producing_slice", "mixed_evidence_slice"})

    def test_adobe_tracking_setup_routes_to_noise(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-adobe-track", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-adobe-track",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "eVar tracking implementation and tag manager server call setup are failing.",
                    "url": "https://experienceleague.adobe.com/tracking-setup",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-adobe-track",
                    "denominator_eligibility_category": "source_specific_support_noise",
                    "deck_ready_denominator_eligible": False,
                    "technical_noise_confidence": 0.94,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "implementation_tracking")
        self.assertEqual(str(row["refined_source_slice_name"]), "adobe_tracking_setup_noise")
        self.assertEqual(str(row["refined_source_slice_category"]), "debt_producing_slice")

    def test_adobe_api_admin_support_routes_to_noise(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-adobe-api", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-adobe-api",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "API admin configuration and virtual report suite setup are blocking access.",
                    "url": "https://experienceleague.adobe.com/api-admin",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-adobe-api",
                    "denominator_eligibility_category": "server_deploy_config_noise",
                    "deck_ready_denominator_eligible": False,
                    "technical_noise_confidence": 0.92,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["source_slice_name"]), "api_admin_config")
        self.assertEqual(str(row["refined_source_slice_name"]), "adobe_api_admin_support_noise")
        self.assertEqual(str(row["refined_source_slice_category"]), "debt_producing_slice")

    def test_adobe_ambiguous_rows_remain_mixed(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-adobe-amb", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-adobe-amb",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "Workspace report suite issue mixes stakeholder dashboard use with processing rule troubleshooting.",
                    "url": "https://experienceleague.adobe.com/workspace-amb",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-adobe-amb",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.5,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertIn(
            str(row["refined_source_slice_name"]),
            {"adobe_workspace_ambiguous", "adobe_api_admin_ambiguous", "adobe_operational_ambiguous", "adobe_tracking_ambiguous"},
        )
        self.assertEqual(str(row["refined_source_slice_category"]), "mixed_evidence_slice")

    def test_official_fields_remain_present_with_refined_output(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[{"episode_id": "ep-fields", "persona_core_eligible": False}],
            episode_rows=[
                {
                    "episode_id": "ep-fields",
                    "source": "google_developer_forums",
                    "normalized_episode": "Dashboard report mismatch for stakeholders.",
                    "url": "https://discuss.google.dev/fields",
                }
            ],
            denominator_rows=[
                {
                    "episode_id": "ep-fields",
                    "denominator_eligibility_category": "ambiguous_review_bucket",
                    "deck_ready_denominator_eligible": True,
                    "technical_noise_confidence": 0.4,
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        for column in [
            "source_slice_id",
            "source_slice_name",
            "source_slice_category",
            "refined_source_slice_id",
            "refined_source_slice_name",
            "refined_source_slice_category",
            "refined_source_slice_parent",
            "refined_source_slice_refinement_status",
        ]:
            self.assertIn(column, outputs["rows_df"].columns)
            self.assertNotEqual(str(row[column]), "")

    def test_official_metrics_and_persona_counts_do_not_change(self) -> None:
        overview_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        quality_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "quality_checks.csv")
        overview = dict(zip(overview_df["metric"].astype(str), overview_df["value"]))
        quality = dict(zip(quality_df["metric"].astype(str), quality_df["value"]))
        self.assertEqual(float(overview["effective_balanced_source_count"]), 5.89)
        self.assertEqual(int(float(overview["weak_source_cost_center_count"])), 4)
        self.assertEqual(int(float(quality["core_readiness_weak_source_cost_center_count"])), 3)
        self.assertEqual(int(float(overview["final_usable_persona_count"])), 3)
        self.assertEqual(str(overview["persona_readiness_state"]), "reviewable_but_not_deck_ready")


if __name__ == "__main__":
    unittest.main()
