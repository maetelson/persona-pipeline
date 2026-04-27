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

    def test_google_other_operational_remains_visible_as_debt(self) -> None:
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
        self.assertEqual(str(row["source_slice_name"]), "other_operational")
        self.assertEqual(str(row["source_slice_category"]), "debt_producing_slice")
        self.assertEqual(str(row["source_slice_quarantine_status"]), "diagnostics_only_not_quarantined")

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
