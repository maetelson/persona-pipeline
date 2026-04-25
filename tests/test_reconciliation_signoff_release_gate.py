"""Regression tests for reconciliation/signoff release-gate target selection."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.reconciliation_signoff_identity import (
    build_promotion_drift_flags,
    select_reconciliation_like_persona,
)


class ReconciliationSignoffReleaseGateTests(unittest.TestCase):
    """Verify release-gate target selection uses semantic evidence instead of fixed ids."""

    def test_select_target_prefers_semantic_validation_cluster(self) -> None:
        dev_df = pd.DataFrame(
            {
                "curated_label": [
                    "reconciliation_signoff_positive",
                    "reconciliation_signoff_positive",
                    "reconciliation_signoff_positive",
                    "reporting_packager_parent",
                    "hard_negative",
                    "ambiguous_boundary",
                ],
                "persona_id_current": [
                    "persona_03",
                    "persona_03",
                    "persona_04",
                    "persona_01",
                    "persona_04",
                    "persona_04",
                ],
            }
        )
        profile_df = pd.DataFrame(
            {
                "persona_id": ["persona_03", "persona_04", "persona_01"],
                "validation_share_pct": [78.0, 22.0, 5.0],
                "trust_medium_or_high_share_pct": [70.0, 15.0, 0.0],
                "manual_reporting_share_pct": [10.0, 55.0, 80.0],
                "report_speed_share_pct": [12.0, 60.0, 85.0],
                "promotion_status": ["exploratory_bucket", "exploratory_bucket", "promoted_persona"],
                "dominant_signature": ["data_quality", "manual_reporting", "manual_reporting"],
            }
        )

        selection = select_reconciliation_like_persona(dev_df, profile_df)

        self.assertEqual(selection["selected_persona_id"], "persona_03")
        self.assertIn("selected by semantic evidence", selection["selection_reason"])
        self.assertGreater(selection["candidate_scores"][0]["selection_score"], selection["candidate_scores"][1]["selection_score"])

    def test_select_target_penalizes_hard_negative_overpull(self) -> None:
        dev_df = pd.DataFrame(
            {
                "curated_label": [
                    "reconciliation_signoff_positive",
                    "reconciliation_signoff_positive",
                    "reconciliation_signoff_positive",
                    "hard_negative",
                    "hard_negative",
                    "ambiguous_boundary",
                    "reporting_packager_parent",
                ],
                "persona_id_current": [
                    "persona_04",
                    "persona_04",
                    "persona_05",
                    "persona_05",
                    "persona_05",
                    "persona_05",
                    "persona_01",
                ],
            }
        )
        profile_df = pd.DataFrame(
            {
                "persona_id": ["persona_04", "persona_05", "persona_01"],
                "validation_share_pct": [68.0, 75.0, 3.0],
                "trust_medium_or_high_share_pct": [60.0, 65.0, 0.0],
                "manual_reporting_share_pct": [15.0, 18.0, 82.0],
                "report_speed_share_pct": [20.0, 24.0, 87.0],
                "promotion_status": ["exploratory_bucket", "promoted_persona", "promoted_persona"],
                "dominant_signature": ["data_quality", "tool_limitation", "manual_reporting"],
            }
        )

        selection = select_reconciliation_like_persona(dev_df, profile_df)

        self.assertEqual(selection["selected_persona_id"], "persona_04")
        score_lookup = {row["persona_id"]: row for row in selection["candidate_scores"]}
        self.assertGreater(score_lookup["persona_04"]["selection_score"], score_lookup["persona_05"]["selection_score"])

    def test_promotion_drift_flags_when_persona_05_promotes_for_side_effect(self) -> None:
        flags = build_promotion_drift_flags(
            {"persona_04": "exploratory_bucket", "persona_05": "promoted_persona"},
            selected_persona_id="persona_03",
        )

        self.assertTrue(flags["persona_04_still_exploratory"])
        self.assertTrue(flags["persona_05_promotion_drift"])
        self.assertTrue(flags["target_is_not_persona_04"])


if __name__ == "__main__":
    unittest.main()
