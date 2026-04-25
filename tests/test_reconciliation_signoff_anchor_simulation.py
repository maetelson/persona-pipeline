"""Tests for reconciliation/signoff anchor simulation helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.reconciliation_signoff_anchor_simulation import (
    anchor_variant_eligibility,
    evaluate_anchor_subset,
)


class ReconciliationSignoffAnchorSimulationTests(unittest.TestCase):
    """Verify anchor-level simulation metrics and eligibility rules."""

    def test_evaluate_anchor_subset_computes_expected_rates(self) -> None:
        anchor_df = pd.DataFrame(
            {
                "anchor_label": [
                    "anchor_positive_reconciliation_signoff",
                    "anchor_positive_reconciliation_signoff",
                    "anchor_hard_negative",
                    "anchor_parent_reporting_packager",
                    "non_anchor_ambiguous",
                ],
                "variant_persona_id": ["persona_04", "persona_01", "persona_04", "persona_01", "persona_04"],
            }
        )
        metrics = evaluate_anchor_subset(anchor_df, "variant_persona_id", target_id="persona_04")
        self.assertEqual(metrics["positive_anchor_capture_rate"], 50.0)
        self.assertEqual(metrics["hard_negative_anchor_false_positive_rate"], 100.0)
        self.assertEqual(metrics["parent_anchor_retention_rate"], 100.0)
        self.assertEqual(metrics["ambiguous_non_anchor_movement_rate"], 100.0)

    def test_anchor_variant_eligibility_requires_meaningful_capture_and_safe_top3(self) -> None:
        identity_gate = {"eligible_for_future_implementation": True}
        eligible = anchor_variant_eligibility(
            identity_gate=identity_gate,
            positive_anchor_capture_rate=84.0,
            baseline_positive_anchor_capture_rate=70.0,
            hard_negative_anchor_false_positive_rate=10.0,
            parent_anchor_retention_rate=100.0,
            ambiguous_non_anchor_movement_rate=15.0,
            baseline_ambiguous_non_anchor_movement_rate=20.0,
            top_3_share_pct=79.8,
            persona_05_drift_risk=False,
        )
        self.assertTrue(eligible["eligible_for_future_implementation"])

        ineligible = anchor_variant_eligibility(
            identity_gate=identity_gate,
            positive_anchor_capture_rate=74.0,
            baseline_positive_anchor_capture_rate=70.0,
            hard_negative_anchor_false_positive_rate=10.0,
            parent_anchor_retention_rate=100.0,
            ambiguous_non_anchor_movement_rate=15.0,
            baseline_ambiguous_non_anchor_movement_rate=20.0,
            top_3_share_pct=83.1,
            persona_05_drift_risk=False,
        )
        self.assertFalse(ineligible["eligible_for_future_implementation"])
        self.assertIn("positive_anchor_capture_not_meaningfully_improved", ineligible["fail_reasons"])
        self.assertIn("top_3_share_not_materially_improved", ineligible["fail_reasons"])


if __name__ == "__main__":
    unittest.main()
