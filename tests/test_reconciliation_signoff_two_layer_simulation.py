"""Tests for the bounded two-layer reconciliation/signoff simulation helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.reconciliation_signoff_two_layer_simulation import (
    evaluate_label_subset,
    two_layer_variant_decision,
)


class ReconciliationSignoffTwoLayerSimulationTests(unittest.TestCase):
    """Validate bounded two-layer gate calculations."""

    def test_evaluate_label_subset_computes_expected_rates(self) -> None:
        df = pd.DataFrame(
            {
                "expansion_label": [
                    "expansion_positive_should_join_persona_04",
                    "expansion_positive_should_join_persona_04",
                    "expansion_hard_negative_block",
                    "expansion_parent_should_stay_persona_01",
                    "expansion_ambiguous_do_not_anchor",
                ],
                "variant_persona_id": ["persona_04", "persona_01", "persona_04", "persona_01", "persona_04"],
            }
        )
        metrics = evaluate_label_subset(
            df,
            label_column="expansion_label",
            positive_label="expansion_positive_should_join_persona_04",
            hard_negative_label="expansion_hard_negative_block",
            parent_label="expansion_parent_should_stay_persona_01",
            ambiguous_label="expansion_ambiguous_do_not_anchor",
            persona_column="variant_persona_id",
            target_persona_id="persona_04",
        )
        self.assertEqual(metrics["positive_capture_rate"], 50.0)
        self.assertEqual(metrics["hard_negative_false_positive_rate"], 100.0)
        self.assertEqual(metrics["parent_retention_rate"], 100.0)
        self.assertEqual(metrics["ambiguous_movement_rate"], 100.0)

    def test_two_layer_variant_decision_requires_expansion_gain_and_top3(self) -> None:
        decision = two_layer_variant_decision(
            identity_overlap=0.95,
            selected_example_overlap_pct=100.0,
            identity_positive_capture_rate=100.0,
            identity_hard_negative_fp_rate=10.0,
            identity_parent_retention_rate=100.0,
            expansion_positive_capture_rate=40.0,
            baseline_expansion_positive_capture_rate=0.0,
            expansion_hard_negative_fp_rate=12.0,
            expansion_parent_retention_rate=100.0,
            expansion_ambiguous_movement_rate=20.0,
            persona_01_leakage_pct=2.0,
            persona_05_drift_risk=False,
            top_3_share_pct=79.9,
        )
        self.assertTrue(decision["future_production_patch_candidate"])

        failed = two_layer_variant_decision(
            identity_overlap=0.95,
            selected_example_overlap_pct=100.0,
            identity_positive_capture_rate=100.0,
            identity_hard_negative_fp_rate=10.0,
            identity_parent_retention_rate=100.0,
            expansion_positive_capture_rate=10.0,
            baseline_expansion_positive_capture_rate=0.0,
            expansion_hard_negative_fp_rate=12.0,
            expansion_parent_retention_rate=100.0,
            expansion_ambiguous_movement_rate=20.0,
            persona_01_leakage_pct=2.0,
            persona_05_drift_risk=False,
            top_3_share_pct=83.0,
        )
        self.assertFalse(failed["future_production_patch_candidate"])
        self.assertIn("expansion_anchor_gate", failed["fail_reasons"])
        self.assertIn("top_3_share_not_materially_improved", failed["fail_reasons"])


if __name__ == "__main__":
    unittest.main()
