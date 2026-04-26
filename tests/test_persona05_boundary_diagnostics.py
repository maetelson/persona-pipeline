"""Regression tests for persona_05 boundary diagnostics."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.persona05_boundary_diagnostics import build_persona05_boundary_outputs


ROOT_DIR = Path(__file__).resolve().parents[1]


class Persona05BoundaryDiagnosticTests(unittest.TestCase):
    """Validate persona_05 boundary diagnostics without changing persona semantics."""

    def test_clean_last_mile_reporting_row_is_flagged_clean(self) -> None:
        outputs = build_persona05_boundary_outputs(
            persona_assignments_df=pd.DataFrame([{"episode_id": "e1", "persona_id": "persona_05"}]),
            episodes_df=pd.DataFrame(
                [
                    {
                        "episode_id": "e1",
                        "source": "metabase_discussions",
                        "normalized_episode": (
                            "Dashboard filter is in the wrong position and removes prior year values from the trend table. "
                            "We need a shareable dashboard view for end users."
                        ),
                        "business_question": "",
                        "bottleneck_text": "tool_limitation",
                        "desired_output": "dashboard_update",
                    }
                ]
            ),
            persona_summary_df=pd.DataFrame([{"persona_id": "persona_05"}]),
            cluster_stats_df=pd.DataFrame([{"persona_id": "persona_05"}]),
        )
        row = outputs["diagnostic_df"].iloc[0]
        self.assertEqual(str(row["persona05_boundary_status"]), "clean_persona05")
        self.assertTrue(bool(row["persona05_reporting_delivery_context"]))
        self.assertTrue(bool(row["persona05_output_construction_blocker"]))

    def test_export_spreadsheet_only_row_is_not_clean_persona05(self) -> None:
        outputs = build_persona05_boundary_outputs(
            persona_assignments_df=pd.DataFrame([{"episode_id": "e2", "persona_id": "persona_05"}]),
            episodes_df=pd.DataFrame(
                [
                    {
                        "episode_id": "e2",
                        "source": "power_bi_community",
                        "normalized_episode": (
                            "Every month I export the report to Excel and manually copy values into a spreadsheet for the board deck."
                        ),
                        "business_question": "",
                        "bottleneck_text": "general_friction",
                        "desired_output": "unspecified_output",
                    }
                ]
            ),
            persona_summary_df=pd.DataFrame([{"persona_id": "persona_05"}]),
            cluster_stats_df=pd.DataFrame([{"persona_id": "persona_05"}]),
        )
        row = outputs["diagnostic_df"].iloc[0]
        self.assertIn(str(row["persona05_boundary_status"]), {"persona01_overlap", "weak_generic"})
        self.assertFalse(bool(row["persona05_output_construction_blocker"]))

    def test_generic_tool_limitation_without_delivery_context_is_not_clean(self) -> None:
        outputs = build_persona05_boundary_outputs(
            persona_assignments_df=pd.DataFrame([{"episode_id": "e3", "persona_id": "persona_05"}]),
            episodes_df=pd.DataFrame(
                [
                    {
                        "episode_id": "e3",
                        "source": "github_discussions",
                        "normalized_episode": (
                            "Need default filters in explores so large datasets make sense. This feature should be available by default."
                        ),
                        "business_question": "",
                        "bottleneck_text": "tool_limitation",
                        "desired_output": "dashboard_update",
                    }
                ]
            ),
            persona_summary_df=pd.DataFrame([{"persona_id": "persona_05"}]),
            cluster_stats_df=pd.DataFrame([{"persona_id": "persona_05"}]),
        )
        row = outputs["diagnostic_df"].iloc[0]
        self.assertIn(str(row["persona05_boundary_status"]), {"persona03_overlap", "weak_generic"})
        self.assertFalse(bool(row["persona05_reporting_delivery_context"]))

    def test_support_setup_row_is_flagged_noise(self) -> None:
        outputs = build_persona05_boundary_outputs(
            persona_assignments_df=pd.DataFrame([{"episode_id": "e4", "persona_id": "persona_05"}]),
            episodes_df=pd.DataFrame(
                [
                    {
                        "episode_id": "e4",
                        "source": "power_bi_community",
                        "normalized_episode": (
                            "Trying to use a service principal to refresh credentials through the gateway automatically."
                        ),
                        "business_question": "",
                        "bottleneck_text": "tool_limitation",
                        "desired_output": "unspecified_output",
                    }
                ]
            ),
            persona_summary_df=pd.DataFrame([{"persona_id": "persona_05"}]),
            cluster_stats_df=pd.DataFrame([{"persona_id": "persona_05"}]),
        )
        row = outputs["diagnostic_df"].iloc[0]
        self.assertEqual(str(row["persona05_boundary_status"]), "support_troubleshooting_noise")

    def test_positive_persona05_requires_both_delivery_context_and_output_blocker(self) -> None:
        outputs = build_persona05_boundary_outputs(
            persona_assignments_df=pd.DataFrame(
                [
                    {"episode_id": "delivery_only", "persona_id": "persona_05"},
                    {"episode_id": "blocker_only", "persona_id": "persona_05"},
                ]
            ),
            episodes_df=pd.DataFrame(
                [
                    {
                        "episode_id": "delivery_only",
                        "source": "hubspot_community",
                        "normalized_episode": "Need a stakeholder-facing monthly report for end users.",
                        "business_question": "",
                        "bottleneck_text": "general_friction",
                        "desired_output": "dashboard_update",
                    },
                    {
                        "episode_id": "blocker_only",
                        "source": "metabase_discussions",
                        "normalized_episode": "Filter configuration is not possible in this chart.",
                        "business_question": "",
                        "bottleneck_text": "tool_limitation",
                        "desired_output": "dashboard_update",
                    },
                ]
            ),
            persona_summary_df=pd.DataFrame([{"persona_id": "persona_05"}]),
            cluster_stats_df=pd.DataFrame([{"persona_id": "persona_05"}]),
        )
        statuses = outputs["diagnostic_df"].set_index("episode_id")["persona05_boundary_status"].to_dict()
        self.assertNotEqual(statuses["delivery_only"], "clean_persona05")
        self.assertNotEqual(statuses["blocker_only"], "clean_persona05")

    def test_live_outputs_include_boundary_summary_without_changing_status(self) -> None:
        persona_summary_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "persona_summary.csv")
        cluster_stats_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "cluster_stats.csv")
        overview_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        metrics = dict(zip(overview_df["metric"].astype(str), overview_df["value"]))

        for frame in [persona_summary_df, cluster_stats_df]:
            for column in [
                "persona05_clean_evidence_count",
                "persona05_overlap_risk_count",
                "persona05_support_noise_count",
                "persona05_boundary_readiness",
                "persona05_boundary_rule_status",
            ]:
                self.assertIn(column, frame.columns)

        persona05 = cluster_stats_df.set_index("persona_id").loc["persona_05"]
        self.assertFalse(bool(persona05["deck_ready_claim_eligible_persona"]))
        self.assertEqual(str(persona05["readiness_tier"]), "blocked_or_constrained_candidate")
        self.assertEqual(str(metrics["persona_readiness_state"]), "reviewable_but_not_deck_ready")
        self.assertEqual(int(float(metrics["final_usable_persona_count"])), 3)
        self.assertEqual(int(float(metrics["production_ready_persona_count"])), 3)
        self.assertEqual(int(float(metrics["review_ready_persona_count"])), 1)
        self.assertEqual(int(float(metrics["deck_ready_claim_eligible_persona_count"])), 4)


if __name__ == "__main__":
    unittest.main()
