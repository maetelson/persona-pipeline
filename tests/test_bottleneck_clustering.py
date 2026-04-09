"""Tests for bottleneck-first clustering and naming."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.bottleneck_clustering import (
    assign_bottleneck_clusters,
    build_bottleneck_feature_table,
    build_cluster_naming_recommendations,
    compare_cluster_versions,
)
from src.utils.io import load_yaml


ROOT = Path(__file__).resolve().parents[1]


class BottleneckClusteringTests(unittest.TestCase):
    """Verify bottleneck-first clustering stays centered on workflow pain."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_yaml(ROOT / "config" / "bottleneck_clustering.yaml")

    def test_feature_scoring_ignores_role_identity(self) -> None:
        episodes_df, labeled_df, axis_wide_df = _sample_inputs()
        feature_df = build_bottleneck_feature_table(episodes_df, labeled_df, axis_wide_df, self.config)
        row_a = feature_df[feature_df["episode_id"] == "ep_manual_analyst"].iloc[0]
        row_b = feature_df[feature_df["episode_id"] == "ep_manual_manager"].iloc[0]
        self.assertEqual(row_a["primary_bottleneck"], "manual_reporting")
        self.assertEqual(row_b["primary_bottleneck"], "manual_reporting")
        self.assertAlmostEqual(float(row_a["manual_reporting"]), float(row_b["manual_reporting"]), places=4)
        self.assertNotEqual(row_a["role_metadata"], row_b["role_metadata"])

    def test_cluster_assignment_groups_by_bottleneck_not_role(self) -> None:
        episodes_df, labeled_df, axis_wide_df = _sample_inputs()
        feature_df = build_bottleneck_feature_table(episodes_df, labeled_df, axis_wide_df, self.config)
        assignments_df = assign_bottleneck_clusters(feature_df, self.config)
        manual_clusters = assignments_df[assignments_df["episode_id"].isin(["ep_manual_analyst", "ep_manual_manager"])]["persona_id"].unique().tolist()
        root_cluster = assignments_df[assignments_df["episode_id"] == "ep_rootcause"]["persona_id"].iloc[0]
        self.assertEqual(len(manual_clusters), 1)
        self.assertNotEqual(manual_clusters[0], root_cluster)

    def test_naming_recommendations_are_problem_focused(self) -> None:
        cluster_audit_df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_01",
                    "cluster_name": "placeholder",
                    "dominant_bottleneck_signals": "manual_reporting | recurring_export_work | spreadsheet_rework",
                }
            ]
        )
        naming_df = build_cluster_naming_recommendations(cluster_audit_df, self.config)
        self.assertIn("Manual Reporting Burden", naming_df.iloc[0]["recommended_cluster_name"])

    def test_compare_cluster_versions_exports_expected_metrics(self) -> None:
        episodes_df, labeled_df, axis_wide_df = _sample_inputs()
        feature_df = build_bottleneck_feature_table(episodes_df, labeled_df, axis_wide_df, self.config)
        assignments_df = assign_bottleneck_clusters(feature_df, self.config)
        merged_df = (
            episodes_df.merge(labeled_df, on="episode_id", how="inner")
            .merge(axis_wide_df, on="episode_id", how="left")
            .merge(feature_df, on="episode_id", how="left")
            .merge(assignments_df, on="episode_id", how="left")
        )
        final_axis_schema = [
            {"axis_name": "workflow_stage", "axis_role": "core"},
            {"axis_name": "analysis_goal", "axis_role": "core"},
            {"axis_name": "bottleneck_type", "axis_role": "core"},
            {"axis_name": "tool_dependency_mode", "axis_role": "core"},
            {"axis_name": "user_role", "axis_role": "optional"},
        ]
        cluster_audit_df = pd.DataFrame(
            [
                {
                    "persona_id": persona_id,
                    "cluster_name": "Manual Reporting Burden" if persona_id == assignments_df.iloc[0]["persona_id"] else "Root-Cause Explanation Burden",
                    "bottleneck_coherence": 0.7,
                }
                for persona_id in assignments_df["persona_id"].unique().tolist()
            ]
        )
        outputs = compare_cluster_versions(axis_wide_df, merged_df, assignments_df, final_axis_schema, cluster_audit_df, self.config)
        self.assertIn("cluster_comparison_before_after_df", outputs)
        self.assertIn("role_feature_importance_before_after_df", outputs)
        self.assertFalse(outputs["cluster_comparison_before_after_df"].empty)


def _sample_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return a compact synthetic corpus for clustering tests."""
    episodes_df = pd.DataFrame(
        [
            {
                "episode_id": "ep_manual_analyst",
                "source": "reddit",
                "normalized_episode": "I export this to Excel every week because the dashboard is not enough for the monthly report.",
                "evidence_snippet": "",
                "role_clue": "analyst",
                "work_moment": "weekly reporting",
                "business_question": "How can I speed up manual reporting for leadership?",
                "tool_env": "Power BI and Excel",
                "bottleneck_text": "manual reporting in spreadsheets",
                "workaround_text": "export to excel every week",
                "desired_output": "presentation-ready output for leadership",
                "segmentation_note": "",
            },
            {
                "episode_id": "ep_manual_manager",
                "source": "reddit",
                "normalized_episode": "I export this to Excel every week because the dashboard is not enough for the monthly report.",
                "evidence_snippet": "",
                "role_clue": "manager",
                "work_moment": "weekly reporting",
                "business_question": "How can I speed up manual reporting for leadership?",
                "tool_env": "Power BI and Excel",
                "bottleneck_text": "manual reporting in spreadsheets",
                "workaround_text": "export to excel every week",
                "desired_output": "presentation-ready output for leadership",
                "segmentation_note": "",
            },
            {
                "episode_id": "ep_rootcause",
                "source": "stackoverflow",
                "normalized_episode": "Leadership wants a breakdown by channel and I can see conversion dropped but cannot explain why.",
                "evidence_snippet": "",
                "role_clue": "analyst",
                "work_moment": "triage",
                "business_question": "Why did conversion drop by segment?",
                "tool_env": "dashboard",
                "bottleneck_text": "numbers are visible but not explainable",
                "workaround_text": "",
                "desired_output": "root cause explanation",
                "segmentation_note": "break down by channel and device",
            },
        ]
    )
    labeled_df = pd.DataFrame(
        [
            {
                "episode_id": "ep_manual_analyst",
                "role_codes": "R_ANALYST",
                "moment_codes": "M_REPORTING",
                "question_codes": "Q_REPORT_SPEED",
                "pain_codes": "P_MANUAL_REPORTING",
                "env_codes": "E_SPREADSHEET|E_SQL_BI",
                "workaround_codes": "W_SPREADSHEET",
                "output_codes": "O_XLSX",
            },
            {
                "episode_id": "ep_manual_manager",
                "role_codes": "R_MANAGER",
                "moment_codes": "M_REPORTING",
                "question_codes": "Q_REPORT_SPEED",
                "pain_codes": "P_MANUAL_REPORTING",
                "env_codes": "E_SPREADSHEET|E_SQL_BI",
                "workaround_codes": "W_SPREADSHEET",
                "output_codes": "O_XLSX",
            },
            {
                "episode_id": "ep_rootcause",
                "role_codes": "R_ANALYST",
                "moment_codes": "M_TRIAGE",
                "question_codes": "Q_DIAGNOSE_ISSUE",
                "pain_codes": "P_TOOL_LIMITATION",
                "env_codes": "E_SQL_BI",
                "workaround_codes": "",
                "output_codes": "O_DASHBOARD",
            },
        ]
    )
    axis_wide_df = pd.DataFrame(
        [
            {
                "episode_id": "ep_manual_analyst",
                "user_role": "analyst",
                "workflow_stage": "reporting",
                "analysis_goal": "report_speed",
                "bottleneck_type": "manual_reporting",
                "tool_dependency_mode": "spreadsheet_heavy",
                "trust_validation_need": "medium",
                "output_expectation": "excel_ready_output",
            },
            {
                "episode_id": "ep_manual_manager",
                "user_role": "manager",
                "workflow_stage": "reporting",
                "analysis_goal": "report_speed",
                "bottleneck_type": "manual_reporting",
                "tool_dependency_mode": "spreadsheet_heavy",
                "trust_validation_need": "medium",
                "output_expectation": "excel_ready_output",
            },
            {
                "episode_id": "ep_rootcause",
                "user_role": "analyst",
                "workflow_stage": "triage",
                "analysis_goal": "diagnose_change",
                "bottleneck_type": "tool_limitation",
                "tool_dependency_mode": "bi_dashboard_heavy",
                "trust_validation_need": "medium",
                "output_expectation": "dashboard_update",
            },
        ]
    )
    return episodes_df, labeled_df, axis_wide_df


if __name__ == "__main__":
    unittest.main()
