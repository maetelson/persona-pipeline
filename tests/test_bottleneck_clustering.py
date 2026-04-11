"""Tests for bottleneck-first clustering and naming."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.bottleneck_clustering import (
    assign_bottleneck_clusters,
    build_bottleneck_feature_table,
    build_cluster_naming_recommendations,
    build_cluster_robustness_outputs,
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

    def test_sparse_signature_without_similar_anchor_keeps_separate_persona(self) -> None:
        config = dict(self.config)
        config["clustering"] = dict(self.config.get("clustering", {}))
        config["clustering"]["min_anchor_size"] = 2
        config["clustering"]["min_anchor_share"] = 0.2
        config["clustering"]["merge_similarity_threshold"] = 0.95
        feature_columns = list(config.get("core_features", []))
        rows = []
        for episode_id, signature, primary, values in [
            ("a1", "primary=manual_reporting||secondary=recurring_export_work", "manual_reporting", {"manual_reporting": 2.1, "recurring_export_work": 1.4}),
            ("a2", "primary=manual_reporting||secondary=recurring_export_work", "manual_reporting", {"manual_reporting": 2.0, "recurring_export_work": 1.3}),
            ("b1", "primary=metric_reconciliation||secondary=dashboard_mistrust", "metric_reconciliation", {"metric_reconciliation": 2.2, "dashboard_mistrust": 1.4}),
            ("b2", "primary=metric_reconciliation||secondary=dashboard_mistrust", "metric_reconciliation", {"metric_reconciliation": 2.0, "dashboard_mistrust": 1.2}),
            ("c1", "primary=root_cause_analysis_difficulty||secondary=segmentation_breakdown_confusion", "root_cause_analysis_difficulty", {"root_cause_analysis_difficulty": 2.3, "segmentation_breakdown_confusion": 1.5}),
        ]:
            row = {
                "episode_id": episode_id,
                "cluster_signature": signature,
                "primary_bottleneck": primary,
                "secondary_bottlenecks": "",
                "primary_score": 2.0,
                "role_metadata": "analyst",
                "source_metadata": "reddit",
            }
            for feature in feature_columns:
                row[feature] = float(values.get(feature, 0.0))
            rows.append(row)
        feature_df = pd.DataFrame(rows)
        assignments_df = assign_bottleneck_clusters(feature_df, config)
        self.assertEqual(assignments_df["persona_id"].nunique(), 3)
        sparse_persona = assignments_df.loc[assignments_df["episode_id"] == "c1", "persona_id"].iloc[0]
        self.assertTrue(isinstance(sparse_persona, str) and sparse_persona.startswith("persona_"))

    def test_fragile_same_primary_cluster_absorbs_into_stable_parent(self) -> None:
        config = dict(self.config)
        config["clustering"] = dict(self.config.get("clustering", {}))
        config["robustness"] = dict(self.config.get("robustness", {}))
        config["clustering"]["min_anchor_size"] = 2
        config["clustering"]["min_anchor_share"] = 0.0
        config["clustering"]["merge_similarity_threshold"] = 0.95
        config["clustering"]["merge_signature_floor"] = 0.95
        config["robustness"]["min_stable_cluster_size"] = 3
        config["robustness"]["min_stable_cluster_share"] = 0.5
        config["robustness"]["same_primary_absorb_similarity"] = 0.75
        feature_columns = list(config.get("core_features", []))
        rows = []
        for episode_id, signature, values in [
            ("a1", "primary=manual_reporting||secondary=recurring_export_work", {"manual_reporting": 2.1, "recurring_export_work": 1.5, "spreadsheet_rework": 1.0}),
            ("a2", "primary=manual_reporting||secondary=recurring_export_work", {"manual_reporting": 2.0, "recurring_export_work": 1.4, "spreadsheet_rework": 1.0}),
            ("a3", "primary=manual_reporting||secondary=recurring_export_work", {"manual_reporting": 2.1, "recurring_export_work": 1.3, "spreadsheet_rework": 1.1}),
            ("a4", "primary=manual_reporting||secondary=recurring_export_work", {"manual_reporting": 2.0, "recurring_export_work": 1.4, "spreadsheet_rework": 1.0}),
            ("b1", "primary=manual_reporting||secondary=spreadsheet_rework", {"manual_reporting": 2.0, "recurring_export_work": 1.2, "spreadsheet_rework": 1.4}),
            ("b2", "primary=manual_reporting||secondary=spreadsheet_rework", {"manual_reporting": 2.0, "recurring_export_work": 1.1, "spreadsheet_rework": 1.3}),
        ]:
            row = {
                "episode_id": episode_id,
                "cluster_signature": signature,
                "primary_bottleneck": "manual_reporting",
                "secondary_bottlenecks": "",
                "primary_score": 2.0,
                "role_metadata": "analyst",
                "source_metadata": "reddit",
            }
            for feature in feature_columns:
                row[feature] = float(values.get(feature, 0.0))
            rows.append(row)
        assignments_df = assign_bottleneck_clusters(pd.DataFrame(rows), config)
        self.assertEqual(assignments_df["persona_id"].nunique(), 1)
        self.assertIn("merged_to_parent", assignments_df["robustness_action"].tolist())

    def test_tiny_unmatched_cluster_collapses_to_residual_family(self) -> None:
        config = dict(self.config)
        config["clustering"] = dict(self.config.get("clustering", {}))
        config["robustness"] = dict(self.config.get("robustness", {}))
        config["clustering"]["min_anchor_size"] = 1
        config["clustering"]["min_anchor_share"] = 0.0
        config["clustering"]["merge_similarity_threshold"] = 0.99
        config["clustering"]["merge_signature_floor"] = 0.99
        config["robustness"]["min_stable_cluster_size"] = 3
        config["robustness"]["min_stable_cluster_share"] = 0.5
        config["robustness"]["micro_cluster_size"] = 1
        config["robustness"]["same_primary_absorb_similarity"] = 0.99
        config["robustness"]["same_name_absorb_similarity"] = 0.99
        config["robustness"]["general_absorb_similarity"] = 0.99
        feature_columns = list(config.get("core_features", []))
        rows = []
        for episode_id, signature, primary, values in [
            ("a1", "primary=manual_reporting||secondary=recurring_export_work", "manual_reporting", {"manual_reporting": 2.0, "recurring_export_work": 1.5}),
            ("a2", "primary=manual_reporting||secondary=recurring_export_work", "manual_reporting", {"manual_reporting": 2.0, "recurring_export_work": 1.4}),
            ("a3", "primary=manual_reporting||secondary=recurring_export_work", "manual_reporting", {"manual_reporting": 2.1, "recurring_export_work": 1.5}),
            ("a4", "primary=manual_reporting||secondary=recurring_export_work", "manual_reporting", {"manual_reporting": 2.0, "recurring_export_work": 1.4}),
            ("b1", "primary=metric_reconciliation||secondary=dashboard_mistrust", "metric_reconciliation", {"metric_reconciliation": 2.1, "dashboard_mistrust": 1.5}),
            ("b2", "primary=metric_reconciliation||secondary=dashboard_mistrust", "metric_reconciliation", {"metric_reconciliation": 2.0, "dashboard_mistrust": 1.4}),
            ("b3", "primary=metric_reconciliation||secondary=dashboard_mistrust", "metric_reconciliation", {"metric_reconciliation": 2.2, "dashboard_mistrust": 1.5}),
            ("b4", "primary=metric_reconciliation||secondary=dashboard_mistrust", "metric_reconciliation", {"metric_reconciliation": 2.1, "dashboard_mistrust": 1.4}),
            ("c1", "primary=root_cause_analysis_difficulty||secondary=segmentation_breakdown_confusion", "root_cause_analysis_difficulty", {"root_cause_analysis_difficulty": 2.5, "segmentation_breakdown_confusion": 1.6}),
        ]:
            row = {
                "episode_id": episode_id,
                "cluster_signature": signature,
                "primary_bottleneck": primary,
                "secondary_bottlenecks": "",
                "primary_score": 2.0,
                "role_metadata": "analyst",
                "source_metadata": "reddit",
            }
            for feature in feature_columns:
                row[feature] = float(values.get(feature, 0.0))
            rows.append(row)
        assignments_df = assign_bottleneck_clusters(pd.DataFrame(rows), config)
        residual_row = assignments_df[assignments_df["episode_id"] == "c1"].iloc[0]
        self.assertEqual(residual_row["robustness_action"], "collapsed_to_residual")
        self.assertIn("Exploratory", residual_row["cluster_name"])

    def test_cluster_robustness_outputs_summarize_stability_metrics(self) -> None:
        cluster_audit_df = pd.DataFrame(
            [
                {"persona_id": "persona_01", "cluster_name": "Manual Reporting Burden", "cluster_size": 20, "cohesion": 0.95, "separation": 0.18},
                {"persona_id": "persona_02", "cluster_name": "Exploratory Residual", "cluster_size": 4, "cohesion": 0.88, "separation": 0.05},
            ]
        )
        persona_assignments_df = pd.DataFrame(
            [
                {"persona_id": "persona_01", "initial_anchor_signature": "sig_a", "robustness_action": "kept_stable"},
                {"persona_id": "persona_01", "initial_anchor_signature": "sig_b", "robustness_action": "merged_to_parent"},
                {"persona_id": "persona_02", "initial_anchor_signature": "sig_c", "robustness_action": "collapsed_to_residual"},
            ]
        )
        outputs = build_cluster_robustness_outputs(cluster_audit_df, persona_assignments_df, self.config)
        summary_lookup = dict(zip(outputs["summary_df"]["metric"], outputs["summary_df"]["value"]))
        self.assertEqual(summary_lookup["robust_cluster_count"], 2)
        self.assertEqual(summary_lookup["thin_evidence_cluster_count"], 1)


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
