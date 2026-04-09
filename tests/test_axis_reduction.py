"""Tests for axis audit, recommendation, and reduction helpers."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.axis_reduction import apply_axis_reduction, build_axis_quality_audit, recommend_axis_reduction


class AxisReductionTests(unittest.TestCase):
    """Verify axis reduction metrics and config-driven actions."""

    def setUp(self) -> None:
        self.episodes_df = pd.DataFrame(
            [
                {
                    "episode_id": "e1",
                    "source": "reddit",
                    "raw_id": "r1",
                    "url": "https://example.com/1",
                    "normalized_episode": "Manual Excel reporting for weekly leadership report and stakeholders do not trust the dashboard.",
                    "evidence_snippet": "",
                    "role_clue": "business analyst",
                    "work_moment": "reporting",
                    "business_question": "Need to explain KPI changes for leadership",
                    "tool_env": "",
                    "bottleneck_text": "manual reporting and reconcile numbers",
                    "workaround_text": "manual workaround",
                    "desired_output": "xlsx",
                    "product_fit": "review",
                    "segmentation_note": "",
                },
                {
                    "episode_id": "e2",
                    "source": "reddit",
                    "raw_id": "r2",
                    "url": "https://example.com/2",
                    "normalized_episode": "Power BI export for board report is slow and stakeholders want a segment breakdown.",
                    "evidence_snippet": "",
                    "role_clue": "finance manager",
                    "work_moment": "reporting",
                    "business_question": "Need report faster",
                    "tool_env": "power bi",
                    "bottleneck_text": "tool limitation",
                    "workaround_text": "",
                    "desired_output": "dashboard",
                    "product_fit": "strong_fit",
                    "segmentation_note": "",
                },
                {
                    "episode_id": "e3",
                    "source": "stackoverflow",
                    "raw_id": "s1",
                    "url": "https://example.com/3",
                    "normalized_episode": "Why did conversion drop and why do numbers not match source data.",
                    "evidence_snippet": "",
                    "role_clue": "",
                    "work_moment": "validation",
                    "business_question": "validate and reconcile reported numbers",
                    "tool_env": "",
                    "bottleneck_text": "validation mismatch",
                    "workaround_text": "",
                    "desired_output": "",
                    "product_fit": "review",
                    "segmentation_note": "",
                },
            ]
        )
        self.labeled_df = pd.DataFrame(
            [
                {
                    "episode_id": "e1",
                    "role_codes": "R_ANALYST",
                    "moment_codes": "M_REPORTING",
                    "question_codes": "Q_REPORT_SPEED",
                    "pain_codes": "P_MANUAL_REPORTING",
                    "env_codes": "unknown",
                    "workaround_codes": "W_MANUAL",
                    "output_codes": "O_XLSX",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.7,
                    "label_reason": "test",
                },
                {
                    "episode_id": "e2",
                    "role_codes": "R_MANAGER",
                    "moment_codes": "M_REPORTING",
                    "question_codes": "Q_REPORT_SPEED",
                    "pain_codes": "P_TOOL_LIMITATION",
                    "env_codes": "E_SQL_BI",
                    "workaround_codes": "unknown",
                    "output_codes": "O_DASHBOARD",
                    "fit_code": "F_STRONG",
                    "label_confidence": 0.8,
                    "label_reason": "test",
                },
                {
                    "episode_id": "e3",
                    "role_codes": "unknown",
                    "moment_codes": "M_VALIDATION",
                    "question_codes": "Q_VALIDATE_NUMBERS",
                    "pain_codes": "P_DATA_QUALITY",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "output_codes": "unknown",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.75,
                    "label_reason": "test",
                },
            ]
        )
        self.candidate_df = pd.DataFrame(
            {
                "axis_name": [
                    "user_role",
                    "workflow_stage",
                    "analysis_goal",
                    "bottleneck_type",
                    "tool_dependency_mode",
                    "trust_validation_need",
                    "analysis_maturity",
                    "demographic_profile",
                ],
                "description": [""] * 8,
            }
        )
        self.current_axis_schema = [
            {"axis_name": "user_role", "why_it_matters": "role", "allowed_values_or_logic": ["analyst", "manager"], "evidence_fields_used": ["role_codes"]},
            {"axis_name": "workflow_stage", "why_it_matters": "stage", "allowed_values_or_logic": ["reporting", "validation"], "evidence_fields_used": ["moment_codes"]},
            {"axis_name": "analysis_goal", "why_it_matters": "goal", "allowed_values_or_logic": ["report_speed", "validate_numbers"], "evidence_fields_used": ["question_codes"]},
            {"axis_name": "bottleneck_type", "why_it_matters": "pain", "allowed_values_or_logic": ["manual_reporting", "tool_limitation"], "evidence_fields_used": ["pain_codes"]},
            {"axis_name": "tool_dependency_mode", "why_it_matters": "tools", "allowed_values_or_logic": ["spreadsheet_heavy", "bi_dashboard_heavy"], "evidence_fields_used": ["env_codes"]},
            {"axis_name": "trust_validation_need", "why_it_matters": "trust", "allowed_values_or_logic": ["high", "medium", "low"], "evidence_fields_used": ["question_codes"]},
        ]
        self.config = {
            "audit": {
                "tiny_class_share_threshold": 0.03,
                "warn_unknown_rate": 0.30,
                "high_unknown_rate": 0.45,
                "min_cluster_contribution": 0.08,
                "strong_cluster_contribution": 0.16,
                "dominant_share_warn": 0.72,
                "dominant_share_drop": 0.90,
                "overlap_merge_threshold": 0.60,
            },
            "axes": {
                "user_role": {"preferred_action": "keep_optional"},
                "workflow_stage": {"preferred_action": "keep_core"},
                "analysis_goal": {"preferred_action": "keep_core"},
                "bottleneck_type": {"preferred_action": "keep_core"},
                "tool_dependency_mode": {"preferred_action": "keep_core"},
                "trust_validation_need": {
                    "preferred_action": "simplify",
                    "simplify_map": {
                        "high": "high_validation_pressure",
                        "medium": "validation_pressure_present",
                        "low": "validation_pressure_present",
                    },
                },
                "analysis_maturity": {
                    "preferred_action": "merge",
                    "target_axis": "tool_dependency_mode",
                    "merge_value_map": {
                        "manual_workaround_heavy": "spreadsheet_heavy",
                        "script_assisted_self_serve": "script_assisted",
                        "warehouse_backed_self_serve": "warehouse_backed",
                        "spreadsheet_led_self_serve": "spreadsheet_heavy",
                    },
                },
                "demographic_profile": {"preferred_action": "drop"},
            },
        }

    def test_axis_audit_and_recommendations_follow_config(self) -> None:
        outputs = build_axis_quality_audit(
            self.episodes_df,
            self.labeled_df,
            self.candidate_df,
            self.current_axis_schema,
            self.config,
        )
        audit_df = outputs["audit_df"]
        self.assertIn("unknown_rate", audit_df.columns)
        recs = recommend_axis_reduction(audit_df, self.config)
        lookup = recs.set_index("axis_name")["recommendation_type"].to_dict()
        self.assertEqual(lookup["user_role"], "keep_optional")
        self.assertEqual(lookup["trust_validation_need"], "simplify")
        self.assertEqual(lookup["analysis_maturity"], "merge")
        self.assertEqual(lookup["demographic_profile"], "drop")

    def test_apply_axis_reduction_builds_core_and_optional_schema(self) -> None:
        outputs = build_axis_quality_audit(
            self.episodes_df,
            self.labeled_df,
            self.candidate_df,
            self.current_axis_schema,
            self.config,
        )
        recs = recommend_axis_reduction(outputs["audit_df"], self.config)
        reduced = apply_axis_reduction(
            outputs["axis_wide_df"],
            outputs["axis_long_df"],
            outputs["audit_df"],
            recs,
            self.candidate_df,
            self.current_axis_schema,
            self.config,
        )
        schema_lookup = {row["axis_name"]: row for row in reduced["reduced_axis_schema"]}
        self.assertEqual(schema_lookup["workflow_stage"]["axis_role"], "core")
        self.assertEqual(schema_lookup["user_role"]["axis_role"], "optional")
        self.assertNotIn("demographic_profile", schema_lookup)
        self.assertIn("before_after_cluster_quality_df", reduced)
        self.assertFalse(reduced["before_after_cluster_quality_df"].empty)


if __name__ == "__main__":
    unittest.main()
