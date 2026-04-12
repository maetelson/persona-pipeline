"""Tests for persona-core taxonomy and admission policy."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.persona_axes import build_axis_assignments, build_persona_core_flags


class PersonaCoreTaxonomyTests(unittest.TestCase):
    """Verify primary-axis inference and persona-core eligibility policy."""

    def test_workflow_stage_infers_from_question_and_output_codes(self) -> None:
        episodes = pd.DataFrame(
            [
                {"episode_id": "e1", "source": "reddit", "work_moment": "", "business_question": "", "bottleneck_text": "", "desired_output": "", "normalized_episode": "Need recurring reporting output."},
                {"episode_id": "e2", "source": "reddit", "work_moment": "", "business_question": "", "bottleneck_text": "", "desired_output": "", "normalized_episode": "Need to diagnose why metrics changed."},
                {"episode_id": "e3", "source": "reddit", "work_moment": "", "business_question": "", "bottleneck_text": "", "desired_output": "", "normalized_episode": "Need to reconcile and validate numbers."},
                {"episode_id": "e4", "source": "reddit", "work_moment": "", "business_question": "", "bottleneck_text": "", "desired_output": "", "normalized_episode": "Need to automate the workflow."},
            ]
        )
        labeled = pd.DataFrame(
            [
                {"episode_id": "e1", "question_codes": "Q_REPORT_SPEED", "pain_codes": "P_MANUAL_REPORTING", "output_codes": "O_XLSX"},
                {"episode_id": "e2", "question_codes": "Q_DIAGNOSE_ISSUE", "pain_codes": "P_TOOL_LIMITATION", "output_codes": "O_DASHBOARD"},
                {"episode_id": "e3", "question_codes": "Q_VALIDATE_NUMBERS", "pain_codes": "P_DATA_QUALITY", "output_codes": "O_VALIDATED_DATASET"},
                {"episode_id": "e4", "question_codes": "Q_AUTOMATE_WORKFLOW", "pain_codes": "P_HANDOFF", "output_codes": "O_AUTOMATION_JOB"},
            ]
        )

        axis_wide_df, _ = build_axis_assignments(episodes, labeled, axis_names=["workflow_stage"])
        lookup = dict(zip(axis_wide_df["episode_id"], axis_wide_df["workflow_stage"]))

        self.assertEqual(lookup["e1"], "reporting")
        self.assertEqual(lookup["e2"], "triage")
        self.assertEqual(lookup["e3"], "validation")
        self.assertEqual(lookup["e4"], "automation")

    def test_persona_core_policy_admits_only_supported_low_signal_rows(self) -> None:
        labeled = pd.DataFrame(
            [
                {"episode_id": "core-1", "labelability_status": "labelable", "persona_core_eligible": True},
                {"episode_id": "core-2", "labelability_status": "borderline", "persona_core_eligible": True},
                {"episode_id": "support-1", "labelability_status": "low_signal", "persona_core_eligible": False},
                {"episode_id": "noise-1", "labelability_status": "low_signal", "persona_core_eligible": False},
                {"episode_id": "incomplete-1", "labelability_status": "labelable", "persona_core_eligible": True},
            ]
        )
        axis_wide_df = pd.DataFrame(
            [
                {"episode_id": "core-1", "bottleneck_type": "manual_reporting", "workflow_stage": "reporting", "analysis_goal": "report_speed", "tool_dependency_mode": "spreadsheet_heavy"},
                {"episode_id": "core-2", "bottleneck_type": "tool_limitation", "workflow_stage": "triage", "analysis_goal": "diagnose_change", "tool_dependency_mode": "unassigned"},
                {"episode_id": "support-1", "bottleneck_type": "tool_limitation", "workflow_stage": "triage", "analysis_goal": "diagnose_change", "tool_dependency_mode": "unassigned"},
                {"episode_id": "noise-1", "bottleneck_type": "general_friction", "workflow_stage": "reporting", "analysis_goal": "report_speed", "tool_dependency_mode": "unassigned"},
                {"episode_id": "incomplete-1", "bottleneck_type": "manual_reporting", "workflow_stage": "unassigned", "analysis_goal": "report_speed", "tool_dependency_mode": "spreadsheet_heavy"},
            ]
        )
        final_axis_schema = [
            {"axis_name": "bottleneck_type", "axis_role": "core"},
            {"axis_name": "workflow_stage", "axis_role": "core"},
            {"axis_name": "analysis_goal", "axis_role": "core"},
            {"axis_name": "tool_dependency_mode", "axis_role": "optional"},
        ]
        unknown_rows_df = pd.DataFrame(
            [
                {"episode_id": "support-1", "unknown_reason": "labelability_failure_product_support", "root_cause_category": "overly_strict_axis_requirement", "persona_core_policy": "supportable_low_signal"},
                {"episode_id": "noise-1", "unknown_reason": "too_generic_or_noisy", "root_cause_category": "generic_chatter_not_persona_usable", "persona_core_policy": "exclude_low_signal"},
            ]
        )

        updated, audit_df = build_persona_core_flags(labeled, axis_wide_df, final_axis_schema, unknown_rows_df)
        eligibility = dict(zip(updated["episode_id"], updated["persona_core_eligible"]))
        reasons = dict(zip(updated["episode_id"], updated["persona_core_reason"]))

        self.assertTrue(bool(eligibility["core-1"]))
        self.assertTrue(bool(eligibility["core-2"]))
        self.assertTrue(bool(eligibility["support-1"]))
        self.assertFalse(bool(eligibility["noise-1"]))
        self.assertFalse(bool(eligibility["incomplete-1"]))
        self.assertEqual(reasons["support-1"], "complete_primary_axes_low_signal_supported<overly_strict_axis_requirement>")
        self.assertEqual(reasons["noise-1"], "excluded_low_signal<too_generic_or_noisy>")
        self.assertEqual(reasons["incomplete-1"], "missing_core_axes<workflow_stage>")
        self.assertEqual(int(audit_df["persona_core_eligible"].sum()), 3)
        categories = dict(zip(audit_df["episode_id"], audit_df["root_cause_category"]))
        self.assertEqual(categories["support-1"], "overly_strict_axis_requirement")
        self.assertEqual(categories["noise-1"], "generic_chatter_not_persona_usable")


if __name__ == "__main__":
    unittest.main()