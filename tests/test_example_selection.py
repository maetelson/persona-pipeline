"""Tests for representative example selection."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.example_selection import select_persona_representative_examples


class ExampleSelectionTests(unittest.TestCase):
    """Verify scoring and selection of representative examples."""

    def setUp(self) -> None:
        self.config = {
            "snippet": {"min_chars": 40, "max_chars": 300},
            "thresholds": {
                "strong_representative_min_score": 8.0,
                "usable_min_score": 5.5,
                "borderline_min_score": 3.0,
                "duplicate_similarity_threshold": 0.65,
            },
            "weights": {
                "bottleneck_specificity": 1.5,
                "workflow_context": 1.1,
                "business_context": 1.0,
                "stakeholder_pressure": 0.9,
                "reporting_pain": 1.2,
                "dashboard_trust": 1.0,
                "excel_rework": 1.0,
                "adhoc_analysis": 0.9,
                "root_cause": 1.0,
                "metric_definition": 1.0,
                "output_need": 0.8,
                "persona_fit": 1.2,
                "genericness_penalty": 1.4,
                "technical_noise_penalty": 1.6,
            },
            "positive_patterns": {
                "bottleneck_specificity": ["don't trust", "numbers don't match", "can't explain", "break down by", "ad hoc", "reconcile numbers"],
                "workflow_context": ["every week", "weekly report", "monthly report", "export to excel", "manual spreadsheet"],
                "business_context": ["leadership", "stakeholder", "finance", "board report", "business analyst"],
                "output_need": ["xlsx", "spreadsheet", "report"],
            },
            "negative_patterns": {
                "technical_noise": ["docker", "npm", "oauth", "stack trace", "javascript error", "pip install"],
                "genericness": ["i’d like to introduce", "what it does", "apply now", "investment thesis"],
            },
            "subpatterns": {
                "export_spreadsheet": ["export to excel", "xlsx", "spreadsheet"],
                "dashboard_trust": ["don't trust", "numbers don't match", "reconcile"],
                "root_cause": ["can't explain", "why did", "drove it"],
                "adhoc_followups": ["ad hoc", "follow-up"],
            },
        }
        self.axis_names = ["workflow_stage", "analysis_goal", "bottleneck_type", "tool_dependency_mode"]

    def test_business_workflow_example_beats_generic_or_technical_noise(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_01",
                    "episode_id": "e1",
                    "source": "reddit",
                    "normalized_episode": "I export this to Excel every week because the dashboard is not enough for the leadership report and stakeholders keep asking follow-up questions.",
                    "business_question": "Need board report",
                    "bottleneck_text": "manual spreadsheet rework and ad hoc follow-up questions",
                    "workaround_text": "manual spreadsheet",
                    "desired_output": "xlsx",
                    "label_confidence": 0.8,
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
                {
                    "persona_id": "persona_01",
                    "episode_id": "e2",
                    "source": "stackoverflow",
                    "normalized_episode": "I’d like to introduce a new charting package. What it does is render SVG charts everywhere.",
                    "business_question": "",
                    "bottleneck_text": "",
                    "workaround_text": "",
                    "desired_output": "",
                    "label_confidence": 0.95,
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
                {
                    "persona_id": "persona_01",
                    "episode_id": "e3",
                    "source": "stackoverflow",
                    "normalized_episode": "My npm install fails with a stack trace after docker build and oauth callback setup.",
                    "business_question": "",
                    "bottleneck_text": "",
                    "workaround_text": "",
                    "desired_output": "",
                    "label_confidence": 0.99,
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
            ]
        )
        outputs = select_persona_representative_examples(df, self.axis_names, self.config, max_items=3)
        selected = outputs["selected_df"]
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected.iloc[0]["episode_id"], "e1")
        self.assertIn(selected.iloc[0]["quote_quality"], {"strong_representative", "usable"})

    def test_duplicate_suppression_keeps_only_best_near_duplicate(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_02",
                    "episode_id": "a1",
                    "source": "reddit",
                    "normalized_episode": "We export to Excel every week because the dashboard is not enough for the monthly report and we still reconcile numbers before sending it.",
                    "business_question": "",
                    "bottleneck_text": "reconcile numbers",
                    "workaround_text": "manual spreadsheet",
                    "desired_output": "xlsx",
                    "label_confidence": 0.8,
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
                {
                    "persona_id": "persona_02",
                    "episode_id": "a2",
                    "source": "reddit",
                    "normalized_episode": "We export to Excel every week because the dashboard is not enough for the monthly report and we still reconcile numbers before sending it to leadership.",
                    "business_question": "",
                    "bottleneck_text": "reconcile numbers",
                    "workaround_text": "manual spreadsheet",
                    "desired_output": "xlsx",
                    "label_confidence": 0.7,
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
            ]
        )
        outputs = select_persona_representative_examples(df, self.axis_names, self.config, max_items=3)
        selected = outputs["selected_df"]
        self.assertEqual(len(selected), 1)

    def test_borderline_classification_for_unclear_context(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_03",
                    "episode_id": "b1",
                    "source": "stackoverflow",
                    "normalized_episode": "Power BI DAX measure issue.",
                    "business_question": "",
                    "bottleneck_text": "",
                    "workaround_text": "",
                    "desired_output": "",
                    "label_confidence": 0.5,
                    "workflow_stage": "triage",
                    "analysis_goal": "diagnose_change",
                    "bottleneck_type": "tool_limitation",
                    "tool_dependency_mode": "bi_dashboard_heavy",
                }
            ]
        )
        outputs = select_persona_representative_examples(df, self.axis_names, self.config, max_items=2)
        combined = pd.concat([outputs["selected_df"], outputs["borderline_df"], outputs["rejected_df"]], ignore_index=True)
        quality = combined.iloc[0]["quote_quality"]
        self.assertIn(quality, {"borderline", "reject"})


if __name__ == "__main__":
    unittest.main()
