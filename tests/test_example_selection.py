"""Tests for representative example selection."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.example_selection import apply_promotion_grounding_policy, select_persona_representative_examples


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
                "max_selected_mismatch_axes": 2,
                "max_selected_critical_mismatch_axes": 1,
                "promoted_fallback_min_score": 2.5,
                "source_diversity_score_margin": 1.25,
            },
            "policy": {
                "grounding_strength": {
                    "strong": {
                        "allowed_quote_qualities": ["strong_representative"],
                        "max_mismatch_axes": 1,
                        "max_critical_mismatch_axes": 0,
                    },
                    "grounded": {
                        "allowed_quote_qualities": ["strong_representative", "usable"],
                        "max_mismatch_axes": 2,
                        "max_critical_mismatch_axes": 0,
                    },
                    "weak": {
                        "allowed_quote_qualities": ["strong_representative", "usable", "borderline"],
                        "max_mismatch_axes": 3,
                        "max_critical_mismatch_axes": 1,
                    },
                },
                "fallback": {
                    "allow_weak_grounding_fallback": True,
                    "weak_selection_strength_label": "weak_grounding_fallback",
                    "coverage_selection_reason": "minimum_coverage_policy",
                    "max_examples_per_persona": 1,
                },
                "promotion_grounding": {
                    "promoted_and_grounded_status": "promoted_and_grounded",
                    "promoted_but_weakly_grounded_status": "promoted_but_weakly_grounded",
                    "promoted_but_ungrounded_status": "promoted_but_ungrounded",
                    "downgraded_due_to_no_grounding_status": "downgraded_due_to_no_grounding",
                    "ungrounded_action": "flag",
                },
                "diversity": {
                    "prefer_new_source_within_score_margin": 1.25,
                    "diversify_subpatterns_until_slot_fraction": 0.5,
                },
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
                "grounding_fit": 1.6,
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

    def test_mismatch_penalty_changes_ranking_for_otherwise_similar_examples(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_04",
                    "episode_id": "m1",
                    "source": "reddit",
                    "normalized_episode": "Every week I still export to Excel because leadership needs the report and I have to reconcile numbers before sending it.",
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
                {
                    "persona_id": "persona_04",
                    "episode_id": "m2",
                    "source": "reddit",
                    "normalized_episode": "Every week I still export to Excel because leadership needs the report and I have to reconcile numbers before sending it.",
                    "workflow_stage": "triage",
                    "analysis_goal": "diagnose_change",
                    "bottleneck_type": "tool_limitation",
                    "tool_dependency_mode": "bi_dashboard_heavy",
                },
            ]
        )
        outputs = select_persona_representative_examples(df, self.axis_names, self.config, max_items=1)
        selected = outputs["selected_df"]
        self.assertEqual(selected.iloc[0]["episode_id"], "m1")
        self.assertGreater(float(selected.iloc[0]["grounding_fit_score"]), 0.0)

    def test_diversity_prefers_new_source_when_scores_are_close(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_05",
                    "episode_id": "d1",
                    "source": "reddit",
                    "normalized_episode": "We export to Excel every week because stakeholders need the monthly report and we still reconcile numbers before sharing it.",
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
                {
                    "persona_id": "persona_05",
                    "episode_id": "d2",
                    "source": "reddit",
                    "normalized_episode": "Every month I export into spreadsheets because the dashboard is not enough for business review and follow-up questions keep coming.",
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
                {
                    "persona_id": "persona_05",
                    "episode_id": "d3",
                    "source": "hubspot_community",
                    "normalized_episode": "The dashboard is not enough for leadership reporting, so I still export to Excel and reconcile numbers before the report goes out.",
                    "workflow_stage": "reporting",
                    "analysis_goal": "report_speed",
                    "bottleneck_type": "manual_reporting",
                    "tool_dependency_mode": "spreadsheet_heavy",
                },
            ]
        )
        outputs = select_persona_representative_examples(df, self.axis_names, self.config, max_items=2)
        selected_sources = set(outputs["selected_df"]["source"].astype(str).tolist())
        self.assertEqual(len(outputs["selected_df"]), 2)
        self.assertIn("hubspot_community", selected_sources)

    def test_policy_backed_fallback_marks_weak_grounding_explicitly(self) -> None:
        audit_df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_02",
                    "episode_id": "w1",
                    "source": "hubspot_community",
                    "grounded_text": "The dashboard update is not enough and we still need a workaround before leadership sees it.",
                    "quote_quality": "borderline",
                    "grounding_strength": "weak",
                    "final_example_score": 3.6,
                    "mismatch_count": 2,
                    "critical_mismatch_count": 0,
                    "score_breakdown": "{}",
                    "why_selected": "Selected for workflow context.",
                    "rejection_reason": "context is too weak or ambiguous for a strong representative example",
                    "matched_axes": "workflow_stage=triage",
                }
            ]
        )
        result = apply_promotion_grounding_policy(
            selected_df=pd.DataFrame(),
            audit_df=audit_df,
            promoted_persona_ids=["persona_02"],
            config=self.config,
        )
        selected = result["selected_df"]
        grounding = result["persona_grounding_df"]
        self.assertEqual(selected.iloc[0]["selection_strength"], "weak_grounding_fallback")
        self.assertTrue(bool(selected.iloc[0]["fallback_selected"]))
        self.assertEqual(str(grounding.iloc[0]["promotion_grounding_status"]), "promoted_but_weakly_grounded")
        self.assertEqual(str(grounding.iloc[0]["grounding_status"]), "weakly_grounded")

    def test_no_policy_fallback_leaves_persona_explicitly_ungrounded(self) -> None:
        config = dict(self.config)
        config["policy"] = dict(self.config["policy"])
        config["policy"]["fallback"] = dict(self.config["policy"]["fallback"])
        config["policy"]["fallback"]["allow_weak_grounding_fallback"] = False
        audit_df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_02",
                    "episode_id": "w1",
                    "source": "hubspot_community",
                    "grounded_text": "The dashboard update is not enough and we still need a workaround before leadership sees it.",
                    "quote_quality": "borderline",
                    "grounding_strength": "weak",
                    "final_example_score": 3.6,
                    "mismatch_count": 2,
                    "critical_mismatch_count": 0,
                    "score_breakdown": "{}",
                    "why_selected": "Selected for workflow context.",
                    "rejection_reason": "context is too weak or ambiguous for a strong representative example",
                    "matched_axes": "workflow_stage=triage",
                }
            ]
        )
        result = apply_promotion_grounding_policy(
            selected_df=pd.DataFrame(),
            audit_df=audit_df,
            promoted_persona_ids=["persona_02"],
            config=config,
        )
        self.assertTrue(result["selected_df"].empty)
        grounding = result["persona_grounding_df"]
        self.assertEqual(str(grounding.iloc[0]["promotion_grounding_status"]), "promoted_but_ungrounded")
        self.assertEqual(str(grounding.iloc[0]["grounding_status"]), "ungrounded")

    def test_every_promoted_persona_gets_an_explicit_grounding_state(self) -> None:
        audit_df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_01",
                    "episode_id": "g1",
                    "source": "reddit",
                    "grounded_text": "We export to Excel every week because the dashboard is not enough for leadership reporting.",
                    "quote_quality": "usable",
                    "grounding_strength": "grounded",
                    "final_example_score": 8.0,
                    "mismatch_count": 0,
                    "critical_mismatch_count": 0,
                    "score_breakdown": "{}",
                    "why_selected": "Selected for workflow context.",
                    "rejection_reason": "",
                }
            ]
        )
        selected_df = pd.DataFrame(
            [
                {
                    "persona_id": "persona_01",
                    "episode_id": "g1",
                    "grounded_text": "We export to Excel every week because the dashboard is not enough for leadership reporting.",
                    "quote_quality": "usable",
                    "grounding_strength": "grounded",
                    "selection_strength": "grounded",
                    "example_rank": 1,
                }
            ]
        )
        result = apply_promotion_grounding_policy(
            selected_df=selected_df,
            audit_df=audit_df,
            promoted_persona_ids=["persona_01", "persona_02"],
            config=self.config,
        )
        grounding = result["persona_grounding_df"].sort_values("persona_id").reset_index(drop=True)
        self.assertEqual(grounding["persona_id"].astype(str).tolist(), ["persona_01", "persona_02"])
        self.assertEqual(set(grounding["grounding_status"].astype(str).tolist()), {"grounded", "ungrounded"})


if __name__ == "__main__":
    unittest.main()
