"""Tests for persona naming, insight generation, and solution linkage."""

from __future__ import annotations

import json
import unittest

import pandas as pd

from src.analysis.persona_messaging import build_persona_messaging_outputs


class PersonaMessagingTests(unittest.TestCase):
    """Verify persona messaging stays bottleneck-centered and evidence-linked."""

    def test_builds_bottleneck_centered_cards(self) -> None:
        outputs = build_persona_messaging_outputs(
            cluster_audit_df=_cluster_audit_df(),
            naming_df=_naming_df(),
            persona_summary_df=_persona_summary_df(),
            examples_df=_examples_df(),
            personas=_legacy_personas(),
        )
        cards = outputs["persona_cards_v2_df"]
        card = cards.iloc[0].to_dict()
        self.assertEqual(card["primary_persona_name"], "Manual Reporting Burden")
        self.assertIn("repeatedly reshape", card["core_insight"])
        self.assertIn("dashboard", card["why_existing_workflow_fails"].lower())
        interventions = json.loads(card["suggested_interventions"])
        self.assertTrue(any("template" in item.lower() or "export" in item.lower() for item in interventions))

    def test_rejects_role_heavy_names_in_audit(self) -> None:
        outputs = build_persona_messaging_outputs(
            cluster_audit_df=_cluster_audit_df(),
            naming_df=_role_heavy_naming_df(),
            persona_summary_df=_persona_summary_df(),
            examples_df=_examples_df(),
            personas=_legacy_personas(),
        )
        audit = outputs["naming_audit_df"]
        self.assertEqual(audit.iloc[0]["name_centering_type"], "bottleneck-centered")
        self.assertEqual(outputs["persona_cards_v2_df"].iloc[0]["primary_persona_name"], "Manual Reporting Burden")

    def test_solution_linkage_is_specific_not_generic(self) -> None:
        outputs = build_persona_messaging_outputs(
            cluster_audit_df=_cluster_audit_df(),
            naming_df=_naming_df(),
            persona_summary_df=_persona_summary_df(),
            examples_df=_examples_df(),
            personas=_legacy_personas(),
        )
        card = outputs["persona_cards_v2_df"].iloc[0]
        self.assertNotIn("AI can help", card["solution_direction"])
        self.assertIn("manual cleanup", card["target_problem"].lower())
        self.assertIn("faster reporting turnaround", card["expected_user_value"].lower())

    def test_output_schema_contains_required_fields(self) -> None:
        outputs = build_persona_messaging_outputs(
            cluster_audit_df=_cluster_audit_df(),
            naming_df=_naming_df(),
            persona_summary_df=_persona_summary_df(),
            examples_df=_examples_df(),
            personas=_legacy_personas(),
        )
        expected = {
            "persona_id",
            "primary_persona_name",
            "persona_subtitle",
            "bottleneck_signature",
            "core_insight",
            "supporting_evidence",
            "repeated_work_pattern",
            "current_workaround",
            "why_existing_workflow_fails",
            "solution_direction",
            "target_problem",
            "expected_user_value",
            "suggested_interventions",
            "confidence_note",
            "representative_examples",
        }
        self.assertTrue(expected.issubset(set(outputs["persona_cards_v2_df"].columns)))

    def test_exploratory_pattern_name_is_not_flagged_role_heavy(self) -> None:
        outputs = build_persona_messaging_outputs(
            cluster_audit_df=_cluster_audit_df(),
            naming_df=_naming_df(),
            persona_summary_df=pd.DataFrame(
                [
                    {
                        "persona_id": "persona_01",
                        "persona_name": "Exploratory Spreadsheet Patchwork Pattern for Pre-Share Validation",
                        "evidence_confidence_tier": "thin",
                        "evidence_caution": "Representative examples are thin.",
                    }
                ]
            ),
            examples_df=pd.DataFrame(),
            personas=_legacy_personas(),
        )
        audit = outputs["naming_audit_df"]
        self.assertEqual(audit.iloc[0]["name_centering_type"], "residual-signature")
        cards = outputs["persona_cards_v2_df"]
        self.assertEqual(cards.iloc[0]["evidence_confidence_tier"], "thin")


def _cluster_audit_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "persona_id": "persona_01",
                "cluster_name": "Manual Reporting Burden + Export-to-Excel Workaround",
                "cluster_size": 120,
                "dominant_bottleneck_signals": "manual_reporting | recurring_export_work | spreadsheet_rework",
                "dominant_output_need_signals": "recurring_export_work | presentation_ready_output_need",
                "dominant_trust_reporting_signals": "reporting_deadline_pressure",
                "dominant_manual_work_signals": "manual_reporting | spreadsheet_rework",
                "role_distribution_json": '[{"label":"analyst","count":70,"share":0.7}]',
                "source_distribution_json": '[{"label":"reddit","count":80,"share":0.67}]',
                "cohesion": 0.91,
                "separation": 0.34,
                "bottleneck_coherence": 0.44,
                "role_dominance": 0.7,
                "cross_cluster_distinctiveness": 0.62,
                "representative_examples": "I export this to Excel every week because the dashboard is not enough.",
                "why_this_cluster_is_distinct": "Cluster is centered on manual reporting.",
            }
        ]
    )


def _naming_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "persona_id": "persona_01",
                "current_cluster_name": "Manual Reporting Burden + Export-to-Excel Workaround",
                "recommended_cluster_name": "Manual Reporting Burden",
            }
        ]
    )


def _role_heavy_naming_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "persona_id": "persona_01",
                "current_cluster_name": "Reporting Analyst",
                "recommended_cluster_name": "Reporting Analyst",
            }
        ]
    )


def _persona_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "persona_id": "persona_01",
                "main_workflow_context": "reporting",
                "dominant_bottleneck": "manual_reporting",
                "analysis_behavior": "report_speed",
                "current_tool_dependency": "spreadsheet_heavy",
                "primary_output_expectation": "excel_ready_output",
            }
        ]
    )


def _examples_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "persona_id": "persona_01",
                "grounded_text": "I export this to Excel every week because the dashboard is not enough for the leadership report.",
            },
            {
                "persona_id": "persona_01",
                "grounded_text": "Stakeholders keep asking follow-up questions after the dashboard review, so I rework the same numbers in spreadsheets.",
            },
        ]
    )


def _legacy_personas() -> list[dict[str, object]]:
    return [
        {
            "cluster_id": "persona_01",
            "persona_name": "Analyst Reporting Persona",
        }
    ]


if __name__ == "__main__":
    unittest.main()
