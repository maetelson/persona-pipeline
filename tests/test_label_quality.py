"""Tests for labelability, repair, and source-aware prompt helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest

import pandas as pd

from src.labeling.labelability import build_labelability_table
from src.labeling.prompt_builder import build_label_prompt
from src.labeling.quality import build_label_quality_audit
from src.labeling.repair import apply_label_repairs, build_axis_label_details
from src.labeling.rule_labeler import prelabel_episodes
from src.utils.io import load_yaml

ROOT = Path(__file__).resolve().parents[1]
_LABEL_EPISODES_SPEC = importlib.util.spec_from_file_location("run_05_label_episodes", ROOT / "run" / "05_label_episodes.py")
if _LABEL_EPISODES_SPEC is None or _LABEL_EPISODES_SPEC.loader is None:
    raise RuntimeError("Unable to load run/05_label_episodes.py for tests.")
_LABEL_EPISODES_MODULE = importlib.util.module_from_spec(_LABEL_EPISODES_SPEC)
_LABEL_EPISODES_SPEC.loader.exec_module(_LABEL_EPISODES_MODULE)
_write_before_after_quality_report = _LABEL_EPISODES_MODULE._write_before_after_quality_report


class LabelQualityTests(unittest.TestCase):
    """Verify label quality helpers behave deterministically."""

    def test_labelability_marks_low_signal(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        episodes = pd.DataFrame(
            [
                {"episode_id": "1", "source": "reddit", "normalized_episode": "dashboard numbers don't match and I export to excel every week", "evidence_snippet": "", "business_question": "", "bottleneck_text": "", "workaround_text": "", "desired_output": ""},
                {"episode_id": "2", "source": "stackoverflow", "normalized_episode": "npm install error", "evidence_snippet": "", "business_question": "", "bottleneck_text": "", "workaround_text": "", "desired_output": ""},
            ]
        )
        result = build_labelability_table(episodes, policy)
        self.assertNotEqual(result.loc[result["episode_id"] == "1", "labelability_status"].iloc[0], "low_signal")
        self.assertEqual(result.loc[result["episode_id"] == "2", "labelability_status"].iloc[0], "low_signal")

    def test_prompt_builder_includes_source_guidance(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        payload = build_label_prompt(
            episode_row=pd.Series({"source": "reddit", "normalized_episode": "stakeholders keep asking follow-ups after dashboard review"}),
            labeled_row=pd.Series({"question_codes": "unknown", "pain_codes": "unknown"}),
            requested_families=["question_codes", "pain_codes"],
            target_reason="low_confidence",
            codebook=load_yaml(ROOT / "config" / "codebook.yaml"),
            policy=policy,
        )
        self.assertIn("reddit", payload["prompt"])
        self.assertIn("few_shot:", payload["prompt"])

    def test_business_community_operator_pain_is_labelable(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "biz-1",
                    "source": "klaviyo_community",
                    "normalized_episode": "Campaigns are not sending and open rates dropped after sending domain warmup.",
                    "evidence_snippet": "",
                    "business_question": "",
                    "bottleneck_text": "",
                    "workaround_text": "",
                    "desired_output": "",
                }
            ]
        )
        result = build_labelability_table(episodes, policy)
        self.assertNotEqual(result.loc[0, "labelability_status"], "low_signal")

    def test_business_community_terms_map_to_broad_labels(self) -> None:
        codebook = load_yaml(ROOT / "config" / "codebook.yaml")
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "biz-2",
                    "source": "shopify_community",
                    "normalized_episode": "Shopify account has price mismatch in the feed sync and product data quality issues.",
                    "business_question": "",
                    "bottleneck_text": "",
                    "tool_env": "",
                    "workaround_text": "",
                    "desired_output": "",
                    "role_clue": "",
                    "work_moment": "",
                    "product_fit": "",
                    "segmentation_note": "",
                }
            ]
        )
        labeled = prelabel_episodes(episodes, codebook)
        self.assertIn("Q_VALIDATE_NUMBERS", labeled.loc[0, "question_codes"])
        self.assertIn("P_DATA_QUALITY", labeled.loc[0, "pain_codes"])

    def test_business_community_prompt_uses_group_guidance(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        payload = build_label_prompt(
            episode_row=pd.Series({"source": "hubspot_community", "normalized_episode": "dashboard filters hide revenue attribution"}),
            labeled_row=pd.Series({"question_codes": "unknown", "pain_codes": "unknown"}),
            requested_families=["question_codes", "pain_codes"],
            target_reason="unknown",
            codebook=load_yaml(ROOT / "config" / "codebook.yaml"),
            policy=policy,
        )
        self.assertIn("src=business_communities", payload["prompt"])
        self.assertIn("Operator support/community language", payload["prompt"])

    def test_repair_and_details_fill_broad_labels(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "1",
                    "source": "reddit",
                    "normalized_episode": "leadership wants a board report and I still export to excel every week",
                    "evidence_snippet": "",
                    "business_question": "",
                    "bottleneck_text": "",
                    "workaround_text": "",
                    "desired_output": "board report",
                }
            ]
        )
        labeled = pd.DataFrame(
            [
                {
                    "episode_id": "1",
                    "role_codes": "unknown",
                    "moment_codes": "M_REPORTING",
                    "question_codes": "unknown",
                    "pain_codes": "unknown",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "output_codes": "unknown",
                    "fit_code": "unknown",
                    "label_confidence": 0.4,
                    "label_reason": "unknown",
                }
            ]
        )
        labelability = pd.DataFrame(
            [{"episode_id": "1", "source": "reddit", "labelability_status": "labelable", "labelability_score": 6, "labelability_reason": "positive", "persona_core_eligible": True}]
        )
        repaired, repairs = apply_label_repairs(episodes, labeled, labelability, policy)
        self.assertEqual(repaired.loc[0, "role_codes"], "R_MANAGER")
        self.assertEqual(repaired.loc[0, "output_codes"], "O_XLSX")
        details = build_axis_label_details(episodes, repaired, labelability)
        self.assertIn("confidence_score", details.columns)
        self.assertFalse(repairs.empty)

    def test_contextual_repairs_fill_partial_unknowns(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "partial-1",
                    "source": "shopify_community",
                    "normalized_episode": "Ads not showing despite no setup issues and product visibility tracking looks wrong in dashboard diagnostics.",
                    "evidence_snippet": "",
                    "business_question": "How can we diagnose and resolve analytics issues faster?",
                    "bottleneck_text": "tool_limitation",
                    "workaround_text": "",
                    "desired_output": "",
                },
                {
                    "episode_id": "partial-2",
                    "source": "metabase_discussions",
                    "normalized_episode": "Export API gives stale results and the reporting workflow still needs manual spreadsheet validation.",
                    "evidence_snippet": "",
                    "business_question": "How can we deliver recurring reporting output faster and with fewer manual steps?",
                    "bottleneck_text": "general_friction",
                    "workaround_text": "manual export",
                    "desired_output": "",
                },
            ]
        )
        labeled = pd.DataFrame(
            [
                {
                    "episode_id": "partial-1",
                    "role_codes": "R_MARKETER",
                    "moment_codes": "unknown",
                    "question_codes": "Q_DIAGNOSE_ISSUE",
                    "pain_codes": "P_TOOL_LIMITATION",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "output_codes": "unknown",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.75,
                    "label_reason": "rule",
                },
                {
                    "episode_id": "partial-2",
                    "role_codes": "unknown",
                    "moment_codes": "unknown",
                    "question_codes": "Q_REPORT_SPEED",
                    "pain_codes": "P_MANUAL_REPORTING",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "output_codes": "O_XLSX",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.8,
                    "label_reason": "rule",
                },
            ]
        )
        labelability = pd.DataFrame(
            [
                {"episode_id": "partial-1", "source": "shopify_community", "labelability_status": "labelable", "labelability_score": 6, "labelability_reason": "positive", "persona_core_eligible": True},
                {"episode_id": "partial-2", "source": "metabase_discussions", "labelability_status": "borderline", "labelability_score": 4, "labelability_reason": "positive", "persona_core_eligible": True},
            ]
        )
        repaired, repairs = apply_label_repairs(episodes, labeled, labelability, policy)
        self.assertEqual(repaired.loc[0, "output_codes"], "O_DASHBOARD")
        self.assertNotEqual(repaired.loc[1, "role_codes"], "unknown")
        self.assertFalse(repairs.empty)

    def test_contextual_repairs_work_when_labeled_rows_already_have_labelability_columns(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "ep-question-gap",
                    "source": "shopify_community",
                    "normalized_episode": "Ads not showing despite no issues and campaign is not serving.",
                    "business_question": "Why are my ads not showing?",
                    "evidence_snippet": "ads not showing",
                    "bottleneck_text": "general_friction",
                    "desired_output": "unspecified_output",
                }
            ]
        )
        labeled = pd.DataFrame(
            [
                {
                    "episode_id": "ep-question-gap",
                    "source": "shopify_community",
                    "role_codes": "R_MARKETER",
                    "question_codes": "unknown",
                    "pain_codes": "P_TOOL_LIMITATION",
                    "output_codes": "O_AUTOMATION_JOB",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.75,
                    "label_reason": "question=unknown:no_match",
                    "labelability_status": "labelable",
                    "labelability_score": 6,
                }
            ]
        )
        labelability = pd.DataFrame(
            [
                {
                    "episode_id": "ep-question-gap",
                    "source": "shopify_community",
                    "labelability_status": "labelable",
                    "labelability_score": 6,
                }
            ]
        )

        repaired, repairs = apply_label_repairs(episodes, labeled, labelability, policy)

        self.assertEqual(repaired.loc[0, "question_codes"], "Q_DIAGNOSE_ISSUE")
        self.assertEqual(repairs.shape[0], 1)

    def test_unknown_reason_breakdown_accounts_for_low_signal_and_partial_gaps(self) -> None:
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "u1",
                    "source": "metabase_discussions",
                    "normalized_episode": "Database migration failed after upgrade and dashboard filters are broken.",
                    "evidence_snippet": "",
                    "business_question": "How can we diagnose and resolve analytics issues faster?",
                    "bottleneck_text": "tool_limitation",
                    "workaround_text": "",
                    "desired_output": "",
                },
                {
                    "episode_id": "u2",
                    "source": "shopify_community",
                    "normalized_episode": "Ads not showing and diagnostics dashboard is the main place we monitor visibility.",
                    "evidence_snippet": "",
                    "business_question": "How can we diagnose and resolve analytics issues faster?",
                    "bottleneck_text": "tool_limitation",
                    "workaround_text": "",
                    "desired_output": "",
                },
                {
                    "episode_id": "u3",
                    "source": "metabase_discussions",
                    "normalized_episode": "Export workflow still needs manual spreadsheet work every week.",
                    "evidence_snippet": "",
                    "business_question": "How can we deliver recurring reporting output faster and with fewer manual steps?",
                    "bottleneck_text": "general_friction",
                    "workaround_text": "manual export",
                    "desired_output": "board report",
                },
            ]
        )
        labeled = pd.DataFrame(
            [
                {
                    "episode_id": "u1",
                    "role_codes": "unknown",
                    "question_codes": "unknown",
                    "pain_codes": "unknown",
                    "output_codes": "unknown",
                    "moment_codes": "unknown",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "fit_code": "unknown",
                    "label_confidence": 0.2,
                    "label_reason": "unknown",
                    "labelability_status": "low_signal",
                    "labelability_score": 1,
                    "labelability_reason": "weak_signal",
                    "persona_core_eligible": False,
                },
                {
                    "episode_id": "u2",
                    "role_codes": "R_MARKETER",
                    "question_codes": "Q_DIAGNOSE_ISSUE",
                    "pain_codes": "P_TOOL_LIMITATION",
                    "output_codes": "unknown",
                    "moment_codes": "unknown",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.8,
                    "label_reason": "rule",
                    "labelability_status": "labelable",
                    "labelability_score": 6,
                    "labelability_reason": "positive",
                    "persona_core_eligible": True,
                },
                {
                    "episode_id": "u3",
                    "role_codes": "unknown",
                    "question_codes": "Q_REPORT_SPEED",
                    "pain_codes": "P_MANUAL_REPORTING",
                    "output_codes": "O_XLSX",
                    "moment_codes": "unknown",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.8,
                    "label_reason": "rule",
                    "labelability_status": "borderline",
                    "labelability_score": 4,
                    "labelability_reason": "positive",
                    "persona_core_eligible": True,
                },
            ]
        )
        labelability = pd.DataFrame(
            [
                {"episode_id": "u1", "source": "metabase_discussions", "labelability_status": "low_signal", "labelability_score": 1, "labelability_reason": "weak_signal", "persona_core_eligible": False},
                {"episode_id": "u2", "source": "shopify_community", "labelability_status": "labelable", "labelability_score": 6, "labelability_reason": "positive", "persona_core_eligible": True},
                {"episode_id": "u3", "source": "metabase_discussions", "labelability_status": "borderline", "labelability_score": 4, "labelability_reason": "positive", "persona_core_eligible": True},
            ]
        )
        details = build_axis_label_details(episodes, labeled, labelability)
        outputs = build_label_quality_audit(episodes, labeled, details, labelability)
        breakdown = outputs["unknown_reason_breakdown_df"]

        self.assertEqual(int(breakdown.loc[breakdown["unknown_reason"] == "overly_strict_axis_requirement", "count"].iloc[0]), 1)
        self.assertEqual(int(breakdown.loc[breakdown["unknown_reason"] == "workflow_stage_missing", "count"].iloc[0]), 1)
        self.assertEqual(int(breakdown.loc[breakdown["unknown_reason"] == "parser_schema_mismatch", "count"].iloc[0]), 1)
        self.assertEqual(
            str(breakdown.loc[breakdown["unknown_reason"] == "overly_strict_axis_requirement", "root_cause_category"].iloc[0]),
            "overly_strict_axis_requirement",
        )
        self.assertEqual(
            str(breakdown.loc[breakdown["unknown_reason"] == "workflow_stage_missing", "root_cause_category"].iloc[0]),
            "output_expectation_not_captured",
        )
        self.assertEqual(
            str(breakdown.loc[breakdown["unknown_reason"] == "parser_schema_mismatch", "root_cause_category"].iloc[0]),
            "parser_schema_mismatch",
        )
        self.assertEqual(
            str(breakdown.loc[breakdown["unknown_reason"] == "overly_strict_axis_requirement", "persona_core_policy"].iloc[0]),
            "supportable_low_signal",
        )
        self.assertIn("likely_remediation_type", breakdown.columns)
        self.assertIn("sample_rows", breakdown.columns)

    def test_repairs_supportable_low_signal_taxonomy_gaps(self) -> None:
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        episodes = pd.DataFrame(
            [
                {
                    "episode_id": "low-signal-1",
                    "source": "metabase_discussions",
                    "normalized_episode": "Dashboard filters are broken after metadata sync and query results are incorrect after the database migration.",
                    "evidence_snippet": "Dashboard filters showing IDs instead of names after sync.",
                    "business_question": "How can we diagnose why the dashboard behavior changed and fix the incorrect results?",
                    "bottleneck_text": "tool_limitation",
                    "workaround_text": "manual export for validation",
                    "desired_output": "validated dataset and dashboard fix",
                }
            ]
        )
        labeled = pd.DataFrame(
            [
                {
                    "episode_id": "low-signal-1",
                    "source": "metabase_discussions",
                    "role_codes": "unknown",
                    "question_codes": "unknown",
                    "pain_codes": "unknown",
                    "output_codes": "unknown",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.2,
                    "label_reason": "low_signal_input",
                    "labelability_status": "low_signal",
                    "labelability_score": 1,
                }
            ]
        )
        labelability = pd.DataFrame(
            [
                {
                    "episode_id": "low-signal-1",
                    "source": "metabase_discussions",
                    "labelability_status": "low_signal",
                    "labelability_score": 1,
                }
            ]
        )

        repaired, repairs = apply_label_repairs(episodes, labeled, labelability, policy)

        self.assertEqual(str(repaired.loc[0, "question_codes"]), "Q_DIAGNOSE_ISSUE")
        self.assertEqual(str(repaired.loc[0, "pain_codes"]), "P_TOOL_LIMITATION")
        self.assertIn(str(repaired.loc[0, "output_codes"]), {"O_VALIDATED_DATASET", "O_DASHBOARD"})
        self.assertEqual(str(repaired.loc[0, "role_codes"]), "R_ANALYST")
        self.assertEqual(str(repairs.loc[0, "root_cause_category"]), "overly_strict_axis_requirement")

    def test_before_after_quality_report_includes_reported_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "data" / "analysis").mkdir(parents=True, exist_ok=True)
            before = pd.DataFrame([{"episode_id": "1", "role_codes": "unknown", "question_codes": "unknown", "pain_codes": "unknown", "output_codes": "unknown"}])
            after = pd.DataFrame(
                [
                    {
                        "episode_id": "1",
                        "role_codes": "R_ANALYST",
                        "question_codes": "Q_REPORT_SPEED",
                        "pain_codes": "P_MANUAL_REPORTING",
                        "output_codes": "O_XLSX",
                        "persona_core_eligible": True,
                    }
                ]
            )
            labelability = pd.DataFrame([{"episode_id": "1", "labelability_status": "labelable"}])
            _write_before_after_quality_report(
                root,
                before,
                after,
                labelability,
                {"reported_baseline_unknown_ratio": 0.717116, "reported_baseline_quality_flag": "LOW_QUALITY"},
            )

            metrics = pd.read_csv(root / "data" / "analysis" / "before_after_label_metrics.csv")
            numeric_lookup = dict(zip(metrics["metric"], metrics["value_numeric"]))
            text_lookup = dict(zip(metrics["metric"], metrics["value_text"].fillna("")))
            self.assertAlmostEqual(float(numeric_lookup["reported_baseline_unknown_ratio"]), 0.717116, places=6)
            self.assertEqual(str(text_lookup["reported_baseline_quality_flag"]), "LOW_QUALITY")


class LabelCliSmokeTests(unittest.TestCase):
    """Verify the label CLI runs lightweight commands."""

    def test_dry_run_labeler_cli(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/15_label_cli.py", "dry-run-labeler"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)


if __name__ == "__main__":
    unittest.main()
