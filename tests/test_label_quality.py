"""Tests for labelability, repair, and source-aware prompt helpers."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest

import pandas as pd

from src.labeling.labelability import build_labelability_table
from src.labeling.prompt_builder import build_label_prompt
from src.labeling.repair import apply_label_repairs, build_axis_label_details
from src.utils.io import load_yaml

ROOT = Path(__file__).resolve().parents[1]


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
