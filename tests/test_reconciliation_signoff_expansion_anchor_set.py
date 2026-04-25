"""Tests for the reconciliation/signoff expansion anchor-set validator."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.reconciliation_signoff_expansion_anchor_set import REQUIRED_COLUMNS, validate_expansion_anchor_set_df


def _row(expansion_label: str, idx: int, source: str, subtype: str = "") -> dict[str, object]:
    row = {column: "" for column in REQUIRED_COLUMNS}
    row.update(
        {
            "episode_id": f"{source}::{expansion_label}::{idx}",
            "source": source,
            "source_url": f"https://example.com/{idx}",
            "current_persona_id": "persona_01",
            "current_cluster_signature": "sig",
            "candidate_source_pool": "test_pool",
            "normalized_episode": "episode",
            "business_question": "question",
            "bottleneck_text": "bottleneck",
            "desired_output": "output",
            "pain_codes": "P_DATA_QUALITY" if expansion_label == "expansion_positive_should_join_persona_04" else "",
            "question_codes": "Q_VALIDATE_NUMBERS" if expansion_label == "expansion_positive_should_join_persona_04" else "",
            "output_codes": "xlsx_report",
            "workflow_stage": "validation" if expansion_label == "expansion_positive_should_join_persona_04" else "reporting",
            "analysis_goal": "validate_numbers" if expansion_label == "expansion_positive_should_join_persona_04" else "report_speed",
            "bottleneck_type": "data_quality" if expansion_label == "expansion_positive_should_join_persona_04" else "manual_reporting",
            "trust_validation_need": "high" if expansion_label == "expansion_positive_should_join_persona_04" else "low",
            "expansion_label": expansion_label,
            "expansion_reason": "reason",
            "expansion_confidence": "medium_high" if expansion_label == "expansion_positive_should_join_persona_04" else "high",
            "manually_reviewed": True,
            "reviewer_note": "reviewed",
            "should_join_persona_04": expansion_label == "expansion_positive_should_join_persona_04",
            "should_remain_persona_01_parent": expansion_label == "expansion_parent_should_stay_persona_01",
            "should_block_persona_04_expansion": expansion_label == "expansion_hard_negative_block",
            "hard_negative_subtype": subtype,
        }
    )
    return row


class ReconciliationSignoffExpansionAnchorSetTests(unittest.TestCase):
    """Validate basic expansion anchor-set integrity rules."""

    def test_validator_accepts_balanced_expansion_set(self) -> None:
        rows: list[dict[str, object]] = []
        sources = [f"source_{idx}" for idx in range(12)]
        for idx in range(50):
            rows.append(_row("expansion_positive_should_join_persona_04", idx, sources[idx % len(sources)]))
        subtypes = [
            "setup_configuration_support",
            "product_helpdesk_or_feature_limitation",
            "docs_tutorial_or_formula_help",
            "ui_bug_or_script_error",
        ]
        for idx in range(25):
            rows.append(_row("expansion_hard_negative_block", idx, sources[idx % len(sources)], subtypes[idx % len(subtypes)]))
        for idx in range(25):
            rows.append(_row("expansion_parent_should_stay_persona_01", idx, sources[idx % len(sources)]))
        for idx in range(15):
            rows.append(_row("expansion_ambiguous_do_not_anchor", idx, sources[idx % len(sources)]))
        expansion_df = pd.DataFrame(rows)
        self.assertEqual(validate_expansion_anchor_set_df(expansion_df), [])

    def test_validator_rejects_baseline_persona04_in_positive_rows(self) -> None:
        rows: list[dict[str, object]] = []
        sources = [f"source_{idx}" for idx in range(12)]
        for idx in range(50):
            row = _row("expansion_positive_should_join_persona_04", idx, sources[idx % len(sources)])
            if idx == 0:
                row["current_persona_id"] = "persona_04"
            rows.append(row)
        subtypes = [
            "setup_configuration_support",
            "product_helpdesk_or_feature_limitation",
            "docs_tutorial_or_formula_help",
            "ui_bug_or_script_error",
        ]
        for idx in range(25):
            rows.append(_row("expansion_hard_negative_block", idx, sources[idx % len(sources)], subtypes[idx % len(subtypes)]))
        for idx in range(25):
            rows.append(_row("expansion_parent_should_stay_persona_01", idx, sources[idx % len(sources)]))
        for idx in range(15):
            rows.append(_row("expansion_ambiguous_do_not_anchor", idx, sources[idx % len(sources)]))
        expansion_df = pd.DataFrame(rows)
        errors = validate_expansion_anchor_set_df(expansion_df)
        self.assertTrue(any("baseline persona_04 rows appear in expansion_positive set" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
