"""Tests for the reconciliation/signoff anchor-set validator."""

from __future__ import annotations

import unittest

import pandas as pd

from src.analysis.reconciliation_signoff_anchor_set import REQUIRED_COLUMNS, validate_anchor_set_df


def _row(anchor_label: str, idx: int, source: str, subtype: str = "") -> dict[str, object]:
    row = {column: "" for column in REQUIRED_COLUMNS}
    row.update(
        {
            "episode_id": f"{source}::{anchor_label}::{idx}",
            "source": source,
            "source_url": f"https://example.com/{idx}",
            "current_persona_id": "persona_04" if anchor_label == "anchor_positive_reconciliation_signoff" else "persona_01",
            "current_cluster_signature": "sig",
            "curation_source_pool": "test_pool",
            "normalized_episode": "episode",
            "business_question": "question",
            "bottleneck_text": "bottleneck",
            "desired_output": "output",
            "pain_codes": "P_DATA_QUALITY" if anchor_label == "anchor_positive_reconciliation_signoff" else "",
            "question_codes": "Q_VALIDATE_NUMBERS" if anchor_label == "anchor_positive_reconciliation_signoff" else "",
            "output_codes": "xlsx_report",
            "workflow_stage": "validation" if anchor_label == "anchor_positive_reconciliation_signoff" else "reporting",
            "analysis_goal": "validate_numbers" if anchor_label == "anchor_positive_reconciliation_signoff" else "report_speed",
            "bottleneck_type": "data_quality" if anchor_label == "anchor_positive_reconciliation_signoff" else "manual_reporting",
            "trust_validation_need": "high" if anchor_label == "anchor_positive_reconciliation_signoff" else "low",
            "anchor_label": anchor_label,
            "anchor_reason": "reason",
            "anchor_confidence": "high" if anchor_label != "non_anchor_ambiguous" else "medium",
            "manually_reviewed": True,
            "reviewer_note": "reviewed",
            "should_anchor_persona_04": anchor_label == "anchor_positive_reconciliation_signoff",
            "should_block_persona_04_anchor": anchor_label == "anchor_hard_negative",
            "should_remain_persona_01_parent": anchor_label == "anchor_parent_reporting_packager",
            "hard_negative_subtype": subtype,
        }
    )
    return row


class ReconciliationSignoffAnchorSetTests(unittest.TestCase):
    """Validate basic anchor-set integrity rules."""

    def test_validator_accepts_balanced_anchor_set(self) -> None:
        rows: list[dict[str, object]] = []
        sources = [f"source_{idx}" for idx in range(12)]
        for idx in range(50):
            rows.append(_row("anchor_positive_reconciliation_signoff", idx, sources[idx % len(sources)]))
        subtypes = [
            "setup_configuration_support",
            "product_helpdesk_or_feature_limitation",
            "docs_tutorial_or_formula_help",
            "ui_bug_or_script_error",
        ]
        for idx in range(30):
            rows.append(_row("anchor_hard_negative", idx, sources[idx % len(sources)], subtypes[idx % len(subtypes)]))
        for idx in range(30):
            rows.append(_row("anchor_parent_reporting_packager", idx, sources[idx % len(sources)]))
        for idx in range(20):
            rows.append(_row("non_anchor_ambiguous", idx, sources[idx % len(sources)]))
        anchor_df = pd.DataFrame(rows)
        self.assertEqual(validate_anchor_set_df(anchor_df), [])

    def test_validator_requires_hard_negative_subtype_coverage(self) -> None:
        rows: list[dict[str, object]] = []
        sources = [f"source_{idx}" for idx in range(12)]
        for idx in range(50):
            rows.append(_row("anchor_positive_reconciliation_signoff", idx, sources[idx % len(sources)]))
        for idx in range(30):
            rows.append(_row("anchor_hard_negative", idx, sources[idx % len(sources)], "setup_configuration_support"))
        for idx in range(30):
            rows.append(_row("anchor_parent_reporting_packager", idx, sources[idx % len(sources)]))
        for idx in range(20):
            rows.append(_row("non_anchor_ambiguous", idx, sources[idx % len(sources)]))
        anchor_df = pd.DataFrame(rows)
        errors = validate_anchor_set_df(anchor_df)
        self.assertTrue(any("hard negative subtype coverage is incomplete" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
