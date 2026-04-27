"""Diagnostics-only tests for Phase 1 deck-ready denominator eligibility fields."""

from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.analysis.deck_ready_denominator_eligibility import build_deck_ready_denominator_eligibility_outputs


ROOT_DIR = Path(__file__).resolve().parents[1]


class DeckReadyDenominatorEligibilityTests(unittest.TestCase):
    """Validate diagnostics-only denominator eligibility classification and invariants."""

    def _build_outputs(self, labeled_rows: list[dict[str, object]], episode_rows: list[dict[str, object]]) -> dict[str, object]:
        labeled_df = pd.DataFrame(labeled_rows)
        episodes_df = pd.DataFrame(episode_rows)
        persona_assignments_df = pd.DataFrame([{"episode_id": "ep-core", "persona_id": "persona_01"}])
        source_balance_audit_df = pd.DataFrame(
            [
                {"source": "stackoverflow", "source_tier": "supporting_validation_source"},
                {"source": "reddit", "source_tier": "supporting_validation_source"},
                {"source": "github_discussions", "source_tier": "supporting_validation_source"},
            ]
        )
        return build_deck_ready_denominator_eligibility_outputs(
            labeled_df=labeled_df,
            episodes_df=episodes_df,
            persona_assignments_df=persona_assignments_df,
            source_balance_audit_df=source_balance_audit_df,
            current_persona_core_coverage_pct=74.5,
        )

    def test_persona_core_rows_are_always_denominator_eligible(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-core",
                    "persona_core_eligible": True,
                    "labelability_status": "high_signal",
                    "labelability_reason": "grounded",
                    "pain_codes": ["P_MANUAL_REPORTING"],
                    "question_codes": ["Q_REPORT_SPEED"],
                    "output_codes": ["O_XLSX"],
                    "label_reason": "clear reporting burden",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-core",
                    "source": "stackoverflow",
                    "normalized_episode": "We spend hours rebuilding a weekly report in spreadsheets.",
                    "business_question": "Why is recurring reporting still manual?",
                    "bottleneck_text": "manual reporting delay",
                    "desired_output": "validated export for stakeholders",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertTrue(bool(row["deck_ready_denominator_eligible"]))
        self.assertEqual(str(row["denominator_eligibility_category"]), "persona_core_evidence")
        self.assertTrue(bool(outputs["summary"]["persona_core_rows_always_eligible_check"]))

    def test_business_reporting_pain_is_retained(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-business",
                    "persona_core_eligible": False,
                    "labelability_status": "high_signal",
                    "labelability_reason": "clear workflow context",
                    "pain_codes": ["P_MANUAL_REPORTING"],
                    "question_codes": ["Q_REPORT_SPEED"],
                    "output_codes": ["O_XLSX"],
                    "label_reason": "reporting burden",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-business",
                    "source": "reddit",
                    "normalized_episode": "Dashboard exports still require manual cleanup before stakeholder delivery.",
                    "business_question": "How do we reduce reporting handoff time?",
                    "bottleneck_text": "manual reporting burden",
                    "desired_output": "stakeholder-ready report delivery",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertTrue(bool(row["deck_ready_denominator_eligible"]))
        self.assertEqual(str(row["denominator_eligibility_category"]), "denominator_eligible_business_non_core")

    def test_stakeholder_metric_mismatch_is_retained(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-mismatch",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "validation workflow",
                    "pain_codes": ["P_DATA_QUALITY"],
                    "question_codes": ["Q_VALIDATE_NUMBERS"],
                    "output_codes": ["O_VALIDATED_DATASET"],
                    "label_reason": "numbers mismatch",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-mismatch",
                    "source": "stackoverflow",
                    "normalized_episode": "Stakeholders are questioning a dashboard total mismatch and need reconciliation.",
                    "business_question": "Why do our totals not reconcile?",
                    "bottleneck_text": "metric mismatch",
                    "desired_output": "validated numbers for leadership",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertTrue(bool(row["deck_ready_denominator_eligible"]))
        self.assertGreaterEqual(int(row["business_context_signal_count"]), 2)

    def test_pure_syntax_debug_rows_are_excluded(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-syntax",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "technical row",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "formula debug",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-syntax",
                    "source": "stackoverflow",
                    "normalized_episode": "DAX syntax error and parser exception when evaluating COUNTROWS.",
                    "business_question": "",
                    "bottleneck_text": "syntax error",
                    "desired_output": "",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertFalse(bool(row["deck_ready_denominator_eligible"]))
        self.assertEqual(str(row["denominator_eligibility_category"]), "syntax_formula_debug_noise")
        self.assertTrue(str(row["denominator_exclusion_reason"]))

    def test_setup_auth_api_and_deploy_noise_is_excluded(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-setup",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "support row",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "setup help",
                },
                {
                    "episode_id": "ep-api",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "api row",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "api help",
                },
                {
                    "episode_id": "ep-server",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "deploy row",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "server help",
                },
            ],
            episode_rows=[
                {
                    "episode_id": "ep-setup",
                    "source": "github_discussions",
                    "normalized_episode": "Login permission denied after setup and OAuth configuration.",
                    "business_question": "",
                    "bottleneck_text": "permission issue",
                    "desired_output": "",
                },
                {
                    "episode_id": "ep-api",
                    "source": "github_discussions",
                    "normalized_episode": "API request and SDK integration debug with endpoint response error.",
                    "business_question": "",
                    "bottleneck_text": "api error",
                    "desired_output": "",
                },
                {
                    "episode_id": "ep-server",
                    "source": "github_discussions",
                    "normalized_episode": "Server deployment config failed after docker runtime change.",
                    "business_question": "",
                    "bottleneck_text": "deploy failure",
                    "desired_output": "",
                },
            ],
        )
        lookup = outputs["rows_df"].set_index("episode_id")
        self.assertEqual(str(lookup.loc["ep-setup", "denominator_eligibility_category"]), "setup_auth_permission_noise")
        self.assertEqual(str(lookup.loc["ep-api", "denominator_eligibility_category"]), "api_sdk_debug_noise")
        self.assertEqual(str(lookup.loc["ep-server", "denominator_eligibility_category"]), "server_deploy_config_noise")
        self.assertFalse(lookup["deck_ready_denominator_eligible"].astype(bool).any())

    def test_source_specific_noise_patterns_classify_correctly(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-adobe",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "implementation support",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "tracking implementation",
                },
                {
                    "episode_id": "ep-domo",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "domo support",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "connector config help",
                },
            ],
            episode_rows=[
                {
                    "episode_id": "ep-adobe",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "Tag manager implementation rule and evar server calls keep failing.",
                    "business_question": "",
                    "bottleneck_text": "implementation support",
                    "desired_output": "",
                },
                {
                    "episode_id": "ep-domo",
                    "source": "domo_community_forum",
                    "normalized_episode": "Beast mode and Magic ETL setup break after connector config changes.",
                    "business_question": "",
                    "bottleneck_text": "support setup",
                    "desired_output": "",
                },
            ],
        )
        lookup = outputs["rows_df"].set_index("episode_id")
        self.assertEqual(str(lookup.loc["ep-adobe", "denominator_eligibility_category"]), "source_specific_support_noise")
        self.assertEqual(str(lookup.loc["ep-domo", "denominator_eligibility_category"]), "source_specific_support_noise")

    def test_generic_low_signal_is_not_used_when_specific_noise_applies(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-stack",
                    "persona_core_eligible": False,
                    "labelability_status": "low_signal",
                    "labelability_reason": "fallback prone",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "formula issue",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-stack",
                    "source": "stackoverflow",
                    "normalized_episode": "Calculated column formula error in a matrix visual measure with dax syntax.",
                    "business_question": "",
                    "bottleneck_text": "formula error",
                    "desired_output": "",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["denominator_eligibility_category"]), "syntax_formula_debug_noise")
        self.assertFalse(bool(row["deck_ready_denominator_eligible"]))

    def test_vendor_and_career_noise_are_excluded(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-vendor",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "feature request",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "vendor ask",
                },
                {
                    "episode_id": "ep-career",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "training row",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "career row",
                },
            ],
            episode_rows=[
                {
                    "episode_id": "ep-vendor",
                    "source": "reddit",
                    "normalized_episode": "Feature request: please add this to the roadmap and next release notes.",
                    "business_question": "",
                    "bottleneck_text": "",
                    "desired_output": "",
                },
                {
                    "episode_id": "ep-career",
                    "source": "reddit",
                    "normalized_episode": "Which certification course should I take for this analytics career path?",
                    "business_question": "",
                    "bottleneck_text": "",
                    "desired_output": "",
                },
            ],
        )
        lookup = outputs["rows_df"].set_index("episode_id")
        self.assertEqual(
            str(lookup.loc["ep-vendor", "denominator_eligibility_category"]),
            "vendor_announcement_or_feature_request_only",
        )
        self.assertEqual(
            str(lookup.loc["ep-career", "denominator_eligibility_category"]),
            "career_training_certification_noise",
        )

    def test_ambiguous_rows_are_included_and_flagged(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-ambiguous",
                    "persona_core_eligible": False,
                    "labelability_status": "medium_signal",
                    "labelability_reason": "mixed row",
                    "pain_codes": ["P_DATA_QUALITY"],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "mixed technical and business signals",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-ambiguous",
                    "source": "stackoverflow",
                    "normalized_episode": "SQL error while reconciling a stakeholder metric mismatch in the dashboard export.",
                    "business_question": "Why do the numbers mismatch for stakeholders?",
                    "bottleneck_text": "metric mismatch and sql error",
                    "desired_output": "validated report export",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertTrue(bool(row["deck_ready_denominator_eligible"]))
        self.assertEqual(str(row["denominator_eligibility_category"]), "ambiguous_review_bucket")
        self.assertTrue(bool(row["ambiguity_flag"]))

    def test_mixed_business_and_technical_rows_become_ambiguous(self) -> None:
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-mixed",
                    "persona_core_eligible": False,
                    "labelability_status": "low_signal",
                    "labelability_reason": "mixed support and reporting context",
                    "pain_codes": ["P_DATA_QUALITY"],
                    "question_codes": ["Q_VALIDATE_NUMBERS"],
                    "output_codes": ["O_VALIDATED_DATASET"],
                    "label_reason": "stakeholder-facing reconciliation with query error",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-mixed",
                    "source": "adobe_analytics_community",
                    "normalized_episode": "Stakeholder-facing report delivery is blocked by a query error while reconciling a number mismatch.",
                    "business_question": "How do we reconcile stakeholder-facing numbers before weekly reporting?",
                    "bottleneck_text": "reconciliation with query error",
                    "desired_output": "validated export for leadership update",
                    "evidence_snippet": "weekly reporting and stakeholder delivery are blocked",
                    "segmentation_note": "mixed business and technical context",
                }
            ],
        )
        row = outputs["rows_df"].iloc[0]
        self.assertEqual(str(row["denominator_eligibility_category"]), "ambiguous_review_bucket")
        self.assertTrue(bool(row["deck_ready_denominator_eligible"]))
        self.assertTrue(bool(row["ambiguity_flag"]))

    def test_output_row_count_matches_labeled_rows_and_current_counts_do_not_change(self) -> None:
        labeled_df = pd.read_parquet(ROOT_DIR / "data" / "labeled" / "labeled_episodes.parquet")
        overview_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
        metrics = dict(zip(overview_df["metric"].astype(str), overview_df["value"]))
        outputs = self._build_outputs(
            labeled_rows=[
                {
                    "episode_id": "ep-a",
                    "persona_core_eligible": False,
                    "labelability_status": "low_signal",
                    "labelability_reason": "thin text",
                    "pain_codes": [],
                    "question_codes": [],
                    "output_codes": [],
                    "label_reason": "thin",
                }
            ],
            episode_rows=[
                {
                    "episode_id": "ep-a",
                    "source": "reddit",
                    "normalized_episode": "Hi",
                    "business_question": "",
                    "bottleneck_text": "",
                    "desired_output": "",
                }
            ],
        )
        self.assertEqual(len(outputs["rows_df"]), 1)
        self.assertTrue((ROOT_DIR / "data" / "raw").exists())
        self.assertEqual(len(labeled_df), 12674)
        self.assertEqual(str(metrics["persona_readiness_state"]), "reviewable_but_not_deck_ready")
        self.assertEqual(int(float(metrics["final_usable_persona_count"])), 3)
        self.assertEqual(int(float(metrics["production_ready_persona_count"])), 3)
        self.assertEqual(int(float(metrics["review_ready_persona_count"])), 1)
        self.assertEqual(int(float(metrics["deck_ready_claim_eligible_persona_count"])), 4)
