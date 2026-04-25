"""Tests for LLM labeler runtime diagnostics and smoke-call helpers."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

from src.labeling.llm_labeler import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    _build_target_rows,
    enrich_with_llm_labels,
    debug_openai_labeler_call,
    llm_runtime_snapshot,
    resolve_llm_runtime,
    should_send_to_llm,
)

ROOT = Path(__file__).resolve().parents[1]
_EXPERIMENT_SPEC = importlib.util.spec_from_file_location(
    "run_18_prove_cache_vs_live_calls",
    ROOT / "run" / "experiments" / "18_prove_cache_vs_live_calls.py",
)
if _EXPERIMENT_SPEC is None or _EXPERIMENT_SPEC.loader is None:
    raise RuntimeError("Unable to load run/experiments/18_prove_cache_vs_live_calls.py for tests.")
_EXPERIMENT_MODULE = importlib.util.module_from_spec(_EXPERIMENT_SPEC)
_EXPERIMENT_SPEC.loader.exec_module(_EXPERIMENT_MODULE)
_base_llm_config = _EXPERIMENT_MODULE._base_llm_config


class LlmLabelerRuntimeTests(unittest.TestCase):
    """Verify runtime scope reporting and smoke-call diagnostics."""

    def test_resolve_llm_runtime_captures_project_scope_and_masks_key(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
                "OPENAI_PROJECT": "proj_labeler",
                "OPENAI_ORG": "org_internal",
                "OPENAI_BASE_URL": "https://example.test/v1",
            },
            clear=False,
        ):
            runtime = resolve_llm_runtime({"enabled": True, "model_primary": "gpt-5.4-mini", "backend": "http"})
            snapshot = llm_runtime_snapshot(runtime)

        self.assertEqual(runtime["mode"], "direct")
        self.assertEqual(runtime["project"], "proj_labeler")
        self.assertEqual(runtime["organization"], "org_internal")
        self.assertEqual(runtime["base_url"], "https://example.test/v1")
        self.assertEqual(snapshot["responses_endpoint"], "https://example.test/v1/responses")
        self.assertTrue(snapshot["api_key_project_scoped"])
        self.assertIn("...", str(snapshot["api_key_masked"]))
        self.assertTrue(str(snapshot["api_key_masked"]).endswith("9012"))

    def test_debug_openai_labeler_call_reports_response_usage(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
            },
            clear=False,
        ):
            with patch(
                "src.labeling.llm_labeler._call_llm_labeler",
                return_value={
                    "parsed": {"confidence": 0.0, "reason": "debug_smoke"},
                    "usage": {"usage_input_tokens": 11, "usage_output_tokens": 7, "usage_total_tokens": 18},
                    "usage_present": True,
                    "response_id": "resp_123",
                    "request_id": "req_123",
                    "endpoint_used": "https://api.openai.com/v1/responses",
                    "duration_ms": 321,
                },
            ):
                result = debug_openai_labeler_call({"enabled": True, "model_primary": "gpt-5.4-mini", "backend": "http"})

        self.assertTrue(result["success"])
        self.assertEqual(result["response_id"], "resp_123")
        self.assertEqual(result["request_id"], "req_123")
        self.assertTrue(result["usage_present"])
        self.assertEqual(int(result["usage"]["usage_total_tokens"]), 18)

    def test_debug_openai_labeler_call_fails_when_runtime_is_dry_run(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "LLM_DRY_RUN": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
            },
            clear=False,
        ):
            result = debug_openai_labeler_call({"enabled": True, "dry_run": True, "model_primary": "gpt-5.4-mini", "backend": "http"})

        self.assertFalse(result["success"])
        self.assertEqual(result["error_class"], "RuntimeConfigurationError")
        self.assertIn("runtime mode dry_run", result["error"])

    def test_default_max_output_tokens_is_consistent(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
            },
            clear=False,
        ):
            runtime = resolve_llm_runtime({"enabled": True, "model_primary": "gpt-5.4-mini", "backend": "http"})
            base_config = _base_llm_config(codebook={}, labeling_policy={})

        self.assertEqual(DEFAULT_MAX_OUTPUT_TOKENS, 80)
        self.assertEqual(int(runtime["max_output_tokens"]), DEFAULT_MAX_OUTPUT_TOKENS)
        self.assertEqual(int(base_config["max_output_tokens"]), DEFAULT_MAX_OUTPUT_TOKENS)

    def test_cache_only_mode_uses_cache_without_live_runtime_disable(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "false",
                "LLM_CACHE_ONLY": "true",
                "OPENAI_API_KEY": "",
            },
            clear=False,
        ):
            runtime = resolve_llm_runtime({"enabled": False, "cache_only": True, "cache_enabled": True})

        self.assertEqual(runtime["mode"], "cache_only")
        self.assertEqual(runtime["skip_reason"], "")
        self.assertTrue(bool(runtime["cache_enabled"]))

    def test_target_unknown_only_skips_fully_resolved_rule_rows(self) -> None:
        labeled_df = pd.DataFrame(
            [
                {
                    "episode_id": "ep_resolved",
                    "role_codes": "R_ANALYST",
                    "moment_codes": "M_REPORTING",
                    "question_codes": "Q_VALIDATE_NUMBERS",
                    "pain_codes": "P_DATA_QUALITY",
                    "env_codes": "E_SPREADSHEET",
                    "workaround_codes": "W_MANUAL",
                    "output_codes": "O_XLSX",
                    "fit_code": "F_STRONG",
                    "label_confidence": 0.9,
                    "label_reason": "rule",
                    "rule_hit_count": 7,
                    "rule_core_known_count": 4,
                    "rule_unknown_family_count": 0,
                    "rule_coarse_match": "",
                    "labelability_status": "labelable",
                    "labelability_score": 0.9,
                    "labelability_reason": "ready",
                    "persona_core_eligible": True,
                }
            ]
        )

        targets = _build_target_rows(
            labeled_df=labeled_df,
            threshold=0.72,
            target_unknown_only=True,
            episode_lookup=pd.DataFrame(),
        )

        self.assertEqual(targets, {})

    def test_persistent_cache_hit_avoids_live_llm_call(self) -> None:
        episodes_df = pd.DataFrame(
            [
                {
                    "episode_id": "ep_cache",
                    "source": "reddit",
                    "raw_id": "r1",
                    "normalized_episode": "Need to validate dashboard numbers before sharing.",
                    "evidence_snippet": "dashboard numbers do not match source of truth",
                    "role_clue": "analyst",
                    "work_moment": "reporting",
                    "business_question": "which number should I trust",
                    "tool_env": "excel",
                    "bottleneck_text": "numbers do not match",
                    "workaround_text": "manual comparison",
                    "desired_output": "xlsx_report",
                    "product_fit": "strong",
                }
            ]
        )
        labeled_df = pd.DataFrame(
            [
                {
                    "episode_id": "ep_cache",
                    "role_codes": "R_ANALYST",
                    "moment_codes": "M_REPORTING",
                    "question_codes": "Q_VALIDATE_NUMBERS",
                    "pain_codes": "P_DATA_QUALITY",
                    "env_codes": "unknown",
                    "workaround_codes": "W_MANUAL",
                    "output_codes": "O_XLSX",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.61,
                    "label_reason": "rule",
                    "rule_hit_count": 3,
                    "rule_core_known_count": 3,
                    "rule_unknown_family_count": 1,
                    "rule_coarse_match": "",
                    "labelability_status": "labelable",
                    "labelability_score": 0.8,
                    "labelability_reason": "ready",
                    "persona_core_eligible": True,
                }
            ]
        )
        cache_path = ROOT / "data" / "test_tmp_llm_cache.jsonl"
        if cache_path.exists():
            cache_path.unlink()
        config = {
            "enabled": True,
            "model_primary": "gpt-5.4-mini",
            "backend": "http",
            "cache_enabled": True,
            "cache_path": cache_path,
            "target_unknown_only": True,
            "codebook": {
                "role_keywords": {"R_ANALYST": ["analyst"]},
                "moment_keywords": {"M_REPORTING": ["report"]},
                "question_codes": {"Q_VALIDATE_NUMBERS": ["validate"]},
                "pain_codes": {"P_DATA_QUALITY": ["quality"]},
                "env_codes": {"E_SPREADSHEET": ["excel"]},
                "workaround_codes": {"W_MANUAL": ["manual"]},
                "output_codes": {"O_XLSX": ["xlsx"]},
                "fit_keywords": {"F_REVIEW": ["review"]},
            },
            "policy": {},
        }
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
            },
            clear=False,
        ):
            with patch(
                "src.labeling.llm_labeler.load_jsonl_cache",
                return_value={
                    "cache_key_ep_cache": {
                        "env_codes": "E_SPREADSHEET",
                        "label_confidence": 0.88,
                        "label_reason": "llm:cache_hit",
                    }
                },
            ) as load_cache_mock:
                with patch("src.labeling.llm_labeler._call_llm_labeler") as call_mock:
                    with patch(
                        "src.labeling.llm_labeler._cache_key_for_prompt",
                        return_value="cache_key_ep_cache",
                    ):
                        result_df, audit_df = enrich_with_llm_labels(episodes_df, labeled_df, config=config)

        self.assertTrue(load_cache_mock.called)
        self.assertFalse(call_mock.called)
        self.assertEqual(str(result_df.iloc[0]["env_codes"]), "E_SPREADSHEET")
        self.assertEqual(str(audit_df.iloc[0]["llm_status"]), "cache_hit")
        self.assertFalse(bool(audit_df.iloc[0]["was_llm_called"]))

    def test_low_signal_generic_row_stays_excluded(self) -> None:
        row = pd.Series(
            {
                "episode_id": "ep_generic",
                "role_codes": "R_ANALYST",
                "moment_codes": "unknown",
                "question_codes": "Q_AUTOMATE_WORKFLOW",
                "pain_codes": "unknown",
                "env_codes": "unknown",
                "workaround_codes": "unknown",
                "output_codes": "O_AUTOMATION_JOB",
                "fit_code": "F_REVIEW",
                "label_confidence": 0.66,
                "rule_hit_count": 1,
                "rule_unknown_family_count": 4,
                "labelability_status": "low_signal",
            }
        )
        episode_row = pd.Series(
            {
                "normalized_episode": "Need ideas for creating a card in Domo for some orders and services.",
                "business_question": "How should I build this card?",
                "bottleneck_text": "",
                "desired_output": "dashboard_update",
                "tool_env": "domo",
            }
        )

        should_target, reason = should_send_to_llm(row=row, threshold=0.72, episode_row=episode_row)
        self.assertFalse(should_target)
        self.assertEqual(reason, "low_signal_input")

    def test_low_signal_setup_noise_stays_excluded(self) -> None:
        row = pd.Series(
            {
                "episode_id": "ep_setup",
                "role_codes": "unknown",
                "moment_codes": "unknown",
                "question_codes": "Q_AUTOMATE_WORKFLOW",
                "pain_codes": "unknown",
                "env_codes": "unknown",
                "workaround_codes": "unknown",
                "output_codes": "O_AUTOMATION_JOB",
                "fit_code": "F_REVIEW",
                "label_confidence": 0.61,
                "rule_hit_count": 1,
                "rule_unknown_family_count": 4,
                "labelability_status": "low_signal",
            }
        )
        episode_row = pd.Series(
            {
                "normalized_episode": "Training question about best practices tutorial for certification exam and how to implement reporting.",
                "business_question": "How do I implement this correctly?",
                "bottleneck_text": "",
                "desired_output": "automation_job",
                "tool_env": "adobe",
            }
        )

        should_target, reason = should_send_to_llm(row=row, threshold=0.72, episode_row=episode_row)
        self.assertFalse(should_target)
        self.assertEqual(reason, "low_signal_input")

    def test_low_signal_metric_discrepancy_row_is_rescued(self) -> None:
        row = pd.Series(
            {
                "episode_id": "ep_rescue",
                "role_codes": "unknown",
                "moment_codes": "unknown",
                "question_codes": "Q_AUTOMATE_WORKFLOW",
                "pain_codes": "unknown",
                "env_codes": "unknown",
                "workaround_codes": "unknown",
                "output_codes": "O_AUTOMATION_JOB",
                "fit_code": "F_REVIEW",
                "label_confidence": 0.61,
                "rule_hit_count": 1,
                "rule_unknown_family_count": 4,
                "labelability_status": "low_signal",
            }
        )
        episode_row = pd.Series(
            {
                "normalized_episode": "Dashboard totals do not match the exported numbers and the blocked review cannot finish until we explain the difference.",
                "business_question": "Why did this change in the report?",
                "bottleneck_text": "cannot reconcile inconsistent numbers",
                "desired_output": "stakeholder_ready_export_or_packaged_report",
                "tool_env": "looker_studio",
            }
        )

        should_target, reason = should_send_to_llm(row=row, threshold=0.72, episode_row=episode_row)
        self.assertTrue(should_target)
        self.assertEqual(reason, "low_signal_discrepancy_rescue:totals_do_not_match")

    def test_low_signal_discrepancy_rescue_is_tracked_in_audit(self) -> None:
        episodes_df = pd.DataFrame(
            [
                {
                    "episode_id": "ep_rescue",
                    "source": "google_developer_forums",
                    "raw_id": "g1",
                    "normalized_episode": "Dashboard totals do not match the exported numbers and the blocked review cannot finish until we explain the difference.",
                    "evidence_snippet": "dashboard totals do not match exported numbers",
                    "role_clue": "analyst",
                    "work_moment": "weekly review",
                    "business_question": "Why did this change in the report?",
                    "tool_env": "looker_studio",
                    "bottleneck_text": "cannot reconcile inconsistent numbers",
                    "workaround_text": "",
                    "desired_output": "stakeholder_ready_export_or_packaged_report",
                    "product_fit": "strong",
                }
            ]
        )
        labeled_df = pd.DataFrame(
            [
                {
                    "episode_id": "ep_rescue",
                    "role_codes": "unknown",
                    "moment_codes": "unknown",
                    "question_codes": "Q_AUTOMATE_WORKFLOW",
                    "pain_codes": "unknown",
                    "env_codes": "unknown",
                    "workaround_codes": "unknown",
                    "output_codes": "O_AUTOMATION_JOB",
                    "fit_code": "F_REVIEW",
                    "label_confidence": 0.61,
                    "label_reason": "rule",
                    "rule_hit_count": 1,
                    "rule_core_known_count": 1,
                    "rule_unknown_family_count": 4,
                    "rule_coarse_match": "",
                    "labelability_status": "low_signal",
                    "labelability_score": 0.4,
                    "labelability_reason": "thin",
                    "persona_core_eligible": True,
                }
            ]
        )
        config = {
            "enabled": True,
            "dry_run": True,
            "model_primary": "gpt-5.4-mini",
            "backend": "http",
            "target_unknown_only": True,
            "codebook": {},
            "policy": {},
        }

        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "LLM_DRY_RUN": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
            },
            clear=False,
        ):
            _, audit_df = enrich_with_llm_labels(episodes_df, labeled_df, config=config)

        self.assertEqual(len(audit_df), 1)
        self.assertTrue(bool(audit_df.iloc[0]["was_llm_targeted"]))
        self.assertTrue(bool(audit_df.iloc[0]["low_signal_discrepancy_rescued"]))
        self.assertEqual(str(audit_df.iloc[0]["discrepancy_rescue_reason"]), "totals_do_not_match")
        self.assertEqual(str(audit_df.iloc[0]["original_label_targeting_reason"]), "low_signal_input")
        self.assertIn("low_signal_discrepancy_rescue", str(audit_df.iloc[0]["llm_target_reason"]))

    def test_default_targeting_threshold_is_unchanged(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENABLE_LLM_LABELER": "true",
                "OPENAI_API_KEY": "sk-proj-1234567890abcdefghijklmnop9012",
            },
            clear=False,
        ):
            runtime = resolve_llm_runtime({"enabled": True, "model_primary": "gpt-5.4-mini", "backend": "http"})

        self.assertEqual(float(runtime["threshold"]), 0.72)


if __name__ == "__main__":
    unittest.main()
