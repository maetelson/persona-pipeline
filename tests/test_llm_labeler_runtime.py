"""Tests for LLM labeler runtime diagnostics and smoke-call helpers."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from src.labeling.llm_labeler import DEFAULT_MAX_OUTPUT_TOKENS, debug_openai_labeler_call, llm_runtime_snapshot, resolve_llm_runtime

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


if __name__ == "__main__":
    unittest.main()
