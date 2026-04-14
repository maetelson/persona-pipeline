"""Run one minimal live OpenAI call through the production labeler client path."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.labeling.llm_labeler import debug_openai_labeler_call
from src.utils.io import load_yaml
from src.utils.run_helpers import load_dotenv


def _env_flag(*keys: str, default: bool = False) -> bool:
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value.lower() == "true"
    return default


def _first_non_empty_env(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return default


def main() -> None:
    load_dotenv(ROOT / ".env")
    codebook = load_yaml(ROOT / "config" / "codebook.yaml")
    labeling_policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
    llm_config = {
        "enabled": _env_flag("ENABLE_LLM_LABELER", "LLM_LABELER_ENABLED", default=False),
        "dry_run": _env_flag("LLM_DRY_RUN", "LLM_LABELER_DRY_RUN", "LABELING_DRY_RUN", default=False),
        "batch_enabled": _env_flag("ENABLE_BATCH_LABELING", default=False),
        "enable_escalation": _env_flag("ENABLE_LLM_ESCALATION", default=False),
        "target_unknown_only": _env_flag("LLM_TARGET_UNKNOWN_ONLY", default=True),
        "cache_enabled": _env_flag("LLM_CACHE_ENABLED", default=True),
        "min_confidence": float(os.getenv("LLM_LABELER_MIN_CONFIDENCE", "0.72")),
        "model_primary": _first_non_empty_env("LLM_MODEL_PRIMARY", "LLM_MODEL", "OPENAI_MODEL", default="gpt-5.4-mini"),
        "model_escalation": _first_non_empty_env("LLM_MODEL_ESCALATION", default="gpt-5.4-mini"),
        "max_output_tokens": int(os.getenv("MAX_LLM_OUTPUT_TOKENS", "120")),
        "prompt_cache_key": _first_non_empty_env("PROMPT_CACHE_KEY", default="persona-label-v1"),
        "prompt_cache_retention": _first_non_empty_env("PROMPT_CACHE_RETENTION", default="session"),
        "backend": _first_non_empty_env("LLM_OPENAI_BACKEND", default="http"),
        "timeout_seconds": int(os.getenv("LLM_LABELER_TIMEOUT_SECONDS", "45")),
        "cache_path": ROOT / "data" / "labeled" / "llm_response_cache.jsonl",
        "codebook": codebook,
        "policy": labeling_policy,
    }

    result = debug_openai_labeler_call(llm_config)
    output = {
        "success": bool(result.get("success", False)),
        "model": str(result.get("model", "") or ""),
        "runtime": dict(result.get("runtime", {}) or {}),
        "response_id": str(result.get("response_id", "") or ""),
        "request_id": str(result.get("request_id", "") or ""),
        "usage_present": bool(result.get("usage_present", False)),
        "usage": dict(result.get("usage", {}) or {}),
        "duration_ms": int(result.get("duration_ms", 0) or 0),
        "endpoint_used": str(result.get("endpoint_used", "") or ""),
        "error_class": str(result.get("error_class", "") or ""),
        "error": str(result.get("error", "") or ""),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    raise SystemExit(0 if output["success"] else 1)


if __name__ == "__main__":
    main()
