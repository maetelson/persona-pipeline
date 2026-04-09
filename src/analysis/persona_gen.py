"""LLM-assisted persona generation with grounded deterministic fallback."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.labeling.prompt_payload import truncate_text
from src.utils.llm_cache import (
    append_jsonl_cache,
    build_prompt_cache_key,
    load_jsonl_cache,
    parse_responses_json,
)

PERSONA_SCHEMA = {
    "persona_name": "",
    "one_line_summary": "",
    "core_demographic": "",
    "top_pain_points": [],
    "co_occurring_needs": [],
    "example_quotes": [],
    "opportunity": "",
}

SYSTEM_PROMPT = "Grounded persona JSON only. Use evidence only. No invented facts."
PERSONA_SCHEMA_ALIASES = {
    "persona_name": "n",
    "one_line_summary": "s",
    "core_demographic": "d",
    "top_pain_points": "p",
    "co_occurring_needs": "c",
    "example_quotes": "q",
    "opportunity": "o",
}
PERSONA_SCHEMA_ALIAS_REVERSE = {alias: key for key, alias in PERSONA_SCHEMA_ALIASES.items()}
PROFILE_FIELD_ALIASES = {
    "cluster_id": "i",
    "size": "z",
    "top_demographics": "d",
    "top_need_codes": "n",
    "top_outputs": "o",
    "top_envs": "v",
    "representative_texts": "t",
}
PERSONA_SCHEMA_JSON = json.dumps(
    {
        PERSONA_SCHEMA_ALIASES[key]: ([] if isinstance(value, list) else value)
        for key, value in PERSONA_SCHEMA.items()
    },
    ensure_ascii=False,
    separators=(",", ":"),
)

CODE_LABELS = {
    "R_ANALYST": "Analyst",
    "R_MANAGER": "Manager",
    "R_MARKETER": "Marketer",
    "M_REPORTING": "Reporting",
    "M_TRIAGE": "Issue triage",
    "M_VALIDATION": "Validation",
    "M_AUTOMATION": "Automation",
    "Q_REPORT_SPEED": "Faster reporting",
    "Q_VALIDATE_NUMBERS": "Number validation",
    "Q_DIAGNOSE_ISSUE": "Issue diagnosis",
    "Q_AUTOMATE_WORKFLOW": "Workflow automation",
    "P_MANUAL_REPORTING": "Manual reporting",
    "P_DATA_QUALITY": "Data quality",
    "P_TOOL_LIMITATION": "Tool limitation",
    "P_HANDOFF": "Handoff friction",
    "E_SQL_BI": "BI and SQL tools",
    "E_SPREADSHEET": "Spreadsheets",
    "E_WAREHOUSE": "Data warehouse",
    "E_PYTHON": "Python",
    "W_MANUAL": "Manual workaround",
    "W_SCRIPT": "Script workaround",
    "W_SPREADSHEET": "Spreadsheet workaround",
    "O_XLSX": "Excel-ready output",
    "O_DASHBOARD": "Dashboard output",
    "O_AUTOMATION_JOB": "Automation job",
    "F_REVIEW": "Needs review",
    "F_STRONG": "Strong fit",
}


def generate_personas(cluster_profiles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate strict persona objects with LLM when enabled, else deterministic fallback."""
    personas: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    runtime = _resolve_runtime()
    cache_store = load_jsonl_cache(runtime["cache_path"]) if runtime["cache_enabled"] else {}
    response_cache: dict[str, dict[str, Any]] = {}
    for profile in cluster_profiles:
        compact_profile = _compact_profile(profile)
        if runtime["enabled"]:
            try:
                cache_key = build_prompt_cache_key(
                    model=runtime["model"],
                    prompt=json.dumps(compact_profile, ensure_ascii=False, separators=(",", ":")),
                    namespace="persona",
                )
                if cache_key in response_cache:
                    persona = _validate_persona(response_cache[cache_key], profile)
                    status = "run_reuse"
                    reason = "persona_llm_run_reuse"
                elif cache_key in cache_store:
                    persona = _validate_persona(cache_store[cache_key], profile)
                    status = "cache_hit"
                    reason = "persona_llm_cache_hit"
                else:
                    persona = _validate_persona(
                        _call_persona_llm(compact_profile=compact_profile, runtime=runtime),
                        profile,
                    )
                    status = "applied"
                    reason = "persona_llm_applied"
                    response_cache[cache_key] = persona
                    if runtime["cache_enabled"]:
                        cache_store[cache_key] = persona
                        append_jsonl_cache(runtime["cache_path"], cache_key, persona)
            except Exception as exc:  # noqa: BLE001
                persona = _fallback_persona(profile)
                status = "fallback"
                reason = f"persona_llm_failed:{type(exc).__name__}"
        else:
            persona = _fallback_persona(profile)
            status = "fallback"
            reason = runtime["reason"]
        personas.append({**persona, "cluster_id": profile["cluster_id"]})
        audit_rows.append(
            {
                "cluster_id": profile["cluster_id"],
                "llm_called": bool(runtime["enabled"] and status == "applied"),
                "model": runtime["model"],
                "status": status,
                "reason": reason,
            }
        )
    return personas, audit_rows


def _resolve_runtime() -> dict[str, Any]:
    """Resolve persona generation runtime from env without blocking fallback usage."""
    enabled = os.getenv("ENABLE_PERSONA_LLM", "false").strip().lower() == "true"
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = (
        os.getenv("PERSONA_LLM_MODEL", "").strip()
        or os.getenv("OPENAI_MODEL", "").strip()
        or "gpt-5.4-mini"
    )
    if not enabled:
        return {"enabled": False, "api_key": api_key, "model": model, "reason": "persona_llm_disabled", "cache_enabled": True, "cache_path": Path("data") / "analysis" / "persona_llm_cache.jsonl"}
    if not api_key:
        return {"enabled": False, "api_key": "", "model": model, "reason": "persona_llm_no_api_key", "cache_enabled": True, "cache_path": Path("data") / "analysis" / "persona_llm_cache.jsonl"}
    if not model:
        return {"enabled": False, "api_key": api_key, "model": "", "reason": "persona_llm_no_model", "cache_enabled": True, "cache_path": Path("data") / "analysis" / "persona_llm_cache.jsonl"}
    return {
        "enabled": True,
        "api_key": api_key,
        "model": model,
        "reason": "",
        "cache_enabled": os.getenv("ENABLE_PERSONA_LLM_CACHE", "true").strip().lower() == "true",
        "cache_path": Path(os.getenv("PERSONA_LLM_CACHE_PATH", str(Path("data") / "analysis" / "persona_llm_cache.jsonl"))),
    }


def _call_persona_llm(compact_profile: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    """Call OpenAI Responses API for one persona summary."""
    payload = {
        "model": runtime["model"],
        "input": "\n".join(
            [
                SYSTEM_PROMPT,
                f"p={json.dumps(compact_profile, ensure_ascii=False, separators=(',', ':'))}",
                f"s={PERSONA_SCHEMA_JSON}",
            ]
        ),
        "temperature": 0.1,
        "max_output_tokens": int(os.getenv("PERSONA_LLM_MAX_OUTPUT_TOKENS", "180")),
        "text": {"format": {"type": "json_object"}},
    }
    request = Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {runtime['api_key']}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=45) as response:
            raw = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"persona llm http {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("persona llm network error") from exc
    parsed = _expand_persona_aliases(parse_responses_json(raw))
    return parsed


def _compact_profile(profile: dict[str, Any]) -> dict[str, Any]:
    """Shrink cluster profile payload to what the persona generator actually needs."""
    return {
        PROFILE_FIELD_ALIASES["cluster_id"]: profile.get("cluster_id", ""),
        PROFILE_FIELD_ALIASES["size"]: profile.get("size", 0),
        PROFILE_FIELD_ALIASES["top_demographics"]: profile.get("top_demographics", [])[:3],
        PROFILE_FIELD_ALIASES["top_need_codes"]: profile.get("top_need_codes", [])[:5],
        PROFILE_FIELD_ALIASES["top_outputs"]: profile.get("top_outputs", [])[:3],
        PROFILE_FIELD_ALIASES["top_envs"]: profile.get("top_envs", [])[:3],
        PROFILE_FIELD_ALIASES["representative_texts"]: [truncate_text(str(text or ""), 120) for text in list(profile.get("representative_texts", [])[:3])],
    }


def _fallback_persona(profile: dict[str, Any]) -> dict[str, Any]:
    """Build a grounded persona without LLM usage."""
    demographics = list(profile.get("top_demographics", []))
    needs = list(profile.get("top_need_codes", []))
    quotes = [str(text)[:180] for text in profile.get("representative_texts", [])[:5]]
    primary_demo = demographics[0] if demographics else "mixed practitioners"
    primary_need = needs[0] if needs else "mixed workflow needs"
    primary_need_label = _humanize_code(primary_need, fallback="workflow needs")
    recommended_name = str(profile.get("recommended_name", "") or "").strip()
    top_bottlenecks = [_humanize_code(code, fallback=code) for code in list(profile.get("top_bottlenecks", []))[:4]]
    return {
        "persona_name": recommended_name or primary_need_label or _humanize_code(primary_demo, fallback="Mixed Workflow Persona"),
        "one_line_summary": f"{recommended_name or primary_need_label} centered on recurring {primary_need_label.lower()} pain",
        "core_demographic": _humanize_code(primary_demo, fallback="mixed practitioners"),
        "top_pain_points": top_bottlenecks or [_humanize_code(code, fallback=code) for code in needs[:4]],
        "co_occurring_needs": [_humanize_code(code, fallback=code) for code in needs[4:8]],
        "example_quotes": quotes,
        "opportunity": f"Improve workflows around {primary_need_label.lower()} with clearer trust, breakdown, and output support.",
    }


def _validate_persona(persona: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    """Validate persona JSON structure and keep it grounded in cluster evidence."""
    fallback = _fallback_persona(profile)
    validated = dict(PERSONA_SCHEMA)
    for key, default in PERSONA_SCHEMA.items():
        value = persona.get(key, default)
        if isinstance(default, list):
            validated[key] = [str(item)[:180] for item in list(value or [])[:5]]
        else:
            validated[key] = str(value or "")[:240]
    for key, value in validated.items():
        if value in ("", []):
            validated[key] = fallback[key]
    return validated


def _expand_persona_aliases(persona: dict[str, Any]) -> dict[str, Any]:
    """Expand compact persona JSON keys back into the public schema."""
    return {
        PERSONA_SCHEMA_ALIAS_REVERSE.get(str(key), str(key)): value
        for key, value in dict(persona or {}).items()
    }


def _humanize_code(code: str, fallback: str) -> str:
    """Turn a code-like token into a more readable display label."""
    text = str(code or "").strip()
    if not text:
        return fallback
    if text in CODE_LABELS:
        return CODE_LABELS[text]
    if "_" not in text and text.isupper():
        return text
    normalized = text.replace("R_", "").replace("Q_", "").replace("P_", "").replace("O_", "").replace("E_", "").replace("W_", "")
    return normalized.replace("_", " ").title() or fallback
