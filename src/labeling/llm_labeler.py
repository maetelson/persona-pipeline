"""Token-aware OpenAI labeling hook with compact prompts and batch preparation."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
from src.labeling.prompt_builder import build_label_prompt
from src.labeling.prompt_payload import (
    LABEL_COLUMNS,
    expand_compact_label_suggestion,
    extract_compact_rule_labels,
    truncate_text,
)
from src.utils.llm_cache import (
    append_jsonl_cache,
    build_prompt_cache_key,
    extract_responses_usage,
    load_jsonl_cache,
    parse_responses_json,
)
from src.utils.pipeline_schema import CORE_LABEL_COLUMNS, is_unknown_like as schema_is_unknown_like

PROMPT_SYSTEM = "Label evidence. JSON only. Use schema keys exactly. Keep strongest supported codes only."

FAMILY_CODEBOOK_MAP = {
    "role_codes": "role_keywords",
    "moment_codes": "moment_keywords",
    "question_codes": "question_codes",
    "pain_codes": "pain_codes",
    "env_codes": "env_codes",
    "workaround_codes": "workaround_codes",
    "output_codes": "output_codes",
    "fit_code": "fit_keywords",
}

GENERIC_SINGLE_CODES = {
    "role_codes": {"R_ANALYST", "R_MANAGER", "R_MARKETER"},
    "question_codes": {"Q_REPORT_SPEED", "Q_AUTOMATE_WORKFLOW", "Q_DIAGNOSE_ISSUE", "Q_VALIDATE_NUMBERS"},
    "pain_codes": {"P_MANUAL_REPORTING", "P_TOOL_LIMITATION", "P_DATA_QUALITY", "P_HANDOFF"},
    "output_codes": {"O_XLSX", "O_DASHBOARD", "O_VALIDATED_DATASET", "O_AUTOMATION_JOB"},
}

_COMPACT_CODEBOOK_JSON_CACHE: dict[tuple[int, tuple[str, ...]], str] = {}


def enrich_with_llm_labels(
    episodes_df: pd.DataFrame,
    labeled_df: pd.DataFrame,
    config: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run direct LLM labeling or prepare batch input while preserving audit detail."""
    if labeled_df.empty:
        return labeled_df.copy(), _empty_audit_df()

    result = labeled_df.copy()
    runtime = resolve_llm_runtime(config=config)
    episode_lookup = (
        episodes_df.drop_duplicates(subset=["episode_id"], keep="first").set_index("episode_id", drop=False)
        if not episodes_df.empty
        else pd.DataFrame()
    )
    targeted_rows = _build_target_rows(result, runtime["threshold"], target_unknown_only=runtime["target_unknown_only"])
    cache_store = load_jsonl_cache(runtime["cache_path"]) if runtime["cache_enabled"] else {}

    if runtime["mode"] == "batch":
        audit_df = _prepare_batch_mode(
            result=result,
            targeted_rows=targeted_rows,
            runtime=runtime,
            episode_lookup=episode_lookup,
            cache_store=cache_store,
        )
        return result, audit_df

    audit_rows: list[dict[str, Any]] = []
    response_cache: dict[str, dict[str, Any]] = {}
    for index, row in result.iterrows():
        should_target, target_reason = should_send_to_llm(row=row, threshold=runtime["threshold"])
        target_meta = targeted_rows.get(str(row["episode_id"])) if should_target else None
        audit_row = _base_audit_row(
            episode_id=str(row["episode_id"]),
            was_rule_labeled=True,
            was_llm_targeted=should_target,
            llm_mode=runtime["mode"],
            llm_target_reason=target_reason,
            model_used=runtime["model_primary"],
            confidence_before=float(row.get("label_confidence", 0.0) or 0.0),
            unknown_before=_count_unknown_codes(row),
        )

        if not target_meta:
            audit_row["llm_status"] = "not_targeted"
            audit_row["llm_reason"] = f"llm:not_targeted:{target_reason}"
            result.at[index, "label_reason"] = _append_reason(str(row.get("label_reason", "")), audit_row["llm_reason"])
            audit_rows.append(audit_row)
            continue

        if runtime["skip_reason"]:
            audit_row["llm_status"] = "disabled"
            audit_row["llm_reason"] = runtime["skip_reason"]
            result.at[index, "label_reason"] = _append_reason(str(row.get("label_reason", "")), runtime["skip_reason"])
            audit_rows.append(audit_row)
            continue

        prompt_payload = _build_prompt_payload(
            episode_row=episode_lookup.loc[str(row["episode_id"])],
            labeled_row=row,
            codebook=runtime["codebook"],
            target_meta=target_meta,
            policy=runtime["policy"],
        )
        audit_row["prompt_chars"] = len(prompt_payload["prompt"])
        audit_row["prompt_cache_key"] = runtime["prompt_cache_key"]
        cache_key = _cache_key_for_prompt(
            model=runtime["model_primary"],
            requested_families=prompt_payload["requested_families"],
            prompt=prompt_payload["prompt"],
        )
        if cache_key in response_cache:
            cached = response_cache[cache_key]
            merged_any = _merge_llm_suggestion(result, index=index, suggestion=cached)
            result.at[index, "label_confidence"] = max(
                float(result.at[index, "label_confidence"] or 0.0),
                float(cached.get("label_confidence", 0.0) or 0.0),
            )
            result.at[index, "label_reason"] = _append_reason(
                str(result.at[index, "label_reason"]),
                str(cached.get("label_reason", "llm:run_reuse")),
            )
            audit_row["llm_status"] = "run_reuse"
            audit_row["llm_reason"] = "llm:run_reuse"
            audit_row["parse_success"] = True
            audit_row["unknown_after"] = _count_unknown_codes(result.loc[index])
            audit_row["label_confidence_after"] = float(result.at[index, "label_confidence"] or 0.0)
            audit_row["fallback_used"] = False
            audit_rows.append(audit_row)
            continue
        if cache_key in cache_store:
            cached = cache_store[cache_key]
            merged_any = _merge_llm_suggestion(result, index=index, suggestion=cached)
            result.at[index, "label_confidence"] = max(
                float(result.at[index, "label_confidence"] or 0.0),
                float(cached.get("label_confidence", 0.0) or 0.0),
            )
            result.at[index, "label_reason"] = _append_reason(
                str(result.at[index, "label_reason"]),
                str(cached.get("label_reason", "llm:cache_hit")),
            )
            audit_row["llm_status"] = "cache_hit"
            audit_row["llm_reason"] = "llm:cache_hit"
            audit_row["parse_success"] = True
            audit_row["unknown_after"] = _count_unknown_codes(result.loc[index])
            audit_row["label_confidence_after"] = float(result.at[index, "label_confidence"] or 0.0)
            audit_row["fallback_used"] = False
            audit_rows.append(audit_row)
            continue

        if runtime["dry_run"]:
            audit_row["llm_status"] = "dry_run"
            audit_row["llm_reason"] = "llm:disabled:dry_run"
            result.at[index, "label_reason"] = _append_reason(
                str(row.get("label_reason", "")),
                f"llm:disabled:dry_run:{target_meta['reason']}",
            )
            audit_rows.append(audit_row)
            continue

        audit_row["was_llm_called"] = True
        model_used = runtime["model_escalation"] if _should_escalate(target_meta, runtime) else runtime["model_primary"]
        audit_row["model_used"] = model_used
        try:
            llm_response = _call_llm_labeler(
                prompt=prompt_payload["prompt"],
                model=model_used,
                api_key=runtime["api_key"],
                timeout_seconds=runtime["timeout_seconds"],
                backend=runtime["backend"],
                max_output_tokens=runtime["max_output_tokens"],
                prompt_cache_key=runtime["prompt_cache_key"],
            )
            suggestion = _validate_llm_suggestion(llm_response["parsed"], runtime["codebook"], requested_families=prompt_payload["requested_families"])
            merged_any = _merge_llm_suggestion(result, index=index, suggestion=suggestion)
            result.at[index, "label_confidence"] = max(
                float(result.at[index, "label_confidence"] or 0.0),
                float(suggestion.get("label_confidence", 0.0) or 0.0),
            )
            result.at[index, "label_reason"] = _append_reason(
                str(result.at[index, "label_reason"]),
                str(suggestion.get("label_reason", "llm:applied")),
            )
            audit_row["llm_status"] = "applied" if merged_any else "no_change"
            audit_row["llm_reason"] = str(suggestion.get("label_reason", "llm:applied"))
            audit_row["parse_success"] = True
            _attach_usage(audit_row, llm_response["usage"])
            response_cache[cache_key] = suggestion
            if runtime["cache_enabled"]:
                cache_store[cache_key] = suggestion
                append_jsonl_cache(runtime["cache_path"], cache_key, suggestion)
        except Exception as exc:  # noqa: BLE001
            audit_row["llm_status"] = "failed"
            audit_row["llm_reason"] = _error_reason_from_exception(exc)
            result.at[index, "label_reason"] = _append_reason(
                str(result.at[index, "label_reason"]),
                audit_row["llm_reason"],
            )
            fallback = _fallback_llm_suggestion(episode_lookup, episode_id=str(row["episode_id"]))
            if fallback:
                merged_any = _merge_llm_suggestion(result, index=index, suggestion=fallback)
                if merged_any:
                    audit_row["fallback_used"] = True
                    result.at[index, "label_confidence"] = max(float(result.at[index, "label_confidence"] or 0.0), 0.65)
                    result.at[index, "label_reason"] = _append_reason(str(result.at[index, "label_reason"]), "llm:fallback_hint")

        audit_row["label_confidence_after"] = float(result.at[index, "label_confidence"] or 0.0)
        audit_row["unknown_after"] = _count_unknown_codes(result.loc[index])
        audit_row["cost_estimate_optional"] = _estimate_cost_optional(audit_row)
        audit_rows.append(audit_row)

    audit_df = pd.DataFrame(audit_rows, columns=_audit_columns())
    return result, audit_df


def should_send_to_llm(row: pd.Series, threshold: float) -> tuple[bool, str]:
    """Return whether a row should reach the LLM and a readable reason string."""
    labelability_status = str(row.get("labelability_status", "") or "")
    if labelability_status == "low_signal":
        return False, "low_signal_input"
    reasons: list[str] = []
    unknown_columns = [column for column in LABEL_COLUMNS if schema_is_unknown_like(row.get(column, "unknown"))]
    missing_core = [column for column in CORE_LABEL_COLUMNS if schema_is_unknown_like(row.get(column, "unknown"))]
    non_core_unknown = [column for column in unknown_columns if column not in CORE_LABEL_COLUMNS]
    coarse_columns = [column for column in CORE_LABEL_COLUMNS if _is_coarse_single_code(column, row.get(column, "unknown"))]
    confidence = float(row.get("label_confidence", 0.0) or 0.0)
    rule_hit_count = int(row.get("rule_hit_count", 0) or 0)
    unknown_family_count = int(row.get("rule_unknown_family_count", 0) or 0)
    fit_code = str(row.get("fit_code", "unknown") or "unknown")

    if fit_code == "F_REVIEW" and not unknown_columns and confidence >= max(threshold, 0.75):
        return False, "review_bucket_high_confidence"
    if non_core_unknown and not missing_core and confidence >= max(threshold, 0.82) and rule_hit_count >= 6:
        return False, f"non_core_unknown_only:{','.join(non_core_unknown)}"
    if confidence < threshold:
        reasons.append(f"low_confidence:{confidence:.2f}<{threshold:.2f}")
    if missing_core:
        reasons.append(f"unknown_codes:{','.join(missing_core)}")
        reasons.append(f"missing_core_family:{','.join(missing_core)}")
    elif non_core_unknown and (confidence < threshold or coarse_columns or rule_hit_count <= 1):
        reasons.append(f"unknown_codes:{','.join(non_core_unknown)}")
    if coarse_columns and confidence < 0.82:
        reasons.append(f"coarse_rule_match:{','.join(coarse_columns)}")
    if rule_hit_count <= 2 and unknown_family_count >= 3:
        reasons.append("sparse_rule_hits")

    if reasons:
        return True, ";".join(reasons)
    return False, "rule_label_sufficient"


def resolve_llm_runtime(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve token-saving runtime settings for direct, batch, or disabled modes."""
    cfg = config or {}
    threshold = float(_first_non_empty("LLM_LABELER_MIN_CONFIDENCE", default=str(cfg.get("min_confidence", 0.72))))
    enabled = _first_non_empty("ENABLE_LLM_LABELER", "LLM_LABELER_ENABLED", default=str(cfg.get("enabled", "false"))).lower() == "true"
    dry_run = _first_non_empty("LLM_DRY_RUN", "LLM_LABELER_DRY_RUN", "LABELING_DRY_RUN", default=str(cfg.get("dry_run", "false"))).lower() == "true"
    batch_enabled = _first_non_empty("ENABLE_BATCH_LABELING", default=str(cfg.get("batch_enabled", "false"))).lower() == "true"
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model_primary = _first_non_empty("LLM_MODEL_PRIMARY", "LLM_MODEL", "OPENAI_MODEL", default=str(cfg.get("model_primary", "gpt-5.4-mini"))).strip()
    model_escalation = _first_non_empty("LLM_MODEL_ESCALATION", default=str(cfg.get("model_escalation", "gpt-5.4-mini"))).strip()
    escalation_enabled = _first_non_empty("ENABLE_LLM_ESCALATION", default=str(cfg.get("enable_escalation", "false"))).lower() == "true"
    backend = _first_non_empty("LLM_OPENAI_BACKEND", default=str(cfg.get("backend", "http"))).strip().lower() or "http"
    max_output_tokens = int(_first_non_empty("MAX_LLM_OUTPUT_TOKENS", default=str(cfg.get("max_output_tokens", 120))))
    timeout_seconds = int(cfg.get("timeout_seconds", 45))
    prompt_cache_key = _first_non_empty("PROMPT_CACHE_KEY", default=str(cfg.get("prompt_cache_key", "persona-label-v1")))
    prompt_cache_retention = _first_non_empty("PROMPT_CACHE_RETENTION", default=str(cfg.get("prompt_cache_retention", "session")))
    batch_max_rows = int(_first_non_empty("BATCH_MAX_ROWS", default=str(cfg.get("batch_max_rows", 200))))
    target_unknown_only = _first_non_empty("LLM_TARGET_UNKNOWN_ONLY", default=str(cfg.get("target_unknown_only", "true"))).lower() == "true"
    cache_enabled = _first_non_empty("LLM_CACHE_ENABLED", default=str(cfg.get("cache_enabled", "true"))).lower() == "true"
    cache_path = Path(str(cfg.get("cache_path", Path("data") / "labeled" / "llm_response_cache.jsonl")))
    codebook = dict(cfg.get("codebook", {}) or {})
    policy = dict(cfg.get("policy", {}) or {})

    skip_reason = ""
    mode = "direct"
    if not enabled:
        skip_reason = "llm:disabled:feature_flag_off"
        mode = "disabled"
    elif not model_primary:
        skip_reason = "llm:disabled:no_model"
        mode = "disabled"
    elif not api_key:
        skip_reason = "llm:disabled:no_api_key"
        mode = "disabled"
    elif backend == "sdk" and importlib.util.find_spec("openai") is None:
        skip_reason = "llm:disabled:sdk_not_available"
        mode = "disabled"
    elif dry_run:
        mode = "dry_run"
    elif batch_enabled:
        mode = "batch"

    return {
        "enabled": enabled,
        "dry_run": dry_run,
        "mode": mode,
        "threshold": threshold,
        "api_key": api_key,
        "model_primary": model_primary,
        "model_escalation": model_escalation,
        "enable_escalation": escalation_enabled,
        "backend": backend,
        "timeout_seconds": timeout_seconds,
        "codebook": codebook,
        "policy": policy,
        "skip_reason": skip_reason,
        "max_output_tokens": max_output_tokens,
        "prompt_cache_key": prompt_cache_key,
        "prompt_cache_retention": prompt_cache_retention,
        "batch_max_rows": batch_max_rows,
        "target_unknown_only": target_unknown_only,
        "cache_enabled": cache_enabled,
        "cache_path": cache_path,
    }


def _build_target_rows(labeled_df: pd.DataFrame, threshold: float, target_unknown_only: bool) -> dict[str, dict[str, Any]]:
    """Build the subset of rows that truly need LLM review."""
    targets: dict[str, dict[str, Any]] = {}
    for _, row in labeled_df.iterrows():
        if target_unknown_only and not _row_has_unresolved_labels(row):
            continue
        should_target, reason = should_send_to_llm(row=row, threshold=threshold)
        if not should_target:
            continue
        targets[str(row["episode_id"])] = {
            "reason": reason,
            "escalate_candidate": ("missing_core_family" in reason) or (reason.count(",") >= 4),
        }
    return targets


def _prepare_batch_mode(
    result: pd.DataFrame,
    targeted_rows: dict[str, dict[str, Any]],
    runtime: dict[str, Any],
    episode_lookup: pd.DataFrame,
    cache_store: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Prepare audit rows for batch mode without calling the API directly."""
    from src.labeling.batch_builder import build_batch_requests

    batch_targets = []
    cached_keys: set[str] = set()
    seen_prompt_keys: set[str] = set()
    for episode_id, target_meta in targeted_rows.items():
        if episode_id not in episode_lookup.index:
            continue
        episode_row = episode_lookup.loc[episode_id]
        labeled_row = result[result["episode_id"].astype(str) == episode_id].iloc[0]
        prompt_payload = _build_prompt_payload(
            episode_row=episode_row,
            labeled_row=labeled_row,
            codebook=runtime["codebook"],
            target_meta=target_meta,
            policy=runtime["policy"],
        )
        cache_key = _cache_key_for_prompt(
            model=runtime["model_primary"],
            requested_families=prompt_payload["requested_families"],
            prompt=prompt_payload["prompt"],
        )
        if cache_key in cache_store:
            cached_keys.add(episode_id)
            continue
        if cache_key in seen_prompt_keys:
            cached_keys.add(episode_id)
            continue
        seen_prompt_keys.add(cache_key)
        batch_targets.append(episode_row)
    batch_input_path = build_batch_requests(
        episode_rows=batch_targets[: runtime["batch_max_rows"]],
        labeled_df=result,
        codebook=runtime["codebook"],
        prompt_cache_key=runtime["prompt_cache_key"],
        model=runtime["model_primary"],
        max_output_tokens=runtime["max_output_tokens"],
    )

    audit_rows: list[dict[str, Any]] = []
    for _, row in result.iterrows():
        episode_id = str(row["episode_id"])
        should_target, target_reason = should_send_to_llm(row=row, threshold=runtime["threshold"])
        target_meta = targeted_rows.get(episode_id) if should_target else None
        audit_row = _base_audit_row(
            episode_id=episode_id,
            was_rule_labeled=True,
            was_llm_targeted=should_target,
            llm_mode="batch" if target_meta else "skipped",
            llm_target_reason=target_reason,
            model_used=runtime["model_primary"],
            confidence_before=float(row.get("label_confidence", 0.0) or 0.0),
            unknown_before=_count_unknown_codes(row),
        )
        if target_meta:
            if episode_id in cached_keys:
                audit_row["llm_status"] = "cache_hit"
                audit_row["llm_reason"] = "llm:cache_hit"
            else:
                audit_row["llm_status"] = "batch_prepared"
                audit_row["llm_reason"] = f"llm:batch_prepared:{batch_input_path.name}"
                audit_row["batch_request_path"] = str(batch_input_path)
                result.at[result.index[result["episode_id"] == episode_id][0], "label_reason"] = _append_reason(
                    str(row.get("label_reason", "")),
                    "llm:batch_prepared",
                )
        else:
            audit_row["llm_status"] = "not_targeted"
            audit_row["llm_reason"] = f"llm:not_targeted:{target_reason}"
        audit_rows.append(audit_row)
    return pd.DataFrame(audit_rows, columns=_audit_columns())


def _build_prompt_payload(
    episode_row: pd.Series,
    labeled_row: pd.Series,
    codebook: dict[str, Any],
    target_meta: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    """Build a short prompt with only the needed families and compact fields."""
    requested_families = _requested_families(labeled_row, target_meta["reason"])
    prompt_payload = build_label_prompt(
        episode_row=episode_row,
        labeled_row=labeled_row,
        requested_families=requested_families,
        target_reason=target_meta["reason"],
        codebook=codebook,
        policy=policy,
    )
    return prompt_payload


def _requested_families(labeled_row: pd.Series, target_reason: str) -> list[str]:
    """Request only the families that are unresolved or likely ambiguous."""
    families: list[str] = []
    for family in CORE_LABEL_COLUMNS + ["moment_codes", "env_codes", "workaround_codes", "fit_code"]:
        value = str(labeled_row.get(family, "unknown") or "unknown")
        if _is_unknown_like(value) or _is_coarse_single_code(family, value):
            families.append(family)
    if "coarse_rule_match" in target_reason:
        families.extend(CORE_LABEL_COLUMNS)
    return list(dict.fromkeys(families)) or CORE_LABEL_COLUMNS


def _compact_codebook(codebook: dict[str, Any], requested_families: list[str]) -> dict[str, str]:
    """Compress codebook entries into short code->hint strings for needed families only."""
    compact: dict[str, str] = {}
    for family in requested_families:
        codebook_key = FAMILY_CODEBOOK_MAP.get(family)
        if not codebook_key:
            continue
        family_entries = codebook.get(codebook_key, {}) or {}
        for code, hints in family_entries.items():
            compact[code] = _compact_hint(list(hints)[:1])
    return compact


def _compact_codebook_json(codebook: dict[str, Any], requested_families: list[str]) -> str:
    """Serialize compact codebook once per requested-family set."""
    cache_key = (id(codebook), tuple(requested_families))
    if cache_key not in _COMPACT_CODEBOOK_JSON_CACHE:
        _COMPACT_CODEBOOK_JSON_CACHE[cache_key] = compact_json(
            _compact_codebook(codebook, requested_families)
        )
    return _COMPACT_CODEBOOK_JSON_CACHE[cache_key]


def _should_escalate(target_meta: dict[str, Any], runtime: dict[str, Any]) -> bool:
    """Escalate only the hardest ambiguous rows when enabled."""
    return bool(runtime["enable_escalation"] and target_meta.get("escalate_candidate"))


def _call_llm_labeler(
    prompt: str,
    model: str,
    api_key: str,
    timeout_seconds: int,
    backend: str,
    max_output_tokens: int,
    prompt_cache_key: str,
) -> dict[str, Any]:
    """Call OpenAI and return parsed JSON plus usage."""
    raw_response = (
        _call_responses_sdk(prompt, model, api_key, timeout_seconds, max_output_tokens, prompt_cache_key)
        if backend == "sdk"
        else _call_responses_http(prompt, model, api_key, timeout_seconds, max_output_tokens, prompt_cache_key)
    )
    return {
        "parsed": parse_responses_json(raw_response),
        "usage": extract_responses_usage(raw_response),
    }


def _call_responses_http(
    prompt: str,
    model: str,
    api_key: str,
    timeout_seconds: int,
    max_output_tokens: int,
    prompt_cache_key: str,
) -> dict[str, Any]:
    """Call the Responses API over HTTP with a compact JSON contract."""
    payload = {
        "model": model,
        "input": prompt,
        "temperature": 0.1,
        "max_output_tokens": max_output_tokens,
        "text": {"format": {"type": "json_object"}},
        "metadata": {
            "prompt_cache_key": prompt_cache_key,
        },
    }
    request = Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return json.load(response)
    except HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            error_body = ""
        raise RuntimeError(f"OpenAI request failed with HTTP {exc.code}: {error_body or 'no response body'}") from exc
    except URLError as exc:
        raise RuntimeError("OpenAI network request failed") from exc


def _call_responses_sdk(
    prompt: str,
    model: str,
    api_key: str,
    timeout_seconds: int,
    max_output_tokens: int,
    prompt_cache_key: str,
) -> dict[str, Any]:
    """Call the Responses API via SDK when available."""
    from openai import OpenAI

    client = OpenAI(api_key=api_key, timeout=timeout_seconds)
    response = client.responses.create(
        model=model,
        input=prompt,
        temperature=0.1,
        max_output_tokens=max_output_tokens,
        text={"format": {"type": "json_object"}},
        metadata={"prompt_cache_key": prompt_cache_key},
    )
    return response.model_dump()


def _validate_llm_suggestion(
    suggestion: dict[str, Any],
    codebook: dict[str, Any],
    requested_families: list[str],
) -> dict[str, Any]:
    """Validate compact JSON output against requested codes only."""
    suggestion = expand_compact_label_suggestion(suggestion)
    validated: dict[str, Any] = {}
    for family in requested_families:
        codebook_key = FAMILY_CODEBOOK_MAP.get(family)
        if not codebook_key:
            continue
        allowed = set((codebook.get(codebook_key, {}) or {}).keys())
        raw_value = suggestion.get(family, [])
        if family == "fit_code":
            value = _normalize_scalar_value(raw_value)
            validated[family] = value if value in allowed else "unknown"
            continue
        normalized_list = _normalize_list_value(raw_value)
        picked = [code for code in normalized_list if str(code) in allowed]
        validated[family] = "|".join(dict.fromkeys(str(code) for code in picked)) if picked else "unknown"

    try:
        validated["label_confidence"] = max(0.0, min(float(suggestion.get("confidence", 0.0)), 1.0))
    except (TypeError, ValueError):
        validated["label_confidence"] = 0.0
    validated["label_reason"] = truncate_text(str(suggestion.get("reason", "llm:applied") or "llm:applied"), 80)
    return validated


def _merge_llm_suggestion(result: pd.DataFrame, index: int, suggestion: dict[str, Any]) -> bool:
    """Merge only unresolved families from the LLM output."""
    merged_any = False
    for column in LABEL_COLUMNS:
        suggested = str(suggestion.get(column, "") or "")
        if not suggested or suggested == "unknown":
            continue
        current = str(result.at[index, column])
        if _is_unknown_like(current):
            result.at[index, column] = suggested
            merged_any = True
    return merged_any


def _fallback_llm_suggestion(episode_lookup: pd.DataFrame, episode_id: str) -> dict[str, str]:
    """Provide a local fallback hint from episode fields."""
    if episode_lookup.empty or episode_id not in set(episode_lookup.index.astype(str)):
        return {}
    episode_row = episode_lookup.loc[episode_id]
    suggestions: dict[str, str] = {}
    tool_env = str(episode_row.get("tool_env", "") or "")
    desired_output = str(episode_row.get("desired_output", "") or "")
    workaround = str(episode_row.get("workaround_text", "") or "")
    if tool_env == "excel":
        suggestions["env_codes"] = "E_SPREADSHEET"
    elif tool_env == "sql_bi":
        suggestions["env_codes"] = "E_SQL_BI"
    if desired_output == "xlsx_report":
        suggestions["output_codes"] = "O_XLSX"
    elif desired_output == "dashboard_update":
        suggestions["output_codes"] = "O_DASHBOARD"
    if "manual" in workaround:
        suggestions["workaround_codes"] = "W_MANUAL"
    return suggestions


def _attach_usage(audit_row: dict[str, Any], usage: dict[str, int]) -> None:
    """Attach token usage fields to the audit row."""
    audit_row["usage_input_tokens"] = int(usage.get("usage_input_tokens", 0))
    audit_row["usage_output_tokens"] = int(usage.get("usage_output_tokens", 0))
    audit_row["usage_total_tokens"] = int(usage.get("usage_total_tokens", 0))


def _estimate_cost_optional(audit_row: dict[str, Any]) -> float:
    """Return a rough token-based estimate placeholder without hardcoding pricing."""
    total_tokens = int(audit_row.get("usage_total_tokens", 0) or 0)
    return round(total_tokens / 1_000_000, 6)


def _error_reason_from_exception(exc: Exception) -> str:
    """Translate runtime failures into stable audit reasons."""
    message = str(exc)
    if "HTTP 401" in message or "HTTP 403" in message:
        return "llm:failed:auth_or_project_error"
    if "HTTP 400" in message:
        return "llm:failed:bad_request"
    if "HTTP 429" in message:
        return "llm:failed:rate_limited"
    if "network request failed" in message.lower():
        return "llm:failed:network_error"
    return f"llm:failed:{type(exc).__name__}"


def _count_unknown_codes(row: pd.Series) -> int:
    """Count unresolved label families for audit reporting."""
    return sum(1 for column in LABEL_COLUMNS if schema_is_unknown_like(row.get(column, "unknown")))


def _is_unknown_like(value: Any) -> bool:
    """Treat blanks and explicit unknown labels as unresolved."""
    return schema_is_unknown_like(value)


def _row_has_unresolved_labels(row: pd.Series) -> bool:
    """Return whether any label family is still unresolved after rule labeling."""
    return any(_is_unknown_like(row.get(column, "unknown")) for column in LABEL_COLUMNS)


def _normalize_list_value(raw_value: Any) -> list[str]:
    """Normalize JSON enum arrays and drop unresolved placeholders."""
    if raw_value is None:
        return []
    if isinstance(raw_value, str):
        values = [raw_value]
    elif isinstance(raw_value, list):
        values = raw_value
    else:
        return []
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or _is_unknown_like(text):
            continue
        normalized.append(text)
    return normalized


def _normalize_scalar_value(raw_value: Any) -> str:
    """Normalize a scalar enum field."""
    text = str(raw_value or "").strip()
    return "unknown" if _is_unknown_like(text) else text


def _cache_key_for_prompt(model: str, requested_families: list[str], prompt: str) -> str:
    """Build a stable cache key from prompt content so duplicate rows reuse one response."""
    return build_prompt_cache_key(
        model=model,
        prompt=f"{','.join(requested_families)}||{prompt}",
        namespace="labeler",
    )


def _compact_hint(hints: list[Any]) -> str:
    """Keep only one short grounding hint per code."""
    for hint in hints:
        text = truncate_text(str(hint or "").strip(), 24)
        if text:
            return text
    return ""


def _is_coarse_single_code(column: str, value: Any) -> bool:
    """Return whether a rule label is too generic to trust without LLM review."""
    text = str(value or "").strip()
    if not text or text == "unknown" or "|" in text:
        return False
    return text in GENERIC_SINGLE_CODES.get(column, set())


def _first_non_empty(*keys: str, default: str = "") -> str:
    """Return the first non-empty environment value or the provided default."""
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
    return str(default or "")


def _append_reason(existing: str, suffix: str) -> str:
    """Append one reason token without losing prior rule evidence."""
    if not existing:
        return suffix
    return f"{existing} | {suffix}"


def _audit_columns() -> list[str]:
    """Return the full row-level audit schema."""
    return [
        "episode_id",
        "was_rule_labeled",
        "was_llm_targeted",
        "was_llm_called",
        "llm_mode",
        "llm_target_reason",
        "llm_reason",
        "llm_status",
        "model_used",
        "usage_input_tokens",
        "usage_output_tokens",
        "usage_total_tokens",
        "cost_estimate_optional",
        "parse_success",
        "fallback_used",
        "label_confidence_before",
        "label_confidence_after",
        "unknown_before",
        "unknown_after",
        "prompt_chars",
        "prompt_cache_key",
        "batch_request_path",
    ]


def _base_audit_row(
    episode_id: str,
    was_rule_labeled: bool,
    was_llm_targeted: bool,
    llm_mode: str,
    llm_target_reason: str,
    model_used: str,
    confidence_before: float,
    unknown_before: int,
) -> dict[str, Any]:
    """Build the default row-level audit structure."""
    return {
        "episode_id": episode_id,
        "was_rule_labeled": was_rule_labeled,
        "was_llm_targeted": was_llm_targeted,
        "was_llm_called": False,
        "llm_mode": llm_mode,
        "llm_target_reason": llm_target_reason,
        "llm_reason": "",
        "llm_status": "",
        "model_used": model_used,
        "usage_input_tokens": 0,
        "usage_output_tokens": 0,
        "usage_total_tokens": 0,
        "cost_estimate_optional": 0.0,
        "parse_success": False,
        "fallback_used": False,
        "label_confidence_before": confidence_before,
        "label_confidence_after": confidence_before,
        "unknown_before": unknown_before,
        "unknown_after": unknown_before,
        "prompt_chars": 0,
        "prompt_cache_key": "",
        "batch_request_path": "",
    }


def _empty_audit_df() -> pd.DataFrame:
    """Return an empty audit dataframe with the standard schema."""
    return pd.DataFrame(columns=_audit_columns())
