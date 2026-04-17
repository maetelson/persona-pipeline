"""Source-aware prompt builder with compact axis guidance and few-shot examples."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.labeling.prompt_constants import PROMPT_SYSTEM, SOURCE_GROUP_HINTS
from src.labeling.prompt_payload import (
    build_compact_episode_payload,
    build_compact_label_schema,
    compact_json,
    extract_compact_rule_labels,
)


def build_label_prompt(
    episode_row: pd.Series,
    labeled_row: pd.Series,
    requested_families: list[str],
    target_reason: str,
    codebook: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    """Build a short but structured source-aware labeling prompt."""
    source = str(episode_row.get("source", "") or "").strip()
    source_key = SOURCE_GROUP_HINTS.get(source, source)
    llm_cfg = dict(policy.get("llm_labeling", {}) or {})
    family_cfg = dict(llm_cfg.get("families", {}) or {})
    few_shot_cfg = dict(llm_cfg.get("few_shot_examples", {}) or policy.get("few_shot_examples", {}) or {})
    source_guidance = str((llm_cfg.get("source_guidance", {}) or {}).get(source_key, "") or "")
    unknown_policy = llm_cfg.get("unknown_allowed_only_when", []) or []
    compact_family_mode = len(requested_families) <= 2

    family_lines: list[str] = []
    for family in requested_families:
        cfg = dict(family_cfg.get(family, {}) or {})
        if not cfg:
            continue
        if compact_family_mode:
            for boundary in cfg.get("boundary_rules", [])[:2]:
                family_lines.append(f"{family}: {boundary}")
            continue
        family_lines.append(f"{family}: {cfg.get('definition','')} choose={cfg.get('choose_when','')} unknown={cfg.get('unknown_when','')}")
        for boundary in cfg.get("boundary_rules", [])[:3]:
            family_lines.append(f"- {boundary}")

    few_shot_lines: list[str] = []
    max_few_shots = 1 if compact_family_mode else 2
    for example in few_shot_cfg.get(source_key, [])[:max_few_shots]:
        labels = example.get("labels", {}) or {}
        compact_labels = {
            key: [value] if key != "fit_code" else value
            for key, value in labels.items()
            if key in requested_families
        }
        if compact_labels:
            few_shot_lines.append(
                f"{example.get('text','')} => {compact_json(compact_labels)}"
            )

    episode_payload = build_compact_episode_payload(episode_row)
    current_labels = extract_compact_rule_labels(labeled_row, requested_families)
    schema_json = compact_json(build_compact_label_schema(requested_families))
    prompt_lines = [
        PROMPT_SYSTEM,
        f"src={source_key}",
        f"target={target_reason}",
    ]
    if source_guidance:
        prompt_lines.append(f"guidance={source_guidance}")
    if unknown_policy:
        prompt_lines.append(f"unknown_policy={' ; '.join(str(item) for item in unknown_policy[:2])}")
    prompt_lines.extend(
        [
            f"families={compact_json(requested_families)}",
            "axis_rules:",
            *family_lines,
        ]
    )
    if few_shot_lines:
        prompt_lines.extend(["few_shot:", *few_shot_lines])
    prompt_lines.extend(
        [
            f"rule_labels={compact_json(current_labels)}",
            f"episode={compact_json(episode_payload)}",
            f"schema={schema_json}",
            "Return only requested families. If evidence is partial but directional, choose the closest broad label with lower confidence.",
        ]
    )
    return {
        "prompt": "\n".join(line for line in prompt_lines if line),
        "requested_families": requested_families,
    }
