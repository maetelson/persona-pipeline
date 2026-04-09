"""Persona naming, insight generation, and solution-linkage outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, load_yaml
from src.utils.pipeline_schema import GENERIC_PERSONA_NAMES, ROLE_HEAVY_NAME_TERMS, TOOL_HEAVY_NAME_TERMS, contains_any_term


def build_persona_messaging_outputs(
    cluster_audit_df: pd.DataFrame,
    naming_df: pd.DataFrame,
    persona_summary_df: pd.DataFrame,
    examples_df: pd.DataFrame,
    personas: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build persona messaging artifacts from clustering outputs."""
    config = load_yaml(Path(__file__).resolve().parents[2] / "config" / "persona_messaging.yaml")
    persona_lookup = {str(item.get("cluster_id", "")): item for item in list(personas or [])}
    summary_lookup = persona_summary_df.set_index("persona_id").to_dict(orient="index") if not persona_summary_df.empty else {}
    naming_lookup = naming_df.set_index("persona_id").to_dict(orient="index") if not naming_df.empty else {}
    example_lookup = (
        examples_df.groupby("persona_id")["grounded_text"].apply(list).to_dict()
        if examples_df is not None and not examples_df.empty
        else {}
    )
    rows: list[dict[str, Any]] = []
    naming_rows: list[dict[str, Any]] = []
    weak_rows: list[dict[str, Any]] = []
    for _, audit_row in cluster_audit_df.iterrows():
        persona_id = str(audit_row.get("persona_id", ""))
        summary = summary_lookup.get(persona_id, {})
        naming = naming_lookup.get(persona_id, {})
        persona = persona_lookup.get(persona_id, {})
        dominant_signals = _split_pipe(audit_row.get("dominant_bottleneck_signals", ""))
        primary_signal = dominant_signals[0] if dominant_signals else "mixed_workflow_friction"
        secondary_signals = dominant_signals[1:3]
        proposed_name = str(naming.get("recommended_cluster_name", "") or naming.get("current_cluster_name", "") or audit_row.get("cluster_name", "") or _template_for(primary_signal, config).get("primary_name", "Mixed Workflow Friction"))
        if _is_generic_name(proposed_name) or _name_centering_type(proposed_name, config) != "bottleneck-centered":
            proposed_name = _fallback_name(primary_signal, secondary_signals, config)
        subtitle = _build_subtitle(primary_signal, secondary_signals, config)
        examples = example_lookup.get(persona_id, [])[:4]
        template = _template_for(primary_signal, config)
        repeated_work_pattern = _build_repeated_work_pattern(template, audit_row, summary)
        why_existing_workflow_fails = _build_workflow_failure(template, audit_row, summary)
        core_insight = _build_core_insight(template, audit_row, summary)
        solution_direction = str(template.get("solution_direction", "")).strip()
        target_problem = str(template.get("target_problem", "")).strip()
        expected_user_value = str(template.get("expected_user_value", "")).strip()
        suggested_interventions = list(template.get("suggested_interventions", []) or [])
        supporting_evidence = _supporting_evidence(audit_row, summary, examples)
        confidence_note, weak_flags = _confidence_and_flags(audit_row, examples, config)
        for flag_type, details in weak_flags:
            weak_rows.append({"persona_id": persona_id, "flag_type": flag_type, "details": details})
        rows.append(
            {
                "persona_id": persona_id,
                "primary_persona_name": proposed_name,
                "persona_subtitle": subtitle,
                "bottleneck_signature": " | ".join(dominant_signals),
                "core_insight": core_insight,
                "supporting_evidence": json.dumps(supporting_evidence, ensure_ascii=False),
                "repeated_work_pattern": repeated_work_pattern,
                "current_workaround": _current_workaround(audit_row, summary, examples),
                "why_existing_workflow_fails": why_existing_workflow_fails,
                "solution_direction": solution_direction,
                "target_problem": target_problem,
                "expected_user_value": expected_user_value,
                "suggested_interventions": json.dumps(suggested_interventions, ensure_ascii=False),
                "confidence_note": confidence_note,
                "representative_examples": json.dumps(examples, ensure_ascii=False),
            }
        )
        naming_rows.append(
            {
                "persona_id": persona_id,
                "proposed_name": proposed_name,
                "why_it_was_chosen": f"Chosen from dominant bottleneck pattern {primary_signal}" + (f" with {', '.join(secondary_signals)} context" if secondary_signals else ""),
                "dominant_bottleneck_signals": " | ".join(dominant_signals),
                "rejected_alternative_names": " | ".join(_alternative_names(primary_signal, secondary_signals, config)),
                "name_centering_type": _name_centering_type(proposed_name, config),
                "readability_rating": _readability_rating(proposed_name),
                "presentation_usability_rating": _presentation_rating(proposed_name),
            }
        )
    persona_cards_df = pd.DataFrame(rows)
    naming_audit_df = pd.DataFrame(naming_rows)
    weak_flags_df = pd.DataFrame(weak_rows)
    return {
        "persona_cards_v2_df": persona_cards_df,
        "naming_audit_df": naming_audit_df,
        "weak_persona_flags_df": weak_flags_df,
        "persona_insights_v2_md": _persona_insights_markdown(persona_cards_df),
        "persona_solution_linkage_md": _persona_solution_markdown(persona_cards_df),
        "before_after_persona_summary_md": _before_after_summary(persona_summary_df, personas or [], persona_cards_df, naming_audit_df),
    }


def write_persona_messaging_outputs(root_dir: Path, outputs: dict[str, Any]) -> dict[str, Path]:
    """Write persona messaging artifacts to analysis output files."""
    output_dir = ensure_dir(root_dir / "data" / "analysis")
    paths = {
        "persona_cards_v2_csv": output_dir / "persona_cards_v2.csv",
        "persona_cards_v2_json": output_dir / "persona_cards_v2.json",
        "persona_insights_v2_md": output_dir / "persona_insights_v2.md",
        "persona_solution_linkage_md": output_dir / "persona_solution_linkage.md",
        "naming_audit_csv": output_dir / "naming_audit.csv",
        "weak_persona_flags_csv": output_dir / "weak_persona_flags.csv",
        "before_after_persona_summary_md": output_dir / "before_after_persona_summary.md",
    }
    outputs["persona_cards_v2_df"].to_csv(paths["persona_cards_v2_csv"], index=False)
    paths["persona_cards_v2_json"].write_text(json.dumps(outputs["persona_cards_v2_df"].to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")
    paths["persona_insights_v2_md"].write_text(outputs["persona_insights_v2_md"], encoding="utf-8")
    paths["persona_solution_linkage_md"].write_text(outputs["persona_solution_linkage_md"], encoding="utf-8")
    outputs["naming_audit_df"].to_csv(paths["naming_audit_csv"], index=False)
    outputs["weak_persona_flags_df"].to_csv(paths["weak_persona_flags_csv"], index=False)
    paths["before_after_persona_summary_md"].write_text(outputs["before_after_persona_summary_md"], encoding="utf-8")
    return paths


def _template_for(primary_signal: str, config: dict[str, Any]) -> dict[str, Any]:
    """Return the message template for a primary bottleneck signal."""
    templates = dict(config.get("name_templates", {}))
    return dict(templates.get(primary_signal, templates.get("mixed_workflow_friction", {})))


def _split_pipe(value: object) -> list[str]:
    """Split a pipe-delimited string into ordered tokens."""
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


def _fallback_name(primary_signal: str, secondary_signals: list[str], config: dict[str, Any]) -> str:
    """Build a bottleneck-first fallback name when the proposed one is too generic."""
    primary = str(_template_for(primary_signal, config).get("primary_name", primary_signal.replace("_", " ").title())).strip()
    if secondary_signals:
        secondary_template = _template_for(secondary_signals[0], config)
        secondary = str(secondary_template.get("primary_name", "")).strip()
        if secondary and secondary != primary and secondary.lower() != "mixed workflow friction":
            return f"{primary} + {secondary}"
    return primary


def _build_subtitle(primary_signal: str, secondary_signals: list[str], config: dict[str, Any]) -> str:
    """Build a concise subtitle grounded in supporting bottleneck context."""
    template = _template_for(primary_signal, config)
    subtitle = str(template.get("subtitle", "")).strip()
    subtitle_patterns = dict(config.get("subtitle_patterns", {}))
    for signal in secondary_signals:
        if signal in subtitle_patterns:
            extra = str(subtitle_patterns[signal]).strip()
            if extra and extra.lower() not in subtitle.lower():
                return f"{subtitle}; {extra}" if subtitle else extra
    return subtitle


def _build_core_insight(template: dict[str, Any], audit_row: pd.Series, summary: dict[str, Any]) -> str:
    """Build one presentation-usable core insight sentence."""
    base = str(template.get("insight", "")).strip()
    workflow = str(summary.get("main_workflow_context", "")).replace("_", " ").strip()
    if workflow:
        return f"{base} This shows up most clearly in {workflow} workflows."
    return base


def _build_repeated_work_pattern(template: dict[str, Any], audit_row: pd.Series, summary: dict[str, Any]) -> str:
    """Build the repeated work pattern string."""
    pattern = str(template.get("repeated_work_pattern", "")).strip()
    output_need = str(audit_row.get("dominant_output_need_signals", "")).strip()
    if output_need:
        return f"{pattern} The cluster repeatedly points to {output_need.replace('|', ', ')} as part of the work."
    return pattern


def _build_workflow_failure(template: dict[str, Any], audit_row: pd.Series, summary: dict[str, Any]) -> str:
    """Explain why the current workflow fails this persona."""
    failure = str(template.get("why_existing_workflow_fails", "")).strip()
    trust = str(audit_row.get("dominant_trust_reporting_signals", "")).strip()
    if trust:
        return f"{failure} Trust and reporting signals show extra friction around {trust.replace('|', ', ')}."
    return failure


def _supporting_evidence(audit_row: pd.Series, summary: dict[str, Any], examples: list[str]) -> list[str]:
    """Build 2-4 compact evidence bullets."""
    bullets: list[str] = []
    if str(summary.get("main_workflow_context", "")).strip():
        bullets.append(f"Workflow context: {str(summary.get('main_workflow_context', '')).replace('_', ' ')}.")
    if str(audit_row.get("dominant_manual_work_signals", "")).strip():
        bullets.append(f"Repeated manual-work signals: {str(audit_row.get('dominant_manual_work_signals', '')).replace('|', ', ')}.")
    if str(audit_row.get("dominant_trust_reporting_signals", "")).strip():
        bullets.append(f"Trust/reporting pressure: {str(audit_row.get('dominant_trust_reporting_signals', '')).replace('|', ', ')}.")
    if examples:
        bullets.append(f"Representative evidence: {examples[0][:180]}")
    return bullets[:4]


def _current_workaround(audit_row: pd.Series, summary: dict[str, Any], examples: list[str]) -> str:
    """Describe the current workaround implied by the cluster evidence."""
    manual_signals = str(audit_row.get("dominant_manual_work_signals", "")).strip()
    if "spreadsheet" in manual_signals or "export" in str(audit_row.get("dominant_output_need_signals", "")).lower():
        return "Users fall back to export, spreadsheet cleanup, or repeated manual packaging before sharing results."
    if "tool_limitation_workaround" in str(audit_row.get("dominant_bottleneck_signals", "")):
        return "Users leave the current tool or invent workaround steps to finish the task."
    if examples:
        return f"Observed workaround: {examples[0][:180]}"
    return "Observed workaround is plausible but not strongly evidenced yet."


def _confidence_and_flags(audit_row: pd.Series, examples: list[str], config: dict[str, Any]) -> tuple[str, list[tuple[str, str]]]:
    """Build a confidence note and weak-persona flags."""
    thresholds = dict(config.get("quality_thresholds", {}))
    flags: list[tuple[str, str]] = []
    separation = float(audit_row.get("separation", 0.0) or 0.0)
    cohesion = float(audit_row.get("cohesion", 0.0) or 0.0)
    role_dominance = float(audit_row.get("role_dominance", 0.0) or 0.0)
    source_dominance = _top_share(audit_row.get("source_distribution_json", "[]"))
    if separation <= float(thresholds.get("weak_separation_max", 0.12)):
        flags.append(("weak_separation", "Cluster separation from neighboring personas is still weak."))
    if cohesion <= float(thresholds.get("weak_cohesion_max", 0.82)):
        flags.append(("weak_cohesion", "Internal cluster cohesion is weaker than preferred."))
    if role_dominance >= float(thresholds.get("high_role_dominance", 0.9)):
        flags.append(("role_skew", "Evidence is still concentrated in one dominant role group."))
    if source_dominance >= float(thresholds.get("high_source_dominance", 0.85)):
        flags.append(("source_concentration", "Evidence is concentrated in one source, so generalization risk remains.")) 
    if len(examples) < int(thresholds.get("low_example_count", 2)):
        flags.append(("limited_examples", "Representative examples are thin, so wording should stay cautious."))
    if not flags:
        return "Evidence is directionally strong enough for strategy discussion, but still exploratory rather than fully validated.", flags
    note = "Caution: " + " ".join(details for _, details in flags)
    return note, flags


def _alternative_names(primary_signal: str, secondary_signals: list[str], config: dict[str, Any]) -> list[str]:
    """Return alternative but non-preferred bottleneck-first names."""
    alternatives: list[str] = []
    for signal in secondary_signals[:2]:
        alt = str(_template_for(signal, config).get("primary_name", signal.replace("_", " ").title())).strip()
        if alt and alt.lower() != "mixed workflow friction":
            alternatives.append(alt)
    return alternatives


def _name_centering_type(name: str, config: dict[str, Any]) -> str:
    """Classify whether a name is role-heavy, tool-heavy, or bottleneck-centered."""
    lowered = str(name or "").strip().lower()
    role_terms = list(config.get("solution_guardrails", {}).get("role_heavy_terms", [])) or ROLE_HEAVY_NAME_TERMS
    tool_terms = list(config.get("solution_guardrails", {}).get("tool_heavy_terms", [])) or TOOL_HEAVY_NAME_TERMS
    if contains_any_term(lowered, role_terms):
        return "role-heavy"
    if contains_any_term(lowered, tool_terms):
        return "tool-heavy"
    return "bottleneck-centered"


def _readability_rating(name: str) -> int:
    """Return a simple readability rating from 1 to 5."""
    length = len(str(name or "").strip())
    if length <= 18:
        return 5
    if length <= 34:
        return 4
    if length <= 52:
        return 3
    return 2


def _presentation_rating(name: str) -> int:
    """Return a simple presentation-usability rating from 1 to 5."""
    lowered = str(name or "").lower()
    if any(token in lowered for token in ["cluster", "type", "user", "persona"]):
        return 2
    if "+" in lowered:
        return 4
    return 5


def _persona_insights_markdown(persona_cards_df: pd.DataFrame) -> str:
    """Render persona insights as markdown."""
    sections = ["# Persona Insights V2", ""]
    for _, row in persona_cards_df.iterrows():
        sections.extend(
            [
                f"## {row['primary_persona_name']}",
                "",
                f"- Subtitle: {row['persona_subtitle']}",
                f"- Core insight: {row['core_insight']}",
                f"- Repeated work pattern: {row['repeated_work_pattern']}",
                f"- Why the workflow fails: {row['why_existing_workflow_fails']}",
                f"- Confidence note: {row['confidence_note']}",
                "",
            ]
        )
        evidence = json.loads(str(row.get("supporting_evidence", "[]") or "[]"))
        for bullet in evidence:
            sections.append(f"- Evidence: {bullet}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def _persona_solution_markdown(persona_cards_df: pd.DataFrame) -> str:
    """Render persona solution linkage as markdown."""
    sections = ["# Persona Solution Linkage", ""]
    for _, row in persona_cards_df.iterrows():
        sections.extend(
            [
                f"## {row['primary_persona_name']}",
                "",
                f"- Bottleneck: {row['bottleneck_signature']}",
                f"- Why current workflow fails: {row['why_existing_workflow_fails']}",
                f"- Solution direction: {row['solution_direction']}",
                f"- Target problem: {row['target_problem']}",
                f"- Expected user value: {row['expected_user_value']}",
            ]
        )
        interventions = json.loads(str(row.get("suggested_interventions", "[]") or "[]"))
        for item in interventions:
            sections.append(f"- Suggested intervention: {item}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def _before_after_summary(
    persona_summary_df: pd.DataFrame,
    personas: list[dict[str, Any]],
    persona_cards_df: pd.DataFrame,
    naming_audit_df: pd.DataFrame,
) -> str:
    """Render a compact before/after summary of persona naming and messaging."""
    old_names = [str(item.get("persona_name", "")) for item in list(personas or [])]
    new_names = persona_cards_df.get("primary_persona_name", pd.Series(dtype=str)).astype(str).tolist()
    role_heavy_before = sum(1 for name in old_names if contains_any_term(name, ROLE_HEAVY_NAME_TERMS))
    role_heavy_after = sum(1 for name in new_names if contains_any_term(name, ROLE_HEAVY_NAME_TERMS))
    sections = [
        "# Before vs After Persona Summary",
        "",
        f"- Persona count: {len(new_names)}",
        f"- Role-heavy names: {role_heavy_before} -> {role_heavy_after}",
        f"- Bottleneck-centered names after: {int((naming_audit_df.get('name_centering_type', pd.Series(dtype=str)) == 'bottleneck-centered').sum())}",
        f"- Persona cards with explicit solution direction: {int(persona_cards_df['solution_direction'].astype(str).str.len().gt(0).sum())}",
        "",
        "## New Names",
        "",
    ]
    for name in new_names:
        sections.append(f"- {name}")
    return "\n".join(sections) + "\n"


def _top_share(distribution_json: object) -> float:
    """Return the top share from a serialized distribution list."""
    try:
        rows = json.loads(str(distribution_json or "[]"))
    except json.JSONDecodeError:
        return 0.0
    if not rows:
        return 0.0
    return float(rows[0].get("share", 0.0) or 0.0)


def _is_generic_name(name: str) -> bool:
    """Return whether a name is too generic for persona presentation."""
    lowered = str(name or "").strip().lower()
    return lowered in GENERIC_PERSONA_NAMES or len(lowered) < 8
