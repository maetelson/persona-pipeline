"""Report-ready exports for persona analysis outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir


def export_persona_reports(
    root_dir: Path,
    personas: list[dict[str, Any]],
    cluster_profiles: list[dict[str, Any]],
    cluster_summary_rows: list[dict[str, Any]],
    quality_checks: dict[str, Any],
) -> dict[str, Path]:
    """Write persona report assets in CSV/JSON/Markdown formats."""
    output_dir = ensure_dir(root_dir / "data" / "analysis")
    persona_table_df = _build_persona_table(personas, cluster_profiles)
    cluster_stats_df = _build_cluster_stats(cluster_profiles, cluster_summary_rows)

    persona_table_path = output_dir / "code_persona_table.csv"
    cluster_stats_path = output_dir / "code_cluster_stats.csv"
    persona_cards_path = output_dir / "code_persona_cards.md"
    quality_checks_path = output_dir / "quality_checks.json"
    personas_json_path = output_dir / "personas.json"

    persona_table_df.to_csv(persona_table_path, index=False)
    cluster_stats_df.to_csv(cluster_stats_path, index=False)
    persona_cards_path.write_text(_build_persona_cards_markdown(personas, cluster_profiles), encoding="utf-8")
    quality_checks_path.write_text(json.dumps(quality_checks, ensure_ascii=False, indent=2), encoding="utf-8")
    personas_json_path.write_text(json.dumps(personas, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "persona_table": persona_table_path,
        "cluster_stats": cluster_stats_path,
        "persona_cards": persona_cards_path,
        "quality_checks": quality_checks_path,
        "personas_json": personas_json_path,
    }


def _build_persona_table(personas: list[dict[str, Any]], cluster_profiles: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten persona objects into a report-friendly table."""
    profile_lookup = {str(row["cluster_id"]): row for row in cluster_profiles}
    rows: list[dict[str, Any]] = []
    for persona in personas:
        cluster_id = str(persona.get("cluster_id", ""))
        profile = profile_lookup.get(cluster_id, {})
        rows.append(
            {
                "cluster_id": cluster_id,
                "persona_name": persona.get("persona_name", ""),
                "one_line_summary": persona.get("one_line_summary", ""),
                "core_demographic": persona.get("core_demographic", ""),
                "percent_of_total_data": round(float(profile.get("share_of_total", 0.0)) * 100, 2),
                "dominant_needs": " | ".join(profile.get("top_need_codes", [])[:5]),
                "demographic_bias": " | ".join(profile.get("top_demographics", [])[:3]),
                "opportunity": persona.get("opportunity", ""),
            }
        )
    return pd.DataFrame(rows)


def _build_cluster_stats(cluster_profiles: list[dict[str, Any]], cluster_summary_rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Build cluster stats table for reporting."""
    summary_lookup = {str(row["cluster_id"]): row for row in cluster_summary_rows}
    rows: list[dict[str, Any]] = []
    for profile in cluster_profiles:
        cluster_id = str(profile["cluster_id"])
        summary = summary_lookup.get(cluster_id, {})
        rows.append(
            {
                "cluster_id": cluster_id,
                "size": int(profile.get("size", 0)),
                "percent_of_total_data": round(float(profile.get("share_of_total", 0.0)) * 100, 2),
                "top_codes": " | ".join(summary.get("top_codes", [])),
                "edge_density": float(summary.get("edge_density", 0.0)),
                "top_demographics": " | ".join(profile.get("top_demographics", [])[:3]),
                "top_need_codes": " | ".join(profile.get("top_need_codes", [])[:5]),
            }
        )
    return pd.DataFrame(rows)


def _build_persona_cards_markdown(personas: list[dict[str, Any]], cluster_profiles: list[dict[str, Any]]) -> str:
    """Build report-style persona cards in markdown."""
    profile_lookup = {str(row["cluster_id"]): row for row in cluster_profiles}
    sections = ["# Persona Cards", ""]
    for persona in personas:
        cluster_id = str(persona.get("cluster_id", ""))
        profile = profile_lookup.get(cluster_id, {})
        sections.extend(
            [
                f"## {persona.get('persona_name', cluster_id)}",
                f"- Cluster: `{cluster_id}`",
                f"- Share of Data: {round(float(profile.get('share_of_total', 0.0)) * 100, 2)}%",
                f"- Core Demographic: {persona.get('core_demographic', '')}",
                f"- One-line Summary: {persona.get('one_line_summary', '')}",
                f"- Top Pain Points: {', '.join(persona.get('top_pain_points', []))}",
                f"- Co-occurring Needs: {', '.join(persona.get('co_occurring_needs', []))}",
                f"- Opportunity: {persona.get('opportunity', '')}",
                "- Example Quotes:",
            ]
        )
        for quote in persona.get("example_quotes", [])[:5]:
            sections.append(f"  - {quote}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"
