"""Audit workbook metric provenance and suspicious denominator/grain mismatches."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.utils.io import read_parquet


def main() -> None:
    """Print reproducible workbook audit metrics from canonical parquet artifacts."""
    parser = argparse.ArgumentParser(description="Audit persona workbook metrics and denominators.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")
    args = parser.parse_args()

    result = build_audit_snapshot(ROOT)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    _print_snapshot(result)


def build_audit_snapshot(root_dir: Path) -> dict[str, Any]:
    """Build a compact snapshot of workbook-facing metrics and detected anomalies."""
    workbook_dir = root_dir / "data" / "analysis" / "workbook_bundle"
    overview_df = read_parquet(workbook_dir / "overview.parquet")
    quality_checks_df = read_parquet(workbook_dir / "quality_checks.parquet")
    cluster_stats_df = read_parquet(workbook_dir / "cluster_stats.parquet")
    source_diagnostics_df = read_parquet(workbook_dir / "source_diagnostics.parquet")
    persona_examples_df = read_parquet(workbook_dir / "persona_examples.parquet")
    labeled_df = read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet")
    labelability_df = read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet")

    overview = _metric_lookup(overview_df, "metric", "value")
    quality_checks = _row_lookup(quality_checks_df, "metric")
    overview_labeled_records = _first_metric_value(
        overview,
        ["labeled_episode_rows", "total_labeled_records"],
        0,
    )
    quality_persona_core_row = _first_metric_row(
        quality_checks,
        ["persona_core_labeled_rows", "persona_core_labeled_count"],
    )
    quality_core_unknown_row = _first_metric_row(
        quality_checks,
        ["persona_core_unknown_ratio", "unknown_ratio"],
    )
    promoted_clusters = cluster_stats_df[cluster_stats_df.get("promotion_status", pd.Series(dtype=str)).astype(str).eq("promoted_persona")].copy()

    labeled_count = int(len(labeled_df))
    core_labeled_count = int(
        len(labeled_df[labeled_df["persona_core_eligible"].fillna(True)])
        if "persona_core_eligible" in labeled_df.columns
        else len(labeled_df)
    )
    largest_persona_size = int(pd.to_numeric(promoted_clusters.get("persona_size", pd.Series(dtype=int)), errors="coerce").fillna(0).max()) if not promoted_clusters.empty else 0
    largest_share_overall = round((largest_persona_size / max(labeled_count, 1)) * 100, 1)
    largest_share_core = round((largest_persona_size / max(core_labeled_count, 1)) * 100, 1)

    selected_examples_by_persona = (
        persona_examples_df.groupby("persona_id").size().to_dict()
        if not persona_examples_df.empty and "persona_id" in persona_examples_df.columns
        else {}
    )
    promoted_without_examples = [
        str(persona_id)
        for persona_id in promoted_clusters.get("persona_id", pd.Series(dtype=str)).astype(str).tolist()
        if int(selected_examples_by_persona.get(str(persona_id), 0)) <= 0
    ]

    suspicious_source_rows: list[dict[str, Any]] = []
    if not source_diagnostics_df.empty and {"source", "metric_name", "metric_value", "grain"}.issubset(source_diagnostics_df.columns):
        for source, group in source_diagnostics_df.groupby("source", dropna=False):
            bridge_bad = group[
                group["grain"].astype(str).eq("mixed_grain_bridge")
                & group["metric_name"].astype(str).str.contains("rate|share|survival", case=False, regex=True)
            ]
            percentage_bad = group[
                group["metric_type"].astype(str).eq("percentage")
                & ~pd.to_numeric(group["metric_value"], errors="coerce").fillna(0.0).between(0.0, 100.0)
            ]
            reasons: list[str] = []
            if not bridge_bad.empty:
                reasons.append("mixed_grain_metric_uses_rate_like_name")
            if not percentage_bad.empty:
                reasons.append("same_grain_percentage_out_of_bounds")
            if reasons:
                suspicious_source_rows.append(
                    {
                        "source": str(source),
                        "reasons": reasons,
                        "bad_metric_names": sorted(set(bridge_bad.get("metric_name", pd.Series(dtype=str)).astype(str).tolist())),
                    }
                )

    labelability_counts = (
        labelability_df.get("labelability_status", pd.Series(dtype=str)).astype(str).value_counts().to_dict()
        if not labelability_df.empty
        else {}
    )

    return {
        "source_of_truth": {
            "entrypoint": "run/pipeline/07_export_xlsx.py -> src.analysis.stage_service.run_final_report_stage -> src.exporters.xlsx_exporter.export_workbook_from_frames",
            "bundle_builder": "src.analysis.stage_service.build_deterministic_analysis_outputs -> src.analysis.workbook_bundle.assemble_workbook_frames",
            "sheets": {
                "overview": "src.analysis.persona_service._build_overview_df + src.analysis.stage_service._update_overview_quality",
                "cluster_stats": "src.analysis.persona_service._build_cluster_stats_df",
                "persona_summary": "src.analysis.persona_service._build_persona_summary_df",
                "quality_checks": "src.analysis.summary.build_quality_checks + src.analysis.diagnostics.finalize_quality_checks + src.analysis.summary.build_quality_checks_df",
                "source_diagnostics": "src.analysis.diagnostics.build_source_diagnostics",
                "persona_examples": "src.analysis.example_selection.select_persona_representative_examples -> src.analysis.persona_service._build_persona_examples_df",
            },
        },
        "current_metrics": {
            "overview_total_labeled_records": int(_num(overview_labeled_records)),
            "overview_quality_flag": str(overview.get("quality_flag", "")),
            "quality_persona_core_labeled_count": int(_num(quality_persona_core_row.get("value", 0))),
            "quality_unknown_ratio": _num(quality_core_unknown_row.get("value", 0)),
            "quality_unknown_ratio_denominator_type": str(quality_core_unknown_row.get("denominator_type", "")),
            "quality_unknown_ratio_denominator_value": int(_num(quality_core_unknown_row.get("denominator_value", 0))),
            "quality_overall_unknown_ratio": _num(quality_checks.get("overall_unknown_ratio", {}).get("value", 0)),
            "quality_overall_unknown_ratio_denominator_type": str(quality_checks.get("overall_unknown_ratio", {}).get("denominator_type", "")),
            "quality_overall_unknown_ratio_denominator_value": int(_num(quality_checks.get("overall_unknown_ratio", {}).get("denominator_value", 0))),
            "quality_largest_cluster_share_of_core_labeled": _num(quality_checks.get("largest_cluster_share_of_core_labeled", {}).get("value", 0)),
            "largest_persona_size": largest_persona_size,
            "derived_largest_cluster_share_overall_labeled_pct": largest_share_overall,
            "derived_largest_cluster_share_core_labeled_pct": largest_share_core,
            "labeled_count": labeled_count,
            "core_labeled_count": core_labeled_count,
            "labelability_status_counts": labelability_counts,
        },
        "promoted_personas_missing_selected_examples": promoted_without_examples,
        "suspicious_source_diagnostics_rows": suspicious_source_rows,
    }


def _metric_lookup(df: pd.DataFrame, key_column: str, value_column: str) -> dict[str, Any]:
    """Return a key-value mapping from a two-column metric frame."""
    if df.empty or key_column not in df.columns or value_column not in df.columns:
        return {}
    return dict(zip(df[key_column].astype(str), df[value_column]))


def _row_lookup(df: pd.DataFrame, key_column: str) -> dict[str, dict[str, Any]]:
    """Return a mapping from metric name to full row dict."""
    if df.empty or key_column not in df.columns:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        rows[str(row.get(key_column, ""))] = row.to_dict()
    return rows


def _first_metric_value(metrics: dict[str, Any], keys: list[str], default: Any) -> Any:
    """Return the first available scalar metric value from a list of aliases."""
    for key in keys:
        if key in metrics:
            return metrics[key]
    return default


def _first_metric_row(rows: dict[str, dict[str, Any]], keys: list[str]) -> dict[str, Any]:
    """Return the first available metric row from a list of aliases."""
    for key in keys:
        if key in rows:
            return rows[key]
    return {}


def _num(value: object) -> float:
    """Convert workbook scalar-like values into a comparable float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _print_snapshot(snapshot: dict[str, Any]) -> None:
    """Render the audit snapshot in a compact human-readable form."""
    print("PERSONA WORKBOOK AUDIT")
    print("")
    print("Source Of Truth")
    for key, value in snapshot["source_of_truth"].items():
        if isinstance(value, dict):
            print(f"- {key}:")
            for inner_key, inner_value in value.items():
                print(f"  - {inner_key}: {inner_value}")
        else:
            print(f"- {key}: {value}")
    print("")
    print("Current Metrics")
    for key, value in snapshot["current_metrics"].items():
        print(f"- {key}: {value}")
    print("")
    print("Promoted Personas Missing Selected Examples")
    if snapshot["promoted_personas_missing_selected_examples"]:
        for persona_id in snapshot["promoted_personas_missing_selected_examples"]:
            print(f"- {persona_id}")
    else:
        print("- none")
    print("")
    print("Suspicious Source Diagnostics Rows")
    if snapshot["suspicious_source_diagnostics_rows"]:
        for row in snapshot["suspicious_source_diagnostics_rows"]:
            print(f"- {row}")
    else:
        print("- none")


if __name__ == "__main__":
    main()
