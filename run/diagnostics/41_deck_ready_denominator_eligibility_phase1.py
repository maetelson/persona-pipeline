"""Generate Phase 1 diagnostics-only deck-ready denominator eligibility artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.deck_ready_denominator_eligibility import (  # noqa: E402
    build_conservative_deck_ready_denominator_metrics,
    build_denominator_classifier_calibration_report,
    build_deck_ready_denominator_eligibility_outputs,
    write_conservative_denominator_artifacts,
    write_deck_ready_denominator_eligibility_artifacts,
)
from src.utils.io import read_parquet  # noqa: E402


def main() -> None:
    """Build diagnostics-only denominator eligibility artifacts from current labeled rows."""
    previous_summary_path = ROOT_DIR / "artifacts" / "readiness" / "deck_ready_denominator_eligibility_summary.json"
    previous_summary: dict[str, object] | None = None
    if previous_summary_path.exists():
        previous_summary = json.loads(previous_summary_path.read_text(encoding="utf-8"))

    labeled_df = read_parquet(ROOT_DIR / "data" / "labeled" / "labeled_episodes.parquet")
    episodes_df = read_parquet(ROOT_DIR / "data" / "episodes" / "episode_table.parquet")
    persona_assignments_df = read_parquet(ROOT_DIR / "data" / "analysis" / "persona_assignments.parquet")
    source_balance_audit_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
    overview_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
    overview_metrics = dict(zip(overview_df["metric"].astype(str), overview_df["value"]))
    current_coverage = float(overview_metrics.get("persona_core_coverage_of_all_labeled_pct", 0.0) or 0.0)

    outputs = build_deck_ready_denominator_eligibility_outputs(
        labeled_df=labeled_df,
        episodes_df=episodes_df,
        persona_assignments_df=persona_assignments_df,
        source_balance_audit_df=source_balance_audit_df,
        current_persona_core_coverage_pct=current_coverage,
    )
    artifact_paths = write_deck_ready_denominator_eligibility_artifacts(
        ROOT_DIR,
        outputs["rows_df"],
        outputs["summary"],
    )
    conservative_metrics = build_conservative_deck_ready_denominator_metrics(
        outputs["rows_df"],
        persona_core_row_count=int(outputs["summary"].get("persona_core_rows", 0) or 0),
    )
    conservative_paths = write_conservative_denominator_artifacts(
        ROOT_DIR,
        outputs["rows_df"],
        conservative_metrics,
    )
    report = build_denominator_classifier_calibration_report(previous_summary, outputs["summary"])
    report_path = (
        ROOT_DIR / "artifacts" / "readiness" / "deck_ready_denominator_classifier_calibration_report.json"
    )
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    implementation_report = {
        "policy_mode": conservative_metrics["denominator_policy_mode"],
        "policy_version": conservative_metrics["denominator_policy_version"],
        "original_persona_core_coverage_pct": conservative_metrics["original_persona_core_coverage_pct"],
        "adjusted_deck_ready_denominator_row_count": conservative_metrics["adjusted_deck_ready_denominator_row_count"],
        "adjusted_deck_ready_denominator_excluded_row_count": conservative_metrics["adjusted_deck_ready_denominator_excluded_row_count"],
        "adjusted_deck_ready_denominator_core_coverage_pct": conservative_metrics["adjusted_deck_ready_denominator_core_coverage_pct"],
        "denominator_exclusion_count_by_category": conservative_metrics["denominator_exclusion_count_by_category"],
        "denominator_exclusion_count_by_source": conservative_metrics["denominator_exclusion_count_by_source"],
        "denominator_exclusion_count_by_source_tier": conservative_metrics["denominator_exclusion_count_by_source_tier"],
        "adjusted_denominator_metric_status": conservative_metrics["adjusted_denominator_metric_status"],
        "note": (
            "Original persona_core_coverage_of_all_labeled_pct remains unchanged and visible. "
            "Adjusted conservative denominator coverage is added as a separate audited metric only."
        ),
    }
    implementation_report_path = (
        ROOT_DIR / "artifacts" / "readiness" / "deck_ready_denominator_conservative_policy_implementation.json"
    )
    implementation_report_path.write_text(
        json.dumps(implementation_report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    implementation_note_path = (
        ROOT_DIR / "docs" / "operational" / "DECK_READY_DENOMINATOR_CONSERVATIVE_POLICY_IMPLEMENTATION.md"
    )
    implementation_note_path.write_text(
        "\n".join(
            [
                "# Deck-Ready Denominator Conservative Policy Implementation",
                "",
                "## Scope",
                "",
                "This pass implements the conservative Scenario H adjusted denominator metric only.",
                "It does not replace the original coverage metric, does not change readiness logic, and does not alter persona counts.",
                "",
                "## Conservative Exclusion Rule",
                "",
                "A row is excluded from the adjusted deck-ready denominator only when all are true:",
                "",
                "1. `persona_core_eligible = False`",
                "2. `deck_ready_denominator_eligible = False`",
                "3. `denominator_eligibility_category` is one explicit technical/support noise category",
                "4. `technical_noise_confidence >= 0.9`",
                "5. the row is not `ambiguous_review_bucket`",
                "6. the row is not `denominator_eligible_business_non_core`",
                "",
                "## Current Audited Values",
                "",
                f"- original_persona_core_coverage_pct: `{conservative_metrics['original_persona_core_coverage_pct']}`",
                f"- adjusted_deck_ready_denominator_row_count: `{conservative_metrics['adjusted_deck_ready_denominator_row_count']}`",
                f"- adjusted_deck_ready_denominator_excluded_row_count: `{conservative_metrics['adjusted_deck_ready_denominator_excluded_row_count']}`",
                f"- adjusted_deck_ready_denominator_core_coverage_pct: `{conservative_metrics['adjusted_deck_ready_denominator_core_coverage_pct']}`",
                "",
                "## Guardrails",
                "",
                "- the original `persona_core_coverage_of_all_labeled_pct` remains visible and unchanged",
                "- adjusted coverage is added as a separate audited metric",
                "- excluded rows remain visible in diagnostics and exclusion artifacts",
                "- ambiguous rows remain included",
                "- no readiness state or persona count changes occur in this pass",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    secondary_gate_report = {
        "coverage_gate_metric_used": overview_metrics.get("coverage_gate_metric_used", ""),
        "original_coverage_gate_status": overview_metrics.get("original_coverage_gate_status", ""),
        "adjusted_coverage_gate_status": overview_metrics.get("adjusted_coverage_gate_status", ""),
        "coverage_gate_passed_by_adjusted_metric": overview_metrics.get("coverage_gate_passed_by_adjusted_metric", ""),
        "adjusted_denominator_policy_applied": overview_metrics.get("adjusted_denominator_policy_applied", ""),
        "adjusted_denominator_policy_reason": overview_metrics.get("adjusted_denominator_policy_reason", ""),
        "original_persona_core_coverage_pct": overview_metrics.get("original_persona_core_coverage_pct", ""),
        "adjusted_deck_ready_denominator_core_coverage_pct": overview_metrics.get(
            "adjusted_deck_ready_denominator_core_coverage_pct", ""
        ),
        "persona_core_coverage_of_all_labeled_pct": overview_metrics.get(
            "persona_core_coverage_of_all_labeled_pct", ""
        ),
        "persona_readiness_state": overview_metrics.get("persona_readiness_state", ""),
        "overall_status": overview_metrics.get("overall_status", ""),
        "quality_flag": overview_metrics.get("quality_flag", ""),
        "effective_balanced_source_count": overview_metrics.get("effective_balanced_source_count", ""),
        "weak_source_cost_center_count": overview_metrics.get("weak_source_cost_center_count", ""),
        "core_readiness_weak_source_cost_center_count": overview_metrics.get(
            "core_readiness_weak_source_cost_center_count", ""
        ),
        "final_usable_persona_count": overview_metrics.get("final_usable_persona_count", ""),
        "production_ready_persona_count": overview_metrics.get("production_ready_persona_count", ""),
        "review_ready_persona_count": overview_metrics.get("review_ready_persona_count", ""),
        "deck_ready_claim_eligible_persona_count": overview_metrics.get(
            "deck_ready_claim_eligible_persona_count", ""
        ),
        "note": (
            "The adjusted conservative denominator metric now participates in the coverage component as a secondary gate only. "
            "The original metric remains visible and unchanged. Non-coverage readiness gates are unchanged."
        ),
    }
    secondary_gate_report_path = (
        ROOT_DIR / "artifacts" / "readiness" / "adjusted_denominator_secondary_gate_implementation.json"
    )
    secondary_gate_report_path.write_text(
        json.dumps(secondary_gate_report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    secondary_gate_note_path = (
        ROOT_DIR / "docs" / "operational" / "ADJUSTED_DENOMINATOR_SECONDARY_GATE_IMPLEMENTATION.md"
    )
    secondary_gate_note_path.write_text(
        "\n".join(
            [
                "# Adjusted Denominator Secondary Gate Implementation",
                "",
                "## Scope",
                "",
                "This pass wires the audited adjusted conservative denominator metric into the coverage component as a secondary gate only.",
                "It does not replace the original coverage metric, does not hide excluded rows, and does not change non-coverage readiness gates.",
                "",
                "## Secondary Gate Logic",
                "",
                "Coverage may pass by the adjusted metric only when all are true:",
                "",
                "1. `adjusted_denominator_metric_status = audited`",
                "2. `denominator_policy_mode = conservative_high_confidence_noise_only`",
                "3. `denominator_policy_version = v1`",
                "4. `adjusted_deck_ready_denominator_core_coverage_pct >= 80.0`",
                "5. `ambiguous_review_bucket` remains included",
                "6. `denominator_eligible_business_non_core` remains included",
                "7. excluded rows remain diagnostics-visible",
                "",
                "## Current Values",
                "",
                f"- original coverage: `{overview_metrics.get('persona_core_coverage_of_all_labeled_pct', '')}`",
                f"- original_persona_core_coverage_pct: `{overview_metrics.get('original_persona_core_coverage_pct', '')}`",
                f"- adjusted coverage: `{overview_metrics.get('adjusted_deck_ready_denominator_core_coverage_pct', '')}`",
                f"- coverage_gate_metric_used: `{overview_metrics.get('coverage_gate_metric_used', '')}`",
                f"- original_coverage_gate_status: `{overview_metrics.get('original_coverage_gate_status', '')}`",
                f"- adjusted_coverage_gate_status: `{overview_metrics.get('adjusted_coverage_gate_status', '')}`",
                f"- coverage_gate_passed_by_adjusted_metric: `{overview_metrics.get('coverage_gate_passed_by_adjusted_metric', '')}`",
                "",
                "## Invariants",
                "",
                f"- persona_readiness_state remains `{overview_metrics.get('persona_readiness_state', '')}`",
                f"- overall_status remains `{overview_metrics.get('overall_status', '')}`",
                f"- quality_flag remains `{overview_metrics.get('quality_flag', '')}`",
                f"- final_usable_persona_count remains `{overview_metrics.get('final_usable_persona_count', '')}`",
                f"- production_ready_persona_count remains `{overview_metrics.get('production_ready_persona_count', '')}`",
                f"- review_ready_persona_count remains `{overview_metrics.get('review_ready_persona_count', '')}`",
                f"- deck_ready_claim_eligible_persona_count remains `{overview_metrics.get('deck_ready_claim_eligible_persona_count', '')}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths["calibration_report_json"] = report_path
    artifact_paths["conservative_exclusions_csv"] = conservative_paths["conservative_exclusions_csv"]
    artifact_paths["conservative_metric_json"] = conservative_paths["conservative_metric_json"]
    artifact_paths["implementation_report_json"] = implementation_report_path
    artifact_paths["implementation_note_md"] = implementation_note_path
    artifact_paths["secondary_gate_report_json"] = secondary_gate_report_path
    artifact_paths["secondary_gate_note_md"] = secondary_gate_note_path
    print(json.dumps({key: str(value) for key, value in artifact_paths.items()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
