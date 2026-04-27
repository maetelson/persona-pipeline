"""Generate Phase 1 diagnostics-only source-slice classification artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.source_slice_classification import (  # noqa: E402
    build_source_slice_classification_outputs,
    write_source_slice_classification_artifacts,
)
from src.utils.io import read_parquet  # noqa: E402


def main() -> None:
    """Build diagnostics-only source-slice artifacts from current labeled rows."""
    labeled_df = read_parquet(ROOT_DIR / "data" / "labeled" / "labeled_episodes.parquet")
    episodes_df = read_parquet(ROOT_DIR / "data" / "episodes" / "episode_table.parquet")
    denominator_rows_df = pd.read_csv(
        ROOT_DIR / "artifacts" / "readiness" / "deck_ready_denominator_eligibility_rows.csv"
    )
    source_balance_audit_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "source_balance_audit.csv")
    overview_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "overview.csv")
    quality_checks_df = pd.read_csv(ROOT_DIR / "data" / "analysis" / "quality_checks.csv")

    outputs = build_source_slice_classification_outputs(
        labeled_df=labeled_df,
        episodes_df=episodes_df,
        denominator_rows_df=denominator_rows_df,
        source_balance_audit_df=source_balance_audit_df,
        overview_df=overview_df,
        quality_checks_df=quality_checks_df,
    )
    artifact_paths = write_source_slice_classification_artifacts(
        ROOT_DIR,
        outputs["rows_df"],
        outputs["summary"],
    )
    report = {
        "total_rows_classified": outputs["summary"]["total_rows_classified"],
        "count_by_source_slice_category": outputs["summary"]["count_by_source_slice_category"],
        "weak_source_slice_summary": outputs["summary"]["weak_source_slice_summary"],
        "official_metrics_unchanged_confirmation": outputs["summary"]["official_metrics_unchanged_confirmation"],
        "boolean_default_note": outputs["summary"]["boolean_default_note"],
        "note": (
            "Phase 1 adds diagnostics-only source-slice fields. "
            "It does not change official source balance, weak-source counts, readiness gates, or persona semantics."
        ),
    }
    report_path = ROOT_DIR / "artifacts" / "readiness" / "source_slice_classification_phase1_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    doc_lines = [
        "# Source Slice Classification Phase 1",
        "",
        "## Scope",
        "",
        "This pass adds diagnostics-only source-slice classification fields.",
        "It does not quarantine slices, does not change official source balance or weak-source counts, and does not alter readiness or persona status logic.",
        "",
        "## Current Outputs",
        "",
        f"- total_rows_classified: `{outputs['summary']['total_rows_classified']}`",
        f"- evidence_producing_slice_count: `{outputs['summary']['evidence_producing_slice_count']}`",
        f"- mixed_evidence_slice_count: `{outputs['summary']['mixed_evidence_slice_count']}`",
        f"- debt_producing_slice_count: `{outputs['summary']['debt_producing_slice_count']}`",
        f"- diagnostics_only_count: `{outputs['summary']['diagnostics_only_count']}`",
        "",
        "## Weak-Source Highlights",
        "",
    ]
    for source, payload in outputs["summary"]["weak_source_slice_summary"].items():
        doc_lines.append(f"- `{source}`: `{payload.get('count_by_slice_category', {})}`")
    doc_lines += [
        "",
        "## Guardrails",
        "",
        "- source tiers remain unchanged",
        "- official effective source balance remains unchanged",
        "- official weak-source counts remain unchanged",
        "- slice rows remain visible in diagnostics",
        "- this phase does not create audited secondary balance or weak-debt metrics",
    ]
    doc_path = ROOT_DIR / "docs" / "operational" / "SOURCE_SLICE_CLASSIFICATION_PHASE1.md"
    doc_path.write_text("\n".join(doc_lines) + "\n", encoding="utf-8")

    artifact_paths["phase1_report_json"] = report_path
    artifact_paths["phase1_note_md"] = doc_path
    print(json.dumps({key: str(value) for key, value in artifact_paths.items()}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
