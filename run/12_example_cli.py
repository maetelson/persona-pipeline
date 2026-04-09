"""Select and audit representative persona examples."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.analysis.example_selection import build_legacy_representative_examples, compare_example_selection
from src.analysis.persona_service import build_persona_outputs, write_persona_outputs
from src.utils.io import read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.example_cli")


def main() -> None:
    """Run representative-example selection and QA commands."""
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "select-representative-examples":
        _select_examples(cluster=args.cluster)
    elif args.command == "qa-representative-examples":
        _qa_examples(cluster=args.cluster)
    elif args.command == "export-example-audit":
        _export_example_audit()
    elif args.command == "compare-example-selection":
        _compare_examples(args.before, args.after)
    else:
        raise ValueError(f"Unsupported command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    """Create CLI parser."""
    parser = argparse.ArgumentParser(description="Representative example selection CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    select_parser = subparsers.add_parser("select-representative-examples")
    select_parser.add_argument("--cluster", default="")

    qa_parser = subparsers.add_parser("qa-representative-examples")
    qa_parser.add_argument("--cluster", default="")

    subparsers.add_parser("export-example-audit")

    compare_parser = subparsers.add_parser("compare-example-selection")
    compare_parser.add_argument("--before", default=str(ROOT / "data" / "analysis" / "persona_examples.csv"))
    compare_parser.add_argument("--after", default=str(ROOT / "data" / "analysis" / "representative_examples_v2.csv"))
    return parser


def _select_examples(cluster: str = "") -> None:
    """Rebuild persona outputs and write representative example artifacts."""
    outputs = _build_outputs()
    if cluster:
        _write_cluster_filtered_examples(outputs, cluster)
        LOGGER.info("Representative examples selected for %s", cluster)
        return
    paths = write_persona_outputs(ROOT, outputs)
    LOGGER.info(
        "Representative examples selected -> %s, %s",
        paths["representative_examples_v2_csv"],
        paths["representative_examples_by_persona_md"],
    )


def _qa_examples(cluster: str = "") -> None:
    """Write a compact QA summary from example selection outputs."""
    outputs = _build_outputs()
    selected_df = outputs["representative_examples_v2_df"]
    audit_df = outputs["example_selection_audit_df"]
    if cluster:
        selected_df = selected_df[selected_df["persona_id"].astype(str) == str(cluster)].copy()
        audit_df = audit_df[audit_df["persona_id"].astype(str) == str(cluster)].copy()
    summary_rows = []
    for persona_id, group in audit_df.groupby("persona_id", dropna=False):
        summary_rows.append(
            {
                "persona_id": persona_id,
                "selected_count": int((group["selection_decision"] == "selected").sum()),
                "borderline_count": int((group["selection_decision"] == "borderline").sum()),
                "rejected_count": int((group["selection_decision"] == "rejected").sum()),
                "avg_final_example_score": round(float(pd.to_numeric(group["final_example_score"], errors="coerce").fillna(0).mean()), 4),
                "strong_count": int((group["quote_quality"] == "strong_representative").sum()),
                "usable_count": int((group["quote_quality"] == "usable").sum()),
            }
        )
    qa_df = pd.DataFrame(summary_rows)
    output_path = ROOT / "data" / "analysis" / "representative_examples_qa.csv"
    qa_df.to_csv(output_path, index=False)
    LOGGER.info("Example QA summary -> %s", output_path)
    if not selected_df.empty:
        LOGGER.info("\n%s", selected_df[["persona_id", "example_rank", "grounded_text", "quote_quality"]].head(12).to_string(index=False))


def _export_example_audit() -> None:
    """Ensure the example audit CSV is refreshed."""
    outputs = _build_outputs()
    output_path = ROOT / "data" / "analysis" / "example_selection_audit.csv"
    outputs["example_selection_audit_df"].to_csv(output_path, index=False)
    LOGGER.info("Example audit exported -> %s", output_path)


def _compare_examples(before_path: str, after_path: str) -> None:
    """Compare old vs new representative example selections."""
    if Path(before_path).exists() and str(Path(before_path).name) != "persona_examples.csv":
        before_df = pd.read_csv(Path(before_path))
    else:
        outputs = _build_outputs()
        persona_source_df = outputs["persona_assignments_df"].merge(
            read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet").merge(
                read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet"),
                on="episode_id",
                how="inner",
            ),
            on="episode_id",
            how="inner",
        )
        before_df = build_legacy_representative_examples(persona_source_df)
        before_df.to_csv(ROOT / "data" / "analysis" / "representative_examples_legacy.csv", index=False)
        after_df = outputs["representative_examples_v2_df"]
        after_df.to_csv(Path(after_path), index=False)
        comparison_df = compare_example_selection(before_df, after_df)
        output_path = ROOT / "data" / "analysis" / "example_selection_comparison.csv"
        comparison_df.to_csv(output_path, index=False)
        LOGGER.info("Example selection comparison -> %s", output_path)
        return
    after_df = pd.read_csv(Path(after_path)) if Path(after_path).exists() else pd.DataFrame()
    comparison_df = compare_example_selection(before_df, after_df)
    output_path = ROOT / "data" / "analysis" / "example_selection_comparison.csv"
    comparison_df.to_csv(output_path, index=False)
    LOGGER.info("Example selection comparison -> %s", output_path)


def _build_outputs() -> dict[str, object]:
    """Build persona outputs using the current reduced axis schema."""
    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    final_axis_schema = _read_final_axis_schema()
    quality_checks = {
        "labeled_count": len(labeled_df),
        "quality_flag": "unknown",
        "unknown_ratio": 0.0,
    }
    return build_persona_outputs(episodes_df, labeled_df, final_axis_schema, quality_checks)


def _read_final_axis_schema() -> list[dict[str, object]]:
    """Read reduced final axis schema from analysis output."""
    path = ROOT / "data" / "analysis" / "final_axis_schema.json"
    if not path.exists():
        return []
    import json

    return list(json.loads(path.read_text(encoding="utf-8")))


def _write_cluster_filtered_examples(outputs: dict[str, object], cluster: str) -> None:
    """Write filtered representative examples for one persona/cluster id."""
    selected_df = outputs["representative_examples_v2_df"]
    filtered = selected_df[selected_df["persona_id"].astype(str) == str(cluster)].copy()
    output_path = ROOT / "data" / "analysis" / f"representative_examples_{cluster}.csv"
    filtered.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
