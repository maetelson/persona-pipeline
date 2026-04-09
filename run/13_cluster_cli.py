"""Run bottleneck-first clustering audit and export commands."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.bottleneck_clustering import export_cluster_examples
from src.analysis.persona_service import build_persona_outputs, write_persona_outputs
from src.utils.io import read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.cluster_cli")


def main() -> None:
    """Run bottleneck-first clustering commands without notebooks."""
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "recluster":
        _recluster(dry_run=False)
    elif args.command == "dry-run-recluster":
        _recluster(dry_run=True)
    elif args.command == "audit-clusters":
        _audit_clusters()
    elif args.command == "compare-clusters":
        _compare_clusters()
    elif args.command == "export-cluster-examples":
        _export_cluster_examples(args.cluster)
    elif args.command == "name-clusters":
        _name_clusters()
    else:
        raise ValueError(f"Unsupported command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    """Build the bottleneck clustering CLI parser."""
    parser = argparse.ArgumentParser(description="Bottleneck-first cluster CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    recluster_parser = subparsers.add_parser("recluster")
    recluster_parser.add_argument("--mode", default="bottleneck_first")

    dry_run_parser = subparsers.add_parser("dry-run-recluster")
    dry_run_parser.add_argument("--mode", default="bottleneck_first")

    subparsers.add_parser("audit-clusters")

    compare_parser = subparsers.add_parser("compare-clusters")
    compare_parser.add_argument("--before", default="")
    compare_parser.add_argument("--after", default="")

    export_parser = subparsers.add_parser("export-cluster-examples")
    export_parser.add_argument("--cluster", required=True)

    name_parser = subparsers.add_parser("name-clusters")
    name_parser.add_argument("--strategy", default="bottleneck")
    return parser


def _recluster(dry_run: bool) -> None:
    """Build bottleneck-first persona outputs and optionally write them."""
    outputs = _build_outputs()
    if dry_run:
        LOGGER.info(
            "Dry run cluster summary -> personas=%s, clusters=%s",
            len(outputs["persona_summary_df"]),
            len(outputs["cluster_meaning_audit_df"]),
        )
        return
    paths = write_persona_outputs(ROOT, outputs)
    LOGGER.info(
        "Bottleneck-first recluster complete -> %s, %s, %s",
        paths["persona_summary_csv"],
        paths["cluster_meaning_audit_csv"],
        paths["cluster_comparison_before_after_md"],
    )


def _audit_clusters() -> None:
    """Refresh cluster meaning audit artifacts."""
    outputs = _build_outputs()
    analysis_dir = ROOT / "data" / "analysis"
    outputs["cluster_meaning_audit_df"].to_csv(analysis_dir / "cluster_meaning_audit.csv", index=False)
    outputs["bottleneck_feature_importance_df"].to_csv(analysis_dir / "bottleneck_feature_importance.csv", index=False)
    outputs["role_feature_importance_before_after_df"].to_csv(analysis_dir / "role_feature_importance_before_after.csv", index=False)
    LOGGER.info(
        "Cluster audit refreshed -> %s, %s, %s",
        analysis_dir / "cluster_meaning_audit.csv",
        analysis_dir / "bottleneck_feature_importance.csv",
        analysis_dir / "role_feature_importance_before_after.csv",
    )


def _compare_clusters() -> None:
    """Export old vs new clustering comparison artifacts."""
    outputs = _build_outputs()
    analysis_dir = ROOT / "data" / "analysis"
    outputs["old_vs_new_cluster_summary_df"].to_csv(analysis_dir / "old_vs_new_cluster_summary.csv", index=False)
    outputs["cluster_comparison_before_after_df"].to_csv(analysis_dir / "cluster_comparison_before_after.csv", index=False)
    (analysis_dir / "cluster_comparison_before_after.md").write_text(outputs["cluster_comparison_before_after_md"], encoding="utf-8")
    LOGGER.info(
        "Cluster comparison exported -> %s, %s",
        analysis_dir / "old_vs_new_cluster_summary.csv",
        analysis_dir / "cluster_comparison_before_after.md",
    )


def _export_cluster_examples(cluster_id: str) -> None:
    """Export representative examples for one cluster."""
    outputs = _build_outputs()
    output_path = ROOT / "data" / "analysis" / f"representative_examples_{cluster_id}.csv"
    export_cluster_examples(outputs["representative_examples_v2_df"], cluster_id, output_path)
    LOGGER.info("Cluster example export -> %s", output_path)


def _name_clusters() -> None:
    """Refresh cluster naming recommendations."""
    outputs = _build_outputs()
    output_path = ROOT / "data" / "analysis" / "cluster_naming_recommendations.csv"
    outputs["cluster_naming_recommendations_df"].to_csv(output_path, index=False)
    LOGGER.info("Cluster naming recommendations -> %s", output_path)


def _build_outputs() -> dict[str, object]:
    """Build persona outputs from current labeled and reduced-axis inputs."""
    episodes_df = read_parquet(ROOT / "data" / "episodes" / "episode_table.parquet")
    labeled_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    final_axis_schema_path = ROOT / "data" / "analysis" / "final_axis_schema.json"
    final_axis_schema = json.loads(final_axis_schema_path.read_text(encoding="utf-8")) if final_axis_schema_path.exists() else []
    quality_checks = {
        "labeled_count": len(labeled_df),
        "quality_flag": "unknown",
        "unknown_ratio": 0.0,
    }
    return build_persona_outputs(episodes_df, labeled_df, final_axis_schema, quality_checks)


if __name__ == "__main__":
    main()
