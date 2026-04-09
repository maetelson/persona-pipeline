"""CLI for label-quality audit, rerun, repair, and QA exports."""

from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.labeling.llm_labeler import resolve_llm_runtime
from src.utils.io import load_yaml, read_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.label_cli")


def build_parser() -> argparse.ArgumentParser:
    """Build the label-quality CLI."""
    parser = argparse.ArgumentParser(description="Label quality CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in [
        "audit-label-quality",
        "audit-unknown",
        "repair-labels",
        "export-low-signal",
        "compare-label-quality",
        "dry-run-labeler",
    ]:
        subparsers.add_parser(name)
    label_parser = subparsers.add_parser("label")
    label_parser.add_argument("--with-confidence", action="store_true")
    label_parser.add_argument("--source-aware", action="store_true")
    return parser


def main() -> None:
    """Dispatch label-quality commands."""
    args = build_parser().parse_args()
    if args.command == "label":
        runpy.run_path(str(ROOT / "run" / "05_label_episodes.py"), run_name="__main__")
        return
    if args.command == "dry-run-labeler":
        policy = load_yaml(ROOT / "config" / "labeling_policy.yaml")
        codebook = load_yaml(ROOT / "config" / "codebook.yaml")
        runtime = resolve_llm_runtime({"policy": policy, "codebook": codebook})
        LOGGER.info("LLM runtime mode=%s model=%s skip_reason=%s", runtime["mode"], runtime["model_primary"], runtime["skip_reason"])
        return
    if args.command == "audit-label-quality":
        _show_file("data/analysis/label_quality_audit.md")
        return
    if args.command == "audit-unknown":
        _show_file("data/analysis/top_unknown_examples.csv")
        return
    if args.command == "export-low-signal":
        _show_file("data/analysis/low_signal_rows.csv")
        return
    if args.command == "repair-labels":
        _show_file("data/analysis/repaired_labels.csv")
        return
    if args.command == "compare-label-quality":
        _show_file("data/analysis/before_after_label_metrics.md")


def _show_file(relative_path: str) -> None:
    """Print a short preview of an existing QA artifact."""
    path = ROOT / relative_path
    if not path.exists():
        raise SystemExit(f"Artifact not found: {path}")
    if path.suffix == ".parquet":
        frame = read_parquet(path)
        LOGGER.info("\n%s", frame.head(20).to_string(index=False))
        return
    LOGGER.info(path.read_text(encoding="utf-8")[:4000])


if __name__ == "__main__":
    main()
