"""Generate persona naming, insight, and solution-linkage artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.persona_messaging import build_persona_messaging_outputs, write_persona_messaging_outputs
from src.utils.logging import get_logger

LOGGER = get_logger("run.persona_cli")


def main() -> None:
    """Run persona messaging commands without notebooks."""
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "generate-persona-cards":
        _write_outputs("cards")
    elif args.command == "generate-persona-insights":
        _write_outputs("insights")
    elif args.command == "link-persona-solutions":
        _write_outputs("solutions")
    elif args.command == "audit-persona-naming":
        _write_outputs("naming")
    elif args.command == "compare-persona-summary":
        _write_outputs("compare")
    else:
        raise ValueError(f"Unsupported command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    """Build parser for persona messaging commands."""
    parser = argparse.ArgumentParser(description="Persona naming, insight, and solution linkage CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("generate-persona-cards")
    subparsers.add_parser("generate-persona-insights")
    subparsers.add_parser("link-persona-solutions")
    subparsers.add_parser("audit-persona-naming")
    compare_parser = subparsers.add_parser("compare-persona-summary")
    compare_parser.add_argument("--before", default="")
    compare_parser.add_argument("--after", default="")
    return parser


def _write_outputs(mode: str) -> None:
    """Build persona messaging outputs and write relevant files."""
    outputs = _build_outputs()
    paths = write_persona_messaging_outputs(ROOT, outputs)
    if mode == "cards":
        LOGGER.info("Persona cards -> %s", paths["persona_cards_v2_csv"])
    elif mode == "insights":
        LOGGER.info("Persona insights -> %s", paths["persona_insights_v2_md"])
    elif mode == "solutions":
        LOGGER.info("Persona solution linkage -> %s", paths["persona_solution_linkage_md"])
    elif mode == "naming":
        LOGGER.info("Naming audit -> %s", paths["naming_audit_csv"])
    else:
        LOGGER.info("Before/after persona summary -> %s", paths["before_after_persona_summary_md"])


def _build_outputs() -> dict[str, object]:
    """Load current analysis artifacts and build persona messaging outputs."""
    analysis_dir = ROOT / "data" / "analysis"
    import pandas as pd

    cluster_audit_df = pd.read_csv(analysis_dir / "cluster_meaning_audit.csv")
    naming_df = pd.read_csv(analysis_dir / "cluster_naming_recommendations.csv")
    persona_summary_df = pd.read_csv(analysis_dir / "persona_summary.csv")
    examples_df = pd.read_csv(analysis_dir / "representative_examples_v2.csv")
    personas_path = analysis_dir / "personas.json"
    personas = json.loads(personas_path.read_text(encoding="utf-8")) if personas_path.exists() else []
    return build_persona_messaging_outputs(
        cluster_audit_df=cluster_audit_df,
        naming_df=naming_df,
        persona_summary_df=persona_summary_df,
        examples_df=examples_df,
        personas=personas,
    )


if __name__ == "__main__":
    main()
