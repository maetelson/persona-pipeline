"""CLI for source-group collection, normalization, and prefilter workflows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.collectors.official_community_collector import OfficialCommunityCollector
from src.collectors.reddit_public_collector import RedditPublicCollector
from src.collectors.review_site_collector import ReviewSiteCollector
from src.filters.relevance import (
    apply_relevance_prefilter,
    build_before_after_comparison,
    build_prefilter_summary,
    build_reddit_subreddit_summary,
    build_source_ratio_summary,
    build_stackoverflow_tag_summary,
    build_top_negative_signal_report,
)
from src.normalizers.base import NORMALIZED_POST_COLUMNS
from src.normalizers.official_community_normalizer import OfficialCommunityNormalizer
from src.normalizers.reddit_public_normalizer import RedditPublicNormalizer
from src.normalizers.review_site_normalizer import ReviewSiteNormalizer
from src.utils.io import ensure_dir, list_jsonl_files, load_yaml, read_jsonl, read_parquet, write_parquet
from src.utils.logging import get_logger
from src.utils.seed_bank import export_seed_artifacts, load_seed_bank, render_optional_queries, validate_seed_bank
from src.utils.source_registry import SourceDefinition, filter_source_definitions, load_source_definitions

LOGGER = get_logger("run.source_cli")


def build_parser() -> argparse.ArgumentParser:
    """Build the source CLI parser."""
    parser = argparse.ArgumentParser(description="Source-group collection CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_name in ["collect", "normalize", "prefilter", "dry-run", "qa-relevance", "show-seeds", "validate-seeds", "export-seed-summary"]:
        command = subparsers.add_parser(command_name)
        _add_target_args(command)
        if command_name in {"prefilter", "qa-relevance"}:
            command.add_argument("--export-borderline", action="store_true", help="Write borderline rows for manual review.")
            command.add_argument("--limit", type=int, default=200, help="Limit rows for QA export.")

    ingest_manual = subparsers.add_parser("ingest-manual")
    _add_target_args(ingest_manual)
    ingest_manual.add_argument("--input-dir", required=True, help="Manual snapshot directory to ingest.")
    return parser


def _add_target_args(parser: argparse.ArgumentParser) -> None:
    """Add common target args to a parser."""
    parser.add_argument("--source-group", choices=["review_sites", "reddit", "official_communities", "existing_forums"], default=None)
    parser.add_argument("--source", default=None)
    parser.add_argument("--include-disabled", action="store_true")


def main() -> None:
    """Dispatch CLI commands."""
    args = build_parser().parse_args()
    definitions = load_source_definitions(ROOT, include_disabled=True)
    selected = filter_source_definitions(
        definitions,
        source_group=args.source_group,
        source_name=args.source,
        include_disabled=args.include_disabled or bool(args.source),
    )
    if not selected:
        raise SystemExit("No matching source definitions found.")

    if args.command == "collect":
        collect_sources(selected)
    elif args.command == "normalize":
        normalize_sources(selected)
    elif args.command == "prefilter":
        prefilter_sources(selected, export_borderline=bool(args.export_borderline), limit=int(args.limit))
    elif args.command == "ingest-manual":
        if len(selected) != 1:
            raise SystemExit("ingest-manual requires exactly one --source target.")
        collect_sources(selected, manual_input_dir=args.input_dir)
        normalize_sources(selected)
        prefilter_sources(selected, export_borderline=True, limit=200)
    elif args.command == "dry-run":
        dry_run(selected)
    elif args.command == "qa-relevance":
        qa_relevance(selected, limit=int(args.limit), export_borderline=bool(args.export_borderline))
    elif args.command == "show-seeds":
        show_seeds(selected)
    elif args.command == "validate-seeds":
        validate_seeds(selected)
    elif args.command == "export-seed-summary":
        export_seed_summary(selected)


def collect_sources(selected: list[SourceDefinition], manual_input_dir: str | None = None) -> None:
    """Collect raw JSONL for selected source definitions."""
    ensure_dir(ROOT / "data" / "analysis")
    rows: list[dict[str, object]] = []
    blocked_rows: list[dict[str, object]] = []
    for definition in selected:
        config = dict(definition.config)
        if manual_input_dir:
            config["manual_input_dir"] = manual_input_dir
        collector = _build_collector(definition, config)
        records = collector.collect()
        output_path = collector.save(records)
        status_values = sorted({str(record.crawl_status or "") for record in records})
        row = {
            "source": definition.source_id,
            "source_name": definition.source_name,
            "source_group": definition.source_group,
            "raw_row_count": len(records),
            "output_path": str(output_path),
            "crawl_statuses": "|".join(status_values),
        }
        rows.append(row)
        if any(status == "blocked_or_manual_required" for status in status_values):
            blocked_rows.append(row)
        LOGGER.info("Collected %s rows for %s -> %s", len(records), definition.source_id, output_path)

    report_df = pd.DataFrame(rows)
    write_parquet(report_df, ROOT / "data" / "analysis" / "source_coverage_report.parquet")
    report_df.to_csv(ROOT / "data" / "analysis" / "source_coverage_report.csv", index=False)
    blocked_df = pd.DataFrame(blocked_rows)
    write_parquet(blocked_df, ROOT / "data" / "analysis" / "blocked_manual_required_report.parquet")
    blocked_df.to_csv(ROOT / "data" / "analysis" / "blocked_manual_required_report.csv", index=False)


def normalize_sources(selected: list[SourceDefinition]) -> None:
    """Normalize selected source raw files into per-source parquet outputs."""
    ensure_dir(ROOT / "data" / "analysis")
    frames: list[pd.DataFrame] = []
    for definition in selected:
        normalizer = _build_normalizer(definition)
        raw_dir = ROOT / "data" / "raw" / definition.source_id
        rows: list[dict[str, object]] = []
        for file_path in list_jsonl_files(raw_dir):
            rows.extend(read_jsonl(file_path))
        normalized_df = normalizer.normalize_rows(rows) if rows else pd.DataFrame(columns=NORMALIZED_POST_COLUMNS)
        output_path = ROOT / "data" / "normalized" / f"{definition.source_id}.parquet"
        write_parquet(normalized_df, output_path)
        frames.append(normalized_df)
        LOGGER.info("Normalized %s rows for %s -> %s", len(normalized_df), definition.source_id, output_path)

    combined_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=NORMALIZED_POST_COLUMNS)
    write_parquet(combined_df, ROOT / "data" / "normalized" / "normalized_source_groups.parquet")
    sample_df = combined_df.groupby("source", dropna=False).head(3).reset_index(drop=True) if not combined_df.empty else combined_df
    write_parquet(sample_df, ROOT / "data" / "analysis" / "source_sample_rows.parquet")
    sample_df.to_csv(ROOT / "data" / "analysis" / "source_sample_rows.csv", index=False)


def prefilter_sources(selected: list[SourceDefinition], export_borderline: bool = False, limit: int = 200) -> None:
    """Run source-aware relevance scoring and write QA outputs."""
    ensure_dir(ROOT / "data" / "analysis")
    ensure_dir(ROOT / "data" / "prefilter")
    rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")
    kept_frames: list[pd.DataFrame] = []
    borderline_frames: list[pd.DataFrame] = []
    rejected_frames: list[pd.DataFrame] = []
    for definition in selected:
        normalized_path = ROOT / "data" / "normalized" / f"{definition.source_id}.parquet"
        if not normalized_path.exists() and definition.source_id in {"reddit", "stackoverflow"}:
            normalized_path = ROOT / "data" / "normalized" / "normalized_posts.parquet"
        normalized_df = read_parquet(normalized_path)
        if definition.source_id in {"reddit", "stackoverflow"} and not normalized_df.empty:
            normalized_df = normalized_df[normalized_df["source"] == definition.source_id].reset_index(drop=True)
        kept_df, borderline_df, rejected_df = apply_relevance_prefilter(normalized_df, rules)
        kept_frames.append(kept_df)
        borderline_frames.append(borderline_df)
        rejected_frames.append(rejected_df)
        write_parquet(kept_df, ROOT / "data" / "prefilter" / f"{definition.source_id}_keep.parquet")
        write_parquet(rejected_df, ROOT / "data" / "prefilter" / f"{definition.source_id}_reject.parquet")
        if export_borderline:
            write_parquet(borderline_df, ROOT / "data" / "prefilter" / f"{definition.source_id}_borderline.parquet")

    kept_df = pd.concat(kept_frames, ignore_index=True) if kept_frames else pd.DataFrame()
    borderline_df = pd.concat(borderline_frames, ignore_index=True) if borderline_frames else pd.DataFrame()
    rejected_df = pd.concat(rejected_frames, ignore_index=True) if rejected_frames else pd.DataFrame()
    summary_df = build_prefilter_summary(kept_df, borderline_df, rejected_df)
    write_parquet(summary_df, ROOT / "data" / "analysis" / "prefilter_summary_report.parquet")
    summary_df.to_csv(ROOT / "data" / "analysis" / "prefilter_summary_report.csv", index=False)
    qa_relevance(selected, limit=limit, export_borderline=export_borderline, precomputed=(kept_df, borderline_df, rejected_df))


def qa_relevance(
    selected: list[SourceDefinition],
    limit: int = 200,
    export_borderline: bool = False,
    precomputed: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None = None,
) -> None:
    """Write QA-oriented relevance reports for selected sources."""
    ensure_dir(ROOT / "data" / "analysis")
    if precomputed is None:
        rules = load_yaml(ROOT / "config" / "relevance_rules.yaml")
        frames = []
        for definition in selected:
            normalized_path = ROOT / "data" / "normalized" / f"{definition.source_id}.parquet"
            if not normalized_path.exists() and definition.source_id in {"reddit", "stackoverflow"}:
                normalized_path = ROOT / "data" / "normalized" / "normalized_posts.parquet"
            normalized_df = read_parquet(normalized_path)
            if definition.source_id in {"reddit", "stackoverflow"} and not normalized_df.empty:
                normalized_df = normalized_df[normalized_df["source"] == definition.source_id].reset_index(drop=True)
            frames.append(apply_relevance_prefilter(normalized_df, rules))
        kept_df = pd.concat([item[0] for item in frames], ignore_index=True) if frames else pd.DataFrame()
        borderline_df = pd.concat([item[1] for item in frames], ignore_index=True) if frames else pd.DataFrame()
        rejected_df = pd.concat([item[2] for item in frames], ignore_index=True) if frames else pd.DataFrame()
    else:
        kept_df, borderline_df, rejected_df = precomputed

    result_df = pd.concat([kept_df, borderline_df, rejected_df], ignore_index=True)
    build_source_ratio_summary(result_df).to_csv(ROOT / "data" / "analysis" / "prefilter_source_ratio_report.csv", index=False)
    build_top_negative_signal_report(result_df).to_csv(ROOT / "data" / "analysis" / "prefilter_top_negative_signals.csv", index=False)
    build_reddit_subreddit_summary(result_df).to_csv(ROOT / "data" / "analysis" / "prefilter_reddit_subreddit_summary.csv", index=False)
    build_stackoverflow_tag_summary(result_df).to_csv(ROOT / "data" / "analysis" / "prefilter_stackoverflow_tag_summary.csv", index=False)

    for label, frame in [("kept", kept_df), ("dropped", rejected_df), ("borderline", borderline_df if export_borderline else borderline_df.head(0))]:
        sample = frame.head(limit).copy()
        sample.to_csv(ROOT / "data" / "analysis" / f"prefilter_{label}_sample_rows.csv", index=False)

    previous_valid_df = read_parquet(ROOT / "data" / "valid" / "valid_candidates.parquet")
    previous_invalid_df = read_parquet(ROOT / "data" / "valid" / "invalid_candidates.parquet")
    for source in sorted({definition.source_id for definition in selected if definition.source_id in {"reddit", "stackoverflow"}}):
        normalized_df = read_parquet(ROOT / "data" / "normalized" / "normalized_posts.parquet")
        normalized_df = normalized_df[normalized_df["source"] == source].reset_index(drop=True)
        comparison = build_before_after_comparison(
            normalized_df,
            kept_df,
            borderline_df,
            previous_valid_df,
            previous_invalid_df,
            source=source,
            rules=load_yaml(ROOT / "config" / "relevance_rules.yaml"),
            limit=limit,
        )
        for name, frame in comparison.items():
            frame.to_csv(ROOT / "data" / "analysis" / f"{source}_{name}.csv", index=False)


def dry_run(selected: list[SourceDefinition]) -> None:
    """Log the selected sources and the expected input/output paths."""
    rows = []
    for definition in selected:
        rows.append(
            {
                "source": definition.source_id,
                "source_name": definition.source_name,
                "source_group": definition.source_group,
                "collector_kind": definition.collector_kind,
                "normalizer_kind": definition.normalizer_kind,
                "raw_output": str(ROOT / "data" / "raw" / definition.source_id / "raw.jsonl"),
                "normalized_output": str(ROOT / "data" / "normalized" / f"{definition.source_id}.parquet"),
            }
        )
    dry_run_df = pd.DataFrame(rows)
    ensure_dir(ROOT / "data" / "analysis")
    dry_run_df.to_csv(ROOT / "data" / "analysis" / "source_cli_dry_run.csv", index=False)
    LOGGER.info("\n%s", dry_run_df.to_string(index=False))


def show_seeds(selected: list[SourceDefinition]) -> None:
    """Print compact seed banks for the selected sources."""
    for definition in selected:
        seed_bank = load_seed_bank(ROOT, definition.source_group, definition.source_id)
        if seed_bank is None:
            LOGGER.info("No compact seed bank configured for %s", definition.source_id)
            continue
        lines = [
            f"source={definition.source_id}",
            f"source_group={definition.source_group}",
            f"max_query_count={seed_bank.max_query_count}",
            "core_seeds:",
        ]
        lines.extend([f"  - {item.seed} :: {item.reason}" for item in seed_bank.core_seeds])
        if seed_bank.optional_templates:
            lines.append("optional_templates:")
            lines.extend([f"  - {item.template} :: {item.reason}" for item in seed_bank.optional_templates])
            rendered = render_optional_queries(seed_bank)
            if rendered:
                lines.append("rendered_optional_examples:")
                lines.extend([f"  - {query}" for query in rendered[: seed_bank.max_query_count]])
        lines.append(f"negative_terms: {', '.join(seed_bank.all_negative_terms)}")
        LOGGER.info("\n%s", "\n".join(lines))


def validate_seeds(selected: list[SourceDefinition]) -> None:
    """Validate compact seed banks and write audit artifacts."""
    artifacts = export_seed_artifacts(ROOT, selected)
    error_count = 0
    seed_bank_groups = {"review_sites", "reddit", "official_communities"}
    for definition in selected:
        seed_bank = load_seed_bank(ROOT, definition.source_group, definition.source_id)
        if seed_bank is None:
            if definition.source_group in seed_bank_groups:
                LOGGER.warning("Missing compact seed bank for %s", definition.source_id)
                error_count += 1
            continue
        findings = validate_seed_bank(seed_bank)
        if findings:
            for finding in findings:
                log_message = "%s [%s] %s"
                if finding["level"] == "error":
                    LOGGER.error(log_message, definition.source_id, finding["code"], finding["message"])
                    error_count += 1
                else:
                    LOGGER.warning(log_message, definition.source_id, finding["code"], finding["message"])
        else:
            LOGGER.info("Validated seed bank for %s with no findings", definition.source_id)
    LOGGER.info("Wrote seed artifacts: %s", ", ".join(str(path) for path in artifacts.values()))
    if error_count:
        raise SystemExit(f"Seed validation failed with {error_count} error(s).")


def export_seed_summary(selected: list[SourceDefinition]) -> None:
    """Export compact seed bank summary and markdown artifacts."""
    artifacts = export_seed_artifacts(ROOT, selected)
    LOGGER.info("Wrote seed artifacts: %s", ", ".join(str(path) for path in artifacts.values()))


def _build_collector(definition: SourceDefinition, config: dict[str, object]):
    """Return the collector instance for one source definition."""
    data_dir = ROOT / "data"
    if definition.collector_kind == "review_sites":
        return ReviewSiteCollector(definition.source_id, config=config, data_dir=data_dir)
    if definition.collector_kind == "reddit":
        return RedditPublicCollector(definition.source_id, config=config, data_dir=data_dir)
    if definition.collector_kind == "official_communities":
        return OfficialCommunityCollector(definition.source_id, config=config, data_dir=data_dir)
    raise ValueError(f"Unsupported collector kind: {definition.collector_kind}")


def _build_normalizer(definition: SourceDefinition):
    """Return the normalizer instance for one source definition."""
    if definition.normalizer_kind == "review_sites":
        return ReviewSiteNormalizer()
    if definition.normalizer_kind == "reddit":
        return RedditPublicNormalizer()
    if definition.normalizer_kind == "official_communities":
        return OfficialCommunityNormalizer()
    if definition.normalizer_kind == "existing_forums":
        raise ValueError("Use the legacy run scripts for existing forum normalization.")
    raise ValueError(f"Unsupported normalizer kind: {definition.normalizer_kind}")


if __name__ == "__main__":
    main()
