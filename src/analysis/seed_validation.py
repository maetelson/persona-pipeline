"""Offline validation of source seed banks against current pipeline artifacts."""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.utils.io import ensure_dir, load_yaml, read_parquet


CANONICAL_AXIS_PATTERNS: dict[str, tuple[str, ...]] = {
    "numbers_mismatch": ("mismatch", "not match", "different", "wrong", "discrepancy", "trust"),
    "metric_definition_confusion": ("metric definition", "formula", "counted", "count distinct", "measure logic"),
    "trend_interpretation_confusion": ("why did", "what changed", "spike", "drop", "trend"),
    "segmentation_breakdown_confusion": ("breakdown", "segment", "channel", "device", "product group"),
    "dashboard_confusion": ("dashboard", "report makes no sense", "chart", "visual"),
    "aggregation_filter_issue": ("group by", "join", "date field", "filter", "double count", "aggregation"),
    "source_of_truth_conflict": ("source of truth", "which number", "backend", "crm", "finance"),
    "business_question_translation_failure": ("what should i analyze", "which report", "what should i look at"),
    "stakeholder_explanation_pressure": ("boss", "leadership", "stakeholder", "explain"),
    "insight_to_action_failure": ("what should i do", "what action", "optimize"),
    "attribution_tracking_issue": ("attribution", "tracking", "conversion path"),
    "funnel_conversion_drop": ("conversion", "revenue down", "checkout", "drop off"),
}

STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "at",
    "by",
    "do",
    "did",
    "does",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "my",
    "not",
    "of",
    "on",
    "or",
    "should",
    "that",
    "the",
    "these",
    "this",
    "to",
    "what",
    "which",
    "why",
}


@dataclass(slots=True)
class SeedVersion:
    """One versioned seed set for a source."""

    source_id: str
    version: str
    active_seeds: list[str]


def validate_seed_system(root_dir: Path, sources: list[str]) -> dict[str, Path]:
    """Compare old vs new seed banks against current pipeline artifacts."""
    output_dir = ensure_dir(root_dir / "data" / "analysis" / "seed_validation")
    frames = _load_pipeline_frames(root_dir)
    versions = _load_seed_versions(root_dir, sources)

    summary_rows: list[dict[str, Any]] = []
    seed_rows: list[dict[str, Any]] = []
    observations: list[str] = []

    for source in sources:
        source_versions = [item for item in versions if item.source_id == source]
        if not source_versions:
            continue
        for item in source_versions:
            seed_result = _score_seed_version(source, item, frames)
            summary_rows.append(seed_result["summary"])
            seed_rows.extend(seed_result["seed_rows"])
        observations.append(_render_source_observation(source, summary_rows))

    summary_df = pd.DataFrame(summary_rows)
    seed_df = pd.DataFrame(seed_rows)
    if not summary_df.empty:
        summary_df = summary_df.sort_values(["source", "version"]).reset_index(drop=True)
    if not seed_df.empty:
        seed_df = seed_df.sort_values(["source", "version", "classification", "labeled_count", "prefilter_count", "seed"], ascending=[True, True, True, False, False, True]).reset_index(drop=True)

    comparison_df = _build_version_comparison(summary_df)
    pruning_df = _build_pruning_candidates(seed_df)
    report_path = output_dir / "seed_validation_report.md"
    report_path.write_text(_render_report(summary_df, comparison_df, seed_df, pruning_df), encoding="utf-8")

    outputs = {
        "summary_csv": output_dir / "seed_validation_summary.csv",
        "seed_csv": output_dir / "seed_validation_by_seed.csv",
        "comparison_csv": output_dir / "seed_validation_comparison.csv",
        "pruning_csv": output_dir / "seed_pruning_candidates.csv",
        "report_md": report_path,
    }
    summary_df.to_csv(outputs["summary_csv"], index=False)
    seed_df.to_csv(outputs["seed_csv"], index=False)
    comparison_df.to_csv(outputs["comparison_csv"], index=False)
    pruning_df.to_csv(outputs["pruning_csv"], index=False)
    return outputs


def _load_pipeline_frames(root_dir: Path) -> dict[str, pd.DataFrame]:
    """Load the smallest set of frames needed for offline seed validation."""
    normalized_parts: list[pd.DataFrame] = []
    normalized_dir = root_dir / "data" / "normalized"
    for path in sorted(normalized_dir.glob("*.parquet")):
        if path.name in {"normalized_posts.parquet", "normalized_source_groups.parquet"}:
            continue
        try:
            frame = read_parquet(path)
        except Exception:
            continue
        if not frame.empty and "source" in frame.columns:
            normalized_parts.append(frame)
    normalized_df = pd.concat(normalized_parts, ignore_index=True) if normalized_parts else pd.DataFrame()
    valid_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet")
    invalid_df = read_parquet(root_dir / "data" / "valid" / "invalid_candidates.parquet")
    prefiltered_df = read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    drop_df = read_parquet(root_dir / "data" / "prefilter" / "relevance_drop.parquet")
    episodes_df = read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet")
    labeled_df = read_parquet(root_dir / "data" / "labeled" / "labeled_episodes.parquet")
    labelability_df = read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet")

    return {
        "normalized": normalized_df,
        "valid": valid_df,
        "invalid": invalid_df,
        "prefiltered": prefiltered_df,
        "drop": drop_df,
        "episodes": episodes_df,
        "labeled": labeled_df,
        "labelability": labelability_df,
    }


def _load_seed_versions(root_dir: Path, sources: list[str]) -> list[SeedVersion]:
    """Load previous committed seeds and current working-tree seeds."""
    import subprocess
    import yaml

    versions: list[SeedVersion] = []
    source_to_group = {
        "reddit": "existing_forums",
        "github_discussions": "existing_forums",
        "stackoverflow": "existing_forums",
        "shopify_community": "business_communities",
        "hubspot_community": "business_communities",
        "klaviyo_community": "business_communities",
    }
    for source in sources:
        group = source_to_group.get(source)
        if not group:
            continue
        path = root_dir / "config" / "seeds" / group / f"{source}.yaml"
        if not path.exists():
            continue
        current_payload = load_yaml(path)
        current_seeds = [str(item.get("seed", "")).strip() for item in current_payload.get("active_core_seeds", current_payload.get("core_seeds", [])) or [] if str(item.get("seed", "")).strip()]
        versions.append(SeedVersion(source_id=source, version="after", active_seeds=current_seeds))

        previous_payload = _load_git_yaml(root_dir, path, yaml)
        if previous_payload:
            previous_seeds = [
                str(item.get("seed", "")).strip()
                for item in previous_payload.get("active_core_seeds", previous_payload.get("core_seeds", [])) or []
                if str(item.get("seed", "")).strip()
            ]
            versions.append(SeedVersion(source_id=source, version="before", active_seeds=previous_seeds))
    return versions


def _load_git_yaml(root_dir: Path, path: Path, yaml_module: Any) -> dict[str, Any]:
    """Load a YAML file from the previous commit when available."""
    import subprocess

    relative_path = path.relative_to(root_dir).as_posix()
    for revision in ("HEAD~1", "HEAD"):
        try:
            result = subprocess.run(
                ["git", "show", f"{revision}:{relative_path}"],
                cwd=root_dir,
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            continue
        if result.returncode != 0 or not result.stdout.strip():
            continue
        payload = yaml_module.safe_load(result.stdout) or {}
        if revision == "HEAD~1":
            return payload
        if payload:
            return payload
    return {}


def _score_seed_version(source: str, seed_version: SeedVersion, frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Score one seed version for one source against current artifacts."""
    normalized = _filter_source(frames["normalized"], source)
    valid = _filter_source(frames["valid"], source)
    invalid = _filter_source(frames["invalid"], source)
    prefiltered = _filter_source(frames["prefiltered"], source)
    dropped = _filter_source(frames["drop"], source)
    episodes = _filter_source(frames["episodes"], source)
    labeled = _join_episode_labels(_filter_source(frames["labeled"], source), _filter_source(frames["labelability"], source))

    seed_rows: list[dict[str, Any]] = []
    matched_union: set[str] = set()
    strong_seed_count = 0
    weak_seed_count = 0
    noise_seed_count = 0

    for seed in seed_version.active_seeds:
        normalized_hits = _match_rows(normalized, seed)
        valid_hits = _match_rows(valid, seed)
        invalid_hits = _match_rows(invalid, seed)
        prefiltered_hits = _match_rows(prefiltered, seed)
        dropped_hits = _match_rows(dropped, seed)
        episode_hits = _match_rows(episodes, seed, text_column="normalized_episode")
        labeled_hits = _match_rows(labeled, seed, text_column="normalized_episode")
        matched_union.update(normalized_hits["raw_id"].astype(str).tolist() if not normalized_hits.empty else [])

        invalid_reason_top = _top_term(invalid_hits, "invalid_reason")
        dropped_reason_top = _top_term(dropped_hits, "dropped_reason")
        unknown_ratio = _unknown_ratio(labeled_hits)
        prefilter_rate = _safe_ratio(len(prefiltered_hits), len(valid_hits))
        labeled_rate = _safe_ratio(len(labeled_hits), len(prefiltered_hits))
        invalid_rate = _safe_ratio(len(invalid_hits), len(normalized_hits))
        axis_coverage = _infer_axis_coverage(seed)
        classification = _classify_seed(
            normalized_count=len(normalized_hits),
            prefilter_rate=prefilter_rate,
            labeled_count=len(labeled_hits),
            invalid_rate=invalid_rate,
            unknown_ratio=unknown_ratio,
            invalid_reason_top=invalid_reason_top,
            dropped_reason_top=dropped_reason_top,
        )
        if classification == "high_signal":
            strong_seed_count += 1
        elif classification == "low_signal_noise":
            noise_seed_count += 1
        else:
            weak_seed_count += 1

        seed_rows.append(
            {
                "source": source,
                "version": seed_version.version,
                "seed": seed,
                "axis_guess": axis_coverage,
                "normalized_count": len(normalized_hits),
                "valid_count": len(valid_hits),
                "invalid_count": len(invalid_hits),
                "prefilter_count": len(prefiltered_hits),
                "drop_count": len(dropped_hits),
                "episode_count": len(episode_hits),
                "labeled_count": len(labeled_hits),
                "prefilter_rate": round(prefilter_rate, 4),
                "labeled_rate": round(labeled_rate, 4),
                "invalid_rate": round(invalid_rate, 4),
                "unknown_ratio": round(unknown_ratio, 4),
                "invalid_reason_top": invalid_reason_top,
                "dropped_reason_top": dropped_reason_top,
                "classification": classification,
            }
        )

    summary = {
        "source": source,
        "version": seed_version.version,
        "seed_count": len(seed_version.active_seeds),
        "matched_raw_rows": len(matched_union),
        "strong_seed_count": strong_seed_count,
        "medium_seed_count": weak_seed_count,
        "noise_seed_count": noise_seed_count,
        "avg_prefilter_rate": round(float(pd.DataFrame(seed_rows)["prefilter_rate"].mean()) if seed_rows else 0.0, 4),
        "avg_labeled_rate": round(float(pd.DataFrame(seed_rows)["labeled_rate"].mean()) if seed_rows else 0.0, 4),
        "avg_unknown_ratio": round(float(pd.DataFrame(seed_rows)["unknown_ratio"].mean()) if seed_rows else 0.0, 4),
    }
    return {"summary": summary, "seed_rows": seed_rows}


def _filter_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Return only rows for one source if the dataframe has a source column."""
    if df.empty or "source" not in df.columns:
        return pd.DataFrame(columns=df.columns)
    return df[df["source"].astype(str) == source].copy().reset_index(drop=True)


def _match_rows(df: pd.DataFrame, seed: str, text_column: str | None = None) -> pd.DataFrame:
    """Return rows whose text matches the seed phrase or most of its content tokens."""
    if df.empty:
        return pd.DataFrame(columns=df.columns)
    if text_column is not None and text_column in df.columns:
        text_series = df[text_column].fillna("").astype(str)
    else:
        parts = []
        for column in ["title", "body", "body_text", "comments_text", "raw_text", "thread_title", "business_question", "bottleneck_text"]:
            if column in df.columns:
                parts.append(df[column].fillna("").astype(str))
        if not parts:
            return df.head(0).copy()
        text_series = parts[0]
        for series in parts[1:]:
            text_series = text_series.str.cat(series, sep=" ")
    normalized_text = text_series.map(_normalize_text)
    normalized_seed = _normalize_text(seed)
    if not normalized_seed:
        return df.head(0).copy()
    phrase_mask = normalized_text.str.contains(re.escape(normalized_seed), regex=True, na=False)
    token_mask = normalized_text.map(lambda value: _seed_token_match(value, normalized_seed))
    mask = phrase_mask | token_mask
    return df[mask].copy().reset_index(drop=True)


def _normalize_text(value: str) -> str:
    """Lowercase and normalize punctuation for loose seed matching."""
    lowered = value.lower().replace("’", "'")
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _seed_token_match(text: str, normalized_seed: str) -> bool:
    """Allow search-style phrase matching when most content tokens are present."""
    seed_tokens = [token for token in normalized_seed.split() if token and token not in STOP_WORDS]
    if not seed_tokens:
        return False
    text_tokens = set(text.split())
    hits = sum(1 for token in seed_tokens if token in text_tokens)
    if len(seed_tokens) <= 2:
        return hits == len(seed_tokens)
    required_hits = max(2, math.ceil(len(seed_tokens) * 0.6))
    return hits >= required_hits


def _join_episode_labels(labeled_df: pd.DataFrame, labelability_df: pd.DataFrame) -> pd.DataFrame:
    """Join labelability data so seed validation can estimate unknown risk."""
    if labeled_df.empty:
        return labeled_df
    frame = labeled_df.copy()
    if not labelability_df.empty and "episode_id" in labelability_df.columns:
        keep_cols = [col for col in ["episode_id", "labelability_status", "labelability_score", "labelability_reason", "persona_core_eligible"] if col in labelability_df.columns]
        frame = frame.merge(labelability_df[keep_cols], on="episode_id", how="left", suffixes=("", "_audit"))
    return frame


def _unknown_ratio(df: pd.DataFrame) -> float:
    """Estimate unknown-risk using rule_unknown_family_count when available."""
    if df.empty or "rule_unknown_family_count" not in df.columns:
        return 0.0
    series = pd.to_numeric(df["rule_unknown_family_count"], errors="coerce").fillna(0)
    return float((series > 0).mean())


def _top_term(df: pd.DataFrame, column: str) -> str:
    """Return the top diagnostic reason from a frame."""
    if df.empty or column not in df.columns:
        return ""
    series = df[column].fillna("").astype(str)
    series = series[series != ""]
    if series.empty:
        return ""
    return str(series.value_counts().index[0])


def _classify_seed(
    normalized_count: int,
    prefilter_rate: float,
    labeled_count: int,
    invalid_rate: float,
    unknown_ratio: float,
    invalid_reason_top: str,
    dropped_reason_top: str,
) -> str:
    """Classify one seed as high-signal, medium-signal, or noisy."""
    if normalized_count == 0:
        return "low_signal_noise"
    if invalid_rate >= 0.7 and any(term in invalid_reason_top for term in ["tutorial", "syntax", "promotional", "spam", "career"]):
        return "low_signal_noise"
    if prefilter_rate <= 0.15 and any(term in dropped_reason_top for term in ["technical", "missing_source_language", "below_threshold"]):
        return "low_signal_noise"
    if labeled_count >= 2 and prefilter_rate >= 0.35 and unknown_ratio <= 0.45:
        return "high_signal"
    if labeled_count >= 1 or prefilter_rate >= 0.2:
        return "medium_signal"
    return "low_signal_noise"


def _infer_axis_coverage(seed: str) -> str:
    """Guess the seed's dominant canonical axis from its wording."""
    lowered = seed.lower()
    for axis, patterns in CANONICAL_AXIS_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return axis
    return "unmapped"


def _safe_ratio(numerator: int, denominator: int) -> float:
    """Return a stable ratio."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _build_version_comparison(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Build before-vs-after summary comparison."""
    if summary_df.empty:
        return pd.DataFrame()
    pivot = summary_df.pivot(index="source", columns="version")
    rows: list[dict[str, Any]] = []
    for source in sorted(summary_df["source"].unique()):
        row: dict[str, Any] = {"source": source}
        for metric in ["matched_raw_rows", "strong_seed_count", "medium_seed_count", "noise_seed_count", "avg_prefilter_rate", "avg_labeled_rate", "avg_unknown_ratio"]:
            before_value = _pivot_value(pivot, metric, source, "before")
            after_value = _pivot_value(pivot, metric, source, "after")
            row[f"{metric}_before"] = before_value
            row[f"{metric}_after"] = after_value
            if isinstance(before_value, (int, float)) and isinstance(after_value, (int, float)):
                row[f"{metric}_delta"] = round(float(after_value) - float(before_value), 4)
        rows.append(row)
    return pd.DataFrame(rows)


def _pivot_value(pivot: pd.DataFrame, metric: str, source: str, version: str) -> Any:
    """Read one pivoted comparison cell safely."""
    key = (metric, version)
    if key not in pivot.columns or source not in pivot.index:
        return None
    return pivot.loc[source, key]


def _build_pruning_candidates(seed_df: pd.DataFrame) -> pd.DataFrame:
    """List seeds that should likely be pruned or narrowed next."""
    if seed_df.empty:
        return pd.DataFrame()
    candidates = seed_df[seed_df["version"] == "after"].copy()
    candidates = candidates[
        (candidates["classification"] == "low_signal_noise")
        | (candidates["unknown_ratio"] >= 0.5)
        | (candidates["invalid_rate"] >= 0.6)
    ].copy()
    if candidates.empty:
        return candidates
    candidates["pruning_reason"] = candidates.apply(_pruning_reason, axis=1)
    return candidates.sort_values(["source", "pruning_reason", "seed"]).reset_index(drop=True)


def _pruning_reason(row: pd.Series) -> str:
    """Return a concise pruning recommendation reason."""
    invalid_reason = str(row.get("invalid_reason_top", "") or "")
    dropped_reason = str(row.get("dropped_reason_top", "") or "")
    if any(term in invalid_reason for term in ["tutorial", "syntax", "promotional", "spam", "career"]):
        return "tutorial_or_setup_noise"
    if float(row.get("unknown_ratio", 0.0) or 0.0) >= 0.5:
        return "high_unknown_ratio_risk"
    if any(term in dropped_reason for term in ["missing_source_language", "below_threshold"]):
        return "community_language_mismatch"
    return "low_signal_or_duplicate_coverage"


def _render_source_observation(source: str, summary_rows: list[dict[str, Any]]) -> str:
    """Render a short source observation line."""
    rows = [row for row in summary_rows if row["source"] == source]
    if len(rows) < 2:
        return f"- {source}: insufficient before/after rows for comparison."
    return f"- {source}: comparison ready."


def _render_report(
    summary_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    seed_df: pd.DataFrame,
    pruning_df: pd.DataFrame,
) -> str:
    """Render a reviewer-friendly markdown report."""
    lines = [
        "# Seed Validation Report",
        "",
        "## Available Validation Paths",
        "",
        "- `run/cli/10_source_cli.py dry-run`: seed/query inspection without recollection.",
        "- `run/cli/10_source_cli.py collect --source ...`: source-specific sample collection when a live check is needed.",
        "- Partial pipeline reruns: `normalize`, `prefilter`, `episodes`, `label` for one source.",
        "- Existing quality reports: `data/analysis/source_diagnostics.csv`, `data/analysis/source_distribution.csv`, source-specific funnel diagnostics.",
        "- Existing stage stats: `data/valid/*.parquet`, `data/prefilter/*.parquet`, `data/episodes/*.parquet`, `data/labeled/*.parquet`.",
        "",
        "## Validation Method",
        "",
        "- Method: offline corpus-fit validation against current pipeline artifacts, not fresh collection.",
        "- Reason: this is the smallest safe validation path that still measures quantitative-pain fit beyond raw count.",
        "- Compared stages: normalized, valid, prefiltered, episode, labeled.",
        "- Unknown-risk estimate: labeled rows with `rule_unknown_family_count > 0` among seed-matched episodes.",
        "- Important limitation: this validates seed fit against the current corpus and downstream rules, not newly recollected web coverage.",
        "- Important limitation: `stackoverflow` has no direct before baseline in the same seed-bank format because it was moved onto a new source-level seed bank.",
        "",
        "## Before Vs After By Community",
        "",
    ]
    if comparison_df.empty:
        lines.append("- No comparison data generated.")
    else:
        for _, row in comparison_df.iterrows():
            source = str(row["source"])
            lines.append(f"### {source}")
            lines.append(
                f"- matched rows: `{row.get('matched_raw_rows_before')}` -> `{row.get('matched_raw_rows_after')}`"
            )
            lines.append(
                f"- strong seeds: `{row.get('strong_seed_count_before')}` -> `{row.get('strong_seed_count_after')}`"
            )
            lines.append(
                f"- noisy seeds: `{row.get('noise_seed_count_before')}` -> `{row.get('noise_seed_count_after')}`"
            )
            lines.append(
                f"- avg prefilter rate: `{row.get('avg_prefilter_rate_before')}` -> `{row.get('avg_prefilter_rate_after')}`"
            )
            lines.append(
                f"- avg labeled rate: `{row.get('avg_labeled_rate_before')}` -> `{row.get('avg_labeled_rate_after')}`"
            )
            lines.append(
                f"- avg unknown ratio: `{row.get('avg_unknown_ratio_before')}` -> `{row.get('avg_unknown_ratio_after')}`"
            )
            lines.append("")

    lines.extend(
        [
            "## Strongest Seeds",
            "",
        ]
    )
    top = seed_df[(seed_df["version"] == "after") & (seed_df["classification"] == "high_signal")].copy()
    if top.empty:
        lines.append("- No high-signal seeds identified under the current corpus-fit thresholds.")
    else:
        for _, row in top.sort_values(["source", "labeled_count", "prefilter_rate"], ascending=[True, False, False]).head(40).iterrows():
            lines.append(f"- `{row['source']}` / `{row['seed']}`: labeled=`{row['labeled_count']}`, prefilter_rate=`{row['prefilter_rate']}`, unknown_ratio=`{row['unknown_ratio']}`")

    lines.extend(["", "## Weakest Or Noisiest Seeds", ""])
    weak = seed_df[(seed_df["version"] == "after") & (seed_df["classification"] == "low_signal_noise")].copy()
    if weak.empty:
        lines.append("- No strongly noisy seeds identified under the current corpus-fit thresholds.")
    else:
        for _, row in weak.sort_values(["source", "invalid_rate", "unknown_ratio"], ascending=[True, False, False]).head(40).iterrows():
            lines.append(f"- `{row['source']}` / `{row['seed']}`: invalid_rate=`{row['invalid_rate']}`, unknown_ratio=`{row['unknown_ratio']}`, invalid_top=`{row['invalid_reason_top']}`, drop_top=`{row['dropped_reason_top']}`")

    lines.extend(["", "## Immediate Pruning Candidates", ""])
    if pruning_df.empty:
        lines.append("- No immediate pruning candidates surfaced from the current offline validation.")
    else:
        for _, row in pruning_df.head(40).iterrows():
            lines.append(f"- `{row['source']}` / `{row['seed']}`: `{row['pruning_reason']}`")

    lines.extend(["", "## Next Iteration Recommendations", ""])
    lines.append("- Keep seeds that improve mismatch, trust, trend, breakdown, explanation, and actionability coverage even when they are not conversion-related.")
    lines.append("- Narrow seeds that mostly map to tutorial/setup noise or consistently die in invalid/prefilter with low operator-pain signal.")
    lines.append("- Revisit downstream invalid/relevance rules for communities where broad quantitative-pain seeds still die before labeling.")
    lines.append("- If needed, run a second validation pass with small live source-specific sample collection only for sources whose offline fit improved but current corpus coverage remains too sparse.")
    return "\n".join(lines) + "\n"
