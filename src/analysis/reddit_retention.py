"""Reddit seed and subreddit retention diagnostics for low-yield retrieval control."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.utils.io import ensure_dir, load_yaml, read_jsonl, read_parquet, write_parquet


RETENTION_COLUMNS = [
    "group_value",
    "raw_count",
    "valid_count",
    "kept_count",
    "borderline_count",
    "prefiltered_count",
    "raw_to_valid_retention",
    "raw_to_prefilter_retention",
    "valid_to_prefilter_retention",
]


def analyze_reddit_retention(root_dir: Path, min_raw_threshold: int = 5) -> dict[str, Path]:
    """Write Reddit seed and subreddit retention artifacts for collection tuning."""
    analysis_dir = ensure_dir(root_dir / "data" / "analysis")
    raw_df = _load_raw_reddit(root_dir)
    valid_df = _filter_source(read_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet"), "reddit")
    keep_df = _filter_source(read_parquet(root_dir / "data" / "prefilter" / "relevance_keep.parquet"), "reddit")
    borderline_df = _filter_source(read_parquet(root_dir / "data" / "prefilter" / "relevance_borderline.parquet"), "reddit")
    policy_df = _build_reddit_policy_audit(root_dir)
    runtime_df = _build_reddit_runtime_audit(root_dir)

    seed_df = _build_retention_table(raw_df, valid_df, keep_df, borderline_df, ["query_seed"])
    subreddit_df = _build_retention_table(raw_df, valid_df, keep_df, borderline_df, ["subreddit_or_forum"])
    seed_subreddit_df = _build_retention_table(raw_df, valid_df, keep_df, borderline_df, ["query_seed", "subreddit_or_forum"])

    outputs = {
        "seed_csv": analysis_dir / "reddit_seed_retention.csv",
        "seed_parquet": analysis_dir / "reddit_seed_retention.parquet",
        "subreddit_csv": analysis_dir / "reddit_subreddit_retention.csv",
        "subreddit_parquet": analysis_dir / "reddit_subreddit_retention.parquet",
        "seed_subreddit_csv": analysis_dir / "reddit_seed_subreddit_retention.csv",
        "seed_subreddit_parquet": analysis_dir / "reddit_seed_subreddit_retention.parquet",
        "policy_csv": analysis_dir / "reddit_collection_policy_audit.csv",
        "policy_parquet": analysis_dir / "reddit_collection_policy_audit.parquet",
        "policy_json": analysis_dir / "reddit_collection_policy_audit.json",
        "report_md": analysis_dir / "reddit_collection_policy_report.md",
    }
    seed_df.to_csv(outputs["seed_csv"], index=False)
    write_parquet(seed_df, outputs["seed_parquet"])
    subreddit_df.to_csv(outputs["subreddit_csv"], index=False)
    write_parquet(subreddit_df, outputs["subreddit_parquet"])
    seed_subreddit_df.to_csv(outputs["seed_subreddit_csv"], index=False)
    write_parquet(seed_subreddit_df, outputs["seed_subreddit_parquet"])
    policy_df.to_csv(outputs["policy_csv"], index=False)
    write_parquet(policy_df, outputs["policy_parquet"])
    outputs["policy_json"].write_text(json.dumps(policy_df.to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")
    outputs["report_md"].write_text(
        render_reddit_retention_report(
            seed_df,
            subreddit_df,
            seed_subreddit_df,
            policy_df=policy_df,
            runtime_df=runtime_df,
            min_raw_threshold=min_raw_threshold,
        ),
        encoding="utf-8",
    )
    return outputs


def render_reddit_retention_report(
    seed_df: pd.DataFrame,
    subreddit_df: pd.DataFrame,
    seed_subreddit_df: pd.DataFrame,
    policy_df: pd.DataFrame | None = None,
    runtime_df: pd.DataFrame | None = None,
    min_raw_threshold: int = 5,
) -> str:
    """Render a markdown report for Reddit seed and subreddit retention."""
    low_yield_seeds = seed_df[(seed_df["raw_count"] >= int(min_raw_threshold)) & (seed_df["prefiltered_count"] == 0)].copy()
    high_yield_seeds = seed_df[seed_df["prefiltered_count"] > 0].copy()
    low_yield_subreddits = subreddit_df[(subreddit_df["raw_count"] >= int(min_raw_threshold)) & (subreddit_df["prefiltered_count"] == 0)].copy()
    high_yield_subreddits = subreddit_df[subreddit_df["prefiltered_count"] > 0].copy()

    lines = [
        "# Reddit Collection Policy Report",
        "",
    ]

    if policy_df is not None and not policy_df.empty:
        lines.extend([
            "## Policy Snapshot",
            "",
            "| policy_name | policy_value |",
            "| --- | --- |",
        ])
        for _, row in policy_df.iterrows():
            lines.append(f"| {row['policy_name']} | {row['policy_value']} |")
        lines.append("")

    if runtime_df is not None and not runtime_df.empty:
        lines.extend([
            "## Latest Runtime Signals",
            "",
            "| metric | value |",
            "| --- | --- |",
        ])
        for _, row in runtime_df.iterrows():
            lines.append(f"| {row['metric_name']} | {row['metric_value']} |")
        lines.append("")

    lines.extend([
        "",
        "## Seed Retention",
        "",
        "| query_seed | raw | valid | keep | borderline | prefiltered | raw_to_prefilter | valid_to_prefilter |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for _, row in seed_df.sort_values(["prefiltered_count", "raw_count", "group_value"], ascending=[False, False, True]).iterrows():
        lines.append(
            "| {seed} | {raw} | {valid} | {keep} | {borderline} | {prefiltered} | {raw_retention:.1%} | {valid_retention:.1%} |".format(
                seed=row["group_value"],
                raw=int(row["raw_count"]),
                valid=int(row["valid_count"]),
                keep=int(row["kept_count"]),
                borderline=int(row["borderline_count"]),
                prefiltered=int(row["prefiltered_count"]),
                raw_retention=float(row["raw_to_prefilter_retention"]),
                valid_retention=float(row["valid_to_prefilter_retention"]),
            )
        )

    lines.extend([
        "",
        "## Subreddit Retention",
        "",
        "| subreddit | raw | valid | keep | borderline | prefiltered | raw_to_prefilter | valid_to_prefilter |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for _, row in subreddit_df.sort_values(["prefiltered_count", "raw_count", "group_value"], ascending=[False, False, True]).iterrows():
        lines.append(
            "| {subreddit} | {raw} | {valid} | {keep} | {borderline} | {prefiltered} | {raw_retention:.1%} | {valid_retention:.1%} |".format(
                subreddit=row["group_value"],
                raw=int(row["raw_count"]),
                valid=int(row["valid_count"]),
                keep=int(row["kept_count"]),
                borderline=int(row["borderline_count"]),
                prefiltered=int(row["prefiltered_count"]),
                raw_retention=float(row["raw_to_prefilter_retention"]),
                valid_retention=float(row["valid_to_prefilter_retention"]),
            )
        )

    lines.extend(["", "## Recommended Policy Moves", ""])
    if not low_yield_seeds.empty:
        lines.append("- Deprioritize or retire broad seeds with raw>=threshold and zero prefiltered rows: " + ", ".join(str(value) for value in low_yield_seeds["group_value"].head(8).tolist()))
    if not high_yield_seeds.empty:
        lines.append("- Keep and prioritize the highest-yield seeds: " + ", ".join(str(value) for value in high_yield_seeds.sort_values(["raw_to_prefilter_retention", "prefiltered_count"], ascending=[False, False])["group_value"].head(8).tolist()))
    if not low_yield_subreddits.empty:
        lines.append("- Deny or demote subreddits with repeated raw volume and zero prefiltered yield: " + ", ".join(str(value) for value in low_yield_subreddits["group_value"].head(8).tolist()))
    if not high_yield_subreddits.empty:
        lines.append("- Prefer communities with observed downstream yield: " + ", ".join(str(value) for value in high_yield_subreddits.sort_values(["raw_to_prefilter_retention", "prefiltered_count"], ascending=[False, False])["group_value"].head(8).tolist()))

    if not seed_subreddit_df.empty:
        lines.extend([
            "",
            "## Seed x Subreddit Highlights",
            "",
            "| query_seed | subreddit | raw | prefiltered | raw_to_prefilter |",
            "| --- | --- | --- | --- | --- |",
        ])
        highlights = seed_subreddit_df[
            (seed_subreddit_df["raw_count"] >= int(min_raw_threshold)) | (seed_subreddit_df["prefiltered_count"] > 0)
        ].sort_values(["prefiltered_count", "raw_count", "query_seed", "subreddit_or_forum"], ascending=[False, False, True, True]).head(20)
        for _, row in highlights.iterrows():
            lines.append(
                "| {seed} | {subreddit} | {raw} | {prefiltered} | {retention:.1%} |".format(
                    seed=row["query_seed"],
                    subreddit=row["subreddit_or_forum"],
                    raw=int(row["raw_count"]),
                    prefiltered=int(row["prefiltered_count"]),
                    retention=float(row["raw_to_prefilter_retention"]),
                )
            )
    return "\n".join(lines) + "\n"


def _build_reddit_policy_audit(root_dir: Path) -> pd.DataFrame:
    """Build an auditable snapshot of the configured Reddit collection policy."""
    config_path = root_dir / "config" / "sources" / "reddit.yaml"
    config = load_yaml(config_path) if config_path.exists() else {}
    page_caps = config.get("per_seed_page_caps", {}) or {}
    rows = [
        {"policy_name": "query_mode", "policy_value": str(config.get("query_mode", ""))},
        {"policy_name": "seed_bank_path", "policy_value": str(config.get("seed_bank_path", ""))},
        {"policy_name": "preferred_subreddit_count", "policy_value": str(len(config.get("preferred_subreddits", []) or []))},
        {"policy_name": "deny_subreddit_pattern_count", "policy_value": str(len(config.get("deny_subreddit_patterns", []) or []))},
        {"policy_name": "default_per_seed_page_cap", "policy_value": str(config.get("default_per_seed_page_cap", config.get("max_pages_per_query", "")))},
        {"policy_name": "per_seed_page_cap_count", "policy_value": str(len(page_caps))},
        {"policy_name": "minimum_rolling_retention_threshold", "policy_value": str(config.get("minimum_rolling_retention_threshold", ""))},
        {"policy_name": "rolling_retention_min_pages", "policy_value": str(config.get("rolling_retention_min_pages", ""))},
        {"policy_name": "comment_expansion_mode", "policy_value": str(config.get("comment_expansion_mode", ""))},
        {"policy_name": "comment_expand_max_posts_per_page", "policy_value": str(config.get("comment_expand_max_posts_per_page", ""))},
        {"policy_name": "comment_expand_max_posts_per_query", "policy_value": str(config.get("comment_expand_max_posts_per_query", ""))},
        {"policy_name": "early_stop_low_yield_pages", "policy_value": str(config.get("early_stop_low_yield_pages", ""))},
        {"policy_name": "early_stop_max_kept_ratio", "policy_value": str(config.get("early_stop_max_kept_ratio", ""))},
        {"policy_name": "early_stop_overlap_ratio", "policy_value": str(config.get("early_stop_overlap_ratio", ""))},
        {"policy_name": "sample_seed_page_caps", "policy_value": "; ".join(f"{key}: {value}" for key, value in list(page_caps.items())[:8])},
    ]
    return pd.DataFrame(rows)


def _build_reddit_runtime_audit(root_dir: Path) -> pd.DataFrame:
    """Build a compact runtime audit from the latest Reddit stage profile, when present."""
    summary_path = root_dir / "data" / "analysis" / "source_stage_profile_summary.csv"
    if not summary_path.exists():
        return pd.DataFrame(columns=["metric_name", "metric_value"])
    summary_df = pd.read_csv(summary_path)
    if summary_df.empty or "source" not in summary_df.columns:
        return pd.DataFrame(columns=["metric_name", "metric_value"])
    matches = summary_df[summary_df["source"].astype(str).str.lower() == "reddit"]
    if matches.empty:
        return pd.DataFrame(columns=["metric_name", "metric_value"])
    row = matches.iloc[0]
    metrics = [
        ("request_count", int(_numeric_or_zero(row.get("request_count", 0)))),
        ("search_request_count", int(_numeric_or_zero(row.get("search_request_count", 0)))),
        ("comments_request_count", int(_numeric_or_zero(row.get("comments_request_count", 0)))),
        ("pagination_iterations", int(_numeric_or_zero(row.get("pagination_iterations", 0)))),
        ("average_items_per_page", round(_numeric_or_zero(row.get("average_items_per_page", 0.0)), 3)),
        ("average_kept_items_per_page", round(_numeric_or_zero(row.get("average_kept_items_per_page", 0.0)), 3)),
        ("query_overlap_skip_count", int(_numeric_or_zero(row.get("query_overlap_skip_count", 0)))),
        ("comment_fetch_count", int(_numeric_or_zero(row.get("comment_fetch_count", 0)))),
        ("comment_skip_count", int(_numeric_or_zero(row.get("comment_skip_count", 0)))),
        ("pagination_low_yield_stop_count", int(_numeric_or_zero(row.get("pagination_low_yield_stop_count", 0)))),
        ("pagination_overlap_stop_count", int(_numeric_or_zero(row.get("pagination_overlap_stop_count", 0)))),
        ("pagination_rolling_retention_stop_count", int(_numeric_or_zero(row.get("pagination_rolling_retention_stop_count", 0)))),
        ("pagination_seed_page_cap_stop_count", int(_numeric_or_zero(row.get("pagination_seed_page_cap_stop_count", 0)))),
        ("pagination_repeated_cursor_stop_count", int(_numeric_or_zero(row.get("pagination_repeated_cursor_stop_count", 0)))),
        ("rate_limit_header_seen_count", int(_numeric_or_zero(row.get("rate_limit_header_seen_count", 0)))),
        ("total_sleep_seconds", round(_numeric_or_zero(row.get("total_sleep_seconds", 0.0)), 3)),
    ]
    return pd.DataFrame(metrics, columns=["metric_name", "metric_value"])


def _numeric_or_zero(value: object) -> float:
    """Return a finite float-like value while treating NaN as zero."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(numeric):
        return 0.0
    return numeric


def _load_raw_reddit(root_dir: Path) -> pd.DataFrame:
    """Load raw Reddit JSONL and normalize grouping fields."""
    raw_df = pd.DataFrame(read_jsonl(root_dir / "data" / "raw" / "reddit" / "raw.jsonl"))
    if raw_df.empty:
        return pd.DataFrame(columns=["source", "raw_id", "query_seed", "subreddit_or_forum"])
    raw_df = _filter_source(raw_df, "reddit")
    raw_df["query_seed"] = raw_df.get("query_seed", pd.Series(dtype=str)).fillna("").astype(str)
    raw_df["subreddit_or_forum"] = raw_df.apply(_raw_subreddit_value, axis=1)
    return raw_df


def _build_retention_table(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    keep_df: pd.DataFrame,
    borderline_df: pd.DataFrame,
    group_fields: list[str],
) -> pd.DataFrame:
    """Aggregate raw, valid, kept, and borderline counts for one Reddit grouping."""
    normalized_raw = _normalize_grouping_frame(raw_df, group_fields)
    normalized_valid = _normalize_grouping_frame(valid_df, group_fields)
    normalized_keep = _normalize_grouping_frame(keep_df, group_fields)
    normalized_borderline = _normalize_grouping_frame(borderline_df, group_fields)

    raw_counts = normalized_raw.groupby(group_fields, dropna=False).size().reset_index(name="raw_count")
    valid_counts = normalized_valid.groupby(group_fields, dropna=False).size().reset_index(name="valid_count")
    keep_counts = normalized_keep.groupby(group_fields, dropna=False).size().reset_index(name="kept_count")
    borderline_counts = normalized_borderline.groupby(group_fields, dropna=False).size().reset_index(name="borderline_count")

    result = raw_counts.merge(valid_counts, on=group_fields, how="left")
    result = result.merge(keep_counts, on=group_fields, how="left")
    result = result.merge(borderline_counts, on=group_fields, how="left")
    for column in ["valid_count", "kept_count", "borderline_count"]:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0).astype(int)
    result["prefiltered_count"] = result["kept_count"] + result["borderline_count"]
    result["raw_to_valid_retention"] = result["valid_count"] / result["raw_count"].clip(lower=1)
    result["raw_to_prefilter_retention"] = result["prefiltered_count"] / result["raw_count"].clip(lower=1)
    result["valid_to_prefilter_retention"] = result["prefiltered_count"] / result["valid_count"].clip(lower=1)

    if len(group_fields) == 1:
        renamed = result.rename(columns={group_fields[0]: "group_value"})
        return renamed[["group_value", *RETENTION_COLUMNS[1:]]].sort_values(["prefiltered_count", "raw_count", "group_value"], ascending=[False, False, True]).reset_index(drop=True)
    return result.sort_values(["prefiltered_count", "raw_count", *group_fields], ascending=[False, False, True, True]).reset_index(drop=True)


def _normalize_grouping_frame(df: pd.DataFrame, group_fields: list[str]) -> pd.DataFrame:
    """Ensure grouping fields exist and are normalized for joins."""
    frame = df.copy()
    for field in group_fields:
        if field not in frame.columns:
            if field == "subreddit_or_forum":
                frame[field] = frame.apply(_raw_subreddit_value, axis=1)
            else:
                frame[field] = ""
        frame[field] = frame[field].fillna("").astype(str).str.strip().str.lower()
    return frame


def _raw_subreddit_value(row: pd.Series) -> str:
    """Return the best subreddit-like value from raw or normalized Reddit rows."""
    existing = str(row.get("subreddit_or_forum", "") or "").strip()
    if existing:
        return existing
    source_meta = row.get("source_meta", {})
    if isinstance(source_meta, dict):
        subreddit = str(source_meta.get("subreddit_name_prefixed", "") or "").strip()
        if subreddit:
            return subreddit
        raw_subreddit = str(source_meta.get("subreddit", "") or "").strip()
        if raw_subreddit:
            return f"r/{raw_subreddit}" if not raw_subreddit.lower().startswith("r/") else raw_subreddit
    return ""


def _filter_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Return one source slice while tolerating empty frames."""
    if df.empty or "source" not in df.columns:
        return pd.DataFrame(columns=df.columns)
    return df[df["source"].astype(str).str.lower() == str(source).lower()].copy().reset_index(drop=True)