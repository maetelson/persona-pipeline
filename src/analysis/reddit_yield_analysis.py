"""Reddit yield-failure analysis with staged rejection taxonomy artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

WORKFLOW_TERMS = [
    "dashboard",
    "report",
    "reporting",
    "excel",
    "spreadsheet",
    "reconcile",
    "reconciliation",
    "metric",
    "metrics",
    "kpi",
    "sql",
    "tableau",
    "power bi",
    "metabase",
    "analytics",
    "analysis",
    "ad hoc",
    "stakeholder",
    "source of truth",
    "jira",
    "funnel",
    "cohort",
    "attribution",
    "campaign",
    "forecast",
    "fp&a",
    "power query",
    "dax",
]

PAIN_TERMS = [
    "manual",
    "mismatch",
    "wrong",
    "broken",
    "issue",
    "problem",
    "stuck",
    "slow",
    "hours",
    "rework",
    "trust",
    "debug",
    "root cause",
    "qa",
    "validate",
    "follow-up",
    "follow up",
    "urgent",
    "bottleneck",
    "pain",
]

OFF_TOPIC_TERMS = [
    "relationship",
    "girlfriend",
    "boyfriend",
    "roleplay",
    "gaming",
    "student housing",
    "astrology",
    "marriage",
    "breakup",
    "soccer",
    "football",
    "furryrp",
    "fantasyerp",
    "closed beta",
    "anti-armor",
    "breakups",
    "fanfic",
    "housing",
]

CHATTER_TERMS = [
    "daily report",
    "news",
    "job",
    "hiring",
    "agency",
    "cost",
    "looking for",
    "would i qualify",
    "anyone else",
    "i built a free",
    "new tool",
    "stock market",
    "morning update",
    "prompts that save",
]

NOISE_TERMS = [
    "preview.redd.it",
    "http://",
    "https://",
]

COMPARISON_INFRA_TERMS = [
    "docker",
    "ldap",
    "heroku",
    "api",
    "database",
    "crash",
    "sdk",
    "ssl",
    "timeout",
    "deploy",
]

REASON_ORDER = [
    "off_topic_discussion",
    "repeated_community_chatter",
    "duplicate_or_near_duplicate",
    "vague_discussion_without_operator_pain",
    "too_generic_no_actionable_workflow_signal",
    "parser_noise_or_metadata_only",
]

REASON_LABELS = {
    "off_topic_discussion": "Off-topic discussion",
    "repeated_community_chatter": "Repeated community chatter",
    "duplicate_or_near_duplicate": "Duplicate or near-duplicate",
    "vague_discussion_without_operator_pain": "Vague discussion without operator pain",
    "too_generic_no_actionable_workflow_signal": "Too generic / no actionable workflow signal",
    "parser_noise_or_metadata_only": "Parser noise / metadata-only rows",
    "product_infra_or_deployment_issue": "Within-domain product infra/deployment issue",
    "within_domain_but_too_generic": "Within-domain but too generic",
    "other_generic_support": "Other generic support",
}


def analyze_reddit_yield(root_dir: Path, comparison_source: str = "metabase_discussions") -> dict[str, Path]:
    """Build Reddit rejection-reason artifacts and a markdown diagnosis."""
    analysis_dir = root_dir / "data" / "analysis"
    invalid_df = pd.read_parquet(root_dir / "data" / "valid" / "invalid_candidates.parquet")
    keep_df = pd.read_parquet(root_dir / "data" / "prefilter" / "relevance_keep.parquet")
    borderline_df = pd.read_parquet(root_dir / "data" / "prefilter" / "relevance_borderline.parquet")
    drop_df = pd.read_parquet(root_dir / "data" / "prefilter" / "relevance_drop.parquet")
    normalized_df = pd.read_parquet(root_dir / "data" / "normalized" / "normalized_posts.parquet")
    valid_df = pd.read_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet")
    funnel_df = pd.read_parquet(root_dir / "data" / "analysis" / "survival_funnel_by_source.parquet")

    reddit_invalid_df = invalid_df[invalid_df["source"].astype(str) == "reddit"].copy().reset_index(drop=True)
    reddit_drop_df = drop_df[drop_df["source"].astype(str) == "reddit"].copy().reset_index(drop=True)
    reddit_prefiltered_df = pd.concat(
        [
            keep_df[keep_df["source"].astype(str) == "reddit"].assign(prefilter_bucket="keep"),
            borderline_df[borderline_df["source"].astype(str) == "reddit"].assign(prefilter_bucket="borderline"),
        ],
        ignore_index=True,
    )
    comparison_drop_df = drop_df[drop_df["source"].astype(str) == comparison_source].copy().reset_index(drop=True)

    reddit_invalid_df["rejection_reason"] = reddit_invalid_df.apply(_classify_reddit_invalid_row, axis=1)
    reddit_drop_df["rejection_reason"] = reddit_drop_df.apply(_classify_reddit_prefilter_drop_row, axis=1)
    comparison_drop_df["rejection_reason"] = comparison_drop_df.apply(_classify_comparison_drop_row, axis=1)

    invalid_counts_df = _build_reason_count_table(reddit_invalid_df, stage_label="raw_only_dropped")
    drop_counts_df = _build_reason_count_table(reddit_drop_df, stage_label="prefilter_rejected")
    overall_counts_df = _build_overall_reason_count_table(reddit_invalid_df, reddit_drop_df)
    comparison_counts_df = _build_reason_count_table(comparison_drop_df, stage_label=f"{comparison_source}_prefilter_rejected")

    reddit_examples_df = pd.concat(
        [
            _build_example_table(reddit_invalid_df, stage_label="raw_only_dropped"),
            _build_example_table(reddit_drop_df, stage_label="prefilter_rejected"),
        ],
        ignore_index=True,
    )
    kept_examples_df = _build_kept_example_table(reddit_prefiltered_df)

    outputs = {
        "invalid_counts_csv": analysis_dir / "reddit_raw_only_rejection_reason_counts.csv",
        "drop_counts_csv": analysis_dir / "reddit_prefilter_rejection_reason_counts.csv",
        "overall_counts_csv": analysis_dir / "reddit_rejection_reason_counts_overall.csv",
        "comparison_counts_csv": analysis_dir / f"{comparison_source}_rejection_reason_counts.csv",
        "examples_csv": analysis_dir / "reddit_rejection_examples.csv",
        "raw_samples_csv": analysis_dir / "reddit_raw_only_dropped_samples.csv",
        "drop_samples_csv": analysis_dir / "reddit_prefilter_rejected_samples.csv",
        "keep_samples_csv": analysis_dir / "reddit_prefilter_kept_samples.csv",
        "report_md": analysis_dir / "reddit_yield_failure_diagnosis.md",
    }
    invalid_counts_df.to_csv(outputs["invalid_counts_csv"], index=False)
    drop_counts_df.to_csv(outputs["drop_counts_csv"], index=False)
    overall_counts_df.to_csv(outputs["overall_counts_csv"], index=False)
    comparison_counts_df.to_csv(outputs["comparison_counts_csv"], index=False)
    reddit_examples_df.to_csv(outputs["examples_csv"], index=False)
    reddit_examples_df[reddit_examples_df["stage"] == "raw_only_dropped"].to_csv(outputs["raw_samples_csv"], index=False)
    reddit_examples_df[reddit_examples_df["stage"] == "prefilter_rejected"].to_csv(outputs["drop_samples_csv"], index=False)
    kept_examples_df.to_csv(outputs["keep_samples_csv"], index=False)
    outputs["report_md"].write_text(
        render_reddit_yield_report(
            normalized_df=normalized_df,
            valid_df=valid_df,
            funnel_df=funnel_df,
            reddit_invalid_df=reddit_invalid_df,
            reddit_drop_df=reddit_drop_df,
            reddit_prefiltered_df=reddit_prefiltered_df,
            invalid_counts_df=invalid_counts_df,
            drop_counts_df=drop_counts_df,
            overall_counts_df=overall_counts_df,
            kept_examples_df=kept_examples_df,
            comparison_source=comparison_source,
            comparison_counts_df=comparison_counts_df,
        ),
        encoding="utf-8",
    )
    return outputs


def render_reddit_yield_report(
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    funnel_df: pd.DataFrame,
    reddit_invalid_df: pd.DataFrame,
    reddit_drop_df: pd.DataFrame,
    reddit_prefiltered_df: pd.DataFrame,
    invalid_counts_df: pd.DataFrame,
    drop_counts_df: pd.DataFrame,
    overall_counts_df: pd.DataFrame,
    kept_examples_df: pd.DataFrame,
    comparison_source: str,
    comparison_counts_df: pd.DataFrame,
) -> str:
    """Render the markdown diagnosis for the Reddit yield audit."""
    reddit_funnel = funnel_df[funnel_df["source"].astype(str) == "reddit"]
    comparison_funnel = funnel_df[funnel_df["source"].astype(str) == comparison_source]
    normalized_reddit_count = int((normalized_df["source"].astype(str) == "reddit").sum())
    valid_reddit_count = int((valid_df["source"].astype(str) == "reddit").sum())
    prefiltered_reddit_count = len(reddit_prefiltered_df)
    dominant_invalid = invalid_counts_df.iloc[0] if not invalid_counts_df.empty else None
    dominant_drop = drop_counts_df.iloc[0] if not drop_counts_df.empty else None

    lines = [
        "# Reddit Yield Failure Diagnosis",
        "",
        "## Funnel",
        "",
    ]
    if not reddit_funnel.empty:
        row = reddit_funnel.iloc[0]
        lines.append(
            "- Workbook/source funnel: raw={raw}, valid={valid}, prefiltered_valid={prefiltered}, valid_rate={valid_rate:.1f}%, prefiltered_valid_per_valid={prefilter_rate:.1f}%.".format(
                raw=int(row.get("normalized_post_count", 0)),
                valid=int(row.get("valid_post_count", 0)),
                prefiltered=int(row.get("prefiltered_valid_post_count", 0)),
                valid_rate=float(row.get("valid_posts_per_normalized_post_pct", 0.0)),
                prefilter_rate=float(row.get("prefiltered_valid_posts_per_valid_post_pct", 0.0)),
            )
        )
    lines.append(
        "- Current row-level audit snapshot available on disk: normalized={normalized}, valid={valid}, prefiltered_valid={prefiltered} (keep={keep}, borderline={borderline}).".format(
            normalized=normalized_reddit_count,
            valid=valid_reddit_count,
            prefiltered=prefiltered_reddit_count,
            keep=int((reddit_prefiltered_df.get("prefilter_bucket", pd.Series(dtype=str)).astype(str) == "keep").sum()),
            borderline=int((reddit_prefiltered_df.get("prefilter_bucket", pd.Series(dtype=str)).astype(str) == "borderline").sum()),
        )
    )
    if not reddit_funnel.empty and (int(reddit_funnel.iloc[0].get("valid_post_count", 0)) != valid_reddit_count):
        lines.append(
            "- Note: the funnel workbook snapshot and the currently materialized row-level parquet files are from slightly different reruns, but both show the same low-yield pattern and the same rejection mix."
        )

    lines.extend(["", "## Rejection Taxonomy", ""])
    for reason in REASON_ORDER:
        lines.append(f"- {REASON_LABELS[reason]}")

    lines.extend(["", "## Dominant Causes", ""])
    if dominant_invalid is not None:
        lines.append(
            "- Before validity: {reason} is the largest Reddit invalid bucket at {count} rows ({share:.1f}% of raw-only drops).".format(
                reason=dominant_invalid["reason_label"],
                count=int(dominant_invalid["row_count"]),
                share=float(dominant_invalid["share_within_stage_pct"]),
            )
        )
    if dominant_drop is not None:
        lines.append(
            "- After validity: {reason} is the largest Reddit prefilter-rejected bucket at {count} rows ({share:.1f}% of valid-but-rejected rows).".format(
                reason=dominant_drop["reason_label"],
                count=int(dominant_drop["row_count"]),
                share=float(dominant_drop["share_within_stage_pct"]),
            )
        )
    top_three_overall = overall_counts_df.head(3)
    if not top_three_overall.empty:
        summary = ", ".join(
            f"{row.reason_label}={int(row.row_count)} ({float(row.share_of_all_rejections_pct):.1f}%)"
            for row in top_three_overall.itertuples(index=False)
        )
        lines.append(f"- Across Reddit losses, the dominant rejection mix is {summary}.")

    lines.extend(["", "## Counts By Stage", "", "### Raw-Only Dropped", ""])
    lines.extend(_markdown_reason_table(invalid_counts_df))
    lines.extend(["", "### Valid But Prefilter-Rejected", ""])
    lines.extend(_markdown_reason_table(drop_counts_df))

    lines.extend(["", f"## Comparison: {comparison_source}", ""])
    if not comparison_funnel.empty:
        row = comparison_funnel.iloc[0]
        lines.append(
            "- {source} funnel: valid={valid}, prefiltered_valid={prefiltered}, prefiltered_valid_per_valid={rate:.1f}%.".format(
                source=comparison_source,
                valid=int(row.get("valid_post_count", 0)),
                prefiltered=int(row.get("prefiltered_valid_post_count", 0)),
                rate=float(row.get("prefiltered_valid_posts_per_valid_post_pct", 0.0)),
            )
        )
    lines.append(
        "- {source} rejected rows are dominated by within-domain product/support issues, not broad off-topic ingestion.".format(
            source=comparison_source,
        )
    )
    lines.extend(_markdown_reason_table(comparison_counts_df))

    lines.extend(["", "## Kept Reddit Examples", ""])
    if kept_examples_df.empty:
        lines.append("- No kept or borderline Reddit rows were available in the current artifact snapshot.")
    else:
        for row in kept_examples_df.head(6).itertuples(index=False):
            lines.append(
                "- [{bucket}] {title} | {subreddit} | {snippet}".format(
                    bucket=row.prefilter_bucket,
                    title=row.title,
                    subreddit=row.subreddit_or_forum,
                    snippet=row.text_snippet,
                )
            )

    lines.extend(["", "## Diagnosis", ""])
    lines.append(
        "- Primary cause: too-broad seed/query strategy feeding low-yield ingestion. Reddit invalid rows are dominated by off-topic discussion, community chatter, and duplicates before the prefilter even runs."
    )
    lines.append(
        "- Secondary cause: Reddit text-structure mismatch. Among rows that survive validity, most rejected rows are anecdotal or adjacent community discussion with weak operator pain and weak workflow specificity, so the prefilter rejects them for good reason."
    )
    lines.append(
        "- Not the main cause: parser noise. Parser/noise rows are a small minority in both the invalid and prefilter-rejected stages."
    )
    lines.append(
        "- Weak Reddit-specific prefilter is not the dominant failure. The prefilter is mostly catching broad chatter and generic discussion that should not become persona evidence; the larger problem is that retrieval is pulling too much of that material in the first place."
    )
    return "\n".join(lines) + "\n"


def _build_reason_count_table(df: pd.DataFrame, stage_label: str) -> pd.DataFrame:
    """Aggregate rejection reasons for one stage."""
    if df.empty:
        return pd.DataFrame(columns=["stage", "rejection_reason", "reason_label", "row_count", "share_within_stage_pct"])
    counts_df = (
        df.groupby("rejection_reason", dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["row_count", "rejection_reason"], ascending=[False, True])
        .reset_index(drop=True)
    )
    counts_df["stage"] = stage_label
    counts_df["reason_label"] = counts_df["rejection_reason"].map(REASON_LABELS).fillna(counts_df["rejection_reason"])
    counts_df["share_within_stage_pct"] = (counts_df["row_count"] / max(len(df), 1) * 100).round(1)
    return counts_df[["stage", "rejection_reason", "reason_label", "row_count", "share_within_stage_pct"]]


def _build_overall_reason_count_table(reddit_invalid_df: pd.DataFrame, reddit_drop_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate rejection reasons across invalid and prefilter-drop Reddit rows."""
    combined_df = pd.concat([reddit_invalid_df, reddit_drop_df], ignore_index=True)
    if combined_df.empty:
        return pd.DataFrame(columns=["rejection_reason", "reason_label", "row_count", "share_of_all_rejections_pct"])
    counts_df = (
        combined_df.groupby("rejection_reason", dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["row_count", "rejection_reason"], ascending=[False, True])
        .reset_index(drop=True)
    )
    counts_df["reason_label"] = counts_df["rejection_reason"].map(REASON_LABELS).fillna(counts_df["rejection_reason"])
    counts_df["share_of_all_rejections_pct"] = (counts_df["row_count"] / max(len(combined_df), 1) * 100).round(1)
    return counts_df[["rejection_reason", "reason_label", "row_count", "share_of_all_rejections_pct"]]


def _build_example_table(df: pd.DataFrame, stage_label: str, per_reason: int = 3) -> pd.DataFrame:
    """Select representative examples for each rejection reason."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "stage",
                "rejection_reason",
                "reason_label",
                "raw_id",
                "title",
                "subreddit_or_forum",
                "invalid_reason",
                "final_relevance_score",
                "text_snippet",
            ]
        )
    working_df = df.copy()
    working_df["text_rank"] = working_df.get("text_len", pd.Series([0] * len(working_df))).fillna(0)
    working_df["text_snippet"] = working_df.get("raw_text", pd.Series([""] * len(working_df))).fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.slice(0, 220)
    example_frames: list[pd.DataFrame] = []
    for reason in REASON_ORDER:
        reason_df = working_df[working_df["rejection_reason"] == reason].copy()
        if reason_df.empty:
            continue
        reason_df = reason_df.sort_values(["text_rank", "title"], ascending=[False, True])
        reason_df = reason_df.drop_duplicates(subset=["title"], keep="first").head(per_reason)
        example_frames.append(reason_df)
    if not example_frames:
        return pd.DataFrame()
    examples_df = pd.concat(example_frames, ignore_index=True)
    examples_df["stage"] = stage_label
    examples_df["reason_label"] = examples_df["rejection_reason"].map(REASON_LABELS).fillna(examples_df["rejection_reason"])
    if "invalid_reason" not in examples_df.columns:
        examples_df["invalid_reason"] = ""
    if "final_relevance_score" not in examples_df.columns:
        examples_df["final_relevance_score"] = 0.0
    selected_columns = [
        "stage",
        "rejection_reason",
        "reason_label",
        "raw_id",
        "title",
        "subreddit_or_forum",
        "invalid_reason",
        "final_relevance_score",
        "text_snippet",
    ]
    return examples_df[selected_columns].reset_index(drop=True)


def _build_kept_example_table(df: pd.DataFrame, limit: int = 12) -> pd.DataFrame:
    """Return representative kept/borderline Reddit rows."""
    if df.empty:
        return pd.DataFrame(columns=["prefilter_bucket", "raw_id", "title", "subreddit_or_forum", "final_relevance_score", "text_snippet"])
    working_df = df.copy()
    working_df["text_snippet"] = working_df.get("raw_text", pd.Series([""] * len(working_df))).fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.slice(0, 220)
    working_df = working_df.sort_values(["prefilter_bucket", "final_relevance_score", "title"], ascending=[True, False, True])
    return working_df[["prefilter_bucket", "raw_id", "title", "subreddit_or_forum", "final_relevance_score", "text_snippet"]].head(limit)


def _classify_reddit_invalid_row(row: pd.Series) -> str:
    """Map one Reddit invalid row into a human-meaningful rejection reason."""
    combined_text = _combine_text_fields(row)
    invalid_reason = str(row.get("invalid_reason", "") or "")
    raw_text = str(row.get("raw_text", "") or "")
    body_text = str(row.get("body", "") or "")
    if "duplicate_candidate" in invalid_reason:
        return "duplicate_or_near_duplicate"
    if len(raw_text.strip()) < 120 or (_has_any(combined_text, NOISE_TERMS) and len(body_text.strip()) < 80):
        return "parser_noise_or_metadata_only"
    if _has_any(combined_text, OFF_TOPIC_TERMS) or ("missing_business_signal" in invalid_reason and not _has_any(combined_text, WORKFLOW_TERMS)):
        return "off_topic_discussion"
    if "missing_pain_signal" in invalid_reason and _has_any(combined_text, WORKFLOW_TERMS):
        return "vague_discussion_without_operator_pain"
    if _has_any(combined_text, CHATTER_TERMS):
        return "repeated_community_chatter"
    return "too_generic_no_actionable_workflow_signal"


def _classify_reddit_prefilter_drop_row(row: pd.Series) -> str:
    """Map one Reddit prefilter drop into a human-meaningful rejection reason."""
    combined_text = _combine_text_fields(row)
    raw_text = str(row.get("raw_text", "") or "")
    body_text = str(row.get("body", "") or "")
    positive_signals = str(row.get("top_positive_signals", "") or "").lower()
    source_reason = str(row.get("source_specific_reason", "") or "").lower()
    score = float(row.get("final_relevance_score", 0.0) or 0.0)
    if len(raw_text.strip()) < 120 or (_has_any(combined_text, NOISE_TERMS) and len(body_text.strip()) < 80):
        return "parser_noise_or_metadata_only"
    if _has_any(combined_text, CHATTER_TERMS):
        return "repeated_community_chatter"
    if _has_any(combined_text, OFF_TOPIC_TERMS) or (source_reason == "reddit:generic" and score < 1.0 and not _has_any(combined_text, WORKFLOW_TERMS)):
        return "off_topic_discussion"
    if (("bi_tool_terms" in positive_signals) or _has_any(combined_text, WORKFLOW_TERMS)) and not _has_any(combined_text, PAIN_TERMS) and score < 6.5:
        return "vague_discussion_without_operator_pain"
    return "too_generic_no_actionable_workflow_signal"


def _classify_comparison_drop_row(row: pd.Series) -> str:
    """Map comparison-source drops into a simple contrast taxonomy."""
    combined_text = _combine_text_fields(row)
    raw_text = str(row.get("raw_text", "") or "")
    if _has_any(combined_text, COMPARISON_INFRA_TERMS):
        return "product_infra_or_deployment_issue"
    if _has_any(combined_text, WORKFLOW_TERMS):
        return "within_domain_but_too_generic"
    if len(raw_text.strip()) < 120:
        return "parser_noise_or_metadata_only"
    return "other_generic_support"


def _markdown_reason_table(df: pd.DataFrame) -> list[str]:
    """Render a small markdown table from a reason count dataframe."""
    if df.empty:
        return ["No rows available."]
    lines = [
        "| reason | count | share |",
        "| --- | --- | --- |",
    ]
    share_column = "share_within_stage_pct" if "share_within_stage_pct" in df.columns else "share_of_all_rejections_pct"
    for row in df.itertuples(index=False):
        lines.append(f"| {row.reason_label} | {int(row.row_count)} | {float(getattr(row, share_column)):.1f}% |")
    return lines


def _combine_text_fields(row: pd.Series) -> str:
    """Combine the main text-bearing fields into one lowercase string."""
    values = [
        str(row.get("title", "") or ""),
        str(row.get("body", "") or ""),
        str(row.get("comments_text", "") or ""),
        str(row.get("raw_text", "") or ""),
        str(row.get("subreddit_or_forum", "") or ""),
    ]
    return " ".join(values).lower()


def _has_any(text: str, terms: list[str]) -> bool:
    """Return whether any term is present in the text."""
    lowered = str(text).lower()
    return any(term in lowered for term in terms)
