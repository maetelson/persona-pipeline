"""Filter invalid rows and write valid candidate parquet outputs."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.analysis.raw_audit import build_downstream_loss_audit, build_source_health_df
from src.filters.dedupe import split_duplicate_posts
from src.filters.invalid_filter import activate_rule_mode, apply_invalid_filter
from src.utils.io import load_yaml, read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.filter_valid")


def main() -> None:
    """Apply invalid filtering and dedupe to normalized posts."""
    normalized_df = read_parquet(ROOT / "data" / "normalized" / "normalized_posts.parquet")
    time_filtered_df = read_parquet(ROOT / "data" / "normalized" / "time_filtered_posts.parquet")
    time_invalid_df = read_parquet(ROOT / "data" / "normalized" / "time_window_invalid.parquet")
    rules = load_yaml(ROOT / "config" / "invalid_rules.yaml")
    filter_mode = os.getenv("VALID_FILTER_MODE", str(rules.get("default_mode", "analysis")))
    rules = activate_rule_mode(rules, mode=filter_mode)

    candidate_df = time_filtered_df if not time_filtered_df.empty else normalized_df
    valid_df, invalid_rule_df = apply_invalid_filter(candidate_df, rules)
    valid_df, duplicate_invalid_df = split_duplicate_posts(valid_df)

    invalid_frames = [frame for frame in [time_invalid_df, invalid_rule_df, duplicate_invalid_df] if not frame.empty]
    if invalid_frames:
        invalid_df = pd.concat(invalid_frames, ignore_index=True)
    else:
        invalid_df = pd.DataFrame(columns=list(candidate_df.columns) + ["invalid_reason"])

    write_parquet(valid_df, ROOT / "data" / "valid" / "valid_candidates.parquet")
    write_parquet(invalid_df, ROOT / "data" / "valid" / "invalid_candidates.parquet")
    write_parquet(invalid_df, ROOT / "data" / "valid" / "invalid_log.parquet")
    loss_audit_df = build_downstream_loss_audit(candidate_df, valid_df, invalid_df)
    write_parquet(loss_audit_df, ROOT / "data" / "valid" / "downstream_loss_audit.parquet")
    validation_dropped_df = (
        pd.concat([frame for frame in [time_invalid_df, invalid_rule_df] if not frame.empty], ignore_index=True)
        if not time_invalid_df.empty or not invalid_rule_df.empty
        else pd.DataFrame(columns=list(candidate_df.columns) + ["invalid_reason"])
    )
    source_health_df = build_source_health_df(
        raw_audit_df=read_parquet(ROOT / "data" / "analysis" / "raw_audit.parquet"),
        page_audit_df=read_parquet(ROOT / "data" / "analysis" / "raw_page_audit.parquet"),
        error_audit_df=read_parquet(ROOT / "data" / "analysis" / "raw_error_audit.parquet"),
        normalized_df=normalized_df,
        validation_dropped_df=validation_dropped_df,
        duplicate_invalid_df=duplicate_invalid_df,
    )
    write_parquet(source_health_df, ROOT / "data" / "analysis" / "source_health_after_fix.parquet")
    source_health_df.to_csv(ROOT / "data" / "analysis" / "source_health_after_fix.csv", index=False)
    business_health_df = _build_business_community_health(
        normalized_df=normalized_df,
        valid_df=valid_df,
        duplicate_invalid_df=duplicate_invalid_df,
    )
    if not business_health_df.empty:
        write_parquet(business_health_df, ROOT / "data" / "analysis" / "business_community_source_health.parquet")
        business_health_df.to_csv(ROOT / "data" / "analysis" / "business_community_source_health.csv", index=False)

    if invalid_df.empty:
        invalid_reason_audit_df = pd.DataFrame(columns=["source", "invalid_reason", "count"])
    else:
        invalid_reason_audit_df = (
            invalid_df.assign(invalid_reason=invalid_df["invalid_reason"].fillna("").astype(str).str.split("|"))
            .explode("invalid_reason")
            .query("invalid_reason != ''")
            .groupby(["source", "invalid_reason"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["source", "count", "invalid_reason"], ascending=[True, False, True])
            .reset_index(drop=True)
        )
    write_parquet(invalid_reason_audit_df, ROOT / "data" / "valid" / "invalid_reason_audit.parquet")

    LOGGER.info("Filter mode=%s", filter_mode)
    LOGGER.info("Wrote valid candidates: %s", len(valid_df))
    LOGGER.info("Wrote invalid candidates: %s", len(invalid_df))


def _build_business_community_health(
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    duplicate_invalid_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build requested diagnostics for public business community sources."""
    collection_path = ROOT / "data" / "analysis" / "business_community_collection_health.csv"
    if not collection_path.exists():
        return pd.DataFrame()
    health_df = pd.read_csv(collection_path)
    if health_df.empty or "source_id" not in health_df.columns:
        return health_df
    rows: list[dict[str, int | str]] = []
    for _, row in health_df.iterrows():
        source_id = str(row.get("source_id", "") or "")
        rows.append(
            {
                "source_id": source_id,
                "discovered_thread_count": int(row.get("discovered_thread_count", 0) or 0),
                "fetched_thread_count": int(row.get("fetched_thread_count", 0) or 0),
                "parse_success_count": int(row.get("parse_success_count", 0) or 0),
                "parse_error_count": int(row.get("parse_error_count", 0) or 0),
                "deduped_count": _count_source(duplicate_invalid_df, source_id),
                "normalized_count": _count_source(normalized_df, source_id),
                "valid_count": _count_source(valid_df, source_id),
            }
        )
    return pd.DataFrame(rows)


def _count_source(df: pd.DataFrame, source_id: str) -> int:
    """Count rows for one source in a dataframe."""
    if df.empty or "source" not in df.columns:
        return 0
    return int((df["source"].astype(str) == source_id).sum())


if __name__ == "__main__":
    main()
