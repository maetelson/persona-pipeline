"""Build business-community discovery inventory diagnostics from current artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.io import ensure_dir, write_parquet


def build_business_source_inventory_audit(
    root_dir: Path,
    sources: list[str] | None = None,
    output_dir_name: str = "business_source_inventory_audit",
) -> dict[str, Path]:
    """Build source and channel inventory diagnostics for business-community sources."""
    selected_sources = {str(source).strip() for source in (sources or []) if str(source).strip()}
    output_dir = ensure_dir(root_dir / "data" / "analysis" / output_dir_name)

    discovery_df = _read_parquet(root_dir / "data" / "analysis" / "business_community_discovery_audit.parquet")
    raw_df = _read_parquet(root_dir / "data" / "analysis" / "raw_audit.parquet")
    normalized_df = _read_parquet(root_dir / "data" / "normalized" / "normalized_posts.parquet")
    valid_df = _read_parquet(root_dir / "data" / "valid" / "valid_candidates.parquet")
    prefiltered_df = _read_parquet(root_dir / "data" / "valid" / "valid_candidates_prefiltered.parquet")
    episodes_df = _read_parquet(root_dir / "data" / "episodes" / "episode_table.parquet")
    labelability_df = _read_parquet(root_dir / "data" / "labeled" / "labelability_audit.parquet")

    if selected_sources:
        discovery_df = _filter_source_column(discovery_df, "source_id", selected_sources)
        raw_df = _filter_source_column(raw_df, "source", selected_sources)
        normalized_df = _filter_source_column(normalized_df, "source", selected_sources)
        valid_df = _filter_source_column(valid_df, "source", selected_sources)
        prefiltered_df = _filter_source_column(prefiltered_df, "source", selected_sources)
        episodes_df = _filter_source_column(episodes_df, "source", selected_sources)
        labelability_df = _filter_source_column(labelability_df, "source", selected_sources)

    channel_df = _build_channel_df(discovery_df)
    summary_df = _build_summary_df(
        root_dir=root_dir,
        discovery_df=discovery_df,
        raw_df=raw_df,
        normalized_df=normalized_df,
        valid_df=valid_df,
        prefiltered_df=prefiltered_df,
        episodes_df=episodes_df,
        labelability_df=labelability_df,
    )

    summary_csv = output_dir / "source_inventory_summary.csv"
    channel_csv = output_dir / "source_inventory_channels.csv"
    summary_md = output_dir / "source_inventory_summary.md"

    summary_df.to_csv(summary_csv, index=False)
    channel_df.to_csv(channel_csv, index=False)
    write_parquet(summary_df, output_dir / "source_inventory_summary.parquet")
    write_parquet(channel_df, output_dir / "source_inventory_channels.parquet")
    summary_md.write_text(_build_summary_markdown(summary_df, channel_df), encoding="utf-8")

    return {
        "summary_csv": summary_csv,
        "summary_parquet": output_dir / "source_inventory_summary.parquet",
        "channels_csv": channel_csv,
        "channels_parquet": output_dir / "source_inventory_channels.parquet",
        "summary_md": summary_md,
    }


def _build_channel_df(discovery_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate discovery counts by source and channel."""
    if discovery_df.empty:
        return pd.DataFrame(
            columns=[
                "source_id",
                "channel",
                "surface_count",
                "inventory_thread_count",
                "accepted_thread_count",
                "seed_filtered_count",
                "excluded_count",
                "duplicate_count",
            ]
        )
    return (
        discovery_df.groupby(["source_id", "channel"], dropna=False)
        .agg(
            surface_count=("surface_url", "nunique"),
            inventory_thread_count=("inventory_thread_count", "sum"),
            accepted_thread_count=("accepted_thread_count", "sum"),
            seed_filtered_count=("seed_filtered_count", "sum"),
            excluded_count=("excluded_count", "sum"),
            duplicate_count=("duplicate_count", "sum"),
        )
        .reset_index()
        .sort_values(["source_id", "accepted_thread_count", "inventory_thread_count"], ascending=[True, False, False])
        .reset_index(drop=True)
    )


def _build_summary_df(
    *,
    root_dir: Path,
    discovery_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    prefiltered_df: pd.DataFrame,
    episodes_df: pd.DataFrame,
    labelability_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build one summary row per source."""
    sources = sorted(
        set(_column_values(discovery_df, "source_id"))
        | set(_column_values(raw_df, "source"))
        | set(_column_values(normalized_df, "source"))
        | set(_column_values(valid_df, "source"))
        | set(_column_values(prefiltered_df, "source"))
        | set(_column_values(episodes_df, "source"))
        | set(_column_values(labelability_df, "source"))
    )
    rows: list[dict[str, object]] = []
    for source in sources:
        source_discovery = discovery_df[discovery_df["source_id"].astype(str).eq(source)].copy() if not discovery_df.empty else pd.DataFrame()
        inventory_count = _sum_column(source_discovery, "inventory_thread_count")
        accepted_count = _sum_column(source_discovery, "accepted_thread_count")
        seed_filtered_count = _sum_column(source_discovery, "seed_filtered_count")
        excluded_count = _sum_column(source_discovery, "excluded_count")
        duplicate_count = _sum_column(source_discovery, "duplicate_count")
        raw_count = _sum_for_source(raw_df, source, "source", "raw_record_count")
        if raw_count <= 0:
            raw_count = _count_jsonl_rows(root_dir / "data" / "raw" / source / "raw.jsonl")
        normalized_count = _count_for_source(normalized_df, source)
        valid_count = _count_for_source(valid_df, source)
        prefiltered_count = _count_for_source(prefiltered_df, source)
        episode_count = _count_for_source(episodes_df, source)
        labelable_count = _count_labelable_source_rows(labelability_df, source)
        rows.append(
            {
                "source_id": source,
                "inventory_thread_count": inventory_count,
                "accepted_thread_count": accepted_count,
                "seed_filtered_count": seed_filtered_count,
                "excluded_count": excluded_count,
                "duplicate_count": duplicate_count,
                "raw_record_count": raw_count,
                "normalized_count": normalized_count,
                "valid_count": valid_count,
                "prefiltered_count": prefiltered_count,
                "episode_count": episode_count,
                "labelable_count": labelable_count,
                "inventory_accept_ratio": _ratio(accepted_count, inventory_count),
                "accept_raw_ratio": _ratio(raw_count, accepted_count),
                "raw_prefilter_ratio": _ratio(prefiltered_count, raw_count),
                "feasibility_hint": _feasibility_hint(
                    inventory_count=inventory_count,
                    accepted_count=accepted_count,
                    raw_count=raw_count,
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["inventory_thread_count", "accepted_thread_count"], ascending=[False, False]).reset_index(drop=True)


def _build_summary_markdown(summary_df: pd.DataFrame, channel_df: pd.DataFrame) -> str:
    """Render a compact operator-facing markdown summary."""
    if summary_df.empty:
        return "# Business Source Inventory Audit\n\nNo discovery audit rows were available."
    lines = ["# Business Source Inventory Audit", ""]
    for row in summary_df.to_dict(orient="records"):
        source_id = str(row.get("source_id", ""))
        lines.append(f"## {source_id}")
        lines.append(
            (
                f"- Inventory: {int(row.get('inventory_thread_count', 0))} "
                f"| Accepted: {int(row.get('accepted_thread_count', 0))} "
                f"| Raw: {int(row.get('raw_record_count', 0))} "
                f"| Prefiltered: {int(row.get('prefiltered_count', 0))}"
            )
        )
        lines.append(
            (
                f"- Ratios: inventory->accept={float(row.get('inventory_accept_ratio', 0.0)):.3f}, "
                f"accept->raw={float(row.get('accept_raw_ratio', 0.0)):.3f}, "
                f"raw->prefilter={float(row.get('raw_prefilter_ratio', 0.0)):.3f}"
            )
        )
        lines.append(f"- Feasibility hint: {row.get('feasibility_hint', '')}")
        source_channels = channel_df[channel_df["source_id"].astype(str).eq(source_id)] if not channel_df.empty else pd.DataFrame()
        if not source_channels.empty:
            best_channels = source_channels.sort_values("accepted_thread_count", ascending=False).head(3)
            for channel_row in best_channels.to_dict(orient="records"):
                lines.append(
                    (
                        f"- Channel {channel_row.get('channel', '')}: "
                        f"inventory={int(channel_row.get('inventory_thread_count', 0))}, "
                        f"accepted={int(channel_row.get('accepted_thread_count', 0))}, "
                        f"seed_filtered={int(channel_row.get('seed_filtered_count', 0))}"
                    )
                )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _feasibility_hint(*, inventory_count: int, accepted_count: int, raw_count: int) -> str:
    """Return a compact hint about whether the source looks supply- or recall-limited."""
    if inventory_count <= 0 and accepted_count <= 0:
        return "discovery inventory audit not populated yet; rerun collect to measure ceiling"
    if raw_count >= 1000:
        return "already above 1000 raw rows"
    if inventory_count < 1000:
        return "inventory ceiling still looks low; source may be supply-limited"
    if accepted_count < 1000:
        return "inventory is available but accepted recall is still below 1000"
    return "discovery inventory and accepted recall both look large enough for 1000+ raw"


def _read_parquet(path: Path) -> pd.DataFrame:
    """Read parquet if present, else return an empty dataframe."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _filter_source_column(df: pd.DataFrame, column: str, sources: set[str]) -> pd.DataFrame:
    """Filter one dataframe by a stable source column when present."""
    if df.empty or column not in df.columns:
        return df
    return df[df[column].astype(str).isin(sources)].copy()


def _column_values(df: pd.DataFrame, column: str) -> list[str]:
    """Return non-empty string values from one dataframe column."""
    if df.empty or column not in df.columns:
        return []
    return [str(value) for value in df[column].dropna().astype(str).tolist() if str(value)]


def _sum_column(df: pd.DataFrame, column: str) -> int:
    """Return integer sum for one dataframe column."""
    if df.empty or column not in df.columns:
        return 0
    return int(pd.to_numeric(df[column], errors="coerce").fillna(0).sum())


def _sum_for_source(df: pd.DataFrame, source: str, source_column: str, value_column: str) -> int:
    """Return integer sum for one source/value column pair."""
    if df.empty or source_column not in df.columns or value_column not in df.columns:
        return 0
    matched = df[df[source_column].astype(str).eq(source)]
    if matched.empty:
        return 0
    return int(pd.to_numeric(matched[value_column], errors="coerce").fillna(0).sum())


def _count_for_source(df: pd.DataFrame, source: str) -> int:
    """Return row count for one source when present."""
    if df.empty or "source" not in df.columns:
        return 0
    return int(df["source"].astype(str).eq(source).sum())


def _count_labelable_source_rows(df: pd.DataFrame, source: str) -> int:
    """Return labelable-row count for one source."""
    if df.empty or "source" not in df.columns:
        return 0
    scoped = df[df["source"].astype(str).eq(source)].copy()
    if scoped.empty:
        return 0
    if "labelability_status" in scoped.columns:
        return int(scoped["labelability_status"].astype(str).isin({"labelable", "borderline"}).sum())
    return len(scoped)


def _ratio(numerator: int, denominator: int) -> float:
    """Return a safe rounded ratio."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _count_jsonl_rows(path: Path) -> int:
    """Count JSONL rows from disk when source-level raw audits are stale or missing."""
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())
