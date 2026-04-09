"""Exploratory persona candidate generation from labeled episodes."""

from __future__ import annotations

import pandas as pd


def build_persona_candidates(labeled_df: pd.DataFrame, priority_df: pd.DataFrame) -> pd.DataFrame:
    """Build exploratory persona candidates from recurring label combinations.

    These are candidate personas for review, not final persona definitions.
    """
    columns = [
        "persona_id",
        "role_codes",
        "moment_codes",
        "question_codes",
        "pain_codes",
        "env_codes",
        "output_codes",
        "episode_count",
        "avg_priority_score",
        "high_priority_count",
        "persona_label",
        "persona_note",
    ]
    if labeled_df.empty:
        return pd.DataFrame(columns=columns)

    merged = labeled_df.merge(priority_df[["episode_id", "priority_score", "priority_band"]], on="episode_id", how="left")
    grouped = (
        merged.groupby(
            ["role_codes", "moment_codes", "question_codes", "pain_codes", "env_codes", "output_codes"],
            dropna=False,
        )
        .agg(
            episode_count=("episode_id", "size"),
            avg_priority_score=("priority_score", "mean"),
            high_priority_count=("priority_band", lambda values: int((values == "high").sum())),
        )
        .reset_index()
        .sort_values(["episode_count", "avg_priority_score"], ascending=[False, False])
        .reset_index(drop=True)
    )
    grouped["persona_id"] = [f"persona_{idx + 1:02d}" for idx in range(len(grouped))]
    grouped["persona_label"] = grouped.apply(_persona_label, axis=1)
    grouped["persona_note"] = "exploratory persona candidate built from recurring labeled episode patterns"
    return grouped[columns]


def _persona_label(row: pd.Series) -> str:
    """Generate a readable exploratory persona label."""
    parts = [
        str(row.get("role_codes", "unknown")),
        str(row.get("pain_codes", "unknown")),
        str(row.get("env_codes", "unknown")),
    ]
    return " / ".join(part for part in parts if part and part != "unknown") or "unknown persona"
