"""Exploratory priority scoring for labeled episodes."""

from __future__ import annotations

from typing import Any

import pandas as pd


def build_priority_scores(labeled_df: pd.DataFrame, scoring: dict[str, Any]) -> pd.DataFrame:
    """Assign exploratory priority scores to labeled episodes.

    This score is directional only and should be treated as an analyst aid,
    not as a final ranking model.
    """
    columns = [
        "episode_id",
        "priority_score",
        "priority_band",
        "pain_score",
        "output_score",
        "role_score",
        "workaround_score",
        "fit_score",
        "unknown_penalty",
        "score_note",
    ]
    if labeled_df.empty:
        return pd.DataFrame(columns=columns)

    weights = scoring.get("weights", {})
    result = labeled_df.copy()
    result["pain_score"] = result["pain_codes"].map(lambda value: _non_unknown_weight(value, weights.get("pain_keyword_match", 0.4)))
    result["output_score"] = result["output_codes"].map(lambda value: _non_unknown_weight(value, weights.get("output_clarity", 0.2)))
    result["role_score"] = result["role_codes"].map(lambda value: _non_unknown_weight(value, weights.get("role_clarity", 0.2)))
    result["workaround_score"] = result["workaround_codes"].map(
        lambda value: _non_unknown_weight(value, weights.get("workaround_presence", 0.2))
    )
    result["fit_score"] = result["fit_code"].map(lambda value: 0.15 if str(value) == "F_STRONG" else 0.05 if str(value) == "F_REVIEW" else 0.0)
    result["unknown_penalty"] = result.apply(_unknown_penalty, axis=1)
    result["priority_score"] = (
        result["pain_score"]
        + result["output_score"]
        + result["role_score"]
        + result["workaround_score"]
        + result["fit_score"]
        - result["unknown_penalty"]
    ).clip(lower=0.0, upper=1.0)

    high = float(scoring.get("default_priority_band", {}).get("high", 0.75))
    medium = float(scoring.get("default_priority_band", {}).get("medium", 0.45))
    result["priority_band"] = "low"
    result.loc[result["priority_score"] >= medium, "priority_band"] = "medium"
    result.loc[result["priority_score"] >= high, "priority_band"] = "high"
    result["score_note"] = "exploratory priority score from rule labels and scoring.yaml"
    return result[columns]


def _non_unknown_weight(value: Any, weight: Any) -> float:
    """Return weight only when a code family is known."""
    return float(weight) if str(value or "") != "unknown" else 0.0


def _unknown_penalty(row: pd.Series) -> float:
    """Apply a small penalty when core fields remain unknown."""
    penalty = 0.0
    for column in ["question_codes", "pain_codes", "env_codes"]:
        if str(row.get(column, "") or "") == "unknown":
            penalty += 0.05
    return penalty
