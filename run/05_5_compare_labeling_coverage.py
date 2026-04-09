"""Compare rule-only labeling coverage against post-LLM labeling coverage."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.utils.io import read_parquet, write_parquet
from src.utils.logging import get_logger

LOGGER = get_logger("run.compare_labeling_coverage")

CORE_COLUMNS = ["role_codes", "question_codes", "pain_codes", "output_codes"]


def main() -> None:
    """Write a before/after coverage comparison for labeling quality review."""
    before_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes_rule_only.parquet")
    after_df = read_parquet(ROOT / "data" / "labeled" / "labeled_episodes.parquet")
    comparison_df = _build_comparison(before_df, after_df)
    output_path = ROOT / "data" / "labeled" / "labeling_coverage_comparison.parquet"
    write_parquet(comparison_df, output_path)
    csv_path = ROOT / "data" / "labeled" / "labeling_coverage_comparison.csv"
    comparison_df.to_csv(csv_path, index=False)
    LOGGER.info("Wrote labeling coverage comparison -> %s", output_path)
    if not comparison_df.empty:
        LOGGER.info("\n%s", comparison_df.to_string(index=False))


def _build_comparison(before_df: pd.DataFrame, after_df: pd.DataFrame) -> pd.DataFrame:
    """Build a small before/after metric table."""
    rows: list[dict[str, float | str | int]] = []
    before_total = len(before_df)
    after_total = len(after_df)
    rows.append(_metric_row("total_rows", before_total, after_total))
    rows.append(_metric_row("unknown_ratio", _unknown_ratio(before_df), _unknown_ratio(after_df)))
    for column, label in [
        ("role_codes", "role_coverage_ratio"),
        ("question_codes", "question_coverage_ratio"),
        ("pain_codes", "pain_coverage_ratio"),
        ("output_codes", "output_coverage_ratio"),
    ]:
        rows.append(_metric_row(label, _coverage_ratio(before_df, column), _coverage_ratio(after_df, column)))
    return pd.DataFrame(rows, columns=["metric_name", "before_value", "after_value", "delta"])


def _metric_row(metric_name: str, before_value: float | int, after_value: float | int) -> dict[str, float | int | str]:
    """Build a one-line comparison row."""
    delta = round(float(after_value) - float(before_value), 4)
    return {
        "metric_name": metric_name,
        "before_value": round(float(before_value), 4) if isinstance(before_value, float) else before_value,
        "after_value": round(float(after_value), 4) if isinstance(after_value, float) else after_value,
        "delta": delta,
    }


def _unknown_ratio(df: pd.DataFrame) -> float:
    """Return the ratio of rows with any unresolved core family."""
    if df.empty:
        return 0.0
    mask = pd.Series(False, index=df.index)
    for column in CORE_COLUMNS:
        if column in df.columns:
            mask = mask | df[column].fillna("").astype(str).isin(["", "unknown", "null", "none", "other"])
    return float(mask.mean())


def _coverage_ratio(df: pd.DataFrame, column: str) -> float:
    """Return the ratio of rows with a known code in one family."""
    if df.empty or column not in df.columns:
        return 0.0
    return float((~df[column].fillna("").astype(str).isin(["", "unknown", "null", "none", "other"])).mean())


if __name__ == "__main__":
    main()
