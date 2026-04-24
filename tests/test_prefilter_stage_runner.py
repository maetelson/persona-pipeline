"""Tests for source-scoped relevance prefilter stage helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
_STAGE_SPEC = importlib.util.spec_from_file_location(
    "run_03_5_prefilter_relevance",
    ROOT / "run" / "pipeline" / "03_5_prefilter_relevance.py",
)
if _STAGE_SPEC is None or _STAGE_SPEC.loader is None:
    raise RuntimeError("Unable to load run/pipeline/03_5_prefilter_relevance.py for tests.")
_STAGE_MODULE = importlib.util.module_from_spec(_STAGE_SPEC)
_STAGE_SPEC.loader.exec_module(_STAGE_MODULE)

_merge_source_rows = _STAGE_MODULE._merge_source_rows
_build_invalid_with_prefilter = _STAGE_MODULE._build_invalid_with_prefilter
_rebalance_merged_results = _STAGE_MODULE._rebalance_merged_results
_slice_selected_sources = _STAGE_MODULE._slice_selected_sources


class PrefilterStageRunnerTests(unittest.TestCase):
    """Verify source-scoped merge helpers preserve untouched stage rows."""

    def test_slice_selected_sources_returns_subset_only(self) -> None:
        frame = pd.DataFrame(
            [
                {"source": "adobe_analytics_community", "raw_id": "a1"},
                {"source": "sisense_community", "raw_id": "s1"},
            ]
        )

        sliced = _slice_selected_sources(frame, {"adobe_analytics_community"})

        self.assertEqual(len(sliced), 1)
        self.assertEqual(str(sliced.iloc[0]["source"]), "adobe_analytics_community")

    def test_merge_source_rows_replaces_only_selected_source(self) -> None:
        existing_df = pd.DataFrame(
            [
                {"source": "adobe_analytics_community", "raw_id": "a_old"},
                {"source": "sisense_community", "raw_id": "s_old"},
            ]
        )
        updated_df = pd.DataFrame(
            [
                {"source": "adobe_analytics_community", "raw_id": "a_new"},
            ]
        )

        merged = _merge_source_rows(existing_df, updated_df, {"adobe_analytics_community"})

        self.assertEqual(set(merged["source"].astype(str)), {"adobe_analytics_community", "sisense_community"})
        adobe_ids = merged.loc[merged["source"].astype(str) == "adobe_analytics_community", "raw_id"].astype(str).tolist()
        sisense_ids = merged.loc[merged["source"].astype(str) == "sisense_community", "raw_id"].astype(str).tolist()
        self.assertEqual(adobe_ids, ["a_new"])
        self.assertEqual(sisense_ids, ["s_old"])

    def test_build_invalid_with_prefilter_replaces_only_selected_low_relevance_rows(self) -> None:
        previous_invalid_df = pd.DataFrame(
            [
                {"source": "adobe_analytics_community", "raw_id": "base_a", "invalid_reason": "missing_pain_signal"},
                {"source": "sisense_community", "raw_id": "base_s", "invalid_reason": "missing_business_signal"},
            ]
        )
        existing_invalid_with_prefilter_df = pd.DataFrame(
            [
                {"source": "adobe_analytics_community", "raw_id": "drop_old_a", "invalid_reason": "low_relevance_prefilter"},
                {"source": "sisense_community", "raw_id": "drop_old_s", "invalid_reason": "low_relevance_prefilter"},
                {"source": "adobe_analytics_community", "raw_id": "base_a", "invalid_reason": "missing_pain_signal"},
            ]
        )
        merged_drop_df = pd.DataFrame(
            [
                {"source": "adobe_analytics_community", "raw_id": "drop_new_a"},
            ]
        )

        rebuilt = _build_invalid_with_prefilter(
            previous_invalid_df=previous_invalid_df,
            merged_drop_df=merged_drop_df,
            selected_sources={"adobe_analytics_community"},
            existing_invalid_with_prefilter_df=existing_invalid_with_prefilter_df,
        )

        adobe_low_relevance = rebuilt[
            (rebuilt["source"].astype(str) == "adobe_analytics_community")
            & (rebuilt["invalid_reason"].astype(str) == "low_relevance_prefilter")
        ]["raw_id"].astype(str).tolist()
        sisense_low_relevance = rebuilt[
            (rebuilt["source"].astype(str) == "sisense_community")
            & (rebuilt["invalid_reason"].astype(str) == "low_relevance_prefilter")
        ]["raw_id"].astype(str).tolist()
        self.assertEqual(adobe_low_relevance, ["drop_new_a"])
        self.assertEqual(sisense_low_relevance, ["drop_old_s"])

    def test_rebalance_merged_results_uses_full_merged_mix_for_source_cap(self) -> None:
        merged_keep_df = pd.DataFrame(
            [
                {"source": "sisense_community", "raw_id": "s_keep", "relevance_decision": "keep", "final_relevance_score": 12.0},
            ]
        )
        merged_borderline_df = pd.DataFrame(
            [
                {"source": "google_developer_forums", "raw_id": "g1", "relevance_decision": "borderline", "final_relevance_score": 6.0},
                {"source": "google_developer_forums", "raw_id": "g2", "relevance_decision": "borderline", "final_relevance_score": 6.5},
            ]
        )
        merged_drop_df = pd.DataFrame(
            columns=["source", "raw_id", "relevance_decision", "final_relevance_score"]
        )
        rules = {
            "source_balance": {
                "enabled": True,
                "max_retained_source_share": 0.80,
                "min_total_retained_rows": 1,
                "protect_keep_score_at_or_above": 12.0,
                "protected_sources": [],
            }
        }

        keep_df, borderline_df, drop_df = _rebalance_merged_results(
            merged_keep_df=merged_keep_df,
            merged_borderline_df=merged_borderline_df,
            merged_drop_df=merged_drop_df,
            rules=rules,
        )

        self.assertEqual(set(borderline_df["raw_id"].astype(str)), {"g1", "g2"})
        self.assertEqual(set(keep_df["raw_id"].astype(str)), {"s_keep"})
        self.assertTrue(drop_df.empty)


if __name__ == "__main__":
    unittest.main()
