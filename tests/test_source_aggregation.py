"""Tests for workbook source aggregation behavior."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from src.analysis.diagnostics import count_raw_jsonl_by_source
from src.analysis.summary import build_final_source_distribution
from src.utils.io import write_jsonl, write_parquet
from src.utils.pipeline_schema import canonical_source_name, source_row_count


def test_canonical_source_name_folds_reddit_variants() -> None:
    """Subreddit-specific Reddit sources should aggregate under reddit."""
    assert canonical_source_name("reddit") == "reddit"
    assert canonical_source_name("reddit_r_excel") == "reddit"
    assert canonical_source_name("reddit_analytics") == "reddit"


def test_source_row_count_counts_reddit_variants_under_reddit() -> None:
    """Row counting for workbook summaries should fold reddit sub-sources."""
    df = pd.DataFrame({"source": ["reddit", "reddit_r_excel", "reddit_analytics", "stackoverflow"]})
    assert source_row_count(df, "reddit") == 3
    assert source_row_count(df, "stackoverflow") == 1


def test_build_final_source_distribution_merges_reddit_subsources() -> None:
    """Workbook source distribution should not double-list reddit variants."""
    normalized_df = pd.DataFrame({"source": ["reddit", "reddit_r_excel", "reddit_analytics", "stackoverflow"]})
    valid_df = pd.DataFrame({"source": ["reddit_r_excel", "stackoverflow"]})
    episodes_df = pd.DataFrame({"episode_id": ["e1"], "source": ["reddit_analytics"]})
    labeled_df = pd.DataFrame({"episode_id": ["e1"]})

    result = build_final_source_distribution(
        normalized_df=normalized_df,
        valid_df=valid_df,
        episodes_df=episodes_df,
        labeled_df=labeled_df,
        root_dir=None,
    )

    reddit_row = result[result["source"] == "reddit"].iloc[0]
    assert len(result[result["source"].astype(str).str.startswith("reddit_")]) == 0
    assert int(reddit_row["normalized_count"]) == 3
    assert int(reddit_row["valid_count"]) == 1
    assert int(reddit_row["episode_count"]) == 1
    assert int(reddit_row["labeled_count"]) == 1


def test_build_final_source_distribution_excludes_stale_raw_only_sources_not_in_registry() -> None:
    """Raw-only stale sources should not leak into workbook source sheets."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "config" / "sources").mkdir(parents=True, exist_ok=True)
        (root / "config" / "sources" / "reddit.yaml").write_text(
            "source_id: reddit\nsource_group: reddit\nenabled: true\n",
            encoding="utf-8",
        )
        write_jsonl(root / "data" / "raw" / "ga4_help_community" / "raw.jsonl", [{"id": "1"}])
        write_parquet(pd.DataFrame({"source": ["reddit"]}), root / "data" / "valid" / "valid_candidates_prefiltered.parquet")

        result = build_final_source_distribution(
            normalized_df=pd.DataFrame({"source": ["reddit"]}),
            valid_df=pd.DataFrame({"source": ["reddit"]}),
            episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["reddit"]}),
            labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
            root_dir=root,
        )

        assert result["source"].astype(str).tolist() == ["reddit"]


def test_build_final_source_distribution_excludes_disabled_zero_yield_sources() -> None:
    """Disabled or zero-yield sources should not appear even when raw artifacts remain."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "config" / "sources").mkdir(parents=True, exist_ok=True)
        (root / "config" / "sources" / "reddit.yaml").write_text(
            "source_id: reddit\nsource_group: reddit\nenabled: true\n",
            encoding="utf-8",
        )
        (root / "config" / "sources" / "amplitude_community.yaml").write_text(
            "source_id: amplitude_community\nsource_group: business_communities\nenabled: false\n",
            encoding="utf-8",
        )
        write_jsonl(root / "data" / "raw" / "amplitude_community" / "raw.jsonl", [{"id": "1"}])
        write_parquet(pd.DataFrame({"source": ["reddit"]}), root / "data" / "valid" / "valid_candidates_prefiltered.parquet")

        result = build_final_source_distribution(
            normalized_df=pd.DataFrame({"source": ["reddit"]}),
            valid_df=pd.DataFrame({"source": ["reddit"]}),
            episodes_df=pd.DataFrame({"episode_id": ["e1"], "source": ["reddit"]}),
            labeled_df=pd.DataFrame({"episode_id": ["e1"]}),
            root_dir=root,
        )

        assert "amplitude_community" not in result["source"].astype(str).tolist()
