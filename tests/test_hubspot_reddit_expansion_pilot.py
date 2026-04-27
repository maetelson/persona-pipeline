"""Tests for the bounded HubSpot + Reddit existing-source expansion pilot."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from src.analysis.hubspot_reddit_expansion_pilot import (
    build_content_hash,
    build_existing_dedupe_index,
    build_pilot_row,
    build_summary,
    canonicalize_url,
    classify_duplicate,
    fit_is_meaningful,
    is_pilot_output_path,
    load_pilot_source_configs,
    normalize_title,
)
from src.collectors.base import RawRecord


class HubspotRedditExpansionPilotTests(unittest.TestCase):
    """Verify dedupe, scoring, and output isolation for the expansion pilot."""

    def test_pilot_configs_load_without_mutating_production(self) -> None:
        root = Path(__file__).resolve().parents[1]
        configs = load_pilot_source_configs(root)
        self.assertTrue(configs["hubspot_community"]["pilot_only"])
        self.assertTrue(configs["reddit"]["pilot_only"])
        self.assertEqual(configs["reddit"]["query_mode"], "source_config")
        self.assertEqual(configs["reddit"]["preferred_subreddits"], ["analytics", "businessintelligence", "marketinganalytics", "excel"])

    def test_url_and_title_normalization_are_stable(self) -> None:
        self.assertEqual(canonicalize_url("HTTPS://example.com/a//b/?x=1"), "https://example.com/a/b")
        self.assertEqual(normalize_title(" Dashboard Numbers Wrong!!! "), "dashboard numbers wrong")

    def test_duplicate_raw_id_and_url_are_detected(self) -> None:
        index = {"raw_id": {"123"}, "url": {"https://example.com/post"}, "canonical_url": set(), "title": set(), "content_hash": set()}
        seen = {"raw_id": set(), "url": set(), "canonical_url": set(), "title": set(), "content_hash": set()}
        record = RawRecord(
            source="hubspot_community",
            source_type="thread",
            raw_id="123",
            url="https://example.com/post",
            title="Title",
            body="Body",
            comments_text="",
            created_at="2026-01-01T00:00:00+00:00",
            fetched_at="2026-01-01T00:00:00+00:00",
            query_seed="seed",
        )
        status, against = classify_duplicate(record, index, seen)
        self.assertEqual(status, "duplicate_raw_id")
        self.assertEqual(against, "existing")

    def test_duplicate_content_hash_is_detected(self) -> None:
        content_hash = build_content_hash("Same title", "Same body")
        index = {"raw_id": set(), "url": set(), "canonical_url": set(), "title": set(), "content_hash": {content_hash}}
        seen = {"raw_id": set(), "url": set(), "canonical_url": set(), "title": set(), "content_hash": set()}
        record = RawRecord(
            source="reddit",
            source_type="thread",
            raw_id="abc",
            url="https://www.reddit.com/r/analytics/comments/abc/example/",
            title="Same title",
            body="Same body",
            comments_text="",
            created_at="2026-01-01T00:00:00+00:00",
            fetched_at="2026-01-01T00:00:00+00:00",
            query_seed="seed",
        )
        status, against = classify_duplicate(record, index, seen)
        self.assertEqual(status, "duplicate_content_hash")
        self.assertEqual(against, "existing")

    def test_duplicate_canonical_url_is_detected(self) -> None:
        index = {
            "raw_id": set(),
            "url": set(),
            "canonical_url": {"https://example.com/post"},
            "title": set(),
            "content_hash": set(),
        }
        seen = {"raw_id": set(), "url": set(), "canonical_url": set(), "title": set(), "content_hash": set()}
        record = RawRecord(
            source="hubspot_community",
            source_type="thread",
            raw_id="xyz",
            url="",
            canonical_url="https://example.com/post/",
            title="Unique title",
            body="Unique body",
            comments_text="",
            created_at="2026-01-01T00:00:00+00:00",
            fetched_at="2026-01-01T00:00:00+00:00",
            query_seed="seed",
        )
        status, against = classify_duplicate(record, index, seen)
        self.assertEqual(status, "duplicate_canonical_url")
        self.assertEqual(against, "existing")

    def test_new_unique_row_scores_without_noise(self) -> None:
        record = RawRecord(
            source="hubspot_community",
            source_type="thread",
            raw_id="new1",
            url="https://community.hubspot.com/t5/Reporting-Analytics/example/td-p/1",
            title="Why does this pipeline report not match CRM totals?",
            body="Leadership keeps asking why the dashboard export and CRM pipeline totals are different.",
            comments_text="",
            created_at="2026-01-01T00:00:00+00:00",
            fetched_at="2026-01-01T00:00:00+00:00",
            query_seed="pipeline report makes no sense",
        )
        row = build_pilot_row(record, "new_unique", "none")
        self.assertTrue(row.valid_candidate)
        self.assertTrue(row.estimated_persona_core_candidate)
        self.assertTrue(fit_is_meaningful(row.persona_01_fit))
        self.assertTrue(fit_is_meaningful(row.persona_04_fit))

    def test_output_path_guard_blocks_production_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            allowed = root / "artifacts" / "source_pilots" / "pilot.json"
            blocked = root / "data" / "raw" / "pilot.json"
            self.assertTrue(is_pilot_output_path(allowed, root))
            self.assertFalse(is_pilot_output_path(blocked, root))

    def test_existing_index_reads_empty_repo_safely(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data" / "raw" / "hubspot_community").mkdir(parents=True, exist_ok=True)
            (root / "data" / "normalized").mkdir(parents=True, exist_ok=True)
            (root / "data" / "labeled").mkdir(parents=True, exist_ok=True)
            (root / "data" / "episodes").mkdir(parents=True, exist_ok=True)
            index, summary = build_existing_dedupe_index(root, "hubspot_community")
            self.assertEqual(summary["existing_raw_count"], 0)
            self.assertEqual(len(index["raw_id"]), 0)

    def test_quality_summary_counts_only_new_unique_rows(self) -> None:
        root = Path(__file__).resolve().parents[1]
        baseline = {
            "persona_readiness_state": "reviewable_but_not_deck_ready",
            "overall_status": "WARN",
            "quality_flag": "EXPLORATORY",
            "total_raw_rows": 100,
            "total_labeled_rows": 10,
            "total_persona_core_rows": 8,
            "effective_balanced_source_count": 5.89,
            "persona_core_coverage_of_all_labeled_pct": 80.0,
            "weak_source_cost_center_count": 0,
            "core_readiness_weak_source_cost_center_count": 0,
            "final_usable_persona_count": 3,
            "production_ready_persona_count": 3,
            "review_ready_persona_count": 1,
            "deck_ready_claim_eligible_persona_count": 4,
            "sources": {},
        }
        unique_record = RawRecord(
            source="hubspot_community",
            source_type="thread",
            raw_id="new1",
            url="https://example.com/new1",
            title="Why does this pipeline report not match CRM totals?",
            body="Leadership keeps asking why the dashboard export and CRM pipeline totals are different.",
            comments_text="",
            created_at="2026-01-01T00:00:00+00:00",
            fetched_at="2026-01-01T00:00:00+00:00",
            query_seed="pipeline report makes no sense",
        )
        duplicate_record = RawRecord(
            source="hubspot_community",
            source_type="thread",
            raw_id="dup1",
            url="https://example.com/dup1",
            title="Course tutorial for dashboard setup",
            body="Tutorial for setup only.",
            comments_text="",
            created_at="2026-01-01T00:00:00+00:00",
            fetched_at="2026-01-01T00:00:00+00:00",
            query_seed="dashboard seed",
        )
        rows = [
            build_pilot_row(unique_record, "new_unique", "none"),
            build_pilot_row(duplicate_record, "duplicate_raw_id", "existing"),
        ]
        summary = build_summary(
            root,
            baseline,
            rows,
            {
                "hubspot_community": {"source": "hubspot_community"},
                "reddit": {"source": "reddit"},
            },
        )
        self.assertEqual(summary["source_summaries"]["hubspot_community"]["new_unique_rows"], 1)
        self.assertEqual(summary["source_summaries"]["hubspot_community"]["valid_candidate_count"], 1)
        self.assertEqual(summary["source_summaries"]["hubspot_community"]["duplicate_rows"], 1)


if __name__ == "__main__":
    unittest.main()
