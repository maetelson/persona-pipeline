"""Run a bounded incremental expansion pilot for HubSpot Community and Reddit."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.hubspot_reddit_expansion_pilot import (
    DEDUP_SUMMARY_JSON,
    NEW_UNIQUE_CSV,
    PILOT_READINESS_JSON,
    PILOT_REPORT_MD,
    RAW_JSONL,
    SAMPLE_CSV,
    SUMMARY_JSON,
    DUPLICATES_CSV,
    baseline_metrics,
    build_existing_dedupe_index,
    build_pilot_row,
    build_summary,
    classify_duplicate,
    is_pilot_output_path,
    load_pilot_source_configs,
    register_pilot_keys,
    render_report,
    run_source_collectors,
)
from src.utils.io import ensure_dir, write_jsonl


def main() -> None:
    """Run the bounded pilot and write artifacts outside production data."""
    root = ROOT
    for path in [DEDUP_SUMMARY_JSON, RAW_JSONL, NEW_UNIQUE_CSV, DUPLICATES_CSV, SUMMARY_JSON, SAMPLE_CSV, PILOT_READINESS_JSON, PILOT_REPORT_MD]:
        if not is_pilot_output_path(root / path, root):
            raise RuntimeError(f"Refusing to write outside pilot artifact roots: {path}")

    baseline = baseline_metrics(root)
    dedupe_indices: dict[str, dict[str, set[str]]] = {}
    dedupe_summaries: dict[str, dict[str, object]] = {}
    for source in ["hubspot_community", "reddit"]:
        dedupe_indices[source], dedupe_summaries[source] = build_existing_dedupe_index(root, source)
    ensure_dir((root / DEDUP_SUMMARY_JSON).parent)
    (root / DEDUP_SUMMARY_JSON).write_text(json.dumps(dedupe_summaries, indent=2, ensure_ascii=False), encoding="utf-8")

    fetched_records = run_source_collectors(root)
    seen_pilot = {source: {"raw_id": set(), "url": set(), "canonical_url": set(), "title": set(), "content_hash": set()} for source in fetched_records}
    scored_rows = []
    for source, records in fetched_records.items():
        for record in records:
            dedupe_status, duplicate_against = classify_duplicate(record, dedupe_indices[source], seen_pilot[source])
            if dedupe_status == "new_unique":
                register_pilot_keys(record, seen_pilot[source])
            scored_rows.append(build_pilot_row(record, dedupe_status, duplicate_against))

    write_jsonl(root / RAW_JSONL, [row.to_dict() for row in scored_rows])
    df = pd.DataFrame([row.to_dict() for row in scored_rows])
    new_unique_df = df[df["dedupe_status"] == "new_unique"].copy()
    duplicate_df = df[df["dedupe_status"] != "new_unique"].copy()
    new_unique_df.to_csv(root / NEW_UNIQUE_CSV, index=False, encoding="utf-8-sig")
    duplicate_df.to_csv(root / DUPLICATES_CSV, index=False, encoding="utf-8-sig")
    new_unique_df.head(100).to_csv(root / SAMPLE_CSV, index=False, encoding="utf-8-sig")

    summary = build_summary(root, baseline, scored_rows, dedupe_summaries)
    (root / SUMMARY_JSON).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    (root / PILOT_READINESS_JSON).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    (root / PILOT_REPORT_MD).write_text(render_report(summary), encoding="utf-8")
    print(json.dumps(summary["pilot_gate_result"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
