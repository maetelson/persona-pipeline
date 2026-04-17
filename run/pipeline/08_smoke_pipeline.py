"""Run a bounded smoke pipeline and report per-source coverage."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.utils.io import read_parquet
from src.utils.run_helpers import load_dotenv
from src.utils.source_registry import load_source_definitions

DEFAULT_SOURCE_FILTER = (
    "reddit,stackoverflow,github_discussions,"
    "reddit_r_excel,reddit_analytics,reddit_business_intelligence,reddit_marketing_analytics,"
    "shopify_community,hubspot_community,klaviyo_community"
)

DEFAULT_LIMIT_ENV = {
    "COLLECT_SOURCE_FILTER": DEFAULT_SOURCE_FILTER,
    "COLLECT_MAX_QUERIES_PER_SOURCE": "1",
    "COLLECT_MAX_PAGES_PER_QUERY": "1",
    "REDDIT_SEARCH_LIMIT": "1",
    "REDDIT_COMMENT_LIMIT": "1",
    "STACKOVERFLOW_PAGE_SIZE": "1",
    "STACKOVERFLOW_MAX_ANSWERS": "1",
    "STACKOVERFLOW_MAX_COMMENTS": "1",
    "GITHUB_ISSUES_PER_QUERY": "1",
    "GITHUB_COMMENTS_PER_ITEM": "1",
    "GITHUB_DISCUSSION_REPLIES_PER_COMMENT": "1",
    "REDDIT_USER_AGENT": "persona-pipeline/0.1 smoke-test",
    "PUBLIC_WEB_USER_AGENT": "persona-pipeline/0.1 smoke-test",
    "ENABLE_LLM_LABELER": "false",
    "LLM_DRY_RUN": "true",
    "ENABLE_BATCH_LABELING": "false",
    "PRESERVE_RAW_ON_EMPTY": "true",
    "BUSINESS_COMMUNITY_MAX_THREADS": "1",
    "BUSINESS_COMMUNITY_MAX_DISCOVERY_PER_URL": "2",
}

PIPELINE_STEPS = [
    "01_collect_all.py",
    "02_normalize_all.py",
    "02.5_filter_time_window.py",
    "03_filter_valid.py",
    "03_5_prefilter_relevance.py",
    "04_build_episodes.py",
    "05_label_episodes.py",
]


def main() -> None:
    """Run the bounded smoke pipeline and write analysis artifacts."""
    args = _parse_args()
    load_dotenv(ROOT / ".env")
    env = _smoke_env(args)
    started = time.monotonic()
    step_rows: list[dict[str, object]] = []
    raw_backup = _backup_raw_files() if args.run_collect else []

    steps = PIPELINE_STEPS if args.run_collect else PIPELINE_STEPS[1:]
    for step in steps:
        remaining = max(1, int(args.total_timeout_seconds - (time.monotonic() - started)))
        timeout = min(args.stage_timeout_seconds, remaining)
        row = _run_step(step, timeout, env)
        step_rows.append(row)
        if row["status"] != "ok" or (time.monotonic() - started) >= args.total_timeout_seconds:
            break

    source_rows = _build_source_rows()
    if raw_backup:
        _restore_raw_files(raw_backup)
    total_seconds = round(time.monotonic() - started, 2)
    _write_reports(source_rows, step_rows, total_seconds, args)

    failed_steps = [row for row in step_rows if row["status"] != "ok"]
    min1_failures = [row for row in source_rows if int(row["raw_count"]) < 1]
    if failed_steps or (args.require_min1 and min1_failures):
        raise SystemExit(1)


def _parse_args() -> argparse.Namespace:
    """Parse smoke pipeline arguments."""
    parser = argparse.ArgumentParser(description="Run a bounded local smoke pipeline.")
    parser.add_argument("--run-collect", action="store_true", help="Run live collection. This can overwrite data/raw source files.")
    parser.add_argument("--stage-timeout-seconds", type=int, default=90, help="Timeout per pipeline step.")
    parser.add_argument("--total-timeout-seconds", type=int, default=600, help="Total smoke run budget.")
    parser.add_argument("--source-filter", default="", help="Comma-separated source IDs; defaults to representative enabled sources.")
    parser.add_argument("--require-min1", action="store_true", help="Exit non-zero when any enabled target has zero raw rows.")
    return parser.parse_args()


def _smoke_env(args: argparse.Namespace) -> dict[str, str]:
    """Return environment variables for bounded smoke execution."""
    env = os.environ.copy()
    for key, value in DEFAULT_LIMIT_ENV.items():
        env.setdefault(key, value)
    if args.source_filter.strip():
        env["COLLECT_SOURCE_FILTER"] = args.source_filter.strip()
    return env


def _backup_raw_files() -> list[dict[str, Path | bool]]:
    """Backup current raw JSONL files so smoke collection cannot erase corpus state."""
    backup_dir = ROOT / "data" / "analysis" / "smoke_raw_backup"
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    backups: list[dict[str, Path | bool]] = []
    for source in _load_enabled_source_rows():
        raw_path = ROOT / "data" / "raw" / str(source["source_id"]) / "raw.jsonl"
        backup_path = backup_dir / f"{source['source_id']}.raw.jsonl"
        existed = raw_path.exists()
        if existed:
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(raw_path, backup_path)
        backups.append({"raw_path": raw_path, "backup_path": backup_path, "existed": existed})
    return backups


def _restore_raw_files(backups: list[dict[str, Path | bool]]) -> None:
    """Restore raw JSONL files after a live smoke collection."""
    for row in backups:
        raw_path = Path(row["raw_path"])
        backup_path = Path(row["backup_path"])
        existed = bool(row["existed"])
        if existed and backup_path.exists():
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup_path, raw_path)
        elif raw_path.exists():
            raw_path.unlink()


def _run_step(step: str, timeout_seconds: int, env: dict[str, str]) -> dict[str, object]:
    """Run one pipeline step with a timeout."""
    started = time.monotonic()
    command = [sys.executable, str(ROOT / "run" / "pipeline" / step)]
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "step": step,
            "status": "timeout",
            "returncode": "",
            "duration_seconds": round(time.monotonic() - started, 2),
            "stdout_tail": _tail(exc.stdout),
            "stderr_tail": _tail(exc.stderr),
        }
    return {
        "step": step,
        "status": "ok" if completed.returncode == 0 else "error",
        "returncode": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 2),
        "stdout_tail": _tail(completed.stdout),
        "stderr_tail": _tail(completed.stderr),
    }


def _tail(value: str | bytes | None, max_chars: int = 1500) -> str:
    """Return a compact tail of command output."""
    if value is None:
        return ""
    text = value.decode(errors="replace") if isinstance(value, bytes) else str(value)
    return text[-max_chars:].replace("\r", "")


def _line_count(path: Path) -> int:
    """Count JSONL rows, returning zero for missing files."""
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _counts_by_source(path: Path) -> dict[str, int]:
    """Return source counts from a parquet file when available."""
    if not path.exists():
        return {}
    frame = read_parquet(path)
    if frame.empty or "source" not in frame.columns:
        return {}
    return frame["source"].fillna("").astype(str).value_counts().to_dict()


def _labeled_counts_by_source() -> dict[str, int]:
    """Return labeled counts by joining labels back to episodes."""
    labeled_path = ROOT / "data" / "labeled" / "labeled_episodes.parquet"
    episode_path = ROOT / "data" / "episodes" / "episode_table.parquet"
    if not labeled_path.exists() or not episode_path.exists():
        return {}
    labeled_df = read_parquet(labeled_path)
    episodes_df = read_parquet(episode_path)
    if labeled_df.empty or episodes_df.empty or "episode_id" not in labeled_df.columns:
        return {}
    source_lookup = episodes_df[["episode_id", "source"]].drop_duplicates("episode_id")
    joined = labeled_df.merge(source_lookup, on="episode_id", how="left")
    return joined["source"].fillna("").astype(str).value_counts().to_dict()


def _build_source_rows() -> list[dict[str, object]]:
    """Build per-source smoke counts from current pipeline outputs."""
    definitions = _load_enabled_source_rows()
    normalized_counts = _counts_by_source(ROOT / "data" / "normalized" / "normalized_posts.parquet")
    valid_counts = _counts_by_source(ROOT / "data" / "valid" / "valid_candidates.parquet")
    episode_counts = _counts_by_source(ROOT / "data" / "episodes" / "episode_table.parquet")
    labeled_counts = _labeled_counts_by_source()
    rows: list[dict[str, object]] = []
    for definition in definitions:
        raw_path = ROOT / "data" / "raw" / definition["source_id"] / "raw.jsonl"
        raw_count = _line_count(raw_path)
        rows.append(
            {
                "source_id": definition["source_id"],
                "source_group": definition["source_group"],
                "collector_kind": definition["collector_kind"],
                "raw_count": raw_count,
                "normalized_count": int(normalized_counts.get(definition["source_id"], 0)),
                "valid_count": int(valid_counts.get(definition["source_id"], 0)),
                "episode_count": int(episode_counts.get(definition["source_id"], 0)),
                "labeled_count": int(labeled_counts.get(definition["source_id"], 0)),
                "pass_min1_raw": raw_count >= 1,
                "raw_path": str(raw_path),
            }
        )
    return sorted(rows, key=lambda row: str(row["source_id"]))


def _load_enabled_source_rows() -> list[dict[str, str]]:
    """Load enabled source metadata, including legacy configs not in the registry helper."""
    rows: dict[str, dict[str, str]] = {}
    for definition in load_source_definitions(ROOT, include_disabled=False):
        rows[definition.source_id] = {
            "source_id": definition.source_id,
            "source_group": definition.source_group,
            "collector_kind": definition.collector_kind,
        }
    for path in sorted((ROOT / "config" / "sources").glob("*.yaml")):
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not payload.get("enabled", True):
            continue
        source_id = str(payload.get("source_id") or payload.get("source") or path.stem)
        rows.setdefault(
            source_id,
            {
                "source_id": source_id,
                "source_group": str(payload.get("source_group", "existing_forums")),
                "collector_kind": str(payload.get("collector_kind", source_id)),
            },
        )
    return list(rows.values())


def _write_reports(
    source_rows: list[dict[str, object]],
    step_rows: list[dict[str, object]],
    total_seconds: float,
    args: argparse.Namespace,
) -> None:
    """Write smoke CSV and markdown reports."""
    analysis_dir = ROOT / "data" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    source_path = analysis_dir / "smoke_pipeline_source_counts.csv"
    step_path = analysis_dir / "smoke_pipeline_steps.csv"
    report_path = analysis_dir / "smoke_pipeline_report.md"
    pd.DataFrame(source_rows).to_csv(source_path, index=False)
    pd.DataFrame(step_rows).to_csv(step_path, index=False)

    min1_pass = sum(1 for row in source_rows if bool(row["pass_min1_raw"]))
    failed_sources = [str(row["source_id"]) for row in source_rows if not bool(row["pass_min1_raw"])]
    failed_steps = [row for row in step_rows if row["status"] != "ok"]
    lines = [
        "# Smoke Pipeline Report",
        "",
        f"- Total duration seconds: `{total_seconds}`",
        f"- Run collect: `{args.run_collect}`",
        f"- Source min-1 raw pass: `{min1_pass}/{len(source_rows)}`",
        f"- Step status: `{'pass' if not failed_steps else 'fail'}`",
        "",
        "## Steps",
        "",
        "| step | status | seconds | returncode |",
        "|---|---|---:|---:|",
    ]
    for row in step_rows:
        lines.append(f"| `{row['step']}` | `{row['status']}` | {row['duration_seconds']} | {row['returncode']} |")
    lines.extend(["", "## Sources", "", "| source_id | raw | normalized | valid | episodes | labeled | min1 |", "|---|---:|---:|---:|---:|---:|---|"])
    for row in source_rows:
        lines.append(
            f"| `{row['source_id']}` | {row['raw_count']} | {row['normalized_count']} | "
            f"{row['valid_count']} | {row['episode_count']} | {row['labeled_count']} | `{row['pass_min1_raw']}` |"
        )
    lines.extend(["", "## Verdict", ""])
    if failed_steps:
        lines.append("`FAIL`: at least one bounded pipeline step failed or timed out.")
    elif failed_sources:
        lines.append("`FAIL`: pipeline completed, but some sources still have zero raw rows.")
        lines.append("")
        lines.append(f"Zero-row sources: `{', '.join(failed_sources)}`")
    else:
        lines.append("`PASS`: bounded pipeline completed and every enabled source has at least one raw row.")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
