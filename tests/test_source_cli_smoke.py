"""CLI smoke tests for the source-group runner."""

from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


class SourceCliSmokeTests(unittest.TestCase):
    """Verify the CLI can run in dry-run and manual-ingest modes."""

    def test_dry_run_cli_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/10_source_cli.py", "dry-run", "--source-group", "review_sites"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "source_cli_dry_run.csv").exists())

    def test_ingest_manual_cli_completes_for_g2(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            shutil.copy(FIXTURES / "review_page.html", temp_path / "review_page.html")
            result = subprocess.run(
                [sys.executable, "run/10_source_cli.py", "ingest-manual", "--source", "g2", "--input-dir", str(temp_path)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue((ROOT / "data" / "raw" / "g2" / "raw.jsonl").exists())
            self.assertTrue((ROOT / "data" / "normalized" / "g2.parquet").exists())

    def test_prefilter_and_qa_relevance_cli_for_existing_forums(self) -> None:
        normalized_path = ROOT / "data" / "normalized" / "normalized_posts.parquet"
        valid_path = ROOT / "data" / "valid" / "valid_candidates.parquet"
        invalid_path = ROOT / "data" / "valid" / "invalid_candidates.parquet"

        backups: dict[Path, bytes | None] = {}
        for path in [normalized_path, valid_path, invalid_path]:
            backups[path] = path.read_bytes() if path.exists() else None

        try:
            rows = [
                json.loads((FIXTURES / "reddit_relevant_excel_reporting.json").read_text(encoding="utf-8")),
                json.loads((FIXTURES / "stackoverflow_irrelevant_debugging.json").read_text(encoding="utf-8")),
            ]
            df = pd.DataFrame(rows)
            normalized_path.parent.mkdir(parents=True, exist_ok=True)
            valid_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(normalized_path, index=False)
            df.to_parquet(valid_path, index=False)
            pd.DataFrame(columns=df.columns).to_parquet(invalid_path, index=False)

            prefilter = subprocess.run(
                [sys.executable, "run/10_source_cli.py", "prefilter", "--source-group", "existing_forums", "--export-borderline", "--limit", "20"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(prefilter.returncode, 0, msg=prefilter.stderr)

            qa = subprocess.run(
                [sys.executable, "run/10_source_cli.py", "qa-relevance", "--source", "reddit", "--limit", "20"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(qa.returncode, 0, msg=qa.stderr)
            self.assertTrue((ROOT / "data" / "analysis" / "prefilter_source_ratio_report.csv").exists())
        finally:
            for path, payload in backups.items():
                if payload is None:
                    if path.exists():
                        path.unlink()
                else:
                    path.write_bytes(payload)


if __name__ == "__main__":
    unittest.main()
