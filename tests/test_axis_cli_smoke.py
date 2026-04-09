"""CLI smoke tests for axis reduction commands."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


class AxisCliSmokeTests(unittest.TestCase):
    """Verify the axis CLI completes and writes outputs."""

    def test_dry_run_axis_reduction_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/11_axis_cli.py", "dry-run-axis-reduction"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "axis_recommendations.csv").exists())

    def test_export_axis_samples_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/11_axis_cli.py", "export-axis-samples", "--axis", "workflow_stage", "--limit", "5"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "axis_samples" / "workflow_stage.csv").exists())


if __name__ == "__main__":
    unittest.main()
