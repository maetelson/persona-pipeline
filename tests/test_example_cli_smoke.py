"""CLI smoke tests for representative example selection."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ExampleCliSmokeTests(unittest.TestCase):
    """Verify the example-selection CLI runs end-to-end."""

    def test_select_representative_examples_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/cli/12_example_cli.py", "select-representative-examples"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "representative_examples_v2.csv").exists())

    def test_compare_example_selection_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/cli/12_example_cli.py", "compare-example-selection"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "example_selection_comparison.csv").exists())


if __name__ == "__main__":
    unittest.main()
