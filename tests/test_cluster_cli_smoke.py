"""CLI smoke tests for bottleneck-first clustering commands."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ClusterCliSmokeTests(unittest.TestCase):
    """Verify the bottleneck clustering CLI runs end-to-end."""

    def test_dry_run_recluster_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/13_cluster_cli.py", "dry-run-recluster", "--mode", "bottleneck_first"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_name_clusters_writes_output(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/13_cluster_cli.py", "name-clusters", "--strategy", "bottleneck"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "cluster_naming_recommendations.csv").exists())


if __name__ == "__main__":
    unittest.main()
