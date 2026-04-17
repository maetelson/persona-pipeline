"""CLI smoke tests for persona messaging commands."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PersonaCliSmokeTests(unittest.TestCase):
    """Verify persona messaging CLI writes expected artifacts."""

    def test_generate_persona_cards_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/cli/14_persona_cli.py", "generate-persona-cards"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "persona_cards_v2.csv").exists())

    def test_audit_persona_naming_completes(self) -> None:
        result = subprocess.run(
            [sys.executable, "run/cli/14_persona_cli.py", "audit-persona-naming"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertTrue((ROOT / "data" / "analysis" / "naming_audit.csv").exists())


if __name__ == "__main__":
    unittest.main()
