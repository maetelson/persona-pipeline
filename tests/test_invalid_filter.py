"""Tests for source-aware invalid filtering."""

from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from src.filters.invalid_filter import activate_rule_mode, apply_invalid_filter
from src.utils.io import load_yaml


class InvalidFilterTests(unittest.TestCase):
    """Verify source-specific signal overrides do not leak across sources."""

    ROOT = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        rules = load_yaml(self.ROOT / "config" / "invalid_rules.yaml")
        self.rules = activate_rule_mode(rules, mode="analysis")


if __name__ == "__main__":
    unittest.main()
