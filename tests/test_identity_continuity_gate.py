"""Tests for identity continuity gate."""

from pathlib import Path
import pytest

from src.analysis.identity_continuity_gate import evaluate_identity_continuity_gate


def test_identity_continuity_gate_baseline_passes():
    """Test that baseline passes the gate."""
    root_dir = Path(__file__).resolve().parents[2]
    results = evaluate_identity_continuity_gate(root_dir)
    assert results["results"]["baseline"]["gate_pass"] is True


def test_identity_continuity_gate_variant_b_fails():
    """Test that variant B fails due to leakage."""
    root_dir = Path(__file__).resolve().parents[2]
    results = evaluate_identity_continuity_gate(root_dir)
    assert results["results"]["variant_B"]["gate_pass"] is False
    assert results["results"]["variant_B"]["persona_01_parent_leakage"] == 191


def test_identity_continuity_gate_no_eligible_variants():
    """Test that no variants are eligible."""
    root_dir = Path(__file__).resolve().parents[2]
    results = evaluate_identity_continuity_gate(root_dir)
    assert len(results["eligible_variants"]) == 0
    assert "not implementation-safe" in results["recommendation"]


def test_identity_change_type_stable():
    """Test identity change type for stable."""
    from src.analysis.identity_continuity_gate import _determine_identity_change_type
    assert _determine_identity_change_type({"persona_04": "persona_04"}, "persona_04", "persona_04", 1.0) == "stable"


def test_identity_change_type_renumbered():
    """Test identity change type for renumbered."""
    from src.analysis.identity_continuity_gate import _determine_identity_change_type
    assert _determine_identity_change_type({"persona_04": "persona_03"}, "persona_04", "persona_03", 0.85) == "renumbered"


def test_identity_change_type_drift():
    """Test identity change type for semantic drift."""
    from src.analysis.identity_continuity_gate import _determine_identity_change_type
    assert _determine_identity_change_type({"persona_04": "persona_03"}, "persona_04", "persona_03", 0.5) == "semantic_drift"