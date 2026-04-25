"""Tests for persona_04 identity preservation."""

from pathlib import Path
import pytest

from src.analysis.persona_04_identity_preservation import (
    trace_persona_04_identity,
    define_persona_04_identity_constraints,
    design_identity_preserving_variants,
    evaluate_variants,
    check_acceptance_criteria,
)


def test_trace_persona_04_identity():
    """Test tracing persona_04 identity."""
    root_dir = Path(__file__).resolve().parents[2]
    profile = trace_persona_04_identity(root_dir)
    assert "anchor_rows_count" in profile
    assert "cluster_signature" in profile


def test_define_constraints():
    """Test identity constraints definition."""
    constraints = define_persona_04_identity_constraints()
    assert constraints["persona_01_leakage_below_ceiling"] == 200


def test_design_variants():
    """Test variant designs."""
    variants = design_identity_preserving_variants()
    assert "A_pre_merge_guard" in variants
    assert "G_no_op_baseline" in variants


def test_evaluate_variants():
    """Test variant evaluation."""
    root_dir = Path(__file__).resolve().parents[2]
    variants = design_identity_preserving_variants()
    results = evaluate_variants(root_dir, variants)
    assert "G_no_op_baseline" in results
    assert results["G_no_op_baseline"]["persona_04_identity_overlap"] == 1.0


def test_check_acceptance_criteria():
    """Test acceptance criteria check."""
    root_dir = Path(__file__).resolve().parents[2]
    variants = design_identity_preserving_variants()
    results = evaluate_variants(root_dir, variants)
    eligible = check_acceptance_criteria(results)
    assert eligible["G_no_op_baseline"] is True
    assert eligible["A_pre_merge_guard"] is True  # Assuming it passes