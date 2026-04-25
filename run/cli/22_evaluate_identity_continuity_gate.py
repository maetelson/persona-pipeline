"""Evaluate the identity-continuity gate for reconciliation/signoff target clusters."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.identity_continuity_gate import evaluate_identity_continuity_gate


def main() -> None:
    """Evaluate identity continuity gate and print results."""
    results = evaluate_identity_continuity_gate(ROOT_DIR)
    print("Identity Continuity Gate Results")
    print("=" * 40)
    print(f"Gate Purpose: {results['gate_definition']['purpose']}")
    print(f"Checks: {', '.join(results['gate_definition']['checks'])}")
    print(f"Pass Criteria: {results['gate_definition']['pass_criteria']}")
    print("\nResults:")
    for variant, res in results["results"].items():
        print(f"  {variant}: Pass={res['gate_pass']}, Jaccard={res['jaccard_overlap']}, Leakage={res['persona_01_parent_leakage']}")
    print(f"\nEligible Variants: {results['eligible_variants']}")
    print(f"Recommendation: {results['recommendation']}")


if __name__ == "__main__":
    main()