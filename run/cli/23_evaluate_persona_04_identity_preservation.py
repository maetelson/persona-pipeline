"""Evaluate persona_04 identity-preserving simulation variants."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.analysis.persona_04_identity_preservation import (
    trace_persona_04_identity,
    define_persona_04_identity_constraints,
    design_identity_preserving_variants,
    evaluate_variants,
    check_acceptance_criteria,
)


def main() -> None:
    """Evaluate persona_04 identity-preserving variants."""
    identity_profile = trace_persona_04_identity(ROOT_DIR)
    constraints = define_persona_04_identity_constraints()
    variants = design_identity_preserving_variants()
    results = evaluate_variants(ROOT_DIR, variants)
    eligible = check_acceptance_criteria(results)

    print("Persona_04 Baseline Identity Profile")
    print("=" * 40)
    for key, value in identity_profile.items():
        print(f"{key}: {value}")

    print("\nIdentity Constraints")
    print("=" * 20)
    for key, value in constraints.items():
        print(f"{key}: {value}")

    print("\nSimulation Variants")
    print("=" * 20)
    for name, config in variants.items():
        print(f"{name}: {config['description']}")

    print("\nGate Results")
    print("=" * 12)
    for variant, res in results.items():
        print(f"{variant}: Eligible={eligible[variant]}, Identity Overlap={res['persona_04_identity_overlap']}, Leakage={res['persona_01_leakage']}")

    eligible_variants = [v for v, e in eligible.items() if e]
    if eligible_variants:
        print(f"\nEligible Variants: {', '.join(eligible_variants)}")
        recommendation = f"Variants {', '.join(eligible_variants)} are eligible for future production implementation."
    else:
        recommendation = (
            "No identity-preserving variants pass acceptance criteria. Clustering-level patch is not currently safe. "
            "Recommend next structural alternatives: manual gold curation expansion, source-volume normalization, "
            "workbook policy redesign, or persona taxonomy redesign."
        )
    print(f"Recommendation: {recommendation}")


if __name__ == "__main__":
    main()