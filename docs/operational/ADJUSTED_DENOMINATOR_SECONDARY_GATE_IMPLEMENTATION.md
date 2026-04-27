# Adjusted Denominator Secondary Gate Implementation

## Scope

This pass wires the audited adjusted conservative denominator metric into the coverage component as a secondary gate only.
It does not replace the original coverage metric, does not hide excluded rows, and does not change non-coverage readiness gates.

## Secondary Gate Logic

Coverage may pass by the adjusted metric only when all are true:

1. `adjusted_denominator_metric_status = audited`
2. `denominator_policy_mode = conservative_high_confidence_noise_only`
3. `denominator_policy_version = v1`
4. `adjusted_deck_ready_denominator_core_coverage_pct >= 80.0`
5. `ambiguous_review_bucket` remains included
6. `denominator_eligible_business_non_core` remains included
7. excluded rows remain diagnostics-visible

## Current Values

- original coverage: `74.5`
- original_persona_core_coverage_pct: `74.5`
- adjusted coverage: `83.25`
- coverage_gate_metric_used: `adjusted_deck_ready_denominator_core_coverage_pct_secondary_gate`
- original_coverage_gate_status: `fail`
- adjusted_coverage_gate_status: `pass`
- coverage_gate_passed_by_adjusted_metric: `True`

## Invariants

- persona_readiness_state remains `reviewable_but_not_deck_ready`
- overall_status remains `WARN`
- quality_flag remains `EXPLORATORY`
- final_usable_persona_count remains `3`
- production_ready_persona_count remains `3`
- review_ready_persona_count remains `1`
- deck_ready_claim_eligible_persona_count remains `4`
