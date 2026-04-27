# Deck-Ready Denominator Conservative Policy Implementation

## Scope

This pass implements the conservative Scenario H adjusted denominator metric only.
It does not replace the original coverage metric, does not change readiness logic, and does not alter persona counts.

## Conservative Exclusion Rule

A row is excluded from the adjusted deck-ready denominator only when all are true:

1. `persona_core_eligible = False`
2. `deck_ready_denominator_eligible = False`
3. `denominator_eligibility_category` is one explicit technical/support noise category
4. `technical_noise_confidence >= 0.9`
5. the row is not `ambiguous_review_bucket`
6. the row is not `denominator_eligible_business_non_core`

## Current Audited Values

- original_persona_core_coverage_pct: `74.5`
- adjusted_deck_ready_denominator_row_count: `11345`
- adjusted_deck_ready_denominator_excluded_row_count: `1329`
- adjusted_deck_ready_denominator_core_coverage_pct: `83.25`

## Guardrails

- the original `persona_core_coverage_of_all_labeled_pct` remains visible and unchanged
- adjusted coverage is added as a separate audited metric
- excluded rows remain visible in diagnostics and exclusion artifacts
- ambiguous rows remain included
- no readiness state or persona count changes occur in this pass
