# Deck-Ready Denominator Eligibility Phase 1

## Purpose

Phase 1 adds diagnostics-only row-level fields for future denominator policy
work. It does not change current readiness behavior, coverage metrics, persona
counts, or workbook status.

## What Phase 1 Adds

Artifacts only:

- `artifacts/readiness/deck_ready_denominator_eligibility_rows.csv`
- `artifacts/readiness/deck_ready_denominator_eligibility_summary.json`

Row-level diagnostics fields:

- `deck_ready_denominator_eligible`
- `denominator_eligibility_category`
- `denominator_exclusion_reason`
- `technical_noise_confidence`
- `business_context_signal_count`
- `technical_noise_signal_count`
- `source_specific_noise_signal_count`
- `ambiguity_flag`

## What Phase 1 Does Not Change

- no clustering changes
- no labeling changes
- no persona assignment changes
- no promotion or readiness logic changes
- no denominator ablation
- no adjusted coverage metric
- no raw or labeled row deletion
- no workbook presentation change

## Interpretation

Phase 1 is an audit layer only. Excluded rows in the diagnostics classification
still remain visible in retained artifacts and continue to count in the current
official denominator until a later audited implementation phase is approved.
