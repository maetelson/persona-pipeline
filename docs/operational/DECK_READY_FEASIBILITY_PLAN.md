# Deck-Ready Feasibility Plan

## Summary

- Current workbook readiness: `reviewable_but_not_deck_ready` / `EXPLORATORY`
- Effective source balance: `5.89`
- Persona core coverage: `74.5`
- Feasibility decision: `deck_ready_feasible_but_requires_large_data_quality_work`
- Recommended next path: `stop and freeze as reviewable release`

## Source Balance Gap

- Gap to deck-ready source-balance floor: `0.11`
- Main imbalance driver: `healthy_high_volume_sources`

## Core Coverage Gap

- Rows needed to reach 75.0: `62`
- Rows needed to reach 80.0: `696`
- Current non-core labeled rows: `3230`

## Scenario Simulation

- `A_no_op_current_baseline`: status `WARN`, readiness `reviewable_but_not_deck_ready`, deck-ready candidate `False`, source balance `5.89`, core coverage `74.5`, junk risk `none`
- `B_high_quality_recovery_of_60_persona_core_rows`: status `WARN`, readiness `reviewable_but_not_deck_ready`, deck-ready candidate `False`, source balance `5.89`, core coverage `75.0`, junk risk `low`
- `C_high_quality_recovery_to_80_0`: status `WARN`, readiness `reviewable_but_not_deck_ready`, deck-ready candidate `False`, source balance `5.89`, core coverage `80.0`, junk risk `high`
- `D_source_volume_normalization_only`: status `WARN`, readiness `reviewable_but_not_deck_ready`, deck-ready candidate `False`, source balance `6.0`, core coverage `74.5`, junk risk `none`
- `E_remove_remaining_true_weak_source_debt_from_deck_ready_denominator`: status `WARN`, readiness `reviewable_but_not_deck_ready`, deck-ready candidate `False`, source balance `5.89`, core coverage `74.5`, junk risk `policy_only`
- `F_combined_realistic_one_source_win_plus_modest_balance_improvement`: status `WARN`, readiness `reviewable_but_not_deck_ready`, deck-ready candidate `False`, source balance `6.0`, core coverage `75.0`, junk risk `medium`
- `G_aggressive_hit_deck_ready_thresholds`: status `OK`, readiness `deck_ready`, deck-ready candidate `True`, source balance `6.0`, core coverage `80.0`, junk risk `high`
