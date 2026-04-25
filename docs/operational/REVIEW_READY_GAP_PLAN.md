# Review-Ready Gap Plan

## Summary

- Current workbook readiness: `exploratory_only` / `UNSTABLE`
- Primary source-side blocker: `weak_source_cost_center_count=4`
- Recommended next path: `weak-source denominator policy cleanup`
- Reviewable without weakening persona standards: `True`
- Deck-ready remains out of scope for this pass.

## Readiness Blockers

- `weak_source_cost_center_count`: current `4`, reviewable threshold `<4`, deck-ready threshold `<2`, blocker type `hard_fail`
- `effective_balanced_source_count`: current `5.89`, reviewable threshold `>=5.0`, deck-ready threshold `>=6.0`, blocker type `warning`
- `persona_core_coverage_of_all_labeled_pct`: current `74.5`, reviewable threshold `70.0`, deck-ready threshold `80.0`, blocker type `warning`
- `weak_source_yield_status`: current `FAIL`, reviewable threshold `WARN_or_OK`, deck-ready threshold `OK`, blocker type `hard_fail`
- `source_diversity_status`: current `FAIL`, reviewable threshold `WARN_or_OK`, deck-ready threshold `OK`, blocker type `hard_fail`
- `overall_status`: current `FAIL`, reviewable threshold `WARN_or_OK`, deck-ready threshold `OK`, blocker type `hard_fail`
- `persona_readiness_gate_status`: current `FAIL`, reviewable threshold `WARN`, deck-ready threshold `OK`, blocker type `hard_fail`

## Weak Source Decisions

- `adobe_analytics_community` -> `parser_or_episode_fidelity_audit_needed` because `episode_yield` / `low_episode_yield` with blended influence `3.2`
- `domo_community_forum` -> `parser_or_episode_fidelity_audit_needed` because `episode_yield` / `low_episode_yield` with blended influence `3.5`
- `google_developer_forums` -> `fix_now_with_evidence` because `valid_filtering` / `low_valid_post_retention: missing_pain_signal` with blended influence `3.7`
- `klaviyo_community` -> `downgrade_to_exploratory_only` because `relevance_prefilter` / `low_prefilter_retention: klaviyo_community:generic` with blended influence `0.9`

## Scenario Simulation

- `A_current_baseline`: overall `FAIL`, readiness `exploratory_only`, weak sources `4`, core coverage `74.5`
- `B_downgrade_weakest_non_contributing_source_from_core_readiness`: overall `WARN`, readiness `reviewable_but_not_deck_ready`, weak sources `3`, core coverage `74.5`
- `C_exclude_true_cost_center_source_from_core_readiness_denominator`: overall `WARN`, readiness `reviewable_but_not_deck_ready`, weak sources `3`, core coverage `75.2`
- `D_fix_one_high_roi_weak_source`: overall `WARN`, readiness `reviewable_but_not_deck_ready`, weak sources `3`, core coverage `74.5`
- `E_strict_source_balance_with_exploratory_only_source_debt_separated`: overall `WARN`, readiness `reviewable_but_not_deck_ready`, weak sources `3`, core coverage `74.5`
- `F_no_op_baseline`: overall `FAIL`, readiness `exploratory_only`, weak sources `4`, core coverage `74.5`
