# Deck-Ready Denominator Eligibility Audit

## Baseline

- total labeled rows: `12674`
- persona-core rows: `9445`
- non-core labeled rows: `3229`
- current persona_core_coverage_of_all_labeled_pct: `74.5`
- rows needed to reach 75.0: `61`
- rows needed to reach 80.0: `695`
- minimum denominator rows that would need to be excluded to reach 80.0 with current persona-core rows: `868`

## Taxonomy

- `persona_core_evidence`
- `denominator_eligible_business_non_core`
- `technical_support_debug_noise`
- `source_specific_support_noise`
- `setup_auth_permission_noise`
- `api_sdk_debug_noise`
- `server_deploy_config_noise`
- `syntax_formula_debug_noise`
- `vendor_announcement_or_feature_request_only`
- `career_training_certification_noise`
- `generic_low_signal`
- `ambiguous_review_bucket`

## Live Classification Summary

- `persona_core_evidence = 9445`
- `server_deploy_config_noise = 1263`
- `ambiguous_review_bucket = 1082`
- `syntax_formula_debug_noise = 305`
- `denominator_eligible_business_non_core = 223`
- `api_sdk_debug_noise = 136`
- `source_specific_support_noise = 111`
- `setup_auth_permission_noise = 107`
- `career_training_certification_noise = 2`

## Ablation Scenarios

| scenario | adjusted denominator | adjusted coverage | gap to 80 | denom effective source count | methodology risk | plausible |
|---|---:|---:|---:|---:|---|---|
| `A_baseline_all_labeled_denominator` | 12674 | 74.52 | 5.48 | 7.38 | none | baseline_not_plausible |
| `B_exclude_clear_technical_support_debug_only` | 12674 | 74.52 | 5.48 | 7.38 | low | not_plausible_no_isolated_bucket_yet |
| `C_exclude_technical_support_setup_api_server_syntax_noise` | 10863 | 86.95 | 0.00 | 6.51 | moderate | numerically_plausible_but_broad |
| `D_exclude_all_clear_non_business_non_core_noise` | 10750 | 87.86 | 0.00 | 6.43 | moderate | numerically_plausible_but_broad |
| `E_exclude_clear_noise_keep_ambiguous_rows` | 10750 | 87.86 | 0.00 | 6.43 | lower_than_D_because_ambiguous_retained | plausible_if_clear_noise_precision_holds |
| `F_exclude_clear_noise_and_ambiguous_rows` | 9668 | 97.69 | 0.00 | 5.53 | high | numerically_high_but_methodologically_not_safe |
| `G_source_tier_aware_non_core_exploratory_excluded_and_clear_noise` | 10413 | 90.70 | 0.00 | 6.10 | high | too_source_selective_for_first_policy |
| `H_conservative_high_confidence_technical_support_noise_only` | 11345 | 83.25 | 0.00 | 6.74 | low_to_moderate | best_first_policy_candidate |

## Sample Review

- sample size: `150`
- `safe_to_exclude_from_deck_ready_denominator = 56`
- `ambiguous_review_needed = 53`
- `actually_business_relevant = 20`
- `actually_persona_core_candidate = 20`
- `keep_in_denominator = 1`
- high-confidence exclusion precision: `100.0%`
- medium-confidence exclusion precision: `20.0%`
- ambiguous bucket still needing review: `96.7%`

## Decision

- recommended next action: `implement_conservative_denominator_eligibility_policy`
- conservative first policy candidate: `Scenario H: exclude only explicit high-confidence technical/support noise (technical_noise_confidence >= 0.9) while retaining ambiguous_review_bucket and denominator_eligible_business_non_core rows.`
- expected adjusted coverage under conservative first policy: `83.25`
- denominator ablation is still an audit result only in this pass; no production readiness metric has been changed.

## Guardrails

- keep raw and labeled rows intact
- keep excluded rows visible in diagnostics by source, source tier, and exclusion reason
- publish original and adjusted denominator metrics side by side if implementation is approved later
- keep ambiguous_review_bucket in the denominator for the first conservative implementation
- do not allow denominator eligibility alone to change persona counts or readiness semantics
