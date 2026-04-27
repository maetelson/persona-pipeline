# Refined Source Balance And Weak Debt Simulation

## Official Baseline

- official effective balance: `5.89`
- largest source share: `30.7`
- top 3 source share: `61.7`
- weak-source count: `4`
- core-readiness weak-source count: `3`

## Refined Source Balance Scenarios

- `A_official_baseline`: balance `5.89`, included `12674`, excluded `0`, crosses 6.0 `False`
- `B_refined_evidence_only`: balance `5.27`, included `9435`, excluded `3239`, crosses 6.0 `False`
- `C_refined_evidence_plus_mixed`: balance `6.0`, included `10349`, excluded `2325`, crosses 6.0 `True`
- `D_refined_debt_quarantined`: balance `6.0`, included `10349`, excluded `2325`, crosses 6.0 `True`
- `E_refined_evidence_plus_mixed_excluding_excluded_tier`: balance `5.91`, included `10270`, excluded `2404`, crosses 6.0 `False`
- `F_refined_conservative_balance_eligibility`: balance `5.19`, included `9356`, excluded `3318`, crosses 6.0 `False`
- `G_refined_weak_source_evidence_only_rescue`: balance `5.76`, included `10665`, excluded `2009`, crosses 6.0 `False`

## Refined Weak-Source Debt Scenarios

- `A_official_source_level`: weak `4`, core-readiness weak `3`, still weak `adobe_analytics_community | domo_community_forum | google_developer_forums | klaviyo_community`
- `B_denominator_eligible_rows_only`: weak `3`, core-readiness weak `3`, still weak `adobe_analytics_community | domo_community_forum | google_developer_forums`
- `C_high_conf_technical_noise_excluded`: weak `3`, core-readiness weak `3`, still weak `adobe_analytics_community | domo_community_forum | google_developer_forums`
- `D_refined_debt_producing_slices_quarantined`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`
- `E_refined_evidence_plus_mixed_only`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`
- `F_refined_evidence_producing_only`: weak `0`, core-readiness weak `0`, still weak `none`
- `G_refined_source_tier_aware_policy`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`

## Google And Adobe

- `google_developer_forums`: still weak `True`, evidence share `0.5708`, mixed share `0.4292`, interpretation `slice_aware_mixed`
- `adobe_analytics_community`: still weak `True`, evidence share `0.2609`, mixed share `0.7391`, interpretation `slice_aware_mixed`

## Comparison To Previous Simulations

- previous debt-quarantine balance: `6.04`
- refined debt-quarantine balance: `6.0`
- previous debt-quarantine weak count: `2`
- refined debt-quarantine weak count: `2`

## Recommendation

- next action: `run_refined_slice_precision_review`
- reason: Refined diagnostics materially improve interpretability, but Google and Adobe still remain slice-aware mixed under refined debt quarantine. A post-refinement precision review is the safest next step before any secondary metric spec.
