# Slice-Aware Weak-Source Debt Simulation

## Official Baseline

- official weak-source cost center count: `4`
- official core-readiness weak-source count: `3`
- official weak sources: `adobe_analytics_community | domo_community_forum | google_developer_forums | klaviyo_community`
- official core-readiness weak sources: `adobe_analytics_community | domo_community_forum | google_developer_forums`

## Scenario Summary

- `A_official_source_level`: weak `4`, core-readiness weak `3`, still weak `adobe_analytics_community | domo_community_forum | google_developer_forums | klaviyo_community`, no longer weak `none`
- `B_denominator_eligible_rows_only`: weak `3`, core-readiness weak `3`, still weak `adobe_analytics_community | domo_community_forum | google_developer_forums`, no longer weak `klaviyo_community`
- `C_high_conf_technical_noise_excluded`: weak `3`, core-readiness weak `3`, still weak `adobe_analytics_community | domo_community_forum | google_developer_forums`, no longer weak `klaviyo_community`
- `D_debt_producing_slices_quarantined`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`, no longer weak `domo_community_forum | klaviyo_community`
- `E_debt_quarantined_mixed_retained`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`, no longer weak `domo_community_forum | klaviyo_community`
- `F_evidence_only`: weak `1`, core-readiness weak `1`, still weak `google_developer_forums`, no longer weak `adobe_analytics_community | domo_community_forum | klaviyo_community`
- `G_evidence_plus_mixed_only`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`, no longer weak `domo_community_forum | klaviyo_community`
- `H_source_tier_aware_slice_policy`: weak `2`, core-readiness weak `2`, still weak `adobe_analytics_community | google_developer_forums`, no longer weak `domo_community_forum | klaviyo_community`

## Weak-Source Interpretation

- `google_developer_forums` -> `slice_aware_mixed`; no-longer-weak under conservative debt quarantine: `False`; driver: real evidence-producing slices remain, but mixed report-delivery UI volume still keeps the source weak
- `adobe_analytics_community` -> `slice_aware_mixed`; no-longer-weak under conservative debt quarantine: `False`; driver: mixed workspace and implementation slices dominate; improvement is not driven by hidden debt because explicit debt slices are minimal
- `domo_community_forum` -> `slice_aware_debt_heavy`; no-longer-weak under conservative debt quarantine: `True`; driver: improvement comes mostly from removing debt-heavy slices rather than rescuing meaningful evidence-producing slices
- `klaviyo_community` -> `excluded_from_deck_ready_core`; no-longer-weak under conservative debt quarantine: `True`; driver: source remains diagnostics-visible, but tier policy keeps it outside deck-ready core interpretation

## Comparison To Balance Scenario F

- same debt-producing-slice quarantine improves quality-adjusted balance to `6.04` and reduces weak-source debt to `2` overall / `2` core-readiness weak sources.

## Recommendation

- next action: `run_slice_classification_precision_review_first`
- reason: Debt-producing-slice quarantine is promising, but slice-aware weak-source judgments still depend heavily on mixed slices in Google and Adobe. A precision review is the narrowest safe next step before proposing any secondary weak-source metric.
