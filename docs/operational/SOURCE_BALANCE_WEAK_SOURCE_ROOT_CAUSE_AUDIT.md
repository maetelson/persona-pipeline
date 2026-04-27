# Source Balance and Weak-Source Root Cause Audit
## Summary
- Official effective balanced source count: `5.89`
- Weak-source cost center count: `4`
- Core-readiness weak-source cost center count: `3`
- Coverage is no longer the primary blocker; source balance and weak-source debt remain.

## Root-Cause Findings
- Weak sources are not uniformly weak as whole sources. Adobe, Google, and Domo each contain evidence-producing slices mixed with debt-producing technical/support slices.
- Klaviyo is the clearest case of bad fit plus overbroad slice mix; its useful reporting slice is real but too small to offset debt.
- Source balance is low partly because true evidence remains concentrated in Power BI and Metabase, and partly because weak sources add visible corpus breadth without enough high-quality downstream influence.
- The denominator policy already solved a coverage-design problem; the remaining issue is now source-internal slice quality and how the balance/debt metrics treat those slices.

## Weak Source Classification
- `google_developer_forums`: good_source_bad_slice_mix, support_setup_noise_dominant, source_specific_language_not_modeled, denominator_design_issue_already_partly_resolved
- `adobe_analytics_community`: good_source_bad_slice_mix, source_specific_language_not_modeled, codebook_boundary_overreach, denominator_design_issue_already_partly_resolved
- `domo_community_forum`: good_source_bad_slice_mix, parser_or_body_fidelity_issue, source_specific_language_not_modeled, support_setup_noise_dominant
- `klaviyo_community`: bad_source_fit, overbroad_seed_or_query, low_incremental_yield, genuinely_low_value_source

## Recommended Next Action
- `write_slice_aware_source_policy_spec`
- Rationale: write a slice-aware source policy before any further remediation so good slices can be preserved and debt-producing slices can be quarantined transparently in diagnostics.
