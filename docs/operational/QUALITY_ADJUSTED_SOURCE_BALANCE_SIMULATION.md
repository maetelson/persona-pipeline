# Quality-Adjusted Source Balance Simulation

## Official Baseline

- official_effective_balanced_source_count: `5.89`
- largest_source_share_pct: `30.7`
- top_3_source_share_pct: `61.7`
- weak_source_cost_center_count: `4`
- core_readiness_weak_source_cost_center_count: `3`

## Scenario Takeaways

- `A_official_baseline`: balance `5.89`, largest share `30.7`, top 3 share `61.7`, crosses floor `False`
- `B_evidence_only`: balance `5.13`, largest share `33.38`, top 3 share `67.34`, crosses floor `False`
- `C_evidence_plus_mixed`: balance `6.04`, largest share `29.83`, top 3 share `60.39`, crosses floor `True`
- `D_evidence_plus_mixed_excluding_excluded_tier`: balance `5.95`, largest share `30.06`, top 3 share `60.85`, crosses floor `False`
- `E_conservative_balance_eligibility`: balance `5.04`, largest share `33.67`, top 3 share `67.92`, crosses floor `False`
- `F_debt_quarantined_simulation`: balance `6.04`, largest share `29.83`, top 3 share `60.39`, crosses floor `True`
- `G_weak_source_evidence_only_rescue`: balance `6.41`, largest share `27.22`, top 3 share `59.88`, crosses floor `True`

## Comparison

- Most defensible improvement: `F_debt_quarantined_simulation`
- Hides too much: `B_evidence_only, D_evidence_plus_mixed_excluding_excluded_tier`
- Too aggressive: `B_evidence_only`
- Mixed-slice recommendation: include mixed slices in first policy; excluding them is premature while Google and Adobe still depend heavily on mixed slices.
- Domo effect: Yes. Domo debt-heavy slices contribute most of the debt-only exclusion pressure and dominate the difference between baseline and debt-excluded scenarios.
- Google/Adobe help: Yes. Google and Adobe good and mixed slices meaningfully improve quality-adjusted balance once debt-only slices are excluded.

## Recommendation

- recommended_scenario: `F_debt_quarantined_simulation`
- recommended_next_action: `proceed_to_slice_aware_weak_source_debt_simulation`
