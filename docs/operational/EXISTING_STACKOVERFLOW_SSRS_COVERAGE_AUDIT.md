"""Audit of existing SSRS-like coverage already present inside the active Stack Overflow corpus."""

# Existing StackOverflow SSRS Coverage Audit

## Existing Stack Overflow Coverage Summary
- `raw_rows = 21544`
- `normalized_rows = 21544`
- `valid_rows = 5668`
- `prefiltered_rows = 3001`
- `episode_rows = 1683`
- `labeled_rows = 1683`
- `persona_core_rows = 839`

Current Stack Overflow persona contribution:
- `persona_01 = 635`
- `persona_02 = 40`
- `persona_03 = 3`
- `persona_04 = 83`
- `persona_05 = 14`
- `persona_06 = 58`
- `persona_08 = 2`
- `persona_10 = 4`

## SSRS-like Coverage Summary
- `normalized_rows = 598`
- `valid_rows = 295`
- `prefiltered_rows = 234`
- `episode_rows = 93`
- `labeled_rows = 93`
- `persona_core_rows = 70`

SSRS-like persona contribution:
- `persona_01 = 69`
- `persona_04 = 1`

## Sample Classification (top 50 by stage strength and specificity)
- `export_layout_delivery_pain = 26`
- `generic_ssrs_syntax_debug = 13`
- `server_deployment_or_config_noise = 3`
- `metric_validation_or_total_mismatch = 3`
- `ambiguous = 2`
- `true_reporting_workflow_pain = 2`
- `database_connection_noise = 1`

## Coverage Status
- `ssrs_coverage_status = present_but_underutilized`
- `recommended_next_action = recover_existing_stackoverflow_ssrs_rows`

## Key Interpretation
- The active Stack Overflow source already includes a `reporting_services` seed-bundle path.
- SSRS/Reporting Services evidence is already present in normalized, valid, episode, labeled, and persona-core rows.
- Zero currently selected examples come from the matched SSRS-like rows.
- That makes existing-row recovery or reuse more defensible than opening a new SSRS expansion pilot first.