"""Narrow recovery audit for the existing Stack Overflow SSRS / Reporting Services slice."""

# StackOverflow SSRS Recovery Audit

## SSRS Recovery Decision Table
- `total_ssrs_like_rows = 598`
- `valid_rows = 295`
- `prefiltered_rows = 234`
- `labeled_rows = 93`
- `persona_core_rows = 70`
- `current_persona_contribution = {'persona_01': 69, 'persona_04': 1}`
- `selected_example_count = 0`
- `strongest_matched_terms = {'reporting-services': 189, 'ssrs': 170, 'pagination': 100, 'subscription': 75, 'tablix': 48, 'reportbuilder': 30, 'export to excel': 21, 'report server': 9, 'matrix report': 8, 'paginated report': 4}`
- `useful_pattern_count = 117`
- `syntax_debug_noise_count = 124`
- `server_config_noise_count = 32`
- `recovery_opportunity = high_for_selected_example_exposure`

## Stage Drop-off Analysis
- `normalized -> valid`: loss `303` (`0.5067`) | `expected_noise_filtering_with_possible_source_specific_miss`
  Evidence: source_diagnostics flags stackoverflow missing_pain_signal at valid_filtering
- `valid -> prefiltered`: loss `61` (`0.2068`) | `relevance_miss`
  Evidence: existing source_diagnostics and SSRS tags show operational export/layout phrasing may under-signal pain
- `prefiltered -> episodes`: loss `141` (`0.6026`) | `episode_quality_issue`
  Evidence: many retained rows appear to be formula/layout questions that do not become distinct episode-quality snippets
- `episodes -> labeled`: loss `0` (`0.0`) | `expected_noise_filtering`
  Evidence: no loss observed; labeled rows equal episode rows
- `labeled -> persona_core`: loss `23` (`0.2473`) | `labelability_issue`
  Evidence: some labeled rows remain non-core, likely because they stay technical or weakly contextualized
- `persona_core -> selected_examples`: loss `70` (`1.0`) | `selected_example_ranking_issue`
  Evidence: 70 persona-core rows and zero selected examples is too sharp to explain as normal scarcity alone

## Reviewed Candidate Classification
- `strong_persona05_subtheme_example = 18`
- `server_config_noise = 11`
- `syntax_debug_noise = 9`
- `ambiguous = 7`
- `strong_persona01_example = 2`
- `usable_supporting_evidence = 2`
- `weak_generic = 1`

## Recovery Recommendation
- `recovery_justified = True`
- `recommended_recovery_target = selected-example exposure only`
- `persona_support_decision = persona_05_subtheme_secondary_only`

## Candidate Selected Examples
- `77409902` | SSRS Subtotals in Filtered Matrix | `strong_persona05_subtheme_example`
- `78064431` | Aligning Two Matrix Tables in SSRS Based on Matching Column Names | `strong_persona05_subtheme_example`
- `76798514` | Empty values issue SSRS Report Builder chart | `strong_persona01_example`
- `72371933` | Unable to COUNTROWS dataset B from tablix with dataset A scope, looking for alternate solutions | `strong_persona05_subtheme_example`
- `67115725` | SSRS inserting blank page | `strong_persona01_example`
- `78760872` | Is there a way in SSRS to only display specific parts of a report when rendered in MHTML, vs when it is rendered in the Report Viewer? | `strong_persona05_subtheme_example`
## Interpretation
- The sharpest SSRS problem is not lack of source coverage. It is zero selected-example exposure despite non-trivial persona-core volume.
- The most defensible first recovery target is persona_01-facing example exposure, not broad filter rescue.
- Some SSRS rows resemble persona_05-style last-mile output construction pain, but the evidence is still secondary and should not drive persona_05 state changes.