# Persona 05 Selected Example Improvement Report

## Decision

Accepted.

## Previous persona_05 selected example

- Episode ID: `metabase_discussions::22296::01`
- Source: `metabase_discussions`
- Source tier: `core_representative_source`
- Weakness: Too technical and implementation-specific; it does not clearly show stakeholder-facing last-mile reporting output pain and reads like generic tool-limitation troubleshooting.

## Validated candidate examples

- `hubspot_community::823169::01` from `hubspot_community` (`supporting_validation_source`, confidence `high`): strong because Clear analyst job, clear target-tracking output, explicit product limitation, and clear business reporting context. Not persona_01 because The pain is not export/rework volume; it is the inability to construct the needed target-tracking output inside the tool. Not persona_03 because The story is output-delivery-specific, not just generic tool limitation.
- `metabase_discussions::99555::01` from `metabase_discussions` (`core_representative_source`, confidence `high`): strong because Strong last-mile presentation problem with an explicit desired report shape and no obvious support-only noise. Not persona_01 because The issue is not recurring export burden; it is a final-layout limitation inside the BI tool. Not persona_03 because The limitation matters because a presentable deliverable is blocked, not just because the tool lacks a feature in the abstract.
- `metabase_discussions::109422::01` from `metabase_discussions` (`core_representative_source`, confidence `medium_high`): strong because This shows a stakeholder-facing reporting usability problem, not just admin setup friction. Not persona_01 because The user is blocked by final output usability and report construction clarity, not by repetitive export/rework. Not persona_03 because The tool limitation is tied to report usability for consumers, which is more specific than generic workaround behavior.
- `power_bi_community::5134064::01` from `power_bi_community` (`core_representative_source`, confidence `medium`): strong because It is a concrete last-mile dashboard behavior problem with a specific report-view goal. Not persona_01 because The difficulty is not repetitive reporting labor; it is that the final dashboard behavior cannot be expressed cleanly in the tool. Not persona_03 because The problem is bound to a presentation-ready output interaction, not just generic tool limitation.
- `github_discussions::4223807485::01` from `github_discussions` (`supporting_validation_source`, confidence `medium`): strong because This clearly affects a shared dashboard deliverable and shows why the output cannot be trusted or reused reliably. Not persona_01 because This is not export burden; it is a shared output construction and interpretation defect. Not persona_03 because The tool limitation matters because it corrupts a stakeholder-facing output, not merely because a feature is missing.

## Selected new examples

- Rank 1: `metabase_discussions::99555::01` from `metabase_discussions` (`core_representative_source`, `curated_override`)
- Rank 2: `metabase_discussions::109422::01` from `metabase_discussions` (`core_representative_source`, `curated_override`)
- Rank 3: `github_discussions::4223807485::01` from `github_discussions` (`supporting_validation_source`, `curated_override`)
- Rank 4: `hubspot_community::823169::01` from `hubspot_community` (`supporting_validation_source`, `curated_override`)
- Rank 5: `metabase_discussions::153908::01` from `metabase_discussions` (`core_representative_source`, `grounded`)

## Exact config/code change

- Added a persona_05 curated example override in `config/example_selection.yaml`.
- Added a narrow curated override path in `src/analysis/example_selection.py`.
- Added regression coverage in `tests/test_example_selection.py`.

## Before vs after

- `selected_example_count`: `1 -> 5`
- `readiness_tier`: `blocked_or_constrained_candidate -> blocked_or_constrained_candidate`
- `deck_ready_claim_eligible_persona`: `False -> False`
- `final_usable_persona_count`: `3 -> 3`
- `production_ready_persona_count`: `3 -> 3`
- `review_ready_persona_count`: `1 -> 1`
- `deck_ready_claim_eligible_persona_count`: `4 -> 4`

## Status confirmation

- persona_05 remains blocked/constrained.
- persona_05 remains non-production.
- persona_05 remains non-review-ready.
- persona_05 remains non-final-usable.
- persona_05 remains not deck-ready-claim-eligible.

## Tests run

- `python run/pipeline/06_cluster_and_score.py`
- `python run/pipeline/07_export_xlsx.py`
- `python run/cli/17_analysis_snapshot.py --compare-latest`
- `python -m unittest tests.test_example_selection tests.test_deck_ready_claims tests.test_source_tier_evidence tests.test_analysis_snapshot_cli tests.test_workbook_export`

## Next recommendation

- If we revisit persona_05, the next smallest safe move should be a boundary diagnostic or narrative clarification pass, not a claim-eligibility change.
