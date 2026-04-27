# Source Slice Classification Phase 1

## Scope

This pass adds diagnostics-only source-slice classification fields.
It does not quarantine slices, does not change official source balance or weak-source counts, and does not alter readiness or persona status logic.

## Current Outputs

- total_rows_classified: `12674`
- evidence_producing_slice_count: `9228`
- mixed_evidence_slice_count: `1187`
- debt_producing_slice_count: `2259`
- diagnostics_only_count: `0`

## Weak-Source Highlights

- `adobe_analytics_community`: `{'evidence_producing_slice': 33, 'mixed_evidence_slice': 612}`
- `domo_community_forum`: `{'debt_producing_slice': 1044}`
- `google_developer_forums`: `{'debt_producing_slice': 150, 'evidence_producing_slice': 283, 'mixed_evidence_slice': 379}`
- `klaviyo_community`: `{'debt_producing_slice': 31, 'evidence_producing_slice': 79}`

## Guardrails

- source tiers remain unchanged
- official effective source balance remains unchanged
- official weak-source counts remain unchanged
- slice rows remain visible in diagnostics
- this phase does not create audited secondary balance or weak-debt metrics
