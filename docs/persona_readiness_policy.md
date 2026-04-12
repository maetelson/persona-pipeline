## Persona Readiness Policy

This policy prevents the workbook from being mistaken for a final persona asset before the evidence base is mature enough.

### Canonical Rule

The workbook readiness decision is centralized and must drive all finality language in:

- overview
- quality_checks
- readme
- persona_summary
- cluster_stats

Below `deck_ready`, the workbook is not a final persona asset.

### Readiness States

- `exploratory_only`
  - below the minimum reviewable thresholds
  - gate status: `FAIL`
  - asset class: `hypothesis_material`

- `reviewable_but_not_deck_ready`
  - clear enough for analyst review, but still blocked from final persona claims
  - gate status: `WARN`
  - asset class: `reviewable_draft`

- `deck_ready`
  - first state allowed to claim a final persona asset for deck and stakeholder use
  - gate status: `OK`
  - asset class: `final_persona_asset`

- `production_persona_ready`
  - stricter version of final readiness for downstream production persona use
  - gate status: `OK`
  - asset class: `final_persona_asset`

### Explicit Thresholds

The readiness gate must evaluate these explicit metrics:

| Metric | Reviewable | Deck-ready | Production-ready |
| --- | --- | --- | --- |
| `overall_unknown_ratio` | `<= 0.30` | `<= 0.20` | `<= 0.15` |
| `persona_core_coverage_of_all_labeled_pct` | `>= 70.0` | `>= 80.0` | `>= 90.0` |
| `promoted_persona_example_coverage_pct` | `>= 80.0` | `>= 100.0` | `>= 100.0` |
| `final_usable_persona_count` | `>= 2` | `>= 3` | `>= 4` |
| `largest_source_influence_share_pct` | `<= 45.0` | `<= 35.0` | `<= 25.0` |
| `fragile_tail_share_of_core_labeled` | `<= 0.12` | `<= 0.08` | `<= 0.05` |

### Interpretation Requirements

- `deck_ready` is the first state allowed to present the workbook as a final persona asset.
- `reviewable_but_not_deck_ready` must still say the workbook is not a final persona asset.
- `exploratory_only` must explicitly restrict use to hypothesis material.
- Persona-level sheets must carry the workbook-level readiness state and usage restriction so a reader cannot miss the gating decision by skipping overview.

### Drift Guard

Tests must fail when any of these drift apart:

- overview readiness state versus persona_summary readiness columns
- overview readiness state versus cluster_stats readiness columns
- overview readiness state versus readme finality wording
- readiness gate status versus asset-class claim allowance
