## Workbook Review-Ready Presentation Plan

### Scope

This document defines Phase 2 workbook-facing presentation for `review_ready_persona`.

This is a presentation contract only.

It does not change:

- clustering
- axis weights
- source rules
- workbook thresholds
- persona promotion scoring
- `final_usable_persona` semantics
- final export counts

It also does not treat `review_ready_persona` as `production_ready_persona`.

### Current XLSX Export Trace

Current path:

1. `run/pipeline/06_cluster_and_score.py`
2. `src.analysis.stage_service.run_analysis_stage`
3. `src.analysis.workbook_bundle.assemble_workbook_frames`
4. `src.exporters.xlsx_exporter.export_workbook_from_frames`

Current xlsx export behavior:

- workbook sheets are created from `WORKBOOK_SHEET_NAMES`
- every bundle frame is exported as a flat sheet
- export-only header renaming happens in `src.exporters.xlsx_exporter._display_frame`
- sheet formatting happens in `src.exporters.xlsx_exporter._format_worksheet`
- README / notes content is generated in `src.exporters.xlsx_exporter._write_readme_sheet`
- optional-column warnings are emitted in `src.exporters.xlsx_exporter._prepare_workbook_frames`
- hard validation failures are raised from the same `_prepare_workbook_frames` function after `validate_workbook_frames(...)`

Current required sheets:

- `overview`
- `counts`
- `source_distribution`
- `taxonomy_summary`
- `cluster_stats`
- `persona_summary`
- `persona_axes`
- `persona_needs`
- `persona_cooccurrence`
- `persona_examples`
- `quality_checks`
- `source_diagnostics`
- `quality_failures`
- `metric_glossary`

README behavior today:

- workbook creates a separate `readme` sheet before all analytical sheets
- readiness metrics are linked from `overview`
- current wording explains review-visible promoted personas, but not the new `review_ready_persona` tier explicitly

### Presentation Decision

Phase 2 should use a **same-sheet, explicit-tier** presentation model.

Decision:

- do **not** create a separate new worksheet only for review-ready personas in Phase 2
- keep `persona_summary` and `cluster_stats` as the canonical persona sheets
- visually separate rows by `readiness_tier`
- keep production-ready counts and review-ready counts separate in `overview`
- keep review-ready personas out of headline production-ready summaries
- keep examples visible, but clearly label review-ready personas as analyst-review-only

Why:

- the row-level contract already exists in the analysis outputs
- same-sheet presentation is lower risk than introducing a second persona table
- it preserves one canonical persona inventory while still making tier boundaries explicit

### Workbook Semantics To Preserve

The workbook must continue to show:

- `production_ready_persona_count = 3`
- `review_ready_persona_count = 1`
- `final_usable_persona_count = 3`
- `persona_04` is review-ready only
- `persona_05` remains blocked / constrained

Strict rules:

- `review_ready_persona` must not affect `final_usable_persona_count`
- `review_ready_persona` must not affect headline production-ready counts
- `production_ready_persona` remains the workbook-facing alias of strict `final_usable_persona`

### Sheet-Level Presentation Plan

#### `overview`

Current role:

- workbook headline metrics
- readiness gate
- top-line counts

Phase 2 presentation:

- keep `final_usable_persona_count` as the strict headline production-ready metric
- add separate visible lines for:
  - `production_ready_persona_count`
  - `review_ready_persona_count`
  - `blocked_or_constrained_persona_count`
  - `exploratory_bucket_count`
- do not relabel review-ready as headline personas

Visible columns:

- still `metric_key`
- `display_label`
- `metric_value`

User-facing explanation:

- `final_usable_persona_count`
  - “Strict final usable persona count for production-ready use.”
- `production_ready_persona_count`
  - “Alias of final usable persona count under current policy.”
- `review_ready_persona_count`
  - “Strong candidates for analyst review only. Not included in final usable persona count.”

Implementation priority:

- high

#### `persona_summary`

Current role:

- primary workbook-facing persona sheet
- narrative and grounded persona detail

Phase 2 presentation:

- keep all personas in one sheet
- sort rows by:
  1. `production_ready_persona desc`
  2. `review_ready_persona desc`
  3. `persona_size desc`
- make the following visible:
  - `readiness_tier`
  - `production_ready_persona`
  - `review_ready_persona`
  - `review_visibility_status`
  - `review_ready_reason`
  - `blocked_reason`
  - `workbook_policy_constraint`

Recommended visible ordering near the left side:

1. `persona_id`
2. `persona_name`
3. `readiness_tier`
4. `production_ready_persona`
5. `review_ready_persona`
6. `review_visibility_status`
7. `persona_size`
8. `share_of_core_labeled`
9. `promotion_status`
10. `promotion_grounding_status`
11. `review_ready_reason`
12. `blocked_reason`
13. `workbook_policy_constraint`

User-facing explanation:

- `readiness_tier`
  - the canonical workbook interpretation tier
- `review_ready_reason`
  - why a persona is visible for analyst review
- `blocked_reason`
  - why it remains non-production

Implementation priority:

- high

#### `cluster_stats`

Current role:

- compact structural and promotion audit sheet

Phase 2 presentation:

- expose the same tier fields as `persona_summary`
- keep structural and robustness columns unchanged
- sort using the same readiness-aware order as `persona_summary`

Visible fields to ensure:

- `readiness_tier`
- `production_ready_persona`
- `review_ready_persona`
- `review_visibility_status`
- `review_ready_reason`
- `blocked_reason`
- `workbook_policy_constraint`

User-facing explanation:

- this is the structural audit view of the same persona tier semantics

Implementation priority:

- high

#### `persona_promotion_path_debug`

Current role:

- debug-only analysis CSV, not a workbook sheet today

Phase 2 presentation:

- no direct xlsx sheet change required in this pass
- but if later exposed, it should remain an audit/debug representation of the same readiness logic

Implementation priority:

- medium

#### `persona_examples`

Current role:

- selected representative examples

Phase 2 presentation:

- do not exclude review-ready personas from examples
- keep example rows available for `persona_04`
- if examples are shown for review-ready personas, the workbook notes must make clear these examples support analyst review, not final validated persona claims

Implementation priority:

- medium

#### `readme`

Current role:

- workbook guide and formula-backed interpretation layer

Phase 2 presentation:

- add explicit text for the two-tier visibility model
- add formula-backed lines for:
  - `production_ready_persona_count`
  - `review_ready_persona_count`
  - `blocked_or_constrained_persona_count`
- clarify that review-ready does not relax production-ready thresholds

Implementation priority:

- high

### README / Notes Wording

Required wording themes:

- “Production-ready personas are strict final usable outputs.”
- “Review-ready personas are strong candidates for analyst review, but are not included in final usable persona count.”
- “Review-ready status does not relax workbook policy or production-ready thresholds.”
- “Review-ready personas need human review before deck-ready or production use.”

Suggested README lines:

- `Production-Ready Persona Count`
  - “Strict final usable persona count for production-ready reporting.”
- `Review-Ready Persona Count`
  - “Strong analyst-review candidates that remain workbook-constrained and are excluded from final usable persona count.”
- `Review-Ready Interpretation`
  - “Review-ready personas are locally grounded and structurally supported, but remain blocked from production-ready use by workbook-global policy.”
- `Blocked Candidates`
  - “Blocked or constrained candidates remain non-production because of thin evidence, weak-source dominance, duplication risk, or workbook policy constraints.”

Suggested review tips text:

- “Do not treat review-ready personas as final validated personas.”
- “Use review-ready rows for analyst review, not headline counts.”
- “Review-ready visibility does not imply threshold relaxation.”

### Optional-Column Warning Decision

Current behavior:

- `_prepare_workbook_frames` emits `RuntimeWarning` for missing optional columns
- this includes genuinely optional columns in minimal synthetic test frames
- the warnings are noisy in tests but are not hard failures unless they violate the validated contract

Decision:

- do **not** suppress warnings globally
- do **not** downgrade new review-ready fields to hidden implicit behavior
- keep warnings meaningful for real export paths
- for minimal synthetic test frames, either:
  - include the new review-ready fields in fixtures when the test is about workbook-facing persona sheets, or
  - explicitly document that optional warnings are expected in narrow fixture tests

Recommended Phase 2 behavior:

- keep `overview` review-ready count metrics as required workbook metrics
- keep row-level review-ready fields required for real `persona_summary` and `cluster_stats`
- allow minimal synthetic tests to omit unrelated persona narrative columns without failing export, as today

### Implementation Checklist

When Phase 2 implementation begins:

1. Update `xlsx_exporter._display_frame(...)` only for presentation, not semantics.
2. Add overview display labels for:
   - `production_ready_persona_count`
   - `review_ready_persona_count`
   - `blocked_or_constrained_persona_count`
3. Reorder `persona_summary` display columns so readiness fields appear near the front.
4. Reorder `cluster_stats` display columns the same way.
5. Add readiness-aware sort order for exported `persona_summary` and `cluster_stats`.
6. Update `readme` sheet wording and formula-backed count rows.
7. Confirm `persona_04` appears as review-ready in workbook-facing sheets.
8. Confirm `persona_05` remains blocked / constrained.
9. Confirm `final_usable_persona_count` remains unchanged.
10. Do not update sheet totals or headline wording to include review-ready personas.

### Test Plan Before / During Implementation

Required tests:

1. Workbook export does not treat `review_ready_persona` as `production_ready_persona`.
2. `final_usable_persona_count` remains `3`.
3. `production_ready_persona_count` remains `3`.
4. `review_ready_persona_count` appears separately as `1`.
5. `persona_04` appears as review-ready in workbook-facing sheets.
6. `persona_05` appears as blocked / constrained, not review-ready.
7. README / notes explicitly say review-ready is not final validated output.
8. No overview headline metric absorbs review-ready rows.
9. `persona_summary` and `cluster_stats` show the same tier for the same persona.
10. Optional-column warnings are either:
    - absent in realistic export tests, or
    - explicitly expected in minimal synthetic-frame tests.

### Recommendation

Phase 2 implementation should **proceed in a separate narrow pass**, not in this planning turn.

Reason:

- the presentation contract is now clear enough
- but xlsx rendering changes touch reviewer-facing wording and visible workbook layout
- that deserves one isolated export-focused pass with targeted regression tests
