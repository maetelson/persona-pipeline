## Workbook Review-Ready Implementation Plan

### Scope

This document translates the workbook policy redesign spec into an implementation plan and output contract.

This document does not approve implementation by itself.

It is specifically scoped to workbook-facing analysis outputs and bundle contracts.

It does not change:

- clustering
- axis weights
- source rules
- promotion scoring
- workbook thresholds
- `final_usable_persona` semantics
- final xlsx rendering in this phase

### Current Workbook-Facing Output Trace

Current generation path:

1. `run/pipeline/06_cluster_and_score.py`
2. `src.analysis.stage_service.run_analysis_stage`
3. `src.analysis.stage_service.build_deterministic_analysis_outputs`
4. `src.analysis.persona_service.build_persona_outputs`
5. workbook-facing frames assembled in `src.analysis.workbook_bundle.assemble_workbook_frames`
6. workbook bundle persisted by `src.analysis.workbook_bundle.write_workbook_bundle`
7. final xlsx later rendered by `src.exporters.xlsx_exporter.export_workbook_from_frames`

Relevant sheet / file producers:

- `overview.csv`
  - `src.analysis.stage_service._build_final_overview_df`
- `persona_summary.csv`
  - `src.analysis.persona_service._build_persona_summary_df`
- `cluster_stats.csv`
  - `src.analysis.persona_service._build_cluster_stats_df`
- `persona_promotion_path_debug.csv`
  - `src.analysis.persona_service._build_persona_promotion_path_debug_df`
- `data/analysis/workbook_bundle/*`
  - `src.analysis.workbook_bundle.assemble_workbook_frames`
  - `src.analysis.workbook_bundle.write_workbook_bundle`
- final xlsx inputs
  - the workbook bundle frames plus `src.exporters.xlsx_exporter._prepare_workbook_frames`

### Output Contract Table

| Output file | Current relevant fields | New fields to add | Source function/module | Risk | Implementation priority |
|---|---|---|---|---|---|
| `overview.csv` | `final_usable_persona_count`, `headline_persona_count`, `promotion_visibility_persona_count`, `promotion_constraint_status`, `promotion_constraint_summary` | `production_ready_persona_count`, `review_ready_persona_count`, `exploratory_persona_count` | `src.analysis.stage_service._build_final_overview_df` | low if counts remain additive only | phase 1 |
| `persona_summary.csv` | `final_usable_persona`, `promotion_status`, `promotion_grounding_status`, `workbook_review_visible`, `reporting_readiness_status`, `promotion_reason`, `grounding_status`, `structural_support_status` | `readiness_tier`, `production_ready_persona`, `review_ready_persona`, `review_ready_reason`, `blocked_reason`, `workbook_policy_constraint`, `review_visibility_status` | `src.analysis.persona_service._build_persona_summary_df` | medium because this is a primary analyst-facing sheet | phase 1 |
| `cluster_stats.csv` | same promotion and readiness fields as `persona_summary.csv` | same new row-level review-ready fields as `persona_summary.csv` | `src.analysis.persona_service._build_cluster_stats_df` | medium because audit parity with `persona_summary.csv` must hold | phase 1 |
| `persona_promotion_path_debug.csv` | `base_promotion_status`, `structural_support_status`, `grounding_status`, `promotion_grounding_status`, `promotion_status`, `final_usable_persona`, `fail_reason` | `readiness_tier`, `review_ready_persona`, `review_ready_reason`, `blocked_reason`, `workbook_policy_constraint`, `review_visibility_status` | `src.analysis.persona_service._build_persona_promotion_path_debug_df` | low-medium, but this is the cleanest place to expose decision reasons | phase 1 |
| `data/analysis/workbook_bundle/*` | parquet copies of workbook-facing frames | no new semantics, only bundle persistence of fields added upstream | `src.analysis.workbook_bundle.assemble_workbook_frames` and `write_workbook_bundle` | low if fields are added to existing frames only | phase 1 |
| final xlsx export | current sheet columns only | later mirror of phase-1 fields once CSV contract stabilizes | `src.exporters.xlsx_exporter.export_workbook_from_frames` | higher because wording and sheet layout matter | phase 2 |

### Field Semantics

Required fields and intended semantics:

#### `readiness_tier`

One of:

- `production_ready_persona`
- `review_ready_persona`
- `blocked_or_constrained_candidate`
- `exploratory_bucket`

This is the primary row-level interpretation field.

#### `production_ready_persona`

- strict boolean
- must match current `final_usable_persona` unless there is a separately documented future change
- not a new scoring layer
- simply the explicit production-ready alias

#### `review_ready_persona`

- boolean
- true only for locally strong personas that remain blocked from production-ready use by workbook-global policy or equivalent global caution
- must not affect `final_usable_persona_count`

#### `review_ready_reason`

- short analyst-facing explanation
- should explain why the row is visible for review
- should not sound like final validation

#### `blocked_reason`

- explicit reason a candidate remains blocked
- should be used for constrained or exploratory candidates

#### `workbook_policy_constraint`

- machine-readable or compact text summary of workbook-global blocker(s)
- examples:
  - `top_3_cluster_share_of_core_labeled=83.3`
  - `weak_source_cost_centers_present`

#### `review_visibility_status`

Recommended values:

- `production_ready_visible`
- `review_ready_visible`
- `blocked_not_review_ready`
- `exploratory_only`

This field is intended for workbook filtering and user-facing explanations.

### Exact Review-Ready Logic

A persona is `review_ready_persona` only if all of the following are true:

1. `structural_support_status` is locally supportive.
   - expected current passing state:
     - `structurally_supported`
     - optionally `structurally_supported_broad_parent` if later needed

2. `grounding_status` is acceptable.
   - no weak fallback
   - no missing-example-only case

3. It is not weak-source dominated.
   - no `weak_source_link`
   - no current weak-source-dominated primary source path

4. It is not near-duplicate.
   - no redundancy state indicating a suppressed duplicate identity

5. It is not thin-evidence.
   - must not fall into the thin-evidence exclusion bucket
   - must have adequate selected examples and bundle evidence

6. It is blocked mainly by workbook-global concentration policy.
   - expected current qualifying signal:
     - `promotion_grounding_status == promotion_constrained_by_workbook_policy`

7. It has semantic distinctiveness.
   - this must come from current semantic evidence, not threshold relaxation
   - current evidence indicates `persona_04` satisfies this

### Expected Current-State Result

Under current artifacts, the implementation should resolve as:

- `persona_01`
  - `production_ready_persona=True`
  - `review_ready_persona=False`

- `persona_02`
  - `production_ready_persona=True`
  - `review_ready_persona=False`

- `persona_03`
  - `production_ready_persona=True`
  - `review_ready_persona=False`
  - note: thin-example caution may still exist, but it should not silently change current strict semantics in this pass

- `persona_04`
  - `production_ready_persona=False`
  - `review_ready_persona=True`
  - `readiness_tier=review_ready_persona`

- `persona_05`
  - `production_ready_persona=False`
  - `review_ready_persona=False`
  - `readiness_tier=blocked_or_constrained_candidate`

Expected top-line counts:

- `production_ready_persona_count = 3`
- `review_ready_persona_count = 1`
- `final_usable_persona_count = 3`

### Implementation Sequence

Recommended sequence:

#### Phase 0: contract freeze

- keep this plan and the redesign spec as source-of-truth
- no workbook rendering changes yet

#### Phase 1: analysis CSV fields only

- update `persona_summary.csv`
- update `cluster_stats.csv`
- update `persona_promotion_path_debug.csv`
- update `overview.csv`
- persist the same fields through `workbook_bundle`

Rules:

- do not change `final_usable_persona`
- do not change `promotion_status`
- do not change xlsx rendering

This is the preferred first implementation step.

#### Phase 2: bundle validation tightening

- extend workbook bundle contract checks so the new review-ready fields are expected where appropriate
- verify `overview`, `cluster_stats`, and `persona_summary` agree

#### Phase 3: xlsx rendering

- only after the CSV and bundle contract is stable
- add explicit workbook notes and separate visibility language

### Tests To Add Before Implementation

Required tests:

1. `production_ready_persona_count` does not increase just because review-ready exists.

2. `final_usable_persona` semantics remain unchanged.

3. `production_ready_persona` matches `final_usable_persona`.

4. `persona_04` becomes `review_ready_persona`.

5. `persona_05` remains not review-ready.

6. Weak-source dominated candidates cannot become review-ready.

7. Near-duplicate candidates cannot become review-ready.

8. Thin-evidence candidates cannot become review-ready.

9. `overview.csv` reports:
   - `final_usable_persona_count`
   - `production_ready_persona_count`
   - `review_ready_persona_count`
   separately and correctly.

10. `persona_summary.csv` and `persona_promotion_path_debug.csv` agree on:
   - `readiness_tier`
   - `review_ready_persona`
   - `blocked_reason`

11. `cluster_stats.csv` and `persona_summary.csv` agree on row-level review-ready interpretation for the same persona ids.

### Recommendation: Implement Now Or Defer?

Recommendation:

- **defer actual implementation to the next pass**

Reason:

- the change itself is low-risk if limited to analysis CSV fields
- but it touches four workbook-facing outputs plus bundle contract parity
- the safest next move is a dedicated analysis-output-only pass that changes:
  - `overview.csv`
  - `persona_summary.csv`
  - `cluster_stats.csv`
  - `persona_promotion_path_debug.csv`
  - workbook bundle validation
- and explicitly does **not** change xlsx export yet

So implementation is safe enough in principle, but still deserves its own narrow pass.

### Risks Before XLSX Export Update

1. `persona_03` already carries thin-example caution while remaining production-ready.
   - implementation must not confuse “existing caution” with “newly blocked”

2. If the same persona gets different `readiness_tier` values in `cluster_stats.csv` and `persona_summary.csv`, reviewers will lose trust quickly.

3. If `overview.csv` adds `review_ready_persona_count` but `headline_persona_count` remains ambiguous, the redesign will still confuse users.

4. If workbook bundle validation is not updated alongside the CSV fields, the xlsx phase will become harder to audit later.

5. Review-ready semantics must remain explicitly non-final.
   - any wording drift toward “final persona” would undermine the entire purpose of the tier split.

