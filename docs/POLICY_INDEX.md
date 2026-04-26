# Policy Index

This file is the source-of-truth index for policy and readiness documents.

Use this document to decide which policy controls a decision before opening multiple overlapping docs.

## Source-of-truth policy map

| Topic | Primary source of truth | Supporting docs |
|---|---|---|
| Persona promotion rules | [`persona_promotion_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_promotion_policy.md) | [`persona_promotion_grounding_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_promotion_grounding_policy.md), [`persona_core_coverage_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_core_coverage_policy.md) |
| Persona grounding requirements | [`persona_promotion_grounding_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_promotion_grounding_policy.md) | [`persona_example_grounding_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_example_grounding_policy.md) |
| Workbook readiness / final usable personas | [`persona_readiness_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_readiness_policy.md) | [`persona_workbook_denominator_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_workbook_denominator_policy.md), [`workbook_metric_truth_table.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\workbook_metric_truth_table.md), [`workbook_policy_redesign_spec.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\workbook_policy_redesign_spec.md), [`operational/WORKBOOK_REVIEW_READY_IMPLEMENTATION_PLAN.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\WORKBOOK_REVIEW_READY_IMPLEMENTATION_PLAN.md), [`operational/WORKBOOK_REVIEW_READY_PRESENTATION_PLAN.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\WORKBOOK_REVIEW_READY_PRESENTATION_PLAN.md) |
| Deck-ready core evidence policy | [`operational/DECK_READY_CORE_POLICY_SPEC.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\DECK_READY_CORE_POLICY_SPEC.md) | [`operational/DECK_READY_READINESS_INTERPRETATION_SPEC.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\DECK_READY_READINESS_INTERPRETATION_SPEC.md), [`source_balance_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\source_balance_policy.md), [`operational/SOURCE_REPRESENTATIVENESS_POLICY_DRAFT.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\SOURCE_REPRESENTATIVENESS_POLICY_DRAFT.md), [`operational/POST_ROLLBACK_DECK_READY_PATH_DIAGNOSIS.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\POST_ROLLBACK_DECK_READY_PATH_DIAGNOSIS.md), [`operational/REVIEWABLE_HANDOFF_SUMMARY.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\REVIEWABLE_HANDOFF_SUMMARY.md) |
| Persona subtheme preservation | [`operational/PERSONA05_SUBTHEME_PRESERVATION_SPEC.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\PERSONA05_SUBTHEME_PRESERVATION_SPEC.md) | [`operational/PERSONA05_MERGE_SUPPRESSION_REVIEW.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\PERSONA05_MERGE_SUPPRESSION_REVIEW.md), [`operational/PERSONA05_BOUNDARY_RULE_REPORT.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\PERSONA05_BOUNDARY_RULE_REPORT.md), [`operational/PERSONA05_EVIDENCE_IMPROVEMENT_PLAN.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\PERSONA05_EVIDENCE_IMPROVEMENT_PLAN.md) |
| Workbook metric provenance / audit interpretation | [`persona_workbook_audit.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_workbook_audit.md) | [`workbook_semantic_changelog.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\workbook_semantic_changelog.md) |
| Source balance policy | [`source_balance_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\source_balance_policy.md) | [`source_diagnostics_reason_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\source_diagnostics_reason_policy.md) |
| Source diagnostics contract | [`source_diagnostics_metric_contract.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\source_diagnostics_metric_contract.md) | [`source_diagnostics_grain_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\source_diagnostics_grain_policy.md), [`source_diagnostics_reason_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\source_diagnostics_reason_policy.md) |
| Quality status / readiness labels | [`quality_status_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\quality_status_policy.md) | [`cluster_robustness_policy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\cluster_robustness_policy.md) |
| Artifact lifecycle and cleanup | [`operational/ARTIFACT_LIFECYCLE.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\ARTIFACT_LIFECYCLE.md) | [`operational/ORCHESTRATION.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\ORCHESTRATION.md), [`operational/DATA_CONTRACT.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\DATA_CONTRACT.md) |
| Pipeline stage order and rerun scope | [`operational/ORCHESTRATION.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\ORCHESTRATION.md) | [`README.md`](C:\Users\hands\OneDrive\Desktop\persona\README.md), [`operational/RUNBOOK.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\RUNBOOK.md) |
| File/schema contract | [`operational/DATA_CONTRACT.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\DATA_CONTRACT.md) | [`operational/SCHEMA.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\SCHEMA.md) |
| Label taxonomy and labeling definitions | [`operational/CODEBOOK.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\operational\CODEBOOK.md) | [`persona_core_taxonomy.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\persona_core_taxonomy.md) |

## Practical reading order

Use this order when the task is ambiguous:

1. `README.md`
2. `docs/operational/ORCHESTRATION.md`
3. `docs/operational/DATA_CONTRACT.md`
4. the one primary policy document for your decision area
5. supporting docs only if the primary policy leaves ambiguity

## Historical or archived material

Treat these as historical context, not current truth:

- `docs/archive/`
- [`logue_seed_strategy_proposal.md`](C:\Users\hands\OneDrive\Desktop\persona\docs\logue_seed_strategy_proposal.md)
- scratch files under `docs/archive/scratch/`

Use them only when you need design history or abandoned alternatives.
