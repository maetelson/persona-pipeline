# Post-Rollback Deck-Ready Path Diagnosis

## Rollback Integrity

- Code and source configs are restored, with `adobe_analytics_community`, `google_developer_forums`, and `domo_community_forum` active again.
- Raw source directories for those three sources are present again.
- Active generated outputs now contain Adobe, Google, and Domo rows again; the partial-removal output state was cleared by the downstream rebuild.
- Result: the stable reviewable baseline is restored.

## Stable Baseline

- `overall_status` = `WARN`
- `quality_flag` = `EXPLORATORY`
- `persona_readiness_state` = `reviewable_but_not_deck_ready`
- `persona_readiness_gate_status` = `WARN`
- `final_usable_persona_count` = `3`
- `production_ready_persona_count` = `3`
- `review_ready_persona_count` = `1`
- `weak_source_cost_center_count` = `4`
- `core_readiness_weak_source_cost_center_count` = `3`
- `exploratory_only_weak_source_debt_count` = `1`
- `effective_balanced_source_count` = `5.89`
- `persona_core_coverage_of_all_labeled_pct` = `74.5`
- `largest_source_influence_share_pct` = `30.7`
- `top_3_cluster_share_of_core_labeled` = `0.8329`
- `promoted_persona_example_coverage_pct` = `100.0`
- `overall_unknown_ratio` = `0.139261`
- `remaining_core_weak_sources` = `['google_developer_forums', 'domo_community_forum', 'adobe_analytics_community']`
- `persona_04_readiness_tier` = `review_ready_persona`
- `persona_05_readiness_tier` = `blocked_or_constrained_candidate`

Persona invariants:
- `persona_04` remains `review_ready_persona`.
- `persona_05` remains `blocked_or_constrained_candidate`.

## Failed Removal Lesson

- Removing Adobe / Google / Domo improved corpus cleanliness but damaged source balance.
- In the narrowed-corpus state, `effective_balanced_source_count` fell to `4.86` and readiness regressed to `FAIL / UNSTABLE / exploratory_only`.
- Broad source deletion is therefore not a safe primary deck-ready path.
- Future deck-ready work must preserve source balance rather than trading it away for cleanliness.

## Blocker Table

- `core_readiness_weak_source_cost_center_count`: current `3` vs deck-ready threshold `max 1`; blocker `hard_fail`; cause: Three current core-readiness weak sources remain, and both Adobe and Google source-specific remediation attempts ended as no-op while Domo is only exploratory-edge.
- `persona_core_coverage_of_all_labeled_pct`: current `74.5` vs deck-ready threshold `80.0`; blocker `hard_fail`; cause: 695 additional persona-core rows are needed, and the largest non-core pools are concentrated in mixed-quality sources such as Domo, Stack Overflow, Google, and Adobe.
- `effective_balanced_source_count`: current `5.89` vs deck-ready threshold `6.0`; blocker `warning`; cause: Healthy high-volume sources dominate influence, while currently weak supporting sources still provide needed long-tail diversity.
- `weak_source_cost_center_count`: current `4` vs deck-ready threshold `diagnostic only`; blocker `warning`; cause: Four visible weak sources remain in diagnostics, although only three still drive core-readiness pressure.
- `largest_source_influence_share_pct`: current `30.7` vs deck-ready threshold `35.0`; blocker `pass`; cause: Current source concentration is elevated but still inside deck-ready tolerance.
- `top_3_cluster_share_of_core_labeled`: current `0.8329` vs deck-ready threshold `current quality pass under <=3 final-usable persona carve-out`; blocker `pass`; cause: Top-persona concentration is acceptable given the current small final-usable set.
- `overall_unknown_ratio`: current `0.139261` vs deck-ready threshold `0.2`; blocker `pass`; cause: Unknown-rate control remains healthy.
- `final_usable_persona_count`: current `3` vs deck-ready threshold `3`; blocker `pass`; cause: Current promoted/final-usable set already meets the deck-ready count floor.
- `promoted_persona_example_coverage_pct`: current `100.0` vs deck-ready threshold `100.0`; blocker `pass`; cause: Grounding coverage is already complete for promoted personas.

## True Bottleneck

- Selected bottleneck: `source-tier policy mismatch`.
- Why: The current reviewable corpus relies on Adobe, Google, and Domo to sustain source balance, but those same sources are not core-representative enough to anchor deck-ready claims and safe source-specific fixes have already stalled.
- Secondary constraint: persona_core_coverage gap remains large at 695 rows to reach 80.0
- Secondary constraint: effective source balance is close to the floor numerically but fragile to source removal
- Secondary constraint: core weak-source debt remains at 3 and cannot be cleared through already-tried safe remediations

## Path Comparison

- `A_targeted_google_valid_filter_remediation`: likely moves google valid retention and maybe grounded contribution; upside `low`; junk risk `medium-high`; source-balance regression risk `low`; preserves reviewable `True`; deck-ready help `False`. Already ended as a no-op because the remaining false-negative pocket was too mixed with noise.
- `B_targeted_adobe_fidelity_remediation`: likely moves Adobe episode yield and labeled contribution; upside `low`; junk risk `high`; source-balance regression risk `low`; preserves reviewable `True`; deck-ready help `False`. Already ended as a no-op because no repeated fixable Adobe-only fidelity bug was found.
- `C_domo_exploratory_exclusion_or_tiering_only`: likely moves core weak-source pressure and core coverage denominator; upside `medium`; junk risk `low`; source-balance regression risk `high`; preserves reviewable `False`; deck-ready help `False`. Broad source deletion already proved that removing balance-supporting weak sources can push readiness back to exploratory_only.
- `D_source_volume_normalization_simulation`: likely moves effective_balanced_source_count; upside `low-medium`; junk risk `low`; source-balance regression risk `none`; preserves reviewable `True`; deck-ready help `False`. Could clear the 6.0 floor numerically, but it would not solve core weak-source debt or the 80.0 core-coverage requirement and may hide real concentration risk.
- `E_add_or_strengthen_representative_sources`: likely moves effective balance, source concentration resilience, and future core-coverage recovery opportunity; upside `high`; junk risk `medium`; source-balance regression risk `low`; preserves reviewable `True`; deck-ready help `True`. Adds the one thing the current corpus lacks: more representative sources that can improve diversity without depending on weak supporting sources.
- `F_core_coverage_recovery_from_high_quality_non_core_rows`: likely moves persona_core_coverage_of_all_labeled_pct; upside `medium`; junk risk `high`; source-balance regression risk `none`; preserves reviewable `True`; deck-ready help `False`. Need 695 additional core rows, and the recoverable pool is concentrated in mixed-quality sources; this does not address the core weak-source count.
- `G_deck_ready_policy_redesign`: likely moves whether deck-ready core membership and weak-source applicability reflect methodological representativeness instead of the full reviewable corpus; upside `high`; junk risk `low`; source-balance regression risk `low`; preserves reviewable `True`; deck-ready help `True`. This is the shortest structural path because current balance still depends on sources that should not necessarily define deck-ready core claims.
- `H_freeze_at_reviewable_release`: likely moves none; upside `stability only`; junk risk `none`; source-balance regression risk `none`; preserves reviewable `True`; deck-ready help `False`. Safest if no structural deck-ready work is approved, but it leaves the current ceiling in place.

## Recommended Next Action

- Recommended next action: `deck_ready_policy_redesign_spec`.
- Why this one: It is the only remaining path that directly addresses the current source-tier mismatch without repeating failed Adobe/Google remediation attempts, without deleting sources again, and without hiding the source-balance lesson learned from the rollback.
- Deck-ready still realistic: `True`.
- Reviewable should remain the current ceiling until that next action is approved: `True`.

## Validation

- Regenerated baseline commands: `06_cluster_and_score.py`, `07_export_xlsx.py`, `17_analysis_snapshot.py --compare-latest`, `33_review_ready_gap_analysis.py`, `35_source_representativeness_audit.py`.
- Tests passed: `tests.test_analysis_snapshot_cli`, `tests.test_source_representativeness_audit`, `tests.test_workbook_export`.
- Test failure to note: `tests.test_review_ready_gap_analysis` still hardcodes `current_persona_core_rows = 9444`, while the regenerated live artifact is `9445`. No code change was made in this diagnosis pass.
