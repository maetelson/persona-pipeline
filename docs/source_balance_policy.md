# Source Balance Policy

## Why this exists

Source breadth alone was overstating health.

The old workbook could report `source_diversity_status = OK` when many sources produced some labeled rows, even if one source still dominated the evidence that actually shaped promoted and grounded personas.

This policy makes source balance auditable across the full local pipeline:

raw -> normalized -> valid -> prefiltered valid -> episodes -> labelable -> labeled -> promoted persona contribution -> grounded persona contribution

## What counts as balance

Source balance is evaluated from three downstream share signals per source:

- labeled episode share
- promoted persona episode share
- grounded promoted persona episode share

These three shares are averaged into a blended influence share. The workbook now uses that blended share to compute:

- `effective_balanced_source_count`
- `largest_source_influence_share_pct`

This means a source can still be large, but it will only look balanced if other sources materially contribute to promoted and grounded persona evidence.

## What this policy does not do

- It does not cap dominant sources arbitrarily.
- It does not fake balance by trimming a strong source out of the workbook.
- It does not treat raw collection volume as equivalent to grounded persona contribution.

If a source dominates because it is genuinely the strongest source, the workbook shows that dominance explicitly and asks for diversification elsewhere.

## Weak-source cost centers

A weak-source cost center is a source with meaningful raw input volume that still fails to become a meaningful downstream contributor.

Current policy classifies a source as a weak-source cost center when all of these are true:

- it has at least 100 raw records
- its blended downstream influence share stays below 10%
- it also collapses on at least one weak-yield symptom:
  - low prefilter retention
  - low episode yield
  - low labelability coverage
  - zero grounded promoted persona contribution

These sources are surfaced through:

- `weak_source_cost_center_count`
- `weak_source_cost_centers`
- `source_balance_audit.csv`

## Collapse-stage diagnosis

Each source is mapped to one dominant collapse stage from its top diagnostic reason:

- `collection_or_normalization`
- `valid_filtering`
- `relevance_prefilter`
- `episode_yield`
- `labelability`
- `persona_contribution`
- `overdominant_source_risk`
- `low_yield_source_waste`

This keeps source tuning targeted instead of generic.

## Policy actions

The source balance audit assigns one next action per source:

- `diversify_other_sources_before_scaling_this_source`
- `tune_source_seeds_and_prefilter_rules`
- `review_source_specific_valid_filtering`
- `tighten_episode_segmentation_for_source`
- `tighten_labelability_and_source_scope`
- `expand_grounded_persona_coverage_before_promotion`
- `raise_targeted_collection_on_underrepresented_source`
- `monitor_source`

These actions are diagnostic only. They are there to make reruns and source tuning explicit.

## Symptom vs root cause vs remediation

The audit should be read in three layers:

1. symptom
   - `collapse_stage`
   - `failure_reason_top`
   - `source_balance_status`
2. root cause
   - `root_cause_category`
   - `likely_false_negative_pattern`
3. remediation
   - `recommended_config_change`
   - `required_regression_check`
   - `owner_action_type`
   - `evidence_to_inspect`
   - `can_auto_tune`
   - `must_manual_review`

The old failure mode was stopping at symptom-level TODO text such as “review source-specific filtering”.
The new contract is: every `fix_now` source must point to either a concrete code/config path to change next or an explicit manual-review artifact that must be inspected before tuning.

## Choosing the right remediation path

Use `root_cause_category` before deciding what to edit:

- `valid_filter_*`
  - inspect `data/valid/invalid_candidates_with_prefilter.parquet`
  - adjust source-aware rules in `src/filters/invalid_filter.py`
- `relevance_prefilter_*`
  - inspect `data/prefilter/relevance_drop.parquet`
  - adjust source-aware rescue terms in `src/filters/relevance.py`
- `episode_segmentation_under_split` or `labelability_quality_gate_too_strict`
  - inspect `data/episodes/episode_debug.parquet` and `data/episodes/episode_audit.parquet`
  - adjust source-specific logic in `src/episodes/builder.py`
- `time_window_freshness_policy`
  - inspect invalid candidates for timestamp distribution first
  - only then consider changing `config/time_window.yaml`
- `grounding_contribution_gap`
  - inspect persona grounding artifacts before changing promotion or example selection logic

Healthy or monitor-only sources should not receive fix-now remediation unless a later rerun introduces a regression.

## What done means

A source-scoped remediation is done only when:

- the targeted source metric for the diagnosed root cause improves
- no healthy source regresses into watchlist or weak-source status
- the rerun audit output changes from generic review language to a concrete remediation state with the expected code/config path and regression target

## Workbook gates

The centralized quality policy uses these source-balance thresholds:

- `effective_balanced_source_count`
  - `WARN` below `6.0`
  - `FAIL` below `5.0`
- `largest_source_influence_share_pct`
  - `WARN` at or above `35.0`
  - `FAIL` at or above `45.0`
- `weak_source_cost_center_count`
  - `WARN` at or above `2`
  - `FAIL` at or above `4`

These gates intentionally combine over-dominant source risk with weak-source waste.

## Output artifacts

The policy is visible in these artifacts:

- `data/analysis/source_diagnostics.csv`
- `data/analysis/source_balance_audit.csv`
- `data/analysis/quality_checks.csv`
- final workbook `source_diagnostics` and `quality_checks` sheets

## Execution order

The policy depends on the standard pipeline order:

1. collect raw data
2. normalize
3. filter valid
4. prefilter relevance
5. build episodes
6. label episodes
7. analyze personas
8. export workbook

## Expected usage

Use this policy when deciding whether to:

- tune source-specific seeds
- tighten source-specific prefilter rules
- review valid-filter rules for a weak source
- improve episode segmentation for a source
- expand grounded examples for a source that only reaches review-visible personas
- leave a dominant source untouched but explicitly diversify the rest of the evidence base
