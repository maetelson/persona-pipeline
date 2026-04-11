# Persona Workbook Denominator Policy

Workbook summary metrics must declare denominator semantics explicitly.

Supported denominator types:

- `labeled_episode_rows`
  - all labeled episode rows written by `05_label_episodes.py`
- `persona_core_labeled_rows`
  - labeled episode rows where `persona_core_eligible = true`
- `promoted_persona_rows`
  - rows assigned to promoted personas only, when a metric is explicitly scoped to promoted personas

Share column naming rules:

- `share_of_all_labeled` must mean denominator `labeled_episode_rows`
- `share_of_core_labeled` must mean denominator `persona_core_labeled_rows`
- `share_of_promoted_persona_rows` must mean denominator `promoted_persona_rows`
- `share_of_total` is forbidden in workbook-facing sheets because it hides denominator meaning

Current workbook policy:

- `cluster_stats` and `persona_summary` describe persona clusters built from persona-core rows
- those sheets therefore use `share_of_core_labeled` as the primary cluster share
- they may also include `share_of_all_labeled` as additional context, but that is secondary and explicitly named
- `quality_checks.largest_cluster_share_of_core_labeled` must use the same denominator as `cluster_stats.share_of_core_labeled`
