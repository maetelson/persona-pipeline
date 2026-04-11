# Source Diagnostics Reason Policy

`source_diagnostics` should explain the strongest actionable bottleneck for each source, not merely confirm that labeled output exists.

## Rule intent

- Keep source-stage math grain-correct.
- Use same-grain post and episode funnel metrics for retention judgments.
- Use cross-grain bridge metrics only for source diagnosis, not as implied funnel survival rates.
- Emit exactly one `top_failure_reason` and one `failure_level` per source so workbook readers can quickly identify the dominant issue.

## Ranked diagnostic reasons

The current rule order is intentionally root-cause first:

1. `no_raw_records`
   - Source has no raw JSONL rows.
2. `raw_not_normalized`
   - Raw rows exist but no normalized post rows were produced.
3. `low_valid_post_retention: <invalid_reason>`
   - Too few normalized posts survive invalid filtering.
   - Uses the dominant `invalid_reason` when available.
4. `low_prefilter_retention: <prefilter_reason>`
   - Too few valid posts survive relevance prefiltering.
   - Uses the dominant `prefilter_reason` when available.
5. `low_episode_yield`
   - Too few episodes are produced per retained prefiltered post.
6. `label_output_missing_after_episode_build`
   - Episodes exist but labeled output is missing.
7. `low_labelable_episode_ratio`
   - Too few labeled episodes remain labelable/borderline after labeling audit.
8. `zero_promoted_persona_contribution`
   - The source contributes labeled evidence but contributes zero promoted persona episodes.
9. `concentration_risk_contribution`
   - The source contributes a dominant share of labeled or promoted evidence, creating concentration risk.
10. `weak_diversity_contribution`
   - The source contributes some evidence but remains a weak fractional source-diversity contributor.
11. `healthy_source_contribution`
   - No stronger bottleneck or concentration warning was detected.

## Severity levels

- `failure`
  - A stage bottleneck or concentration condition is severe enough to materially weaken the source.
- `warning`
  - The source remains usable for review, but a real bottleneck or contribution risk exists.
- `pass`
  - No stronger source-specific issue is currently detected.

## Intervention policy

- `recommended_seed_set` is populated only when the top reason is `low_prefilter_retention: ...`
- The field is blank unless a local source seed file exists under `config/seeds/**/<source>.yaml`.
- No generic placeholder recommendation should be emitted when the repository does not contain a real seed intervention for that source.

## Workbook interpretation

- `top_failure_reason` should describe why a source is weak or risky.
- `failure_level` should describe how urgent the issue is.
- Strong sources may still show `concentration_risk_contribution` when they dominate the evidence base.
- Small but otherwise healthy sources may show `weak_diversity_contribution` even when their same-grain funnels look healthy.