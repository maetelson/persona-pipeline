"""Source diagnostics grain policy for the persona workbook."""

# Source Diagnostics Grain Policy

`source_diagnostics` no longer presents one source row as a single linear funnel. The old sheet mixed raw-record rows, post rows, and episode rows in adjacent columns, which made values like `episode_survival_rate = 123.5` look like a broken funnel even though the real issue was mixed grain math.

## Grain model

- `other`
  - `raw_record_count`
  - Counts raw JSONL rows under `data/raw/{source}/*.jsonl`
  - This is ingest volume, not guaranteed to be the same entity as normalized posts
- `post`
  - `normalized_post_count`
  - `valid_post_count`
  - `prefiltered_valid_post_count`
  - `valid_posts_per_normalized_post_pct`
  - `prefiltered_valid_posts_per_valid_post_pct`
  - These are same-grain post funnel metrics
- `episode`
  - `episode_count`
  - `labeled_episode_count`
  - `labelable_episode_count`
  - `labeled_episodes_per_episode_pct`
  - `labelable_episodes_per_labeled_episode_pct`
  - These are same-grain episode funnel metrics
- `mixed_grain_bridge`
  - `episodes_per_prefiltered_valid_post`
  - `labeled_episodes_per_prefiltered_valid_post`
  - `labelable_episodes_per_prefiltered_valid_post`
  - These are cross-grain bridge metrics between post rows and episode rows
  - They can exceed `1.0` and must not be named `rate`, `share`, or `survival`

## Why the old version was misleading

- `episode_count / prefiltered_valid_count` compared episode rows against post rows but was named like a funnel survival rate.
- `labelable_count` and `labeled_count` were both episode-level counts, but they were shown in the wrong order for a funnel because `labeled_count` included low-signal labeled rows.
- The old wide table visually implied a single stage pipeline even when adjacent columns came from different entities.

## New workbook contract

- Every `source_diagnostics` row now carries:
  - `section`
  - `grain`
  - `metric_name`
  - `metric_type`
  - `denominator_metric`
  - `denominator_grain`
  - `metric_definition`
- Same-grain percentages are bounded to `0-100`.
- Mixed-grain bridge metrics use explicit `*_per_*` naming and are not treated as rates.
- Validation fails workbook generation if legacy ambiguous columns or mixed-grain rate-like names reappear.
