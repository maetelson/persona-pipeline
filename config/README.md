# Config Guide

Use this folder when you need to change collection scope, filtering behavior, episode rules, or workbook policy without rewriting code.

## Start Here

- High-frequency edits:
  - `sources/`: enable or tune one source at a time
  - `query_map.yaml`: active live query inventory
  - `relevance_rules.yaml`: keep/borderline/drop behavior
  - `invalid_rules.yaml`: invalid-signal filtering
  - `segmentation_rules.yaml`: episode split thresholds
  - `labeling_policy.yaml`: labeling and LLM guidance
  - `time_window.yaml`: collection date windows

- Lower-frequency edits:
  - `codebook.yaml`: label definitions
  - `scoring.yaml`: persona scoring weights
  - `pipeline_thresholds.yaml`: readiness and warning thresholds
  - `export_schema.yaml`: final workbook sheet/column contract
  - `axis_reduction.yaml`, `bottleneck_clustering.yaml`, `example_selection.yaml`, `persona_messaging.yaml`, `rebalancing.yaml`

## Folder Map

- `sources/`: one YAML per source or source variant
- `seeds/`: curated query/seed inputs grouped by source family
- Pipeline rules:
  - `invalid_rules.yaml`
  - `relevance_rules.yaml`
  - `segmentation_rules.yaml`
  - `labeling_policy.yaml`
  - `time_window.yaml`
- Analysis/export rules:
  - `codebook.yaml`
  - `scoring.yaml`
  - `pipeline_thresholds.yaml`
  - `export_schema.yaml`
  - persona/axis/clustering support configs

## Notes

- Keep configs split by responsibility; this repo intentionally avoids one giant YAML.
- Ignore `__pycache__/` and runtime artifact folders when judging structure complexity.
