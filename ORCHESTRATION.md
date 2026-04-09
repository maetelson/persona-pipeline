# Orchestration

## Pipeline order

Recommended:

0. `run/00_run_all.py`

Expanded sequence used by `00_run_all.py`:

1. `run/00_generate_time_slices.py`
2. `run/01_collect_all.py`
3. `run/01_5_expand_queries_from_raw.py`
4. `run/02_normalize_all.py`
5. `run/02.5_filter_time_window.py`
6. `run/03_filter_valid.py`
7. `run/04_build_episodes.py`
8. `run/05_label_episodes.py`
9. `run/06_1_discover_persona_axes.py`
10. `run/06_cluster_and_score.py`
11. `run/07_export_xlsx.py`

## Stage boundaries

### Time slices

- input: `config/time_window.yaml`
- output: `data/analysis/time_slices.parquet`
- default recent-5y slices:
  - `2021-04-01 ~ 2022-12-31`
  - `2023-01-01 ~ 2024-12-31`
  - `2025-01-01 ~ 2026-04-30`
- source overrides may switch to monthly or quarterly slicing

### Collect

- input: source config only
- output: `data/raw/{source}/raw.jsonl`
- no invalid filtering
- no dedupe
- no episode segmentation
- query design assets now exist in:
  - `config/query_seed_taxonomy.yaml`
  - `config/query_expansion_rules.yaml`
  - `config/query_map.yaml`
- query design is now centered on role × problem × tool × work moment combinations
- source applicability in the current query map is explicitly defined for:
  - `reddit`
  - `stackoverflow`
  - `github_discussions`
- current collectors consume the priority seed inventory / operational query map from `config/query_map.yaml`
- active live query count currently used by collectors: `54`
  - `reddit`: `41`
  - `stackoverflow`: `43`
  - `github_discussions`: `27`
- `query_text` should prioritize pain phrases, trust phrases, and workflow phrases over broad head terms
- broad head terms belong primarily in taxonomy and expansion review, not as standalone live seeds
- collection loop is `query × time window × page`
- source config still controls pagination and slice size
- each raw row stores:
  - `window_id`
  - `window_start`
  - `window_end`
- Reddit uses a combined recent-5y collection window in practice because the public endpoint does not reliably support exact multi-year slicing.
- Reddit date enforcement therefore happens conservatively in `02.5_filter_time_window.py`.
- collection audit output includes:
  - `raw_audit.parquet`
  - `raw_page_audit.parquet`
  - `raw_query_window_summary.parquet`
  - `raw_query_window_matrix.parquet`
  - `raw_low_yield_queries.parquet`
  - `raw_error_audit.parquet`
  - `raw_error_summary.parquet`

### Query expansion

- input: first-pass raw jsonl, with normalized fallback
- output:
  - `data/analysis/query_term_frequency.parquet`
  - `data/analysis/query_expansion_candidates.parquet`
- purpose:
  - surface repeated noun phrases
  - surface tool co-mentions
  - surface pain phrases
  - surface stakeholder/reporting phrases
- also surface validation/trust language and spreadsheet/manual-work language
- exploratory only
- does not auto-merge back into `config/query_map.yaml`
- intended flow:
  - taxonomy and expansion rule review
  - priority seed query review and operational query check
  - 1st-pass raw collection
  - raw-driven 2nd-pass candidate extraction
  - human review
  - query map update
  - recollect

### Normalize

- input: raw jsonl
- output: `data/normalized/normalized_posts.parquet`
- common schema only

### Time window

- input: normalized posts
- output:
  - `time_filtered_posts.parquet`
  - `time_window_invalid.parquet`
  - `missing_created_at.parquet`
- filtering uses `created_at` again after normalize
- rows without `created_at` are split into a separate missing-date log

### Valid filter

- input: time-filtered posts
- output:
  - `valid_candidates.parquet`
  - `invalid_candidates.parquet`
- business/pain signal gate
- default filter mode is `collection` for raw expansion work; switch to `analysis` for stricter review
- invalid reason required
- downstream loss audit includes source-level valid/invalid ratios

### Episode build

- input: valid candidates
- output: `episode_table.parquet`
- audit: `episode_audit.parquet`
- threshold audit: `data/analysis/pipeline_threshold_audit.parquet`
- one post may create multiple episodes
- conservative segmentation:
  - split only when question type, bottleneck, tool, collaborator, and output meaningfully shift
  - default rule requires at least two meaningful signature changes before creating a new episode
  - boilerplate, promo, duplicate notices, and repeated filler should not create new episodes
  - monitor `avg_episodes_per_post` to catch over-segmentation
- threshold behavior:
  - exploratory profile: warn and continue
  - scaling profile with strict gate: fail can stop this stage

### Label

- input: episode table
- output:
  - `labeled_episodes.parquet`
  - `label_audit.parquet`
  - `llm_label_audit.parquet`
- threshold audit: `data/analysis/pipeline_threshold_audit.parquet`
- threshold behavior:
  - exploratory profile: high unknown ratio or low coverage becomes warning/caution
  - scaling profile with strict gate: fail can stop this stage

### Analysis

- persona-axis discovery runs before persona generation
- inputs:
  - `data/labeled/labeled_episodes.parquet`
  - `data/episodes/episode_table.parquet`
- outputs:
  - `data/analysis/axis_candidates.csv`
  - `data/analysis/axis_candidates.parquet`
  - `data/analysis/final_axis_schema.json`
  - `data/analysis/persona_axis_discovery.json`
- purpose:
  - identify the right persona axes from repeated workflow context, bottlenecks, trust needs, tool dependence, and output expectations
  - avoid consumer-style demographic axes unless strongly supported by the corpus

- input: labeled episodes
- output:
  - `cluster_summary.parquet`
  - `persona_candidates.parquet`
  - `priority_scores.parquet`
- threshold audit: `data/analysis/pipeline_threshold_audit.parquet`
- exploratory profile:
  - clustering/persona can still run below recommendation
  - outputs must carry `exploratory_only=true` and lower reliability flags when thresholds are weak
- scaling profile:
  - strict cluster gate can skip cluster/persona generation when readiness is below threshold

### Export

- input: parquet artifacts from prior stages
- output: `persona_pipeline_output.xlsx`

## Re-run behavior

- all stages overwrite their own output files
- no database migration or server state required
- safe to rerun from any stage if upstream files are already present
