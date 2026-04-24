# Runbook

## Execution discipline

- Treat downstream pipeline stages as dependency-sensitive by default.
- Never launch dependent stages such as `03 -> 03.5 -> 04 -> 05 -> 06.1 -> 06 -> 07` through `multi_tool_use.parallel` or any other parallel wrapper.
- Safe parallelism is limited to:
  - read-only inspection
  - unit tests
  - source-specific raw collection that writes to disjoint `data/raw/{source}/` paths
- Good example:

```bash
python run/pipeline/03_filter_valid.py
python run/pipeline/03_5_prefilter_relevance.py
python run/pipeline/04_build_episodes.py
python run/pipeline/05_label_episodes.py
python run/pipeline/06_1_discover_persona_axes.py
python run/pipeline/06_cluster_and_score.py
python run/cli/17_analysis_snapshot.py --compare-latest
```

- Bad example:

```text
do not launch 03_filter_valid.py and 03_5_prefilter_relevance.py together in a parallel wrapper
do not launch 04_build_episodes.py, 05_label_episodes.py, and 06_cluster_and_score.py together in a parallel wrapper
```

## 1. 설치

```bash
pip install -r requirements.txt
```

## 2. 필수/권장 환경변수

필수:

- `REDDIT_USER_AGENT` for Reddit live collection

권장:

- `STACKEXCHANGE_KEY`
- `GITHUB_TOKEN`
- `OPENAI_API_KEY` when enabling LLM labeling
- `LLM_MODEL` or `OPENAI_MODEL` when enabling LLM labeling

PowerShell 예시:

```powershell
$env:REDDIT_USER_AGENT="persona-pipeline/0.1 (by /u/your_reddit_username)"
$env:REDDIT_SEARCH_LIMIT="10"
$env:REDDIT_COMMENT_LIMIT="20"
$env:STACKOVERFLOW_PAGE_SIZE="10"
$env:STACKOVERFLOW_MAX_ANSWERS="5"
$env:STACKOVERFLOW_MAX_COMMENTS="10"
$env:GITHUB_TOKEN=""
$env:GITHUB_ISSUES_PER_QUERY="5"
$env:GITHUB_COMMENTS_PER_ITEM="10"
$env:GITHUB_DISCUSSION_REPLIES_PER_COMMENT="5"
$env:ENABLE_LLM_LABELER="true"
$env:LLM_DRY_RUN="false"
$env:LLM_MODEL="gpt-5.4-mini"
$env:OPENAI_API_KEY="your_api_key"
```

## 3. 실제 실행 순서

한 번에 전체 실행:

```bash
python run/pipeline/00_run_all.py
```

```bash
python run/pipeline/00_generate_time_slices.py
```

수동으로 단계 실행:

```bash
python run/pipeline/00_generate_time_slices.py
python run/pipeline/01_collect_all.py
python run/pipeline/01_5_expand_queries_from_raw.py
python run/pipeline/02_normalize_all.py
python run/pipeline/02.5_filter_time_window.py
python run/pipeline/03_filter_valid.py
python run/pipeline/03_5_prefilter_relevance.py
python run/pipeline/04_build_episodes.py
python run/pipeline/05_label_episodes.py
python run/pipeline/06_1_discover_persona_axes.py
python run/pipeline/06_cluster_and_score.py
python run/pipeline/07_export_xlsx.py
```

개발용 기본 검증 루프:

```bash
python run/pipeline/03_filter_valid.py
python run/pipeline/03_5_prefilter_relevance.py
python run/pipeline/04_build_episodes.py
python run/pipeline/05_label_episodes.py
python run/pipeline/06_1_discover_persona_axes.py
python run/pipeline/06_cluster_and_score.py
python run/cli/17_analysis_snapshot.py --compare-latest
```

- 이 경로는 `xlsx` 없이 canonical workbook bundle과 analysis artifacts만으로 품질 변화를 확인하는 개발용 표준 루프입니다.
- `07_export_xlsx.py`는 최종 deliverable 확인, export-layer 변경, workbook-facing semantics 변경 때만 기본적으로 요구됩니다.

Reddit collection policy diagnostics:

```bash
python run/diagnostics/19_analyze_reddit_retention.py
```

- Aggregate `reddit` uses curated source-config seeds, subreddit targeting, and collector-side low-intent skips.
- The retention command writes per-seed, per-subreddit, and seed-by-subreddit retention artifacts under `data/analysis/`.
- It also writes Reddit policy audit artifacts that show the configured policy and the latest observed early-stop/runtime counters.

### Source retirement note

- Current query-map snapshot: `161` priority seed queries and `68` active expanded queries.
- If a source is retired, remove it from `config/sources/` first and then clean up stale `data/raw/{source}/` folders as a separate manual step.
- Workbook source sheets now require both an enabled source config and downstream evidence, so raw-only stale folders no longer appear in export output.

## 3.5 Query 설계 파일

- `config/query_seed_taxonomy.yaml`
  - broad head term, role, problem, tool, work moment, workflow, trust, segmentation, output vocabulary bank
- `config/query_expansion_rules.yaml`
  - priority 조합, source 적용 규칙, phrase template, demote/hard exclude/review bucket 규칙
- `config/query_map.yaml`
  - 실제 priority seed inventory 169개 / operational live query map 54개
  - 각 `query_text`는 broad term보다 pain phrase를 우선 반영한 live retrieval seed
  - broad head term은 taxonomy vocabulary로 남기고, query map은 pain/trust/workflow phrase 중심으로 유지

현재 priority seed 요약:

| Source | Applicable seeds |
|---|---:|
| Reddit | 31 |
| Stack Overflow | 43 |
| GitHub Discussions | 14 |

중요:
- active collector는 이제 `config/query_map.yaml`을 source applicability 기준으로 순회합니다.
- query는 broad head term보다 pain phrase, trust phrase, workflow phrase를 먼저 반영합니다.
- broad head term은 taxonomy에서 관리하고, live seed는 노이즈를 줄이기 위해 pain 중심으로 유지합니다.
- source config는 page 수, time slice 크기, sleep 같은 실행 파라미터를 제어합니다.
- `01_5_expand_queries_from_raw.py`는 1차 raw에서 2차 query 후보를 추출하지만 `query_map.yaml`에 자동 병합하지는 않습니다.
- 원칙: taxonomy와 expansion rules는 풍부하게, 실제 `query_text`는 짧고 검토 가능한 pain-oriented seed로 유지합니다.
- 현재 taxonomy 기준 축:
  - role: analyst, marketer, manager, strategy planner, ops planner, business user
  - problem: why did it drop, validate numbers, metric mismatch, reporting bottleneck, segment comparison, stakeholder explanation, ad hoc analysis, manual spreadsheet work, root cause analysis, automation gap
  - tool: Excel, spreadsheet, dashboard, BI, SQL, data warehouse, data team, report
  - work moment: weekly reporting, monthly reporting, validation, anomaly triage, ad hoc request, automation handoff

## 3.6 Time slicing

공통 time window는 [time_window.yaml](C:\Users\hands\OneDrive\Desktop\persona\config\time_window.yaml)에서 관리합니다.

기본 최근 5년 slice:
- `recent_5y_01`: `2021-04-01 ~ 2022-12-31`
- `recent_5y_02`: `2023-01-01 ~ 2024-12-31`
- `recent_5y_03`: `2025-01-01 ~ 2026-04-30`

source별 finer slicing 지원:
- `explicit`: config에 선언된 slice 사용
- `quarterly`: 분기 단위 분할
- `monthly`: 월 단위 분할

생성 결과:
- `data/analysis/time_slices.parquet`

## 3.7 Pipeline thresholds

- config: `config/pipeline_thresholds.yaml`
- default active profile: `exploratory`
- `exploratory` profile:
  - small raw/valid/episode volume에서도 가능한 한 다음 단계로 진행
  - pass/warn/fail를 모두 남기되 기본 gate는 `warn`
  - cluster/persona가 생성돼도 `exploratory_only=true`일 수 있음
- `scaling` profile:
  - raw/valid/episode가 충분히 쌓였을 때 사용
  - gate 기본값이 `strict`
  - episode/labeling fail은 stage stop 가능
  - cluster readiness fail은 cluster/persona skip 가능
- audit output:
  - `data/analysis/pipeline_threshold_audit.parquet`
  - `data/analysis/pipeline_threshold_audit.csv`

## 4. 단계별 설명

### `00_generate_time_slices.py`

- enabled source별 time slice plan 생성
- 출력: `data/analysis/time_slices.parquet`
- collector 실행 전에 어떤 window가 적용되는지 확인 가능

### `01_collect_all.py`

- source별 collector 실행
- raw는 `data/raw/{source}/raw.jsonl`
- raw 단계에서는 invalid 판단, dedupe, segmentation 금지
- active collector query 입력은 `config/query_map.yaml`
- 수집 단위는 `query × time window × page`
- source yaml은 query inventory 자체가 아니라 실행 파라미터 역할
- raw row에는 `window_id`, `window_start`, `window_end` 포함
- Reddit는 public search의 제약 때문에 exact multi-year slice 대신 combined 5-year window로 수집하고, 실제 날짜 컷은 `02.5_filter_time_window.py`에서 다시 적용합니다.
- audit 출력:
  - `data/analysis/raw_audit.parquet`: source별 raw 개수와 최상위 status
  - `data/analysis/raw_page_audit.parquet`: query/window/page별 raw count와 stop reason
  - `data/analysis/raw_query_window_summary.parquet`: query/window별 누적 raw count
  - `data/analysis/raw_query_window_matrix.parquet`: source × query × time slice matrix
  - `data/analysis/raw_low_yield_queries.parquet`: zero/near-zero yield query-window audit
  - `data/analysis/raw_error_audit.parquet`: query/window/page별 세부 error row
  - `data/analysis/raw_error_summary.parquet`: error code/type 기준 집계 요약

### `01_5_expand_queries_from_raw.py`

- 1차 수집 raw에서 반복 표현 추출
- 입력: `data/raw/{source}/raw.jsonl` 우선, 없으면 normalized fallback
- 출력:
  - `data/analysis/query_term_frequency.parquet`
  - `data/analysis/query_expansion_candidates.parquet`
- 추출 대상:
  - 반복 noun phrase
  - tool co-mentions
  - pain phrase
  - stakeholder/reporting phrase
  - validation/trust phrase
  - spreadsheet/manual-work phrase
- 목적:
  - 2차 query 후보를 사람이 검토할 수 있게 남김
  - canonical query map 자동 수정은 하지 않음
- 권장 운영 흐름:
  - 1차 query taxonomy 작성
  - 1차 raw 수집
  - raw 기반 2차 expansion candidate 생성
  - 사람이 후보 검토
  - query map 보강 후 재수집

### `02_normalize_all.py`

- raw를 공통 `normalized_posts` 스키마로 변환
- 출력: `data/normalized/normalized_posts.parquet`

### `02.5_filter_time_window.py`

- `config/time_window.yaml` 기준 기간 필터
- 출력:
  - `data/normalized/time_filtered_posts.parquet`
  - `data/normalized/time_window_invalid.parquet`
  - `data/normalized/missing_created_at.parquet`
- `created_at`이 없으면 `missing_created_at`
- normalize 이후에도 `created_at` 기준으로 2차 time filter 수행

### `03_filter_valid.py`

- business signal + pain signal 기반 valid/invalid 분기
- 기본값은 raw 확대 단계용 `collection` mode
- 필요 시 `VALID_FILTER_MODE=analysis`로 더 엄격하게 전환
- tutorial / syntax / promo / homework / career-advice 제거
- duplicate는 invalid로 이동
- 출력:
  - `data/valid/valid_candidates.parquet`
  - `data/valid/invalid_candidates.parquet`
  - `data/valid/downstream_loss_audit.parquet`
  - `data/valid/invalid_reason_audit.parquet`

### `04_build_episodes.py`

- post 1개를 여러 episode로 분절 가능
- 새 episode 조건:
  - 질문 유형 변화
  - 병목 변화
  - 사용 도구 변화
  - 협업 상대 변화
  - 기대 산출물 변화
- 과분절 방지:
  - 질문/병목/도구/협업상대/산출물 중 의미 있는 변화가 최소 2개 이상일 때만 기본적으로 분리
  - boilerplate, duplicate notice, welcome/help text, 홍보성 부연은 분절하지 않음
  - 높은 유사도의 인접 블록은 다시 합침
  - 짧은 tail과 comment성 follow-up은 앞 episode에 붙임
- 과소분절 방지:
  - 질문 유형, 핵심 병목, 도구, 협업 상대, 기대 산출물이 실제로 바뀌고 충분히 길면 분리
  - 너무 긴 episode는 다시 나눔
- audit:
  - `data/episodes/episode_audit.parquet`
  - `avg_episodes_per_post`를 source/post 단위로 남김
- threshold checks:
  - `episode_total`
  - `avg_episodes_per_raw_post`
  - `max_episodes_per_raw_post`
  - exploratory profile에서는 fail도 hard stop 대신 caution/warn 중심

### `05_label_episodes.py`

- `codebook.yaml` 기반 rule prelabel
- `unknown`, 낮은 확신, core family(role/question/pain/output) 누락 row는 LLM 대상
- 별도 targeting 함수가 row별 `should_send_to_llm() -> (bool, reason)` 형태로 이유를 남김
- 핵심 family가 이미 명확하고 confidence가 높은 row, review bucket row, 비핵심 family만 비어 있는 row는 rule-only로 끝내 토큰을 절약
- direct mode는 소량 검증용, batch mode는 `data/labeled/llm_batch_requests.jsonl`을 만들어 대량 처리용 입력만 준비
- prompt는 고정 prefix/system prompt와 row-specific payload를 분리하고, 필요한 family codebook만 부분 주입
- 응답은 자유서술이 아니라 짧은 structured JSON만 허용해 출력 토큰을 줄임
- `ENABLE_LLM_LABELER=true` and `LLM_DRY_RUN=false`일 때만 actual-run으로 OpenAI API usage가 발생
- `LLM_DRY_RUN=true`면 실제 API 호출 없이 targeting/audit만 점검
- prompt caching 관련 env:
  - `PROMPT_CACHE_KEY`
  - `PROMPT_CACHE_RETENTION`
- 모델 관련 env:
  - `LLM_MODEL_PRIMARY`
  - `LLM_MODEL_ESCALATION`
  - `ENABLE_LLM_ESCALATION`
  - `MAX_LLM_OUTPUT_TOKENS`
  - `ENABLE_BATCH_LABELING`
- disabled reason은 아래처럼 구체적으로 남김
  - `llm:disabled:feature_flag_off`
  - `llm:disabled:no_api_key`
  - `llm:disabled:no_model`
  - `llm:disabled:dry_run`
  - `llm:disabled:sdk_not_available`
- 실제 호출이 있었는지는 아래로 판단 가능
  - `data/labeled/labeling_audit.parquet`의 `was_llm_called`
  - usage columns (`usage_input_tokens`, `usage_output_tokens`, `usage_total_tokens`)
  - OpenAI project usage/balance 변화
- 출력:
  - `data/labeled/labeled_episodes.parquet`
  - `data/labeled/label_audit.parquet`
  - `data/labeled/labeling_audit.parquet`
  - `data/labeled/llm_label_audit.parquet`
- threshold checks:
  - `labeled_row_total`
  - `unknown_ratio`
  - `role/question/pain/output coverage ratio`
  - `role/question/pain dominant code share`
  - exploratory profile에서는 unknown ratio가 높아도 warning 우선
  - `max_unknown_ratio` 초과 시 `persona reliability low` 경고

### `06_cluster_and_score.py`

- persona generation 전에 labeled corpus에서 persona axis discovery를 먼저 수행하거나, 별도 스크립트 `06_1_discover_persona_axes.py`를 독립 실행할 수 있음
- axis discovery 출력:
  - `data/analysis/axis_candidates.csv`
  - `data/analysis/axis_candidates.parquet`
  - `data/analysis/final_axis_schema.json`
  - `data/analysis/persona_axis_discovery.json`
- axis는 demographic 중심이 아니라 다음과 같은 서비스 적합 축을 우선 탐색:
  - user role
  - workflow stage
  - analysis goal
  - bottleneck type
  - tool dependency mode
  - trust / validation need
  - output expectation
- exploratory cluster/persona/priority 생성
- clustering readiness checks:
  - `labeled_rows_for_clustering`
  - `distinct_role_codes`
  - `distinct_question_codes`
  - `distinct_pain_codes`
- exploratory profile:
  - threshold 미달이어도 cluster/persona 결과를 생성 가능
  - 대신 `exploratory_only`, `cluster_reliability`, `threshold_reason`을 남김
- scaling profile:
  - strict cluster gate면 readiness fail 시 cluster/persona skip
- 출력:
  - `data/analysis/cluster_summary.parquet`
  - `data/analysis/persona_candidates.parquet`
  - `data/analysis/priority_scores.parquet`

### `07_export_xlsx.py`

- 최종 workbook 생성
- `ThresholdAudit` 시트 포함
- `RunMeta`에 `active_profile`, `exploratory_only`, `cluster_reliability` 포함
- 출력: `data/output/persona_pipeline_output.xlsx`
- 개발용 기본 검증에서는 생략 가능
- bundle 기반 검증은 `run/cli/17_analysis_snapshot.py`로 대체 가능

### `17_analysis_snapshot.py`

- `xlsx` 없이 canonical workbook bundle과 analysis artifacts를 읽어 validation snapshot 생성
- 출력:
  - `data/analysis/validation_snapshot.json`
  - `data/analysis/validation_snapshot.md`
  - optional `data/analysis/validation_delta.json`
- 목적:
  - readiness / quality / weak source / usable persona / source delta를 machine-readable하게 확인
  - 개발용 rerun에서 `07_export_xlsx.py`를 기본 단계로 강제하지 않음

## 5. Source별 raw 보존 구조

### Reddit

- `source_meta.raw_post`
- `source_meta.raw_comments`
- Aggregate `reddit` is intentionally narrower than before: it prioritizes higher-intent subreddits and problem-oriented seeds, and rejects obvious low-yield posts before comment hydration.
- Source-specific policy is configured in `config/sources/reddit.yaml` and includes subreddit rules, per-seed page caps, rolling-retention thresholds, comment-expansion controls, and early-stop conditions.
- Audit artifacts for this policy are written to `data/analysis/reddit_collection_policy_audit.csv`, `data/analysis/reddit_collection_policy_audit.json`, and `data/analysis/reddit_collection_policy_report.md`.

### Stack Overflow

- `source_meta.raw_question`
- `source_meta.raw_question_comments`
- `source_meta.raw_answers`
- `source_meta.raw_answer_comments`

### GitHub issue

- `source_meta.raw_issue`
- `source_meta.raw_issue_comments`

### GitHub discussion

- `source_meta.raw_discussion`
- `source_meta.raw_discussion_comments`
- `source_meta.raw_discussion_replies`

## 6. 현재 미완료 범위

- LLM labeling 기본 비활성화
- analysis는 exploratory only
