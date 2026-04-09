# Tasks

## Done

- project folder skeleton created
- file-based pipeline stages wired end to end
- reddit collector implemented
- stackoverflow collector implemented
- github issues collector implemented
- github discussions collector hook implemented
- time window stage added
- invalid filter implemented
- multi-episode builder implemented
- rule prelabeler implemented
- LLM hook implemented with safe fallback
- exploratory analysis layer implemented
- xlsx exporter aligned to current outputs

## In Progress / Partial

- GitHub discussions live collection
  requires `GITHUB_TOKEN`
- OpenAI LLM labeling
  API path implemented, default disabled

## Next priority

1. Implement `discourse` live collector and normalizer
2. Implement `hackernews` live collector and normalizer
3. Implement `youtube` live collector and normalizer
4. Tighten invalid filter to reduce remaining false positives
5. Improve episode builder boundary quality
6. Improve codebook coverage and reduce `unknown`
7. Add real OpenAI retry/backoff for LLM labeling
8. Improve xlsx formatting and analyst-facing readability

## Known placeholders

- discourse collector: stub
- hackernews collector: stub
- youtube collector: stub
- analysis outputs: exploratory only
- persona candidates: not final personas
- priority scores: not production ranking
