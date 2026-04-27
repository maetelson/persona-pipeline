# Final Release Checklist

## Validation

- Run `python run/cli/17_analysis_snapshot.py --compare-latest`
- Run `python -m unittest tests.test_analysis_snapshot_cli tests.test_source_tier_evidence tests.test_deck_ready_claims tests.test_deck_ready_denominator_eligibility tests.test_source_slice_classification`
- Confirm `persona_readiness_state = reviewable_but_not_deck_ready`
- Confirm `overall_status = WARN`
- Confirm `quality_flag = EXPLORATORY`
- Confirm `final_usable_persona_count = 3`
- Confirm `review_ready_persona_count = 1`
- Confirm `deck_ready_claim_eligible_persona_count = 4`

## Artifacts To Include

- Final workbook output already produced under `data/output/`
- `artifacts/release/final_reviewable_claim_release.json`
- `docs/operational/FINAL_REVIEWABLE_CLAIM_RELEASE.md`
- `docs/operational/REFINED_SLICE_PRECISION_REVIEW.md`
- Conservative denominator release notes and artifacts if referenced in the release narrative

## README / Release Wording

- Call the package a `reviewable claim release`
- State that `3` personas are final-usable and production-ready
- State that `1` additional persona is review-ready / claim-eligible under constraint
- State that adjusted coverage is audited and secondary, not a silent replacement for original coverage
- State that full deck-ready status is blocked by source balance and weak-source debt

## LinkedIn / GitHub Positioning Caution

- Do not market the workbook as a full deck-ready persona corpus
- Do not imply source balance is solved
- Do not imply weak-source debt is solved
- Do not imply persona_04 is fully promoted
- Do not imply persona_05 is a final standalone persona

## What Not To Claim

- No `deck-ready` claim for the full workbook
- No `production-persona-ready` claim for the full workbook
- No statement that all key blockers were resolved
- No statement that refined source-slice diagnostics are now official readiness gates
