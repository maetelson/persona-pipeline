# Deck-Ready Feasibility Final Decision

## Decision

- Final decision: `freeze_as_reviewable_claim_release`
- Feasibility assessment: `deck_ready_not_achievable_with_current_safe_paths`

The current workbook should be frozen as a **reviewable but not deck-ready** release. Safe bounded remediation paths are exhausted for now, and recent attempts either failed, regressed a blocker, or produced negligible metric movement.

## Current Baseline

- `persona_readiness_state = reviewable_but_not_deck_ready`
- `overall_status = WARN`
- `quality_flag = EXPLORATORY`
- `final_usable_persona_count = 3`
- `production_ready_persona_count = 3`
- `review_ready_persona_count = 1`
- `deck_ready_claim_eligible_persona_count = 4`
- `persona_core_coverage_of_all_labeled_pct = 74.5`
- `persona_core_labeled_rows = 9445`
- `labeled_episode_rows = 12674`
- `rows_needed_to_reach_80.0 = 695`
- `effective_balanced_source_count = 5.89`
- `core_readiness_weak_source_cost_center_count = 3`
- `weak_source_cost_center_count = 4`
- `selected_example_grounding_issue_count = 4`
- `largest_source_influence_share_pct = 30.7`

## Path Exhaustion Table

| Path | Objective | Result | Metric moved | Why it failed or was insufficient | Retry? | Retry condition |
|---|---|---|---|---|---|---|
| Adobe remediation | Improve Adobe yield safely | Failed | None | No safe parser or episode fix was identified. | No | Only with new source-specific parser evidence and regressions. |
| Google remediation | Recover more Google valid rows safely | Failed | None | No safe valid-filter expansion without support noise. | No | Only with isolated source-native false negatives and low noise. |
| Weak-source deletion | Reduce weak-source debt | Insufficient and regressive | Source balance worsened | Deletion regressed breadth and source balance. | No | Only if replacement evidence is already proven. |
| Tableau | Add new representative source | Not viable | None | Automated access path was not viable enough. | No | Only with a new stable public automated path. |
| Supermetrics | Add new HTML-only representative source | Failed | None | HTML listing discovery produced zero usable rows. | No | Only with newly verified public listing access. |
| Reddit RevOps bundle | Add operator-language Reddit evidence | Failed quality gate | None | Too noisy, weak valid ratio, weak labelable ratio. | No | Only with a materially different Reddit slice. |
| GitHub Discussions multi-repo | Add public repo-based evidence | Failed quality gate | None | Developer/support noise dominated. | No | Only with a materially different repo surface. |
| GitHub Discussions narrowed | Salvage GitHub via narrower bundle | Failed quality gate | None | Quality got worse after narrowing. | No | None under the current repo family. |
| Lightdash single repo | Test best remaining GitHub repo | Failed quality gate | Ratio improved but still insufficient | Low volume, support noise, weak persona-core fit. | No | Only with a new Lightdash public surface. |
| Stack Overflow SSRS expansion | Add non-PowerBI Stack Overflow evidence | Pilot not justified | None | Existing Stack Overflow already contains meaningful SSRS coverage. | No | Only if current SSRS slice becomes absent or exhausted. |
| Non-core recovery | Recover safe non-core rows into persona-core | Failed | Optimistic upper bound only to ~75.15 | Review sample found zero safe recover-to-core rows. | No | Only with a manually curated high-precision allowlist. |
| HubSpot + Reddit existing-source expansion | Improve balance with existing healthy sources | Failed incremental gate | Coverage `74.50 -> 74.52`, balance `5.89 -> 5.90` | Strict dedupe showed almost no genuinely new evidence. | No | Only with a clearly new seed/window proven before crawling. |

## Current Blockers

| Metric | Current | Deck-ready threshold | Gap | Safe path remains? | Notes |
|---|---:|---:|---:|---|---|
| `persona_core_coverage_of_all_labeled_pct` | 74.5 | 80.0 | 5.5 | No | Needs about `695` more safe persona-core rows. |
| `effective_balanced_source_count` | 5.89 | 6.0 | 0.11 | No | Existing-source bounded expansion failed under strict dedupe. |
| `core_readiness_weak_source_cost_center_count` | 3 | 1 | 2 | No | Adobe/Google fixes were not safe; deletion regressed balance. |
| `weak_source_cost_center_count` | 4 | 1 | 3 | No | No safe remediation or removal path remains. |
| `selected_example_grounding_issue_count` | 4 | 0 | 4 | Yes, but not material | Example cleanup may help readability, not deck-ready readiness. |
| `largest_source_influence_share_pct` | 30.7 | 35.0 | already passing | Not a blocker | Concentration is not the main issue. |

## Why Deck-Ready Is Not Achievable Now

The remaining blockers are not cosmetic. They are workbook-level evidence blockers:

- core coverage is materially short of the deck-ready floor
- effective source balance is still below the required floor
- weak-source debt remains too high for core-readiness

The key finding across all recent work is consistency:

- remediation paths for weak sources were unsafe or regressive
- new source-family pilots failed access or quality gates
- non-core recovery had no safe precision
- existing-source expansion produced almost no incremental evidence after strict dedupe

That combination makes another micro-pilot hard to justify.

## Freeze Recommendation

Freeze the current workbook as a **reviewable_but_not_deck_ready** release.

Recommended release wording:

- This workbook is a `reviewable_but_not_deck_ready` persona workbook.
- It currently supports `3` final-usable and production-ready personas.
- It preserves `1` review-ready and deck-ready-claim-eligible persona that should remain constrained until workbook-level blockers clear.
- It preserves `1` future subtheme or blocked candidate for later work.
- It is **not** yet a full deck-ready or production-ready corpus.

Suggested README or release-note wording:

> This release is a reviewable draft persona workbook. It is suitable for analyst review and scoped evidence-backed narrative use, but it is not a final deck-ready or production-ready persona corpus. The workbook currently supports three final-usable personas, preserves one review-ready and deck-ready-claim-eligible persona under constraint, and keeps one future subtheme candidate blocked pending stronger corpus support.

## Minimum Future Requirement If Reopened

If work resumes later, the minimum non-trivial requirement is not another micro-pilot. It is one of:

- a **new high-quality representative source family** with proven automated access and materially different evidence
- or a **large new existing-dataset increment** that is genuinely unique after dedupe

Minimum outcomes required to materially reopen deck-ready:

- add about `695` new safe persona-core rows without threshold relaxation
- raise `effective_balanced_source_count` above `6.0`
- reduce `core_readiness_weak_source_cost_center_count` from `3` to at most `1`
- avoid increasing weak-source debt or source concentration
- preserve current persona semantics, especially `persona_04`, while keeping `persona_05` blocked unless independently justified

## Risks And Assumptions

Risks:

- Some real but currently inaccessible evidence may still exist outside the audited safe paths.
- A future new source family could still change the decision.
- Policy relaxation could create a misleading appearance of readiness without improving methodological integrity.

Assumptions:

- The audited safe bounded paths reasonably represent the currently available remediation surface.
- Current thresholds and semantics remain fixed.
- Strict deduplication and noise controls are required to prevent fake readiness gains.
