# Weak Source Denominator Policy

## Purpose

This policy does **not** relax workbook thresholds or persona quality standards.

Its purpose is narrower:

- keep all weak-source diagnostics visible
- keep meaningful weak sources in core workbook hard-fail pressure
- separate only exploratory-only weak-source debt from the core readiness denominator

## What Changes

The workbook now distinguishes between:

- `weak_source_cost_center_count`
  - all weak sources remain visible here for diagnostics
- `core_readiness_weak_source_cost_center_count`
  - only weak sources that should still count toward core workbook hard-fail pressure
- `exploratory_only_weak_source_debt_count`
  - weak sources that remain visible but no longer block reviewable readiness

## Eligibility For Exploratory-Only Weak Source Debt

A source can be treated as exploratory-only weak-source debt only if all are true:

- it is currently a weak source cost center
- it has very low blended influence
- it lacks meaningful promoted-persona evidence
- it matches the narrow downgrade-to-exploratory-only policy profile
- separating it does not change persona counts or persona readiness tiers

Under the current artifacts, this policy is expected to apply only to:

- `klaviyo_community`

These sources remain core-readiness weak sources and are **not** separated:

- `google_developer_forums`
- `adobe_analytics_community`
- `domo_community_forum`

## What Does Not Change

This policy does not:

- change clustering
- change labeling
- change source collection
- change relevance rules
- change workbook thresholds
- change persona promotion scoring
- change review-ready eligibility logic
- change final usable persona semantics

## Current Interpretation

The current effect is:

- total weak-source diagnostics remain visible
- only core-readiness weak sources count toward workbook hard-fail pressure
- exploratory-only weak-source debt is reported separately
- workbook readiness can move to `reviewable_but_not_deck_ready`
- deck-ready remains out of scope

## Current Outcome Contract

After this cleanup, the expected invariant is:

- `final_usable_persona_count = 3`
- `production_ready_persona_count = 3`
- `review_ready_persona_count = 1`
- `persona_04` remains review-ready only
- `persona_05` remains blocked/constrained

The policy is successful only if those persona standards remain unchanged.
