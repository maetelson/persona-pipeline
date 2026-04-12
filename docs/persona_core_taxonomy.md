# Persona-Core Taxonomy

## Purpose

Persona-core should explain the rows that are stable enough to support clustering and workbook messaging without collapsing the taxonomy into vague catch-all buckets.

## Primary Core Axes

- `bottleneck_type`
  - the recurring friction blocking progress
- `workflow_stage`
  - where the user is stuck: triage, reporting, automation, or validation
- `analysis_goal`
  - the job-to-be-done: diagnose, validate, report faster, or automate

These three axes define the minimum interpretable persona signature.

## Optional Overlay Axes

- `user_role`
- `output_expectation`
- `tool_dependency_mode`
- `trust_validation_need`
- `analysis_maturity`

These remain useful for copy, examples, and product framing, but they are not reliable enough to remain hard persona-core gates.

`tool_dependency_mode` is intentionally optional because the corpus often expresses a clear bottleneck, stage, and goal without naming the exact execution stack.

## Admission Policy

A row is persona-core eligible when:

1. all primary core axes are assigned
2. and the row is either:
  - `labelable`
  - `borderline`
  - `low_signal` with a supportable coverage category:
    - `missing_taxonomy_value`
    - `overly_strict_axis_requirement`
    - `weak_workflow_context`
    - `insufficient_bottleneck_signal`
    - `output_expectation_not_captured`

Low-signal rows in `generic_chatter_not_persona_usable`, `parser_schema_mismatch`, or `multi_axis_conflict` stay out of persona-core even if they appear partially mappable. This prevents silent bucket inflation.

## Exclusions

- announcement-style help posts remain excluded
- weak/noisy product threads remain excluded
- rows missing one or more primary axes remain excluded until deterministic mapping is improved

## Interpretation Rule

If a row has a stable bottleneck, workflow stage, and goal, it can shape persona structure.

If a row only contributes optional overlays, it can still inform messaging and examples, but it should not define persona identity.