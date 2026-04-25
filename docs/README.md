# Docs Guide

Use this folder for reference material after you already know which part of the pipeline you are touching.

## Read Order

- Start with [README.md](../README.md) for entrypoints and the repo map.
- Then use [`operational/`](./operational/) for execution and file-contract docs.
- Use [`POLICY_INDEX.md`](./POLICY_INDEX.md) when you need to know which policy document is the current source of truth.
- Use policy docs in this folder only when working on persona quality, readiness, source balance, or workbook semantics.

## What Lives Here

- `operational/`: run order, contracts, runbook, QA, and maintenance docs
- `POLICY_INDEX.md`: source-of-truth map for promotion, source balance, workbook readiness, diagnostics, and artifact lifecycle
- policy docs:
  - `persona_*`
  - `source_*`
  - `workbook_*`
  - `cluster_robustness_policy.md`
  - `quality_status_policy.md`
- `archive/`: old notes and scratch material, not the main source of truth

## Ignore When Navigating

- `archive/`
- generated reports under `data/analysis/`
- `__pycache__/`
