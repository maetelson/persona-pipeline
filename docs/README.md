# Docs Guide

Use this folder for reference material after you already know which part of the pipeline you are touching.

## Read Order

- Start with [README.md](../README.md) for entrypoints and the repo map.
- Then use [`operational/`](./operational/) for execution and file-contract docs.
- Use policy docs in this folder only when working on persona quality, readiness, source balance, or workbook semantics.

## What Lives Here

- `operational/`: run order, contracts, runbook, QA, and maintenance docs
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
