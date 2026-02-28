# TICKET-005 — Baseline comparison across manuscript versions

## Problem
Diagnostics are most useful when comparing deltas across versions (e.g., after an edit pass). The proposed pipeline calls for baseline comparisons, but no mechanism exists today.

## Goal
Support comparing two diagnostics runs (A vs B) and generating a delta report.

## Scope
- Define a delta contract and report format:
  - changes in rollup metrics
  - newly introduced / resolved highlights
  - largest shifts in burst terms, themes, repetition clusters, etc.
- Provide a CLI entrypoint, e.g.:
  - `python scripts/narrative_diagnostics_compare.py --a <run_dir> --b <run_dir> --out <dir>`

## Acceptance criteria
- Produces:
  - `diagnostics_delta.json`
  - `diagnostics_delta_report.md`
- Delta output includes enough context to understand what changed (not just numbers).

## Dependencies
- TICKET-001 (contract versioning rules)
- TICKET-004 (bundle shape)

