# TICKET-006 — Trend dashboards under outputs/

## Problem
Once multiple runs exist, users need a quick way to see trends (improving/worsening) without manually diffing reports.

## Goal
Produce simple trend artifacts across runs in a directory, suitable for quick inspection and future UI work.

## Suggested outputs
- `trend_summary.json`: per-run rollups in time order
- `trend_table.csv`: one row per run, columns for key metrics
- Optional: `trend_report.md` with short commentary

## Acceptance criteria
- Given a directory of run folders, the trend generator produces the artifacts above.
- Sorting/selection rules are deterministic (e.g., by `created_at` then `run_id`).

## Dependencies
- TICKET-004 (bundle outputs exist and are consistent)

