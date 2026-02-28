# TICKET-004 — Merge metrics and emit unified report

## Problem
Even after running diagnostics, results remain fragmented and hard to interpret as a single narrative health report.

## Goal
Merge per-tool outputs into the shared contract (TICKET-001) and emit:
- `diagnostics_bundle.json` (machine-readable)
- `diagnostics_report.md` (human readable)

## Suggested implementation
- Add a merger module (script or library) that:
  - reads the run manifest + per-tool outputs
  - normalizes into `metrics.*` and `highlights[]`
  - computes a small set of rollup scores (optional, but should be explicitly labeled as heuristics)
- Report (MD) should include:
  - executive summary
  - top issues (with anchors)
  - per-diagnostic sections
  - appendices linking to raw artifacts

## Acceptance criteria
- `diagnostics_bundle.json` validates against the shared contract definition (as defined in TICKET-001).
- `diagnostics_report.md` is readable without opening raw JSON/CSV outputs.

## Dependencies
- TICKET-001 (contract)
- TICKET-003 (runner artifacts)

