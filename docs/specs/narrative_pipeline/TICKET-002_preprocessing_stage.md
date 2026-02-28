# TICKET-002 — Standard preprocessing stage

## Problem
Diagnostics scripts assume different tokenization and text normalization strategies, and some are expensive (or brittle) without a shared pre-processing output.

## Goal
Standardize a preprocessing stage that creates a shared input bundle for all diagnostics tools.

## Scope
- Define preprocessing outputs written to a run directory, e.g.:
  - `manuscript_raw.txt`
  - `manuscript_normalized.txt`
  - `manuscript_tokens.json`
  - `paragraph_index.json` (paragraph id → offsets, section membership)
- Use existing building block(s):
  - `scripts/pre_processing.py` (preferred starting point)

## Acceptance criteria
- Preprocessing emits deterministic artifacts for the same input.
- Diagnostics stages can reuse preprocessing artifacts instead of re-tokenizing from scratch.
- Preprocessing artifacts include stable paragraph/section anchors suitable for reporting.

## Out of scope
- Perfect semantic paragraph alignment across multiple manuscript versions (handled by baseline comparison ticket).

## References
- `scripts/pre_processing.py`
- `scripts/burst_monitor.py` (expects `manuscript_tokens.json` in a preprocessing dir)

