# TICKET-007 — Seedpipe pipeline integration

## Problem
The pipeline is proposed as a first-class entry in the pipeline catalog, but currently exists only as a set of scripts.

## Goal
Integrate the Narrative Diagnostics Observatory Pipeline into Seedpipe-style specs so it can be run like other pipes, with stage validation.

## Scope
- Add a new pipe spec (naming TBD), e.g.:
  - `pipes/narrative-diagnostics-pipe/spec/phase1/pipeline.yaml`
  - `pipes/narrative-diagnostics-pipe/src/stages/preprocessing.py`
  - `pipes/narrative-diagnostics-pipe/src/stages/run_diagnostics.py`
  - `pipes/narrative-diagnostics-pipe/src/stages/merge_report.py`
- Ensure schema validation runs after each stage (where applicable).

## Acceptance criteria
- Running the pipe produces the same outputs as the script runner (TICKET-003/004), or clearly documents differences.
- Stage outputs are versioned and include a manifest for reproducibility.

## Dependencies
- TICKET-002/003/004

