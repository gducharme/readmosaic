# TICKET-003 — Parallel diagnostics runner

## Problem
The pipeline is currently a set of standalone scripts with manual invocation. There is no single entrypoint that runs all diagnostics, captures outputs, and produces a consistent run directory layout.

## Goal
Create a single runner (script and/or Seedpipe stage) that:
1) performs preprocessing, then
2) runs all diagnostics in parallel where safe, then
3) collects per-tool outputs into a consistent artifact directory.

## Suggested implementation
- New entrypoint script, e.g. `scripts/narrative_diagnostics_runner.py`, that:
  - accepts `--input <path>` and `--outdir <dir>`
  - creates a `run_id` directory, e.g. `outputs/narrative_diagnostics/<run_id>/`
  - runs tools:
    - `scripts/analyzer.py`
    - `scripts/signal_density.py`
    - `scripts/surprisal_scout.py`
    - `scripts/entropy_evaluator.py`
    - `scripts/burst_monitor.py`
    - `scripts/theme_mapper.py`
    - `scripts/pattern_extractor.py`
  - captures `stdout/stderr` and writes per-tool result files (JSON/CSV/MD as each tool supports)
- Use Python `concurrent.futures` for parallelism (process-level is likely safest for ML deps).

## Acceptance criteria
- One command produces a populated run directory with:
  - preprocessing artifacts
  - per-tool outputs
  - a `run_manifest.json` describing what ran and where outputs are
- Runner fails fast with clear errors if prerequisites are missing (e.g., NLTK resources, model downloads).

## Dependencies
- Depends on preprocessing output definitions (TICKET-002).

## References
- `docs/suggested_pipelines.md`

