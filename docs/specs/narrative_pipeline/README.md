# Narrative Diagnostics Observatory Pipeline — Ticket Set

This folder contains implementation tickets for the **Narrative Diagnostics Observatory Pipeline** proposed in `docs/suggested_pipelines.md`.

## Goals
- Produce a single diagnostics bundle across semantic repetition, entropy, burstiness, surprisals, and themes.
- Emit both:
  - a human-readable report (Markdown), and
  - a machine-readable summary (JSON) with a stable contract.

## Source building blocks
- `scripts/analyzer.py` (semantic repetition)
- `scripts/signal_density.py`
- `scripts/surprisal_scout.py`
- `scripts/entropy_evaluator.py`
- `scripts/burst_monitor.py`
- `scripts/theme_mapper.py`
- `scripts/pattern_extractor.py`
- `scripts/pre_processing.py`

## Ticket order (suggested)
1. `TICKET-001_shared_contract.md`
2. `TICKET-002_preprocessing_stage.md`
3. `TICKET-003_parallel_diagnostics_runner.md`
4. `TICKET-004_merge_and_report.md`
5. `TICKET-005_baseline_compare.md`
6. `TICKET-006_trend_outputs.md`
7. `TICKET-007_seedpipe_integration.md`
8. `TICKET-008_minimal_tests_and_fixtures.md`

