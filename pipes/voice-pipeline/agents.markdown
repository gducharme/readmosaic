# Seedpipe agent guide

- Never edit files under `generated/`; they are compiler output and will be overwritten.
- Put hand-written stage logic in `src/stages/*.py`.
- If pipeline structure changes, update `spec/phase1/pipeline.yaml` and re-run `seedpipe-compile`.
- Keep contract schemas in `spec/phase1/contracts/` in sync with artifact formats.
- CLI entrypoints may be unavailable until installation; use `python -m tools.scaffold|compile|run` from a checkout.
