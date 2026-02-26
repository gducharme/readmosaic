# Seedpipe agent guide

- Never edit files under `generated/`; they are compiler output and will be overwritten.
- Put hand-written stage logic in `src/stages/*.py`.
- If pipeline structure changes, update `spec/phase1/pipeline.yaml` and re-run `seedpipe-compile`.
- Keep contract schemas in `spec/phase1/contracts/` in sync with artifact formats.
- `artifacts/inputs/` should contain the artifacts required to start a run.
- `artifacts/outputs/<run_id>/` should contain stage artifacts for that specific run ID.
- CLI entrypoints may be unavailable until installation; use `python -m tools.scaffold|compile|run` from a checkout.

## Translate pipeline alignment (phase1)

The canonical pipeline spec is `spec/phase1/pipeline.yaml` with these stage IDs, in order:

1. `source_ingest` *(active; `placeholder: false`)*
2. `translate_pass1` *(active; `placeholder: false`)*
3. `translate_pass2` *(active; `placeholder: false`)*
4. `candidate_assembly` *(placeholder)*
5. `review_grammar` *(placeholder)*
6. `review_typography` *(placeholder)*
7. `review_critics` *(placeholder)*
8. `map_review_to_paragraphs` *(placeholder)*
9. `aggregate_paragraph_reviews` *(placeholder)*
10. `final_assembly_and_publish_gate` *(placeholder)*

Implementation expectations:
- Only non-placeholder stages must have runnable implementations in `src/stages/`.
- For the current phase, `src/stages/source_ingest.py`, `src/stages/translate_pass1.py`, and `src/stages/translate_pass2.py` are required by the spec.
- Remaining placeholder stages are kept in the spec for dependency wiring and future implementation.
