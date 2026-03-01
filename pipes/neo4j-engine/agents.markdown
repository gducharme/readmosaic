# Seedpipe agent guide

- Never edit files under `generated/`; they are compiler output and will be overwritten.
- Put hand-written stage logic in `src/stages/*.py`.
- If pipeline structure changes, update `spec/phase1/pipeline.yaml` and re-run `seedpipe-compile`.
- Keep contract schemas in `spec/phase1/contracts/` in sync with artifact formats.
- `artifacts/inputs/` should contain the artifacts required to start a run.
- `artifacts/outputs/<run_id>/` should contain stage artifacts for that specific run ID.
- CLI entrypoints may be unavailable until installation; use `python -m tools.scaffold|compile|run` from a checkout.
- Use `seedpipe-scaffold --loop` to generate a loop-enabled starter pipeline with `pipeline_type: looping`, `max_loops`, and `reentry`/`go_to` stage wiring.

## Practical implementation notes

- After stage-order edits in `spec/phase1/pipeline.yaml`, use a new `run-id`. Reusing an old run ID can fail with `ValueError: run manifest stage order does not match compiled flow`.
- Runtime schema validation loads declared output payloads as JSON. Declaring `.txt`, `.md`, or `.csv` outputs with schemas can fail at JSON parsing.
- Preferred output pattern:
  - Keep machine-contract outputs in JSON artifacts declared in `pipeline.yaml`.
  - Write human-readable `.md` or `.csv` as side artifacts from stage code unless wrapped in JSON.
- Side artifacts are a convenience layer; the canonical contract should stay in JSON for downstream stage consumption.
- In loop pipelines, prefer returning `ItemResult(ok=False, error=...)` for business-rule failures in `run_item` and let runtime route failed cohorts through `go_to` reentry.
- For narrative diagnostics, keep explicit lanes:
  - `run_document_diagnostics` for document-level metrics.
  - `run_paragraph_diagnostics` for paragraph-level metrics.
  - `run_hybrid_diagnostics` for global baseline plus local anchors.
  - Merge lanes in `merge_report` into a stable bundle contract.

## Fast debug checklist

- Compile failures:
  - Confirm every object-form input/output defines `family`, `pattern`, and `schema`.
  - Confirm schema files exist under `spec/stages/<stage_id>/...`.
- Run failures:
  - Confirm stage code writes every declared output artifact.
  - Confirm produced output payload shape matches declared stage schema.
  - Use a new `run-id` after stage-graph edits.

## Neo4j Reality Ingest Pipeline Notes

- **Inputs.** Drop the chapter Markdown under `artifacts/inputs/` (or point `REALITY_MARKDOWN_PATH` to a specific file). The pipeline assumes UTF-8 text and uses the first discovered `.md` file if multiple exist.
- **Pipeline flow.**
  - `parse_markdown` (whole run) → `artifacts/parsed_chapter.json`.
  - `build_ontology` → `artifacts/active_ontology.json` (pulls aliases, states, relations from Neo4j + writes `ActiveOntology` artifact).
  - `extract_graph` → `artifacts/extracted_graph_payload.json` (LLM extraction with schema validation and stored raw output).
  - `resolve_entities` → `artifacts/resolution_plan.json` (vector/fuzzy matching, conflict detection, deterministic UUID assignment).
  - `review_diff` → `artifacts/diff_report.json` (Rich diff with colors; interactive prompt `[A/E/R]` unless `REALITY_DIFF_DECISION` forces a choice).
  - `commit_graph` → `artifacts/commit_report.json` (idempotent Cypher writes into Neo4j once the diff is accepted).
- **Diff gate overrides.** Set `REALITY_DIFF_DECISION=accepted`/`edited`/`rejected` to bypass the prompt in automation. Defaults to `prompt` for manual confirmation.
- **Local LLM profile.** Use `REALITY_ADAPTER=litellm`, `REALITY_MODEL=lfm2-24b-a2b`, `REALITY_LLM_BASE_URL=http://127.0.0.1:1234/v1`, and `REALITY_LLM_API_KEY=lm-studio`. The pipeline hard-fails if the configured non-stub adapter cannot initialize or call the model endpoint.
- **Neo4j expectations.** Neo4j runs at `bolt://localhost:7687` using `neo4j/mosaic_founding`. APOC must be enabled (see docker-compose spec). The commit stage uses the parameterized queries defined in `docs/specs/neo4j engine/CYPHER_LIBRARY.md`.
- **Artifacts.** Additional durable outputs live under `pipes/neo4j-engine/artifacts` (parsed text, raw LLM output, extracted payload). Keep these artifacts for auditing, especially the JSON diff report before committing.
