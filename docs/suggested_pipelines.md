# Suggested Pipelines

## Purpose
This document proposes a consolidated pipeline catalog based on the current ReadMosaic codebase: existing Seedpipe flows, standalone scripts, web/runtime functionality, and nearby extensions that can be implemented with minimal architectural drift.

Status legend:
- `Active`: implemented and runnable now.
- `Partial`: core pieces exist, but some stages are placeholders or manual.
- `Proposed`: not yet implemented as a full automated pipeline.

---

## 1) Translation Production Pipeline
- Status: `Partial`
- Primary goal: deterministic multi-language manuscript translation with staged artifacts.
- Current building blocks:
  - `pipes/translate/spec/phase1/pipeline.yaml`
  - `pipes/translate/src/stages/source_ingest.py`
  - `pipes/translate/src/stages/translate_pass1.py`
  - `pipes/translate/src/stages/translate_pass2.py`
  - `pipes/translate/src/stages/candidate_assembly.py`
- Suggested stage flow:
  1. source ingest (`paragraphs.jsonl`, `manifest.json`)
  2. pass 1 translation (per language)
  3. pass 2 translation/script conversion (per language)
  4. candidate manuscript assembly
  5. publish gate
- Future improvements:
  - complete placeholder review/gate stages in spec
  - parameterize language bundles per run
  - enforce schema checks after each stage

## 2) Translation Quality Gate Pipeline
- Status: `Partial`
- Primary goal: block low-quality translations before final publish.
- Current building blocks:
  - `scripts/grammar_auditor.py`
  - `scripts/typographic_precision_review.py`
  - `scripts/critics_runner.py`
  - `scripts/paragraph_issue_bundle.py`
  - `scripts/confidence_review.py`
  - `scripts/html_review.py`
- Suggested stage flow:
  1. run grammar + typography + critics reviews
  2. normalize/anchor findings to paragraph IDs
  3. aggregate scores + create rework queue
  4. render confidence and HTML reviewer views
  5. gate publish on threshold policy
- Future improvements:
  - add deterministic paragraph mapping stage in `src/stages`
  - codify scoring thresholds in versioned policy JSON
  - add regression suite with known good/bad translated paragraphs

## 3) Slop Stop Editorial Pipeline
- Status: `Active` (base), `Partial` (advanced)
- Primary goal: remove repetitive/low-signal language and improve narrative sharpness.
- Current building blocks:
  - `pipes/slop-stop-pipe/spec/phase1/pipeline.yaml`
  - `pipes/slop-stop-pipe/src/stages/preprocessing.py`
  - `pipes/slop-stop-pipe/src/stages/word_frequency_benchmark.py`
  - `pipes/slop-stop-pipe/src/stages/transform.py`
  - `scripts/slop_scrubber.py`
  - `scripts/direct_signal_filter.py`
  - `scripts/simile_lint_pass.py`
- Suggested stage flow:
  1. normalize manuscript
  2. lexical frequency benchmark
  3. style/slop detectors
  4. transformation and rewrite suggestion emit
  5. reviewer pass + publish
- Future improvements:
  - replace placeholder `future_review` with policy-driven reviewer logic
  - add hard-fail classes for cliche endings and heavy hedge density
  - auto-generate patch-ready edit objects for MIR

## 4) Narrative Diagnostics Observatory Pipeline
- Status: `Proposed` (all components exist)
- Primary goal: produce a single diagnostics bundle across semantic repetition, entropy, burstiness, surprisals, and themes.
- Current building blocks:
  - `scripts/analyzer.py`
  - `scripts/signal_density.py`
  - `scripts/surprisal_scout.py`
  - `scripts/entropy_evaluator.py`
  - `scripts/burst_monitor.py`
  - `scripts/theme_mapper.py`
  - `scripts/pattern_extractor.py`
- Suggested stage flow:
  1. pre-processing
  2. run all diagnostics in parallel
  3. merge metrics into one report contract
  4. emit human report + machine-readable JSON summary
- Future improvements:
  - standardize shared metrics schema
  - add baseline comparison across manuscript versions
  - produce trend dashboards under `outputs/` and `mosaic_outputs/`

## 5) Lexical Upgrade Pipeline
- Status: `Proposed`
- Primary goal: increase lexical precision and reduce overuse clusters while preserving meaning.
- Current building blocks:
  - `scripts/word_frequency_benchmark.py`
  - `scripts/lexical_entropy_amplifier.py`
  - `scripts/lexical_enhancer.py`
  - `scripts/vivid_verb_upgrader.py`
- Suggested stage flow:
  1. detect overused terms
  2. generate rewrite options
  3. apply LM rewrite candidates
  4. interactive accept/reject loop
  5. validate post-edit entropy/signal deltas
- Future improvements:
  - add paragraph-level rollback and audit trail
  - gate substitutions with NER/entity-preservation checks
  - include language-specific lexical resources for translation outputs

## 6) Dialogue and Typography Precision Pipeline
- Status: `Proposed`
- Primary goal: enforce punctuation, quotation, and formatting consistency.
- Current building blocks:
  - `scripts/quotation_delimiter_auditor.py`
  - `scripts/typographic_precision_review.py`
  - `scripts/grammar_auditor.py`
- Suggested stage flow:
  1. run quote/delimiter audit
  2. run typography audit
  3. run grammar cross-check
  4. aggregate by paragraph and severity
- Future improvements:
  - add autofix mode for safe punctuation normalizations
  - enforce locale-aware typography profiles per target language

## 7) Mosaic Orchestrator Pipeline
- Status: `Active`
- Primary goal: parallel tool sweep with fidelity-context packaging and culling directives.
- Current building blocks:
  - `mosaic_mo.py`
  - `tool_wrapper.py`
  - `scripts/paragraph_issue_bundle.py`
  - `scripts/culling_resolver.py`
- Suggested stage flow:
  1. NLTK preflight
  2. parallel tool execution
  3. objective/proposal artifact generation
  4. issue bundling and culling resolution
  5. final directives report
- Future improvements:
  - migrate into Seedpipe spec/stages for compile-time validation
  - add stage-level retry policy and resumable state locks
  - add deterministic run manifest hashes for all artifacts

## 8) Human-in-the-Loop Refinement Pipeline
- Status: `Active` (manual loop)
- Primary goal: iterative acceptance of rewrite proposals with traceability.
- Current building blocks:
  - `scripts/mosaic_refiner.py`
  - `scripts/confidence_review.py`
  - `scripts/html_review.py`
  - `mosaic_work/session_state.json`
- Suggested stage flow:
  1. load manuscript + edits
  2. fuzzy-anchor paragraph targeting
  3. generate refined candidates
  4. approve/reject/regenerate loop
  5. save session and final manuscript
- Future improvements:
  - export accepted edits to patch format
  - support multi-reviewer adjudication and conflict resolution
  - add automatic post-acceptance quality recheck

## 9) Voice Rendering and Audio Publish Pipeline
- Status: `Active`
- Primary goal: generate audiobook-style MP3 output from markdown manuscript input.
- Current building blocks:
  - `pipes/voice-pipeline/spec/phase1/pipeline.yaml`
  - `pipes/voice-pipeline/src/stages/voice.py`
  - `scripts/kokoro_paragraph_reader.py`
- Suggested stage flow:
  1. manuscript ingest and markdown normalization
  2. Kokoro synthesis to WAV
  3. ffmpeg conversion to MP3
  4. publish artifact manifest
- Future improvements:
  - paragraph-level timestamps for audio navigation
  - multilingual voice profiles mapped to translation outputs
  - quality checks for clipping/silence and failed segments

## 10) Web Content Sync and Publish Pipeline
- Status: `Proposed`
- Primary goal: move approved manuscripts/translations into web content safely.
- Current building blocks:
  - `web/server.js`
  - `web/data/*`
  - `deploy/data/archive/*`
- Suggested stage flow:
  1. validate content shape and naming
  2. sync finalized markdown into `web/data/<lang>/`
  3. archive previous version
  4. run API smoke checks for language/chapter endpoints
- Future improvements:
  - atomic versioned publish directories
  - content integrity hashes and rollback pointer
  - CI checks that prevent broken chapter references

## 11) Event Sourcing Extraction Pipeline
- Status: `Proposed`
- Primary goal: transform manuscript events into graph-ready event sourcing records.
- Current building blocks:
  - `docs/event_sourcing_spec.md`
  - `schema.cypher`
  - `migrations/001_years.cypher`
  - `migrations/002_seed.cypher`
- Suggested stage flow:
  1. detect event candidates in manuscript
  2. map to `uid/type/timestamp/location/actors`
  3. validate against event schema rules
  4. emit Cypher import payloads
  5. apply migrations/updates
- Future improvements:
  - add causal-chain consistency checks
  - add idempotent re-import support per run
  - attach provenance links to source paragraph IDs

## 12) Contract and Schema Validation Pipeline
- Status: `Proposed`
- Primary goal: enforce artifact contract integrity across scripts and pipes.
- Current building blocks:
  - `scripts/schema_validator.py`
  - `schemas/*.schema.json`
  - `pipes/*/spec/phase1/contracts/*.schema.json`
- Suggested stage flow:
  1. collect produced JSON/JSONL artifacts
  2. validate records against expected schemas
  3. classify failures by stage and contract
  4. gate downstream publish/merge
- Future improvements:
  - make validation mandatory in generated wrappers
  - add schema version compatibility matrix
  - generate auto-fix hints for common shape mismatches

---

## Cross-Pipeline Improvements (Recommended)
1. Define a single canonical artifact contract for manuscript units (`paragraph_id`, `content_hash`, stable ordering) and reuse it in all pipelines.
2. Standardize run metadata (`run_id`, attempt, manifest hash, timestamps, model metadata) across scripts and Seedpipe flows.
3. Add a shared quality scorecard contract so diagnostics and reviewers can gate on common thresholds.
4. Create a pipeline test harness with fixtures in `docs/` or `artifacts/inputs/` and expected outputs for regression checks.
5. Promote ad hoc script chains into formal Seedpipe specs where deterministic stage I/O and retries matter.
6. Add CI jobs for schema validation, smoke runs, and web content integrity after translation/refinement changes.
7. Build a small run registry index (JSON or SQLite) for comparing quality trends across runs and manuscript versions.

## Suggested Implementation Order
1. Translation Quality Gate Pipeline
2. Narrative Diagnostics Observatory Pipeline
3. Slop Stop advanced stages (`future_review` replacement + policy gating)
4. Contract and Schema Validation Pipeline
5. Web Content Sync and Publish Pipeline
6. Event Sourcing Extraction Pipeline
