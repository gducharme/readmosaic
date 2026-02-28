# TICKET-001 — Define shared diagnostics contract

## Problem
The Narrative Diagnostics Observatory Pipeline currently has multiple scripts that emit their own shapes. There is no single, versioned report contract for downstream tools, UI, or gating.

## Goal
Define a **shared, versioned JSON contract** for a unified diagnostics bundle, with a minimal stable core that can evolve without breaking consumers.

## Proposed contract (v1)
- `contract_version` (string): e.g. `"narrative_diagnostics.v1"`
- `run` (object):
  - `run_id` (string, uuid)
  - `created_at` (string, ISO-8601 UTC)
  - `input` (object): `source_path`, `source_hash` (optional), `language` (optional)
  - `tool_versions` (object): per-script version identifiers (best-effort)
- `artifacts` (object): filenames/paths for per-tool raw outputs (optional but encouraged)
- `metrics` (object):
  - `semantic_repetition` (object)
  - `signal_density` (object)
  - `surprisal` (object)
  - `entropy` (object)
  - `burstiness` (object)
  - `themes` (object)
  - `patterns` (object)
- `highlights` (array): normalized list of key findings (each includes `kind`, `severity`, `message`, and optional `anchors`)
- `anchors` (object, optional): mapping strategy used (paragraph ids, line offsets, etc.)

## Acceptance criteria
- A new markdown spec document exists describing:
  - required fields vs optional fields
  - field types and semantics
  - compatibility rules for v1 → v2
- Contract includes a clear anchoring story (even if initial implementation is coarse).
- Contract supports linking back to paragraph-level or section-level context.

## Notes / dependencies
- Many existing scripts already import `schema_validator.validate_payload`; this ticket should decide whether:
  - to add a new schema file for the unified contract, or
  - to keep the unified contract as a markdown spec first, then add JSON Schema in a follow-up.

## References
- `docs/suggested_pipelines.md` (Narrative Diagnostics Observatory Pipeline section)
- `scripts/schema_validator.py`

