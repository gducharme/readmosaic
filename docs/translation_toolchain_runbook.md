# Translation Toolchain Runbook

## Schema package location

All translation toolchain contracts live under `schemas/translation_toolchain/`.

- `paragraph_state_row.schema.json` validates `state/paragraph_state.jsonl` rows.
- `paragraph_scores_row.schema.json` validates `state/paragraph_scores.jsonl` rows.
- `rework_queue_row.schema.json` validates `state/rework_queue.jsonl` rows.
- `candidate_map_row.schema.json` validates `final/candidate_map.jsonl` rows.
- `normalized_review_row.schema.json` validates `review/normalized/*.jsonl` rows.
- `manifest.schema.json` validates `manifest.json`.
- `defs.schema.json` contains shared enums and constraints.

## Required constraints

Shared constraints are defined once in `defs.schema.json` and reused across row schemas:

- `paragraph_index` is 1-based (`minimum: 1`).
- `content_hash` must use the `sha256:` prefix with a 64-character lowercase hex digest.
- `*_content_hash` fields always reference the content object relevant to that row type (source paragraph, candidate text, or reviewed candidate).
- `language_code` is **loosely validated** as BCP47-like tags (examples: `en`, `en-US`, `en-us`, `zh-Hans`).
- `paragraph_state` is an explicit enum:
  - `ingested`
  - `translated_pass1`
  - `reviewed_pass1`
  - `rework_required`
  - `translated_pass2`
  - `reviewed_pass2`
  - `approved`
  - `rejected`
- `candidate_map` uses `selection_outcome` (`approved|rejected`) to avoid overloading paragraph lifecycle `state`.

Additional row invariants:

- `rework_queue_row.state` is required and fixed as `rework_required`.
- `rework_queue_row.priority` uses a dedicated `priority_level` enum (`low|medium|high|critical`).
- `paragraph_state_row.attempt` is disallowed for `ingested` rows and required for all other states.
- All `scores` and `thresholds` values in `paragraph_scores_row` are bounded to `[0,1]`.
- `normalized_review_row.state_after_review` is required.
- `manifest` pins normalized review artifacts to the glob `review/normalized/*.jsonl` for file-level verification.
- `manifest.counts` (when present): `paragraphs_total` is the total paragraph count for the run, while `paragraphs_approved` and `paragraphs_rework` are status buckets and are expected to be computed from final paragraph outcomes.
- `normalized_review_row.findings[].span` uses `{start, length}` in Unicode code points over candidate text, representing `[start, start + length)`.

## `schema_version` strategy (forward compatibility)

All schema-bound JSON objects require a `schema_version` field.

Current strategy:

- Format is semantic versioning (`MAJOR.MINOR.PATCH`).
- Current major version is `1` and enforced by pattern `^1\.\d+\.\d+$`.
- Producers should increment:
  - `PATCH` for clarifications and non-structural schema text updates.
  - `MINOR` for backward-compatible changes that do **not** add unrecognized top-level fields to strict rows.
  - `MAJOR` for breaking changes.

Consumer behavior:

1. Reject payloads where `schema_version` is missing.
2. Reject payloads whose major version is unsupported.
3. Map all `1.x.y` payloads to the v1 schema files currently in this repository (`schemas/translation_toolchain/*.schema.json`) and validate against those contracts.
4. Because these schemas use `additionalProperties: false`, unknown fields are invalid. New fields therefore require a coordinated schema update.

## Upgrade behavior

When introducing a breaking schema change:

1. Create `v2` schema files (or update IDs with v2 pathing) under `schemas/translation_toolchain/`.
2. Update all writers to emit `schema_version` with major `2`.
3. Keep readers dual-stack (v1 + v2) during migration.
4. Backfill historical artifacts only when required for downstream consumers; otherwise, keep immutable v1 artifacts and validate them with v1 schemas.
5. After migration cutover, remove v1 write paths first, then remove v1 read paths after data retention/SLA windows are met.
