# Translation Toolchain Runbook

## Canonical artifact contract

All translation toolchain contracts live under `schemas/translation_toolchain/`.

Schema → artifact mapping:

- `paragraph_state_row.schema.json` → `state/paragraph_state.jsonl`
- `paragraph_scores_row.schema.json` → `state/paragraph_scores.jsonl`
- `rework_queue_row.schema.json` → `state/rework_queue.jsonl`
- `candidate_map_row.schema.json` → `final/candidate_map.jsonl`
- `normalized_review_row.schema.json` → `review/normalized/*.jsonl`
- `manifest.schema.json` → `manifest.json`
- `defs.schema.json` → shared enums/primitive contracts

The runtime state machine in `lib/paragraph_state_machine.py` is the source of truth for paragraph lifecycle values.

For normative lifecycle/attempt rules and machine-checkable guarantees, see:

- **Canonical paragraph lifecycle statuses**
- **Identifier, attempt, and lineage semantics**
- **Enforced invariants (single source of truth)**

## Canonical paragraph lifecycle statuses

`status` must be one of:

- `ingested`
- `translated_pass1`
- `translated_pass2`
- `candidate_assembled`
- `review_in_progress`
- `review_failed`
- `rework_queued`
- `reworked`
- `ready_to_merge`
- `manual_review_required`
- `merged`

## Identifier, attempt, and lineage semantics

- `paragraph_id` is the canonical paragraph key across state/review/queue/map artifacts.
- `paragraph_index` is 1-based canonical run ordering (and is used for `final/candidate.md` line-range mapping).
- `attempt` on paragraph state rows is runtime attempt progress and is 0-based.
- `attempt` is always present in `state/paragraph_state.jsonl` rows (including `ingested`, where it is `0`).
- In reviewed examples below, `attempt` values of `1`/`2` reflect first/second completed review transitions from an initial ingested `attempt: 0` baseline.
- `failure_history[].attempt` reflects a recorded review failure attempt and is emitted 1-based by runtime transition logic.
- `failure_history[].attempt` is a failure-event counter and is semantically distinct from `paragraph_state_row.attempt` (which is 0-based runtime progress); values may coincide numerically on failed transitions but should not be treated as interchangeable fields.
- `content_hash` is the canonical lineage key for paragraph-level artifacts; optional `source_content_hash` and `candidate_content_hash` preserve source/candidate lineage when present.

## Row-level schema expectations

### `state/paragraph_state.jsonl`

Required fields:

- `paragraph_id`
- `status`
- `attempt` (0-based in runtime)
- `failure_history` (required once failure lineage exists; optional on initial ingested seed rows)
- `excluded_by_policy`
- `content_hash`

Important constraints:

- `excluded_by_policy: true` requires `exclude_reason` (**schema-enforced** via `if/then`).
- `blocking_issues` is deduplicated when present (**schema-enforced** for `string_list` fields via `uniqueItems: true`).
- `scores` is an object of `{string -> number}`; metric names are stage-local and dynamic.
- Additional runtime timestamps may be present (for example: `reviewed_at`, `updated_at`, `last_failed_at`, `last_success_at`).

### `review/normalized/*.jsonl`

Required fields:

- `paragraph_id`
- `scores`
- `issues`
- `blocking_issues`
- `hard_fail`
- `issue_count`
- `critical_count`
- `blocker_count`

Behavioral notes:

- Mapping failures are normalized as blocking issues with `code/category = mapping_error`.
- `scores` is stage-local and may include keys such as `semantic_fidelity` and `grammar`.

### `final/candidate_map.jsonl`

Required fields:

- `paragraph_id`
- `paragraph_index` (1-based)
- `start_line`
- `end_line`

Optional traceability fields:

- `run_id`
- `document_id`
- `content_hash`

Behavioral notes:

- Ranges are inclusive over `final/candidate.md` line numbers.
- `end_line >= start_line` is enforced by runtime and by contract tests.
- Operational rule: `final/candidate_map.jsonl` must be distributed with `manifest.json` so run-level linkage is preserved when optional `run_id`/`document_id` are omitted.

### `state/rework_queue.jsonl`

Required fields:

- `paragraph_id`
- `content_hash`
- `attempt`
- `failure_reasons`
- `failure_history`
- `required_fixes`

Behavioral notes:

- Queue rows are a projection of current `status == rework_queued` state rows.
- Projection lineage invariant: `paragraph_id + content_hash + attempt` in each queue row must match its source paragraph-state row.
- `failure_history` is carried forward to preserve retry lineage.

### `state/paragraph_scores.jsonl`

Required fields:

- `paragraph_id`
- `status`
- `attempt`
- `scores`
- `blocking_issues`
- `updated_at`

### `manifest.json`

Required fields:

- `run_id`
- `pipeline_profile`
- `source`
- `model`
- `pass1_language`
- `pass2_language` (nullable)
- `created_at`

Optional lineage field:

- `source_content_hash`

`manifest.json` is the canonical run-level linkage for row files that omit explicit `run_id`.


## Enforced invariants (single source of truth)

- **Schema-enforced:** `paragraph_state_row` requires `paragraph_id`, `status`, `attempt` (0-based integer), `excluded_by_policy`, and `content_hash`; `failure_history` becomes required operationally once failures are recorded.
- **Schema-enforced:** when `excluded_by_policy` is `true`, `exclude_reason` is required (`if/then` in `paragraph_state_row.schema.json`).
- **Schema-enforced:** `failure_history_entry` must be either modern (`attempt >= 1` and ISO-8601 `timestamp`) or legacy sentinel (`attempt: null` and `timestamp: null`) via `oneOf`.
- **Schema-enforced:** `candidate_map_row` requires mapping fields only (`paragraph_id`, `paragraph_index`, `start_line`, `end_line`); `run_id`/`document_id` are optional trace fields.
- **Test-enforced:** `candidate_map_row.end_line >= start_line` is asserted in `tests/test_translation_toolchain_schema_contracts.py`.
- **Test-enforced:** contract tests assert attempt semantics (`state.attempt >= 0`; `failure_history[].attempt >= 1` when non-null).

## Representative examples

### Happy path

```json
{
  "paragraph_id": "p_0001",
  "status": "ready_to_merge",
  "attempt": 1,
  "failure_history": [],
  "excluded_by_policy": false,
  "content_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "scores": {"semantic_fidelity": 0.94, "grammar": 0.91},
  "blocking_issues": [],
  "reviewed_at": "2026-01-15T10:00:00Z",
  "updated_at": "2026-01-15T10:00:00Z",
  "last_failed_at": null,
  "last_success_at": "2026-01-15T10:00:00Z"
}
```

### Rework path

```json
{
  "paragraph_id": "p_0200",
  "status": "rework_queued",
  "attempt": 2,
  "failure_history": [
    {
      "attempt": 2,
      "issues": ["critical_grammar"],
      "timestamp": "2026-01-18T08:10:00Z"
    }
  ],
  "excluded_by_policy": false,
  "content_hash": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "scores": {"semantic_fidelity": 0.87, "grammar": 0.61},
  "blocking_issues": ["critical_grammar"],
  "reviewed_at": "2026-01-18T08:10:00Z",
  "updated_at": "2026-01-18T08:10:00Z"
}
```

### Mapping-error path

```json
{
  "paragraph_id": "p_0101",
  "status": "manual_review_required",
  "attempt": 2,
  "failure_history": [
    {
      "attempt": 2,
      "issues": ["mapping_error"],
      "timestamp": "2026-02-01T10:00:00Z"
    }
  ],
  "excluded_by_policy": false,
  "content_hash": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "scores": {"semantic_fidelity": 0.88},
  "blocking_issues": ["mapping_error"],
  "reviewed_at": "2026-02-01T10:00:00Z",
  "updated_at": "2026-02-01T10:00:00Z",
  "last_failed_at": "2026-02-01T10:00:00Z",
  "last_success_at": null
}
```
