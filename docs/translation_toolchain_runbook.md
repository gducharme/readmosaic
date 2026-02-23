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

## Run lock semantics (`RUNNING.lock`)

`scripts/translation_toolchain.py` owns run-exclusivity via `runs/<run_id>/RUNNING.lock`.

### Constants

- `LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS = 10`.
- `LOCK_STALE_TTL_SECONDS = 120`.
- `LOCK_STALE_RETRY_BASE_SLEEP_SECONDS = 0.05` (with jitter) to avoid tight spin loops in stale-lock races.
- `LOCK_FILE_NAME = RUNNING.lock`.
- `LOCK_STALE_TTL_SECONDS` must remain greater than `2 * LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS`.

### Lock file shape

Lock payload is JSON and must contain exactly the required operational fields:

- `pid` (integer): process ID that acquired the lock.
- `host` (string): hostname where the process is running.
- `started_at` (string, ISO-8601 UTC): run start timestamp.
- `last_heartbeat_at` (string, ISO-8601 UTC): last heartbeat write timestamp.
- `run_id` (string): run identifier tied to the lock.

Invalid JSON, missing fields, empty strings, or malformed timestamps are treated as an invalid lock condition.

### Startup behavior when `RUNNING.lock` already exists

1. The tool tries an atomic create (`O_CREAT|O_EXCL`) for `RUNNING.lock`.
2. If creation fails because the file exists:
   - Parse and validate existing lock content.
   - If `last_heartbeat_at` is within `LOCK_STALE_TTL_SECONDS`, startup aborts with an "active run" error.
   - If `last_heartbeat_at` is older than `LOCK_STALE_TTL_SECONDS`, treat as stale and archive it before retrying acquisition.
   - Stale retry includes a short randomized backoff to reduce lock-thrashing under concurrent startup.

### Stale-lock archival behavior

Stale locks are **never silently deleted**.

- Existing `RUNNING.lock` is atomically renamed to:
  - `RUNNING.stale.<timestamp>.lock` where `<timestamp>` is the stale lock heartbeat epoch seconds.
- Stale archival rename is followed by a best-effort directory fsync for durability.
- If that filename already exists, a numeric suffix is added:
  - `RUNNING.stale.<timestamp>.<n>.lock`.
- Archival rename handles concurrent name-claim races by retrying with incremented suffixes until replace succeeds.
- After archival, acquisition retries until a fresh lock is created or a fresh competing lock wins.

### Race-safe acquisition/release flow

Acquisition:

1. Attempt `O_EXCL` lock create.
2. On `FileExistsError`, inspect existing lock.
3. Fresh lock => exit without mutating lock file.
4. Stale lock => rename to `RUNNING.stale.*.lock` and loop back to step 1.
5. Successful create writes lock JSON with all required fields.
6. Lock creation is flushed and fsynced for crash durability.

Heartbeat:

- While running, update `last_heartbeat_at` every `LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS`.
- Before each heartbeat rewrite, verify `RUNNING.lock` still exists, still belongs to the same `run_id`, and still matches expected file identity.
- Heartbeat rewrites are atomic (`write temp + fsync + replace`) to avoid partial/corrupt lock JSON on interruption.
- If ownership/identity checks fail, heartbeat aborts with invalid-lock behavior instead of recreating/replacing a lock no longer owned by this process.

Release:

1. Read and validate current `RUNNING.lock`.
2. Verify on-disk `run_id` matches caller `run_id`.
3. Only then unlink `RUNNING.lock`.
4. If IDs mismatch, refuse removal to avoid deleting another process's lock.
5. Release verifies lock file identity (device/inode) before unlink to reduce TOCTOU replacement risk.

### Failure modes and recovery

- **Interrupted process (SIGKILL/crash/host reboot)**:
  - Lock may remain on disk.
  - Recovery path is automatic: next startup detects stale heartbeat, archives stale lock, and reacquires.
- **Corrupt lock file**:
  - Startup exits with invalid-lock error and requires operator inspection/fix.
  - Recommended operator action: inspect content, rename `RUNNING.lock` to `RUNNING.invalid.<timestamp>.lock` for audit, then restart acquisition.
- **Interrupted during atomic heartbeat rewrite**:
  - `RUNNING.lock` remains valid (old-or-new), but `.{RUNNING.lock}.tmp.<pid>` temp files may remain.
  - Startup performs best-effort cleanup for old temp files; operators may safely remove leftover temp files if needed.
- **Concurrent startup races**:
  - Atomic create ensures only one process acquires active lock.
  - Losers observe fresh lock and exit as active-run conflict.
  - If stale lock archival races occur, jittered backoff reduces retry contention.
- **Release mismatch**:
  - If lock `run_id` changed before release, process exits with invalid-lock error and does not unlink.

### Clock and filesystem assumptions

- Staleness is computed from wall-clock timestamps (`time.time()` vs `last_heartbeat_at`); large backward/forward clock jumps can delay or accelerate stale detection.
- This lock design assumes a POSIX-like local/shared filesystem with atomic rename semantics; avoid eventually consistent/object-store backends.
- Lock behavior is intended for shared-disk coordination, not distributed consensus across independent storage replicas.

### Expected CLI exit codes and messages (`scripts/translation_toolchain.py`)

- `0` (`EXIT_OK`): lock acquired and released successfully.
- `2` (`EXIT_ACTIVE_LOCK`): active fresh lock exists. Message includes `Run already active: fresh RUNNING.lock exists ...`.
- `3` (`EXIT_INVALID_LOCK`): lock invalid/corrupt or safe-release check failed. Message starts with `Invalid lock encountered:` or `Failed to release lock safely:`.
- `4` (`EXIT_LOCK_RACE`): OS-level lock acquisition/write failure. Message starts with `Failed to acquire RUNNING.lock:`.

Operationally, stale-lock archival emits an audit message to stderr of the form:

- `Archived stale lock to <path>/RUNNING.stale.<timestamp>.lock (...)`.
