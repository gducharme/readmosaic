# Narrative Diagnostics Contract v1

## Contract ID
- `contract_version`: `narrative_diagnostics.v1`

## Top-level fields
- `contract_version` (string, required)
- `run` (object, required)
- `metrics` (object, required)
- `highlights` (array, required)
- `anchors` (object, optional)
- `artifacts` (object, optional)

## `run`
- `run_id` (string, required)
- `created_at` (ISO-8601 string, required)
- `input` (object, optional)
  - `source_path` (string, optional)
  - `source_hash` (string, optional)
  - `language` (string, optional)
- `tool_versions` (object, optional)

## `metrics`
Required keys in v1:
- `semantic_repetition`
- `signal_density`
- `surprisal`
- `entropy`
- `burstiness`
- `themes`
- `patterns`

Each metric entry is an object and should include at minimum:
- `summary` (object)
- `highlights` (array)

## `highlights`
Each highlight object should contain:
- `kind` (string)
- `severity` (string, e.g. `info|low|medium|high`)
- `message` (string)
- `anchors` (object, optional; paragraph-level anchors preferred)
- `source_metric` (string, optional)

## Compatibility rules
- New optional keys may be added without version bump.
- Renaming/removing required keys requires a version bump (`v2`).
- Consumers should ignore unknown keys.
- Anchoring strategy must remain paragraph-compatible (or include explicit migration notes).
