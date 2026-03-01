# seedpipe

## Seedpipe

**Seedpipe is a universal, deterministic pipeline substrate for agent-first workflows.**

It provides the minimal control plane required for autonomous systems to run, resume, compose, and audit long-running work — using the filesystem as the single source of truth and a compiler-owned runtime for execution.

Instead of introducing a scheduler, queue, or orchestration service, Seedpipe models work as a **deterministic artifact state machine**:

- Every run is fully reproducible from its inputs, spec, and source.
- `rerun = resume` by construction.
- All state is explicit, machine-readable, and auditable.
- Agents operate the system by **writing files, executing the runner, and inspecting artifacts** — no hidden APIs.

This makes Seedpipe:

- **Agent-native** – autonomous coding/ops agents can safely control and evolve pipelines
- **Composable** – pipelines chain through inbox/outbox conventions and fan-out into child runs
- **Chainable** – pipelines chain can support entire product lifecycle, piece-by-piece until full workflow is captured in pipeline
- **Idempotent by default** – side effects are tracked through artifact truth
- **Portable** – no control-plane service or runtime database required
- **Replayable & forkable** – any run can be reproduced or branched deterministically

In practical terms, the same substrate can power:

- CI/CD promotion and rollback
- data platform ELT and backfills
- financial and operational lifecycles
- incident response workflows
- iterative optimization loops

without changing the core model.

> **Seedpipe is not a workflow engine.  
> It is the smallest expressive basis for reliable work in an agent-driven environment.**

`seedpipe` can be installed directly from a local checkout without publishing to PyPI.

## Roadmap tracking docs

- Phase 4 agent-operable control plane tracking doc: `docs/phase3_agent_control_plane.md`

## Install from a local path

From another project, add this repository as a local dependency:

```bash
python -m pip install /path/to/seedpipe
```

For editable development installs:

```bash
python -m pip install -e /path/to/seedpipe
```

## What gets installed

- `seedpipe` package.
- `tools` package (including `tools.compile` and `tools.scaffold`).
- `seedpipe-compile` CLI entrypoint.
- `seedpipe-scaffold` CLI entrypoint.
- `seedpipe-run` CLI entrypoint.

After install, you can run:

```bash
seedpipe-compile --help
seedpipe-scaffold --help
seedpipe-run --help
```

If these entrypoints are unavailable in your environment, run the modules directly from a checkout:

```bash
python -m tools.compile --help
python -m tools.scaffold --help
python -m tools.run --help
```

Or import in Python:

```python
from tools.compile import compile_pipeline, CompilePaths
from tools.scaffold import scaffold_project
```

## Start a new project with scaffold

To bootstrap a minimal Seedpipe layout in your current directory:

```bash
seedpipe-scaffold
```

This creates:

- `agents.markdown` (agent guidance, including artifact directory expectations and *never edit `generated/` directly*)
- `agents-readme.markdown` (copy of this repository README for agent context)
- `spec/phase1/pipeline.yaml`
- `spec/phase1/contracts/*.schema.json`
- `spec/stages/<stage_id>/*.schema.json` (default runtime-enforced stage output schemas)
- `artifacts/inputs/.gitkeep`
- `artifacts/outputs/.gitkeep`
- starter stage implementations in `src/stages/*.py`

To scaffold somewhere else:

```bash
seedpipe-scaffold --dir /path/to/your/project
```

Use `--force` to overwrite an existing scaffold.

To scaffold a loop-enabled starter pipeline:

```bash
seedpipe-scaffold --loop
```

## Simple pipeline example

The scaffold command currently writes this starter pipeline:

```yaml
pipeline_id: phase1-default
item_unit: item
determinism_policy: strict
stages:
  - id: ingest
    mode: whole_run
    inputs: []
    outputs:
      - items.jsonl
  - id: transform
    mode: per_item
    inputs:
      - items.jsonl
    outputs:
      - transformed.jsonl
  - id: validate
    mode: whole_run
    inputs:
      - transformed.jsonl
    outputs:
      - validation_report.json
  - id: future_review
    mode: whole_run
    placeholder: true
    inputs:
      - validation_report.json
    outputs:
      - reviewed_report.json
  - id: publish
    mode: whole_run
    inputs:
      - reviewed_report.json
    outputs:
      - published_manifest.json
```

With `seedpipe-scaffold --loop`, scaffold writes:

```yaml
pipeline_id: example-pipeline-loop
item_unit: item
determinism_policy: strict
pipeline_type: looping
max_loops: 3
stages:
  - id: ingest
    mode: whole_run
    inputs: []
    outputs:
      - family: items
        pattern: items.jsonl
        schema: items_row.schema.json
  - id: seed
    mode: per_item
    inputs:
      - family: items
        pattern: items.jsonl
        schema: items_row.schema.json
    outputs:
      - family: seeded
        pattern: seeded.jsonl
        schema: items_row.schema.json
    reentry: retry_seed
  - id: transform
    mode: per_item
    inputs:
      - family: seeded
        pattern: seeded.jsonl
        schema: items_row.schema.json
    outputs:
      - family: transformed
        pattern: transformed.jsonl
        schema: transformed_row.schema.json
    go_to: retry_seed
  - id: publish
    mode: whole_run
    inputs:
      - family: transformed
        pattern: transformed.jsonl
        schema: transformed_row.schema.json
    outputs:
      - family: manifest
        pattern: manifest.json
        schema: manifest.schema.json
```

## More full-featured pipeline example

This example combines stage fan-out (`foreach`/`key`), family outputs and inputs (`family` + `pattern`), per-item and whole-run stages, schema validation, and loop rerouting (`reentry` + `go_to`):

```yaml
pipeline_id: localization-release
item_unit: paragraph
determinism_policy: strict
pipeline_type: looping
max_loops: 3
params:
  targets:
    languages: [fr, de, es]
stages:
  - id: ingest_source
    mode: whole_run
    outputs:
      - items.jsonl

  - id: draft_translation
    foreach: params.targets.languages
    key: lang
    mode: per_item
    inputs:
      - items.jsonl
    outputs:
      - family: pass1_translations
        pattern: pass1_pre/{lang}/paragraphs.jsonl
        schema: paragraphs.schema.json
    reentry: retry_draft

  - id: qa_pass
    foreach: params.targets.languages
    key: lang
    mode: whole_run
    inputs:
      - family: pass1_translations
        pattern: pass1_pre/{lang}/paragraphs.jsonl
        schema: paragraphs.schema.json
    outputs:
      - family: qa_reports
        pattern: qa/{lang}/report.json
        schema: qa-report.schema.json
    go_to: retry_draft

  - id: publish
    mode: whole_run
    inputs:
      - qa/fr/report.json
      - qa/de/report.json
      - qa/es/report.json
    outputs:
      - published_manifest.json
```

## `pipeline.yaml` reference and design guidance

`spec/phase1/pipeline.yaml` is the pipeline contract used by the compiler. It can be written as YAML (recommended for readability) or JSON (valid YAML). The compiler loads this file, normalizes defaults, validates structure, and emits runnable code under `generated/`.

### Top-level fields

- `pipeline_id` *(string, required)*
  - Unique identifier for the pipeline.
  - Used in generated metadata and compile reports.

- `item_unit` *(string, optional, default: `item`)*
  - Human/semantic label for the per-item unit being processed.
  - Example values: `item`, `record`, `paragraph`, `doc`.

- `determinism_policy` *(enum, optional, default: `strict`)*
  - Allowed values: `strict`, `best_effort`.
  - Current compiler/runtime validates and propagates this value into generated flow metadata.
  - Practical recommendation: use `strict` unless you have a clear reason to track weaker determinism guarantees.

- `pipeline_type` *(enum, optional, default: `straight`)*
  - Allowed values: `straight`, `looping`.
  - `straight` forbids stage loop metadata (`reentry`, `go_to`).
  - `looping` enables runtime loop execution for per-item failures.

- `max_loops` *(integer, optional, default: `0`)*
  - Global loop budget for runtime loop-capable flows.
  - Must be `>= 0`.
  - For `pipeline_type: straight`, this must remain `0`.
  - For `pipeline_type: looping`, this must be `>= 1`.

- `stages` *(array, required, at least one stage)*
  - Ordered list of stages to execute.
  - Order is meaningful: stages can only consume artifacts produced by earlier stages.

### Stage fields

Each stage entry supports:

- `id` *(string, required)*
  - Unique stage identifier.
  - Used for generated module names and item-state provenance.

- `mode` *(enum, optional, default: `whole_run`)*
  - `whole_run`: runs once via `run_whole(ctx)`.
  - `per_item`: iterates items from `items.jsonl`, runs `run_item(ctx, item)` for each item, and appends item-state transitions.

- `inputs` *(array of strings, optional, default: `[]`)*
  - Declares artifacts required before stage execution.
  - Compiler enforces that each input is produced by a previous stage.
  - Runtime wrapper validates all listed inputs exist.

- `outputs` *(array of strings, optional, default: `[]`)*
  - Declares artifacts that must exist after the stage finishes.
  - Runtime wrapper validates all declared outputs exist.
  - If your stage doesn’t write one of these files, the run fails.

- `placeholder` *(boolean, optional, default: `false`)*
  - Marks a stage as planned/no-op implementation.
  - Compiler skips importing user stage code for placeholder stages.
  - Placeholder stages skip forward-input dependency checks so they can reference planned artifacts not yet produced upstream.

- `reentry` *(string, optional)*
  - Declares a named loop anchor for this stage.
  - Valid only when `pipeline_type: looping`.
  - Reentry names must be unique across all stages.

- `go_to` *(string, optional)*
  - Declares a loop jump target by reentry name.
  - Valid only when `pipeline_type: looping`.
  - Target must resolve to an earlier stage's `reentry` name.

### Optional DSL expansion (`foreach`, `key`, `family`, `pattern`, `schema`)

The compiler can also expand a higher-level DSL into the same linear stage model used by runtime wrappers.

Supported forms:

- Stage fan-out:
  - `foreach: <dot.path.to.list>` with `key: <var>` at stage level creates one concrete stage per value.
  - Concrete stage IDs are suffixed as `<id>__<value>` (sanitized for module-safe names).

- Family outputs:
  - In `outputs`, an object entry can declare a keyed family artifact:
    - `family`: family name
    - `pattern`: templated concrete path (for example `pass1_pre/{lang}/paragraphs.jsonl`)
    - optional key source via either:
      - `key: <var>` (uses current scope variable), or
      - `foreach` + `key` inside the output object (fan-out inside a single stage)

- Family inputs:
  - In `inputs`/`outputs`, object entries consistently use `family`, `pattern`, and `schema`; the concrete path is rendered from `pattern` using the current key scope.

- Minimal two-stage example (produce then consume by family key):

```yaml
pipeline_id: translation-pipeline
params:
  targets:
    languages: [fr, de]
stages:
  - id: produce_pass1
    outputs:
      - family: pass1_translations
        foreach: params.targets.languages
        key: lang
        pattern: pass1_pre/{lang}/paragraphs.jsonl
        schema: paragraphs.schema.json

  - id: consume_pass1
    foreach: params.targets.languages
    key: lang
    inputs:
      - paragraphs.jsonl
      - family: pass1_translations
        pattern: pass1_pre/{lang}/paragraphs.jsonl
        schema: paragraphs.schema.json
    outputs:
      - pass2_pre/{lang}/paragraphs.jsonl
```

In this example, both stages use the same `pattern` template, and `key: lang` drives deterministic `{lang}` interpolation for concrete artifact paths.

- Templating:
  - String `inputs` and `outputs` may include `{var}` placeholders resolved from stage/output scope variables.

- Runtime stage context keys:
  - Generated flow passes resolved stage `keys` into `StageContext`.
  - Generated flow also passes `expected_outputs`: a per-output list containing the original `pattern`, concrete `path`, and the `keys` used to render that path.
  - This gives stage/runtime code an explicit link between output template patterns and concrete key values.

- Schema enforcement via `schema` key:
  - When an output is declared with `schema`, runtime validates the produced artifact after stage execution.
  - `.jsonl` outputs are validated row-by-row; `.json` outputs are validated as a single document.
  - By default, stage schema files should live at `spec/stages/<stage_id>/<schema>`.
  - Example: stage `source_ingest` output with `schema: paragraphs.schema.json` resolves to `spec/stages/source_ingest/paragraphs.schema.json`.

The expanded result is still validated using normal Phase-1 rules (`inputs`/`outputs` become plain string arrays before validation and code generation).

### Artifact wiring rules (important)

1. **No forward references in `inputs`**: a stage cannot consume an artifact that has not already been declared as an output of an earlier stage.
2. **Declare what you actually produce**: declared outputs are enforced at runtime.
3. **Use stable artifact names**: downstream stage contracts depend on exact names.
4. **Loop routing is explicit**: in `looping` pipelines, `go_to` must reference a known earlier `reentry`; failed per-item cohorts are rerouted to that reentry stage.

### How this affects compile and run flows

- During **compile** (`seedpipe-compile`), the spec is:
  - loaded and normalized with defaults,
  - validated for required fields and ordering rules,
  - transformed into generated wrappers/flow/models.

- During **run** (`seedpipe-run`), generated wrappers:
  - validate stage inputs before execution,
  - call your stage implementation (`src/stages/*.py`) unless placeholder,
  - validate stage outputs after execution.
  - enforce any declared output `schema` files from `spec/stages/<stage_id>/`.
  - snapshot stage outputs into loop-scoped paths (`<stage>/loops/<NNNN>/...`) and maintain a manifest artifact index so downstream logical artifact names resolve to latest concrete files.
  - artifact index entries must stay within the run directory (relative paths without `..`); `seedpipe-run` raises when a snapshot path is absolute or escapes the workdir.
  - manifest tracks `loop_iteration` (starts at 1 for the first pass and increments before rerunning a failed cohort); pipelines error when a reroute would raise `loop_iteration` **greater than** `max_loops`.
  - for `pipeline_type: looping`, collect failed per-item results (stage business failures and runtime validation failures), and rerun only that failed cohort from the configured `go_to` reentry stage until success or `max_loops` is reached.

### Best practices when creating/generating pipelines

- Start simple: `ingest` → one or more transforms → validation/publish.
- Use `per_item` only when input rows have stable `item_id` values.
- Keep `stages` linear and explicit; avoid overloading one stage with too many responsibilities.
- Keep contracts in `spec/phase1/contracts` aligned with emitted artifact formats.
- Re-run the compiler after any `pipeline.yaml` change.
- Do not hand-edit `generated/`; it is compiler-owned output.

## Compile a pipeline specification

Run the compiler against a pipeline file and contracts directory:

```bash
seedpipe-compile \
  --pipeline ./spec/phase1/pipeline.yaml \
  --contracts-dir ./spec/phase1/contracts \
  --output-dir ./generated
```

This generates orchestration/runtime code and metadata in `./generated`.

If your repository uses the default layout (`spec/phase1/pipeline.yaml`,
`spec/phase1/contracts`, `generated`), run:

```bash
python -m tools.compile
```

This command compiles the pipeline using defaults and refreshes everything under `generated/`.

> `generated/` is compiler-owned output. Make implementation changes in `src/stages/`, then re-run compilation.

You can also use the installed entrypoint:

```bash
seedpipe-compile
```

## Run a compiled pipeline

After compiling, execute the generated flow with:

```bash
seedpipe-run --run-id my-run-001
```

By default this loads `generated/flow.py`, creates a run directory at
`./artifacts/outputs/<run-id>`, and executes the flow from inside that run directory.

Use:
- `--generated-dir` when compiled flow code lives somewhere other than `./generated`.
- `--inputs-dir` to set the consumable input root (default: `./artifacts/inputs`).
- `--output-dir` to override the run output directory.
- `--attempt` to set a non-default retry attempt number.

`seedpipe-run` will error if the run output directory already exists (including the default `./artifacts/outputs/<run-id>` path), and will error if the inputs directory does not exist.

## Use the compiler from Python

```python
from pathlib import Path
from tools.compile import CompilePaths, compile_pipeline

result = compile_pipeline(
    CompilePaths(
        pipeline_path=Path("spec/phase1/pipeline.yaml"),
        contracts_dir=Path("spec/phase1/contracts"),
        output_dir=Path("generated"),
    )
)

print(result["pipeline_id"], result["output_dir"])
```
