# seedpipe

`seedpipe` can be installed directly from a local checkout without publishing to PyPI.

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
- `artifacts/inputs/.gitkeep`
- `artifacts/outputs/.gitkeep`
- starter stage implementations in `src/stages/*.py`

To scaffold somewhere else:

```bash
seedpipe-scaffold --dir /path/to/your/project
```

Use `--force` to overwrite an existing scaffold.

## Simple pipeline example

The scaffold command writes this starter pipeline:

```yaml
pipeline_id: example-pipeline
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
  - id: publish
    mode: whole_run
    inputs:
      - transformed.jsonl
    outputs:
      - manifest.json
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
  - Input/output validation still applies, so placeholders should be used carefully.

### Artifact wiring rules (important)

1. **No forward references in `inputs`**: a stage cannot consume an artifact that has not already been declared as an output of an earlier stage.
2. **Declare what you actually produce**: declared outputs are enforced at runtime.
3. **Use stable artifact names**: downstream stage contracts depend on exact names.

### How this affects compile and run flows

- During **compile** (`seedpipe-compile`), the spec is:
  - loaded and normalized with defaults,
  - validated for required fields and ordering rules,
  - transformed into generated wrappers/flow/models.

- During **run** (`seedpipe-run`), generated wrappers:
  - validate stage inputs before execution,
  - call your stage implementation (`src/stages/*.py`) unless placeholder,
  - validate stage outputs after execution.

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