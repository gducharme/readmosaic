# Translation + Quality Gate Toolchain Proposal (Revised v5)

## Objective
Design a translation workflow that:
- reuses existing scripts,
- includes explicit quality gates for grammar, vocabulary, style, voice, and semantic fidelity,
- supports iterative repair when one-shot quality is weak,
- guarantees stage-to-stage compatibility via a shared artifact contract,
- and supports **paragraph-level rework without rerunning the entire manuscript**.

Guiding constraint:
> The toolchain is an orchestrated batch pipeline with resumability — **not** a workflow engine.

Out of scope for this design: DAG schedulers, distributed queues, databases, and cross-run dependency orchestration.

---

## Key Revision: Tamazight Two-Pass Strategy
For Tamazight, quality is often better when processing is split into two passes:

1. **Pass 1 (semantic transfer): Source language → Tamazight content in Latin script**
2. **Pass 2 (script rendering): Tamazight Latin-script content → Tifinagh/Kabyle script form**

This keeps semantic transfer separate from representational script conversion and improves diagnosability.

---

## Core Design Principle: Canonical Interchange Shape
Use preprocessing-like artifacts as the canonical machine interface between stages:

- `paragraphs.jsonl`
- `sentences.jsonl`
- `words.jsonl`
- `manuscript_tokens.json`
- `manifest.json`
- `paragraph_state.jsonl`
- `RUNNING.lock`

Not every script must natively emit canonical shapes. Keep existing script outputs and add lightweight adapters:
- `normalize_translation_output.py`
- `normalize_review_output.py`

---

## Canonical Paragraph Lifecycle (State Machine)
Each paragraph has stable `paragraph_id` + immutable `content_hash` (hash of source paragraph text only).

### Canonical exclusion policy location
`excluded_by_policy` is stored per paragraph in `runs/<run_id>/state/paragraph_state.jsonl` as a boolean plus optional `exclude_reason`.

`manifest.json` may define exclusion rules/templates, but resolved exclusions must be materialized into paragraph state before Phase B and copied forward unchanged unless explicitly reauthorized.

### Canonical states
- `ingested`
- `translated_pass1`
- `translated_pass2` (Tamazight mode)
- `candidate_assembled`
- `review_in_progress`
- `review_failed` *(transient: review completed and did not meet gate before queueing)*
- `rework_queued`
- `reworked`
- `ready_to_merge`
- `manual_review_required`
- `merged`

### Exclusion lifecycle semantics
- Excluded paragraphs (`excluded_by_policy=true`) remain in `paragraph_state.jsonl` for auditability but are **out of pipeline scope**.
- Their `status` remains `ingested` unless an explicit future state is added; they are omitted from translation/review/rework/merge transitions.
- Excluded paragraphs must not appear in `rework_queue.jsonl` or be counted as merge blockers.
- Required-paragraph checks in Phase F are computed only over rows with `excluded_by_policy=false`.
- Exclusion flags and reasons are immutable for a run unless explicitly reauthorized and re-materialized before Phase B.

### Transition rules
- `ingested -> translated_pass1 -> translated_pass2 (optional) -> candidate_assembled`.
- `candidate_assembled -> review_in_progress`.
- If review passes: `ready_to_merge`.
- If review fails: set `review_failed`, then:
  - if attempts remain: `rework_queued`
  - else: `manual_review_required`
- `rework_queued -> reworked -> candidate_assembled -> review_in_progress`.
- `ready_to_merge -> merged` during final assembly.

### `paragraph_state.jsonl` example
```json
{"paragraph_id":"p_0001","content_hash":"sha256:...","status":"ready_to_merge","attempt":1,"failure_history":[],"scores":{"grammar":0.94,"vocabulary":0.88,"style":0.86,"voice":0.90,"semantic_fidelity":0.92},"blocking_issues":[],"updated_at":"2026-02-22T12:00:00Z"}
{"paragraph_id":"p_0002","content_hash":"sha256:...","status":"manual_review_required","attempt":4,"failure_history":["voice_below_threshold","critical_grammar","critical_grammar"],"scores":{"grammar":0.58,"vocabulary":0.77,"style":0.68,"voice":0.63,"semantic_fidelity":0.81},"blocking_issues":["critical_grammar"],"updated_at":"2026-02-22T12:01:00Z"}
```

---

## Proposed Workflow (with explicit I/O contracts)

## Phase A — Source ingest
### Input
- `manuscript/source.md` (or `.txt`)

### Command
```bash
python scripts/pre_processing.py \
  --file manuscript/source.md \
  --output-dir runs/tx_001/source_pre
```

### Output
- canonical bundle in `runs/tx_001/source_pre/`
- initialize `runs/tx_001/state/paragraph_state.jsonl` (`status=ingested`)
- compute + persist `content_hash` for each paragraph
- create `runs/tx_001/RUNNING.lock` for run exclusivity (contains pid/host/start_time/heartbeat)


### RUNNING.lock stale-lock recovery
- A run refreshes lock heartbeat periodically.
- On start/resume, if lock exists and heartbeat is fresh (within TTL), abort with "run already active".
- If lock exists but heartbeat expired, create `RUNNING.stale.<timestamp>.lock` audit record, then allow resume to acquire a new lock.
- Never silently delete a stale lock without writing the audit record.

---

## Phase B — Translation pass 1 (semantic transfer)
### Input
- `runs/tx_001/source_pre/paragraphs.jsonl`
- optional subset list from `runs/tx_001/state/rework_queue.jsonl`

### Command
```bash
python scripts/translate.py \
  --language Tamazight \
  --model <MODEL_ID> \
  --preprocessed runs/tx_001/source_pre \
  --output-root runs/tx_001/translate_pass1
```

### Output
- normalize to `runs/tx_001/pass1_pre/`
- update paragraph status to `translated_pass1`

---

## Phase C — Translation pass 2 (script rendering for Tamazight)
> Required in Tamazight two-pass mode.

### Input
- `runs/tx_001/pass1_pre/paragraphs.jsonl`

### Command
```bash
python scripts/translate.py \
  --language Tifinagh \
  --model <MODEL_ID> \
  --preprocessed runs/tx_001/pass1_pre \
  --output-root runs/tx_001/translate_pass2
```

### Output
- normalize to `runs/tx_001/pass2_pre/`
- update paragraph status to `translated_pass2`

For non-Tamazight languages, pass1 output feeds candidate assembly directly.

---

## Phase C.5 — Candidate assembly (producer for review input)
This phase explicitly produces the manuscript consumed by manuscript-level reviewers.

### Producer invariants (stability contract)
- `scripts/assemble_candidate.py` is the **only producer** of `candidate.md` + `candidate_map.jsonl`.
- It enforces **one paragraph = one contiguous block**.
- Paragraph blocks are separated by **exactly one blank line**.

### Input
- paragraph texts from latest normalized translation stage (`pass2_pre` for Tamazight, otherwise `pass1_pre`)

### Output
- `runs/tx_001/final/candidate.md`
- `runs/tx_001/final/candidate_map.jsonl`

`candidate_map.jsonl` minimum shape:
```json
{"paragraph_id":"p_0001","paragraph_index":1,"start_line":1,"end_line":4}
{"paragraph_id":"p_0002","paragraph_index":2,"start_line":6,"end_line":8}
```
`paragraph_index` is **1-based** in this proposal.

Update all included paragraph states to `candidate_assembled`.

---

## Phase D — Review chain (paragraph-scoped aggregation)
Review aggregation requires paragraph-addressable outputs.

### Pipeline-profile conditional review inputs
Define relative `review_pre_dir` by `pipeline_profile`:
- `tamazight_two_pass` -> `review_pre_dir = pass2_pre`
- `standard_single_pass` -> `review_pre_dir = pass1_pre`

All paragraph-scoped reviewers must read from `runs/<run_id>/<review_pre_dir>` (never hard-code `pass2_pre`).

### D1 Grammar (already paragraph-friendly)
```bash
python scripts/grammar_auditor.py \
  --preprocessed runs/tx_001/<review_pre_dir> \
  --model <MODEL_ID> \
  --output-dir runs/tx_001/review/grammar
```

### D2 Typography (manuscript-level reviewer)
```bash
python scripts/typographic_precision_review.py \
  --file runs/tx_001/final/candidate.md \
  --model <MODEL_ID> \
  --output-dir runs/tx_001/review/typography
```

### D3 Critics (manuscript-level reviewer)
```bash
python scripts/critics_runner.py \
  --model <MODEL_ID> \
  --manuscript runs/tx_001/final/candidate.md \
  --critics-dir prompts/critics \
  --output runs/tx_001/review/critics/review.json
```

### D4 Manuscript-level reviewer anchoring contract
Outputs from D2/D3 must include at least one anchor per issue:
- `line` or `start_line`/`end_line`, **or**
- bounded `quote` that can be located in `candidate.md`.

`map_review_to_paragraphs.py` uses anchors + `candidate_map.jsonl` to emit paragraph-scoped rows.
`map_review_to_paragraphs.py` deterministic resolution order:
1. If `start_line`/`end_line` exists, map by line-range overlap.
2. Else if single `line` exists, map by containing paragraph range.
3. Else if `quote` exists, map exact match within paragraph ranges; tie-break by earliest line, then lowest `paragraph_index`.
4. If still ambiguous or not found, emit `mapping_error` for that issue and set paragraph/run hard-fail policy trigger.

### D5 Minimal review output contract (required)
Every normalized review row must include:

```json
{
  "paragraph_id": "p_0002",
  "scores": {"grammar": 0.62, "semantic_fidelity": 0.83},
  "issues": [],
  "hard_fail": true
}
```

Any extra fields are optional.

### D6 Aggregation + queueing
Note: grammar outputs are already paragraph-scoped; `normalize_review_output.py` may pass them through unchanged (optionally emitting `review/normalized/grammar_paragraph_rows.jsonl` for symmetry).

- `scripts/aggregate_paragraph_reviews.py` merges normalized review rows by `paragraph_id`
- writes:
  - `runs/tx_001/state/paragraph_scores.jsonl`
  - `runs/tx_001/state/rework_queue.jsonl`
  - updated `paragraph_state.jsonl`

Pass -> `ready_to_merge`; Fail -> `review_failed` then `rework_queued` or `manual_review_required`.

---

## Phase E — Rework loop (targeted, non-blocking)
### Behavior
- Rework only `rework_queued` paragraphs.
- Do not re-run successful paragraphs.
- Continue flow progress for all `ready_to_merge` paragraphs.

### Command
```bash
python scripts/translation_toolchain.py \
  --run-id tx_001 \
  --mode rework-only \
  --max-paragraph-attempts 4
```

### Rework packet shape
```json
{
  "paragraph_id": "p_0002",
  "content_hash": "sha256:...",
  "current_text": "...",
  "source_text": "...",
  "failure_reasons": ["voice_below_threshold", "critical_grammar"],
  "failure_history": ["voice_below_threshold", "critical_grammar"],
  "required_fixes": ["preserve semantics", "raise voice fidelity", "fix grammar"],
  "attempt": 2
}
```

On each failed attempt append to `failure_history`.

---

## Phase F — Final assembly and publish gate
Only paragraphs in `ready_to_merge` may be assembled into final output.

### Definition: required paragraph
A **required paragraph** is any paragraph ingested in Phase A with `excluded_by_policy=false` in `paragraph_state.jsonl`.

### Merge rules
- preserve original paragraph order,
- require all required `paragraph_id`s present,
- verify each merged translation paragraph references the same source `content_hash` recorded at ingest,
- block publish if any required paragraph is in `rework_queued` or `manual_review_required`.

### Output
- `runs/tx_001/final/final.md`
- `runs/tx_001/final/final_pre/`
- update assembled states to `merged`
- remove `RUNNING.lock`


---

## Golden path run example (single run_id)
This is one end-to-end example for `run_id=tx_001` using `pipeline_profile=tamazight_two_pass`.

### Command sequence (happy path)
```bash
python scripts/translation_toolchain.py \
  --mode full \
  --run-id tx_001 \
  --pipeline-profile tamazight_two_pass \
  --source manuscript/source.md \
  --model <MODEL_ID> \
  --max-paragraph-attempts 4

python scripts/translation_toolchain.py \
  --mode status \
  --run-id tx_001
```

Assumptions for this golden path:
- `--mode full` executes Phase A through Phase F end-to-end and initializes `runs/tx_001/manifest.json` during Phase A.
- `translation_toolchain.py --mode` supports exactly `full|rework-only|status` in this proposal.
- `--mode status --run-id <id>` is the canonical status form used in this doc.
- This happy-path snapshot assumes no paragraph failures requiring explicit `--mode rework-only`; if failures occur, `--mode rework-only` is used per Phase E.
- Raw translation output folders (`translate_pass1/`, `translate_pass2/`) may exist but are optional/ephemeral; `pass1_pre/` and `pass2_pre/` are canonical.

### Expected abbreviated **post-run** directory tree
```text
runs/
└── tx_001/
    ├── manifest.json
    ├── source_pre/
    │   ├── paragraphs.jsonl
    │   ├── sentences.jsonl
    │   ├── words.jsonl
    │   └── manuscript_tokens.json
    ├── pass1_pre/
    │   ├── paragraphs.jsonl
    │   └── sentences.jsonl
    ├── pass2_pre/
    │   ├── paragraphs.jsonl
    │   └── sentences.jsonl
    ├── state/
    │   ├── paragraph_state.jsonl
    │   ├── paragraph_scores.jsonl
    │   └── rework_queue.jsonl (empty in no-failure happy path)
    ├── review/
    │   ├── grammar/
    │   ├── typography/
    │   ├── critics/
    │   └── normalized/
    │       ├── typography_paragraph_rows.jsonl
    │       └── critics_paragraph_rows.jsonl
    ├── final/
    │   ├── candidate.md
    │   ├── candidate_map.jsonl
    │   ├── final.md
    │   └── final_pre/
    │       └── paragraphs.jsonl
    └── gate/
        └── gate_report.json
```

In this post-run snapshot, all required paragraphs reached `merged` during final assembly, `final/final.md` was written, and `RUNNING.lock` has been removed.
`final/final_pre/` contains the canonical preprocessed bundle for `final.md` (same shape as `source_pre/`, e.g., `paragraphs.jsonl`, `sentences.jsonl`, `words.jsonl`, `manuscript_tokens.json`).


---

## Output-to-Input Mapping Table

| Producer stage | Produced artifact | Consumer stage | Expected shape |
|---|---|---|---|
| Pre-processing | `source_pre/paragraphs.jsonl` | Translate pass1 | `paragraph_id`, `text`, `content_hash` |
| Translate pass1/2 | raw text | Normalizer | free text/markdown |
| Normalizer | `pass*_pre/*` | Candidate assembler | canonical bundle |
| Candidate assembler | `final/candidate.md` + `final/candidate_map.jsonl` | Manuscript-level reviewers | markdown + paragraph map |
| Manuscript-level reviewers | issue outputs with anchors | Paragraph mapper | reviewer-native outputs |
| Paragraph mapper | `review/normalized/*.jsonl` | Aggregator | minimal review contract rows |
| Aggregator | `rework_queue.jsonl`, updated state | Rework runner / status CLI | paragraph task packets |
| Merger | ready paragraphs | Final package | ordered markdown + canonical bundle |

---

## Artifact Contract Proposal
Each run should include:

- `runs/<run_id>/RUNNING.lock` (**runtime-only**; present only while run is active and removed during successful completion in Phase F)
- `runs/<run_id>/manifest.json`
- `runs/<run_id>/state/paragraph_state.jsonl`
- `runs/<run_id>/state/rework_queue.jsonl`
- `runs/<run_id>/state/paragraph_scores.jsonl`
- `runs/<run_id>/source_pre/*`
- `runs/<run_id>/pass1_pre/*`
- `runs/<run_id>/pass2_pre/*` (if enabled)
- `runs/<run_id>/final/candidate.md`
- `runs/<run_id>/final/candidate_map.jsonl`
- `runs/<run_id>/review/normalized/*.jsonl`
- `runs/<run_id>/final/final.md`
- `runs/<run_id>/gate/gate_report.json`

`manifest.json` should minimally record:
- `run_id`, `language`
- `pipeline_profile` (`standard_single_pass` | `tamazight_two_pass`)
- `semantic_language` (e.g., `tamazight`)
- `script_mode` (`latin` pass1, `tifinagh` pass2)
- stage input/output paths
- per-stage model + prompt IDs
- thresholds and rework policy
- required/excluded paragraph policy
- counts by canonical state

---

## Minimal Implementation Changes
1. `scripts/translation_toolchain.py` with `--mode full|rework-only|status`.
2. `scripts/translation_toolchain.py --mode status --run-id <run_id>` (state counts).
3. `scripts/normalize_translation_output.py`.
4. `scripts/assemble_candidate.py`.
5. `scripts/map_review_to_paragraphs.py`.
6. `scripts/aggregate_paragraph_reviews.py`.
7. Structured critic output support in `scripts/critics_runner.py`.
8. `docs/translation_toolchain_runbook.md`.

---

## Stage 1 gate policy (keep simple)
Pass paragraph if:
- `hard_fail == false`, and
- each required score meets threshold:
  - `grammar`, `vocabulary`, `style`, `voice`, `semantic_fidelity`.

Stage 1 gate considers only `{grammar, vocabulary, style, voice, semantic_fidelity}`; extra review metrics are informational.

No weighted composites in Stage 1.

---

## Recommended defaults for current open decisions
1. **Merge control**: Stage 1 uses explicit approval gate; Stage 2 may enable auto-merge via feature flag.
2. **Immediate `manual_review_required` triggers**:
   - `mapping_error` unresolved after normalization
   - `semantic_fidelity` below hard floor
   - repeated identical hard-fail reason across >=2 attempts
   - source `content_hash` lineage mismatch / stale-source detection
   - missing/corrupt required paragraph artifact in normalizer/parser pipeline
3. **Rework batching**: enable small-batch rework in Stage 1 (default configurable range 5-20 paragraphs per request).
