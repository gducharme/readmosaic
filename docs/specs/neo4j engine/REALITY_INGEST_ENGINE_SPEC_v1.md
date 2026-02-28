# Reality Ingestion Engine Production Spec (v1)

## 1) Objective

Design a generic Python class library, `RealityIngestor`, that ingests one Markdown chapter and updates a Neo4j metagraph with high fidelity across any narrative mode (combat, diplomacy, biological drift, introspection).

The system must:
- Parse text into immutable chunks.
- Retrieve current world state before extraction.
- Extract graph payload with structured LLM output and strict validation.
- Resolve entities against existing graph IDs with vector-based conflict detection.
- Present a human-reviewed diff.
- Commit idempotent transactions into Neo4j.

---

## 2) Runtime and Environment Assumptions

From repository `docker-compose.yaml`:
- Neo4j Bolt URI: `bolt://localhost:7687`
- Username: `neo4j`
- Password: `mosaic_founding`
- APOC: enabled

Required Python runtime:
- Python 3.11+
- `neo4j`, `rich`, `spacy`, `jsonschema`, `rapidfuzz`, `sentence-transformers`
- One LLM stack:
  - `litellm` + `instructor`, or
  - `langchain` with structured output parser

Environment variables:
- `REALITY_NEO4J_URI=bolt://localhost:7687`
- `REALITY_NEO4J_USER=neo4j`
- `REALITY_NEO4J_PASSWORD=mosaic_founding`
- `REALITY_MODEL=gpt-4o` (swappable)
- `REALITY_EMBED_MODEL=text-embedding-3-large` (or local sentence-transformer)
- `REALITY_ARTIFACT_DIR=./artifacts/reality_ingestor`
- `REALITY_CONFLICT_THRESHOLD=0.88`

---

## 3) Package and Script Layout

Recommended module layout:

```text
reality_ingestor/
  __init__.py
  config.py
  errors.py
  models.py
  schemas.py
  markdown_parser.py
  ontology_retriever.py
  extractor.py
  resolver.py
  diff_validator.py
  committer.py
  cypher_library.py
  llm/
    __init__.py
    base.py
    litellm_adapter.py
    langchain_adapter.py
  reality_ingestor.py
scripts/
  reality_ingest_cli.py
```

### Script Responsibilities
- `markdown_parser.py`: chapter normalization, chunk hashing, paragraph sequence.
- `ontology_retriever.py`: Agent 1 recon (BM25 + vector + recent state pull).
- `extractor.py`: Agent 2 structured extraction with retry/repair policy.
- `resolver.py`: Agent 3 entity matching, conflict detection, UUID plan generation.
- `diff_validator.py`: Rich terminal diff and HITL prompt `[A]ccept [E]dit [R]eject`.
- `committer.py`: batched idempotent writes with transactional safety.
- `cypher_library.py`: all parameterized query strings (no inline ad-hoc Cypher).
- `reality_ingestor.py`: top-level orchestrator class with required methods.
- `reality_ingest_cli.py`: command entrypoint.

---

## 4) Metagraph Schema (Canonical)

### Node Labels and Core Properties

1. `Chunk`
- `text: string`
- `hash: string` (sha256, unique)
- `sequence_id: int`
- `chapter_id: string`
- `source_path: string`

2. `Event`
- `uuid: string` (deterministic for idempotency)
- `type: string`
- `description: string`
- `timestamp: string|null`
- `sequence: int`
- `chapter_id: string`

3. `Entity` (base label) with sublabels:
- `Character`
- `Location`
- `Faction`
- `Object`
- `Concept`

Shared properties:
- `uuid: string` (unique)
- `name: string`
- `aliases: list<string>`
- `aliases_text: string` (joined aliases for fulltext indexing)
- `baseline_state: string|null`
- `embedding: list<float>|null`

4. `State`
- `uuid: string` (unique)
- `attribute: string`
- `value: string`
- `valid_from_event: string`
- `valid_until_event: string|null`
- `created_at: string` (ISO-8601)

Type compatibility note:
- Graph model includes `Entity:Object`.
- Extractor schema v1 uses the provided enum (`Character|Location|Faction|Concept`).
- Resolver may map extracted `Concept` entries into `Object` label when ontology evidence supports material artifacts.

### Relationship Types

- `(Entity)-[:PARTICIPATED_IN {role: string, intent: string}]->(Event)`
- `(Event)-[:OCCURRED_IN]->(Entity:Location)`
- `(Event)-[:CAUSED]->(Event)`
- `(Entity)-[:HAS_STATE]->(State)`
- `(Event)-[:DOCUMENTED_BY]->(Chunk)`
- `(Entity)-[:INTERACTS_WITH {nature: string, weight: float, context: string, source_event_uuid: string}]->(Entity)`
- `(Event)-[:NEXT]->(Event)` for event sequence ordering

### Graph Integrity Rules

- Never overwrite existing state values in place.
- All chapter ingestion writes must be idempotent.
- All created `Event` nodes must link to at least one `Chunk`.
- `OCCURRED_IN` must point to `Entity:Location`.
- `NEXT` chain must be linear within chapter sequence.

---

## 5) Class Contract: `RealityIngestor`

```python
class RealityIngestor:
    def parse_markdown(self, markdown_path: str) -> ParsedChapter: ...
    def build_ontology_context(self, parsed: ParsedChapter) -> ActiveOntology: ...
    def extract_graph_json(self, parsed: ParsedChapter, ontology: ActiveOntology) -> ExtractedGraphPayload: ...
    def resolve_entities(self, payload: ExtractedGraphPayload, ontology: ActiveOntology) -> ResolutionPlan: ...
    def commit_to_graph(self, parsed: ParsedChapter, payload: ExtractedGraphPayload, plan: ResolutionPlan) -> CommitReport: ...
```

### 5.1 `parse_markdown(markdown_path)`

Input:
- Markdown file path.

Behavior:
- Read UTF-8 text.
- Normalize line endings.
- Split into chunks by paragraph blocks (double newline boundaries).
- Compute `chunk.hash = sha256(normalized_text)`.
- Assign `sequence_id` by source order.
- Compute `chapter_id = sha256(full_chapter_text)[0:16]`.

Output:
- `ParsedChapter` with raw text, chunk list, chapter hash/id, source path.

Failure modes:
- Missing file -> `MarkdownParseError`.
- Empty chapter after normalization -> `MarkdownParseError`.

### 5.2 `build_ontology_context(parsed)`

Input:
- Parsed chapter (chunks + full text).

Behavior (Agent 1):
- Build mention probes from text (spaCy NER + noun chunks + capitalized spans).
- Run fulltext query on entity names/aliases (BM25).
- Run vector similarity query from chunk embeddings to entity embeddings.
- Pull recent active states and recent interactions for matched entities.
- Build `Active_Ontology.json` artifact with:
  - known entities and UUIDs
  - alias table
  - recent state snapshots per entity
  - observed relationship natures
  - observed event types
  - retrieval evidence scores

Output:
- `ActiveOntology` object + JSON artifact path.

Failure modes:
- Neo4j unavailable -> `OntologyBuildError`.
- Missing fulltext/vector indexes -> warning + degraded mode, never silent.

### 5.3 `extract_graph_json(parsed, ontology)`

Input:
- Parsed chapter + active ontology.

Behavior (Agent 2):
- Optionally run coreference prepass (spaCy + coref plugin where installed).
- Prompt model with:
  - chapter text
  - chunk boundaries
  - active ontology
  - required JSON schema
  - explicit "no invented locations/entities not text-grounded" rule
- Use structured output mode (instructor/OpenAI schema or LangChain structured parser).
- Validate returned object using `jsonschema`.
- Run semantic checks:
  - every participant `entity_temp_id` exists in `entities`.
  - every `location_temp_id` references entity type `Location`.
  - every `triggered_by_event_id` exists in `events`.
  - relationship weights in [-1.0, 1.0].

Output:
- `ExtractedGraphPayload`.

Failure modes:
- Invalid JSON -> repair workflow (see Section 9).
- Schema failure after repair retries -> `ExtractionSchemaError`.

### 5.4 `resolve_entities(payload, ontology)`

Input:
- Extracted payload + active ontology.

Behavior (Agent 3 pre-commit):
- Resolve `is_new == false` by UUID/alias exact lookup.
- Resolve unresolved items by fuzzy alias match.
- For `is_new == true`, run vector match against existing entities:
  - if score >= threshold and context compatibility passes -> mark conflict candidate.
  - do not auto-create duplicate when high-confidence match exists.
- Generate deterministic UUIDs for approved new entities.
- Build resolution plan:
  - `resolved_entities`
  - `new_entities`
  - `conflicts`
  - `warnings`

Conflict rule (required):
- If extracted entity text is generic (for example, "the tall man") and vector top-1 strongly matches an existing character UUID, emit `ResolutionConflict` and require human action before commit.

Output:
- `ResolutionPlan`.

Failure modes:
- Any unresolved mandatory entity -> `ResolutionError`.
- Conflicts present and not explicitly overridden -> `ResolutionConflictError`.

### 5.5 `commit_to_graph(parsed, payload, plan)`

Input:
- Parsed chapter, validated payload, resolved plan.

Behavior:
- Generate visual diff via `rich`:
  - green: nodes/edges to create
  - yellow: existing nodes/states to update/close
  - red: conflicts/hallucination risks
- Prompt:
  - `[A]ccept` -> write transaction
  - `[E]dit JSON` -> open temp JSON in `$EDITOR`, re-validate, re-diff
  - `[R]eject` -> abort without writes
- On accept:
  - run idempotent transaction batch
  - write chunks, entities, events, sequence links, causality links, state transitions, interactions
  - emit `CommitReport` with IDs and counts

Failure modes:
- Transaction errors -> rollback + `GraphCommitError`.
- User rejection -> `CommitRejected`.

---

## 6) Multi-Agent Flow (Execution Order)

1. Agent 1: Context Retriever
- Inputs: chapter text/chunks.
- Queries: BM25 + vector + recent states/relations.
- Output: `Active_Ontology.json`.

2. Agent 2: Universal Extractor
- Inputs: chapter + `Active_Ontology.json`.
- Process: NLP prepass + LLM structured extraction.
- Output: schema-valid extraction payload JSON.

3. Agent 3: Resolution and Cypher Engine
- Inputs: payload + ontology.
- Process: resolve/flag conflicts + diff + commit.
- Output: committed graph mutations + audit report.

Pipeline hard gate:
- No commit if red conflicts are unresolved.

---

## 7) Required Extraction JSON Contract (v1)

The extractor must return a strictly validated object conforming to this schema:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "entities": {
      "type": "array",
      "description": "All actors, locations, or concepts present or referenced.",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "temp_id": {"type": "string"},
          "name": {"type": "string"},
          "type": {"type": "string", "enum": ["Character", "Location", "Faction", "Concept"]},
          "is_new": {"type": "boolean", "description": "True if not found in Active_Ontology"}
        },
        "required": ["temp_id", "name", "type", "is_new"]
      }
    },
    "events": {
      "type": "array",
      "description": "Sequential distinct actions or realizations occurring in the text.",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "event_id": {"type": "string"},
          "description": {"type": "string"},
          "location_temp_id": {"type": "string"},
          "participants": {
            "type": "array",
            "items": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "entity_temp_id": {"type": "string"},
                "role": {"type": "string", "description": "e.g., Initiator, Target, Observer"}
              },
              "required": ["entity_temp_id", "role"]
            }
          }
        },
        "required": ["event_id", "description", "location_temp_id", "participants"]
      }
    },
    "state_changes": {
      "type": "array",
      "description": "Any change in an entity's physical, emotional, or epistemic status.",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "entity_temp_id": {"type": "string"},
          "attribute": {"type": "string", "description": "e.g., Epistemic_Knowledge, Physical_Health, Loyalty"},
          "new_value": {"type": "string"},
          "triggered_by_event_id": {"type": "string"}
        },
        "required": ["entity_temp_id", "attribute", "new_value", "triggered_by_event_id"]
      }
    },
    "relationships": {
      "type": "array",
      "description": "Underlying dynamic shifts between entities.",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "source_temp_id": {"type": "string"},
          "target_temp_id": {"type": "string"},
          "nature": {"type": "string", "description": "High-level categorization, e.g., Dominance_Asserted, Alliance_Formed"},
          "weight": {"type": "number", "minimum": -1.0, "maximum": 1.0}
        },
        "required": ["source_temp_id", "target_temp_id", "nature", "weight"]
      }
    }
  },
  "required": ["entities", "events", "state_changes", "relationships"]
}
```

Mandatory semantic checks (in addition to schema validation):
- Every participant `entity_temp_id` must exist in `entities`.
- Every `location_temp_id` must point to a `Location` entity.
- Every state `triggered_by_event_id` must exist in `events`.
- No event may reference an empty participant list.

---

## 8) LLM Independence Design

Use an adapter interface to decouple orchestration from provider stack.

```python
from typing import Protocol, Any

class LLMAdapter(Protocol):
    def structured_extract(
        self,
        *,
        model: str,
        prompt: str,
        json_schema: dict[str, Any],
        temperature: float = 0.0,
        timeout_s: int = 90
    ) -> dict[str, Any]:
        ...
```

Required adapters:
- `LiteLLMAdapter` (supports OpenAI-compatible and local endpoints).
- `LangChainAdapter` (supports chain composition and provider abstraction).

Selection logic:
- Runtime config chooses adapter.
- No ingestion code outside adapter may import provider-specific SDKs.

---

## 9) JSON Parsing and Validation Failure Policy (Mandatory)

Robust parser policy for `extract_graph_json()`:

1. Attempt 1 (strict structured output)
- Call adapter with required schema.
- If provider returns parsed object, run schema validation directly.

2. Attempt 2 (raw JSON parse fallback)
- If output is string:
  - strip code fences
  - trim leading/trailing non-json noise
  - parse with `json.loads`

3. Attempt 3 (repair prompt)
- If parsing fails:
  - send original model output and parser error to a dedicated repair prompt:
    - "Return valid JSON only, no commentary."
  - re-validate.

4. Attempt 4 (final deterministic repair)
- Optional local sanitizer: remove trailing commas, unescape control chars conservatively.
- parse + validate.

5. Hard fail
- Persist:
  - raw output text
  - parse errors
  - repair attempts
- Raise `ExtractionSchemaError` with artifact path.

Never silently coerce semantically invalid payloads.

---

## 10) Entity Resolution Policy

Resolution precedence:

1. Explicit mapping from ontology alias map.
2. Exact normalized name match.
3. High-score fuzzy match (`rapidfuzz`) with threshold (for example 92).
4. Vector nearest-neighbor check for unresolved entities.

Conflict criteria:
- Vector score >= `REALITY_CONFLICT_THRESHOLD`.
- Role/action context suggests same actor.
- Extractor labeled `is_new=true`.

Action:
- Mark red conflict in diff.
- Require human accept/override/edit before commit.

Output model requirements:
- `resolved_entity_uuid_by_temp_id`
- `new_entity_records`
- `conflicts[]`
- `manual_overrides[]`

---

## 11) Diff Validator and Human-in-the-Loop Gate

Diff sections in terminal:
- Green section: creates
  - new entities
  - new events
  - new states
  - new relationships
- Yellow section: updates
  - closed old states (`valid_until_event`)
  - alias expansions
- Red section: risks/conflicts
  - unresolved references
  - likely duplicate entities
  - events requiring location not grounded in text

User prompt contract:
- `A`: commit
- `E`: edit JSON (temp file), then re-validate + re-diff
- `R`: reject and stop

Audit output:
- `artifacts/reality_ingestor/<run_id>/diff_report.json`
- `artifacts/reality_ingestor/<run_id>/decision.json`

---

## 12) Commit Semantics and Idempotency

Idempotency strategy:
- `Chunk.hash` unique.
- deterministic `Event.uuid = uuid5(namespace, chapter_id + event_id)`.
- deterministic `State.uuid = uuid5(namespace, entity_uuid + attribute + triggered_by_event_id + new_value)`.
- `MERGE` for every node and edge create path.

Transaction boundaries:
- Single write transaction per chapter commit (preferred).
- If transaction exceeds memory/time, split:
  - tx1 chunks + entities
  - tx2 events + links
  - tx3 states + interactions
- On split mode, persist checkpoint after each successful tx.

State versioning (SCD Type-2):
- Find open state for same `(entity_uuid, attribute)` where `valid_until_event IS NULL`.
- Close previous state by setting `valid_until_event`.
- Create new `State` and link via `HAS_STATE`.
- Never mutate historical `value`.

---

## 13) Observability and Audit Artifacts

For each run, persist:
- `parsed_chunks.json`
- `Active_Ontology.json`
- `extracted_graph_payload.json`
- `resolution_plan.json`
- `diff_report.json`
- `commit_report.json`
- `errors.json` (if any)

Structured log keys:
- `run_id`
- `chapter_id`
- `model`
- `adapter`
- `duration_ms`
- `retrieval_hits`
- `conflict_count`
- `writes_created`
- `writes_updated`

---

## 14) Security and Safety

- No automatic commit when unresolved red conflicts exist.
- Do not pass secrets in prompt payloads.
- Truncate or hash very long text snippets in logs.
- Enforce label whitelist: `Character|Location|Faction|Object|Concept`.
- Reject dynamic Cypher label injection outside whitelist.

---

## 15) Acceptance Criteria

1. Genericity
- Same code path handles:
  - violent battle chapter
  - diplomatic negotiation chapter
  - biological mutation chapter

2. Extraction reliability
- At least one retry/repair path for malformed JSON.
- All committed payloads schema-valid and referentially complete.

3. Resolution safety
- High-confidence duplicate risk must block automatic create.

4. State integrity
- Historical state records remain immutable.
- New state writes close prior open state when applicable.

5. Traceability
- Every event links to at least one source chunk.
- Full run artifacts captured under one run directory.

---

## 16) CLI Contract

Example:

```bash
python scripts/reality_ingest_cli.py \
  --markdown ./chapters/042.md \
  --model gpt-4o \
  --adapter litellm \
  --dry-run false
```

Flags:
- `--markdown`: required path to chapter file.
- `--model`: LLM model name.
- `--adapter`: `litellm|langchain`.
- `--dry-run`: if true, execute through diff but skip commit.
- `--accept`: optional non-interactive accept for CI only when no red conflicts.

Exit codes:
- `0` success
- `2` user reject
- `3` extraction/validation failure
- `4` resolution conflicts unresolved
- `5` graph commit failure

---

## 17) Implementation Notes for `scripts/reality_ingest_cli.py`

Minimum command flow:

```python
ingestor = RealityIngestor.from_env()
parsed = ingestor.parse_markdown(args.markdown)
ontology = ingestor.build_ontology_context(parsed)
payload = ingestor.extract_graph_json(parsed, ontology)
plan = ingestor.resolve_entities(payload, ontology)
report = ingestor.commit_to_graph(parsed, payload, plan)
```

This sequence is mandatory and must stay stable for observability and testing.
