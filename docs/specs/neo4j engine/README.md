# Reality Ingestion Engine Spec Set

This folder defines the production implementation spec for a generic Markdown-to-Neo4j "Reality Ingestion Engine".

## Scope
- Ingest unstructured chapter Markdown.
- Extract a complete event-sourced knowledge graph payload using a 3-agent pipeline.
- Resolve entities against existing graph state with conflict detection.
- Commit idempotent graph updates into Neo4j.
- Enforce a human-in-the-loop diff gate before write.

## Documents
- `REALITY_INGEST_ENGINE_SPEC_v1.md`
  - Full architecture, module boundaries, method contracts, error handling, and acceptance criteria.
- `CYPHER_LIBRARY.md`
  - Parameterized Cypher query catalog for retrieval, resolution support, and commit transactions.

## Runtime Baseline
- Neo4j endpoint: `bolt://localhost:7687`
- Neo4j auth from `docker-compose.yaml`: `neo4j/mosaic_founding`
- APOC plugin: enabled

## Suggested Implementation Order
1. Create package skeleton and config (`reality_ingestor/*`).
2. Add constraints/index bootstrap and retrieval queries.
3. Implement `RealityIngestor.parse_markdown()` and `build_ontology_context()`.
4. Implement strict extraction + JSON validation.
5. Implement resolution + conflict detector.
6. Implement diff gate + commit transaction layer.
7. Add CLI + integration tests for battle, negotiation, and mutation chapter styles.
