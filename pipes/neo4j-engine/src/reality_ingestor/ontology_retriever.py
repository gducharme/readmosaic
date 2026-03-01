from __future__ import annotations

from dataclasses import asdict
from datetime import datetime

try:
    from neo4j import Neo4jError  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - fallback when neo4j driver is unavailable
    Neo4jError = Exception

from .cypher_library import (
    Q_FETCH_ENTITIES,
    Q_FETCH_ENTITY_STATES,
    Q_FETCH_EVENT_TYPES,
    Q_FETCH_RELATIONSHIPS,
)
from .errors import OntologyBuildError
from .models import ActiveOntology, OntologyEntity, RelationshipSnapshot, StateSnapshot


class OntologyRetriever:
    def __init__(self, driver):
        self.driver = driver

    def build_context(self, parsed, config) -> ActiveOntology:
        run_id = config.run_id
        timestamp = datetime.utcnow().isoformat()
        entities: list[OntologyEntity] = []
        states: list[StateSnapshot] = []
        relationships: list[RelationshipSnapshot] = []
        retrieval = {"bm25_hits": 0, "vector_hits": 0, "state_snapshots": 0}
        event_types: list[dict[str, int]] = []

        try:
            with self.driver().session(database="neo4j") as session:
                ent_records = session.run(Q_FETCH_ENTITIES, limit=200).data()
                for record in ent_records:
                    entities.append(
                        OntologyEntity(
                            uuid=record["uuid"],
                            name=record.get("name", ""),
                            type=next((label for label in record.get("labels", []) if label != "Entity"), "Entity"),
                            aliases=record.get("aliases") or [],
                            baseline_state=record.get("baseline_state"),
                        )
                    )
                retrieval["bm25_hits"] = len(ent_records)

                entity_ids = [record["uuid"] for record in ent_records]
                if entity_ids:
                    state_records = session.run(Q_FETCH_ENTITY_STATES, entity_uuids=entity_ids, limit=200).data()
                    retrieval["state_snapshots"] = len(state_records)
                    for record in state_records:
                        states.append(
                            StateSnapshot(
                                entity_uuid=record["entity_uuid"],
                                attribute=record["attribute"],
                                value=record["value"],
                                valid_from_event=record.get("valid_from_event"),
                                valid_until_event=record.get("valid_until_event"),
                                created_at=record.get("created_at"),
                            )
                        )
                rel_records = session.run(Q_FETCH_RELATIONSHIPS, limit=200).data()
                for record in rel_records:
                    relationships.append(
                        RelationshipSnapshot(
                            source_uuid=record["source_uuid"],
                            target_uuid=record["target_uuid"],
                            nature=record.get("nature", ""),
                            weight=record.get("weight"),
                            context=record.get("context"),
                        )
                    )
                event_records = session.run(Q_FETCH_EVENT_TYPES, limit=50).data()
                event_types = [dict(event_type=record["event_type"], count=record["freq"]) for record in event_records]
        except Neo4jError as exc:
            raise OntologyBuildError("Failed to retrieve ontology context") from exc

        return ActiveOntology(
            run_id=run_id,
            timestamp=timestamp,
            entities=entities,
            states=states,
            relationships=relationships,
            retrieval=retrieval,
            event_types=event_types,
        )
