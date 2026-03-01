from __future__ import annotations

from datetime import datetime
import uuid

from neo4j import Neo4jError

from .cypher_library import (
    Q_CLOSE_OPEN_STATE_FOR_ATTRIBUTE,
    Q_LINK_ENTITY_HAS_STATE,
    Q_LINK_ENTITY_PARTICIPATED_IN_EVENT,
    Q_LINK_EVENT_DOCUMENTED_BY_CHUNK,
    Q_LINK_EVENT_NEXT,
    Q_LINK_EVENT_OCCURRED_IN_LOCATION,
    Q_UPSERT_CHUNK,
    Q_UPSERT_ENTITY_BASE,
    Q_UPSERT_EVENT,
    Q_UPSERT_INTERACTS_WITH,
    Q_UPSERT_STATE,
    Q_SET_ENTITY_SUBLABEL,
)
from .errors import GraphCommitError
from .models import CommitReport, ParsedChapter


class Committer:
    def __init__(self, driver, config):
        self.driver = driver
        self.config = config

    def commit_to_graph(self, parsed: ParsedChapter, payload: dict[str, object], plan) -> CommitReport:
        metrics = {
            "created_entities": 0,
            "created_events": 0,
            "created_states": 0,
            "created_relationships": 0,
            "duration_ms": 0,
        }
        start = datetime.utcnow()
        entity_map = {item["temp_id"]: item["uuid"] for item in plan.new_entities}
        entity_map.update({item["temp_id"]: item["uuid"] for item in plan.resolved_entities})

        try:
            with self.driver().session(database="neo4j") as session:
                session.execute_write(
                    self._write_transaction,
                    parsed,
                    payload,
                    entity_map,
                    metrics,
                )
        except Neo4jError as exc:
            raise GraphCommitError("Failed to commit graph payload") from exc

        metrics["duration_ms"] = (datetime.utcnow() - start).total_seconds() * 1000
        return CommitReport(run_id=self.config.run_id, status="success", metrics=metrics)

    def _write_transaction(self, tx, parsed, payload, entity_map, metrics):
        records = payload.get("entities", [])
        for chunk in parsed.chunks:
            tx.run(
                Q_UPSERT_CHUNK,
                hash=chunk.hash,
                text=chunk.text,
                sequence_id=chunk.sequence_id,
                chapter_id=parsed.chapter_id,
                source_path=parsed.source_path,
            )
        for entity in records:
            uuid_value = entity_map.get(entity["temp_id"], str(uuid.uuid4()))
            tx.run(
                Q_UPSERT_ENTITY_BASE,
                uuid=uuid_value,
                name=entity["name"],
                aliases=[],
                aliases_text="",
                baseline_state=None,
                embedding=[],
            )
            tx.run(
                Q_SET_ENTITY_SUBLABEL,
                uuid=uuid_value,
                entity_type=entity.get("type", "Entity"),
            )
            metrics["created_entities"] += 1

        new_event_nodes: list[str] = []
        for idx, event in enumerate(payload.get("events", [])):
            event_uuid = str(
                uuid.uuid5(uuid.NAMESPACE_DNS, f"{parsed.chapter_id}:{event['event_id']}")
            )
            tx.run(
                Q_UPSERT_EVENT,
                event_uuid=event_uuid,
                event_type="GENERIC",
                description=event.get("description", ""),
                timestamp=None,
                sequence=idx,
                chapter_id=parsed.chapter_id,
            )
            metrics["created_events"] += 1
            first_chunk = parsed.chunks[0]
            tx.run(
                Q_LINK_EVENT_DOCUMENTED_BY_CHUNK,
                event_uuid=event_uuid,
                chunk_hash=first_chunk.hash,
            )
            location_uuid = entity_map.get(event.get("location_temp_id", ""))
            if location_uuid:
                tx.run(
                    Q_LINK_EVENT_OCCURRED_IN_LOCATION,
                    event_uuid=event_uuid,
                    location_uuid=location_uuid,
                )
                metrics["created_relationships"] += 1
            for participant in event.get("participants", []):
                entity_uuid = entity_map.get(participant.get("entity_temp_id", ""))
                if not entity_uuid:
                    continue
                tx.run(
                    Q_LINK_ENTITY_PARTICIPATED_IN_EVENT,
                    entity_uuid=entity_uuid,
                    event_uuid=event_uuid,
                    role=participant.get("role", ""),
                    intent="",
                )
                metrics["created_relationships"] += 1
            new_event_nodes.append(event_uuid)
        for prev, nxt in zip(new_event_nodes, new_event_nodes[1:]):
            tx.run(Q_LINK_EVENT_NEXT, from_event_uuid=prev, to_event_uuid=nxt)
            metrics["created_relationships"] += 1

        if payload.get("relationships"):
            reference_event = new_event_nodes[0] if new_event_nodes else ""
            for relation in payload.get("relationships", []):
                source_uuid = entity_map.get(relation.get("source_temp_id", ""))
                target_uuid = entity_map.get(relation.get("target_temp_id", ""))
                if not source_uuid or not target_uuid:
                    continue
                tx.run(
                    Q_UPSERT_INTERACTS_WITH,
                    source_uuid=source_uuid,
                    target_uuid=target_uuid,
                    nature=relation.get("nature", ""),
                    context=relation.get("context", ""),
                    source_event_uuid=reference_event,
                    weight=relation.get("weight", 0.0),
                    updated_at=datetime.utcnow().isoformat(),
                )
                metrics["created_relationships"] += 1

        for state in payload.get("state_changes", []):
            entity_uuid = entity_map.get(state.get("entity_temp_id", ""))
            if not entity_uuid:
                continue
            tx.run(
                Q_CLOSE_OPEN_STATE_FOR_ATTRIBUTE,
                entity_uuid=entity_uuid,
                attribute=state.get("attribute", ""),
                valid_until_event=state.get("triggered_by_event_id"),
                closed_at=datetime.utcnow().isoformat(),
            )
            state_uuid = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{entity_uuid}:{state.get('attribute', '')}:{state.get('triggered_by_event_id', '')}",
                )
            )
            tx.run(
                Q_UPSERT_STATE,
                state_uuid=state_uuid,
                attribute=state.get("attribute", ""),
                value=state.get("new_value", ""),
                valid_from_event=state.get("triggered_by_event_id"),
                created_at=datetime.utcnow().isoformat(),
            )
            tx.run(Q_LINK_ENTITY_HAS_STATE, entity_uuid=entity_uuid, state_uuid=state_uuid)
            metrics["created_states"] += 1
