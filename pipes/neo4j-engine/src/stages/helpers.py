from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.models import (
    ActiveOntology,
    OntologyEntity,
    RelationshipSnapshot,
    StateSnapshot,
)


def load_artifact(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def hydrate_ontology(data: dict) -> ActiveOntology:
    entities = [
        OntologyEntity(
            uuid=entry.get("uuid", ""),
            name=entry.get("name", ""),
            type=entry.get("type", "Entity"),
            aliases=entry.get("aliases", []),
            baseline_state=entry.get("baseline_state"),
        )
        for entry in data.get("entities", [])
    ]
    states = [
        StateSnapshot(
            entity_uuid=item.get("entity_uuid", ""),
            attribute=item.get("attribute", ""),
            value=item.get("value", ""),
            valid_from_event=item.get("valid_from_event"),
            valid_until_event=item.get("valid_until_event"),
            created_at=item.get("created_at"),
        )
        for item in data.get("states", [])
    ]
    relationships = [
        RelationshipSnapshot(
            source_uuid=item.get("source_uuid", ""),
            target_uuid=item.get("target_uuid", ""),
            nature=item.get("nature", ""),
            weight=item.get("weight"),
            context=item.get("context"),
        )
        for item in data.get("relationships", [])
    ]
    return ActiveOntology(
        run_id=data.get("run_id", ""),
        timestamp=data.get("timestamp", ""),
        entities=entities,
        states=states,
        relationships=relationships,
        retrieval=data.get("retrieval", {}),
        event_types=data.get("event_types", []),
    )
