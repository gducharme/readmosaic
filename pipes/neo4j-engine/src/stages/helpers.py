from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.models import (
    ActiveOntology,
    OntologyEntity,
    RelationshipSnapshot,
    StateSnapshot,
)


def _iter_artifact_specs(ctx: Any, kind: str):
    attrs = (
        ("input", ("inputs", "input_artifacts", "stage_inputs", "expected_inputs", "artifacts")),
        ("output", ("outputs", "output_artifacts", "stage_outputs", "expected_outputs", "artifacts")),
    )
    names: tuple[str, ...] = ()
    for artifact_kind, candidates in attrs:
        if artifact_kind == kind:
            names = candidates
            break
    for attr in names:
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            for row in value:
                if isinstance(row, dict):
                    yield row
                elif isinstance(row, str) and row.strip():
                    yield {"path": row.strip()}


def _resolve_from_ctx(ctx: Any, *, kind: str, suffix: str) -> Path | None:
    for spec in _iter_artifact_specs(ctx, kind):
        concrete_path = spec.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        if not concrete_path.endswith(suffix):
            continue
        return Path(concrete_path)
    return None


def resolve_input_path(ctx: Any, name: str) -> Path:
    return _resolve_from_ctx(ctx, kind="input", suffix=name) or Path(name)


def resolve_output_path(ctx: Any, name: str) -> Path:
    return _resolve_from_ctx(ctx, kind="output", suffix=name) or Path(name)


def load_artifact(ctx: Any, name: str) -> dict:
    path = resolve_input_path(ctx, name)
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_artifact(ctx: Any, name: str, payload: dict) -> Path:
    output_path = resolve_output_path(ctx, name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


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
