from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Chunk:
    chunk_id: str
    text: str
    hash: str
    sequence_id: int


@dataclass
class ParsedChapter:
    chapter_id: str
    chapter_hash: str
    source_path: str
    full_text: str
    chunks: List[Chunk]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chapter_id": self.chapter_id,
            "chapter_hash": self.chapter_hash,
            "source_path": self.source_path,
            "full_text": self.full_text,
            "chunk_count": len(self.chunks),
            "chunks": [chunk.__dict__ for chunk in self.chunks],
        }


@dataclass
class OntologyEntity:
    uuid: str
    name: str
    type: str
    aliases: List[str]
    baseline_state: Optional[str]


@dataclass
class StateSnapshot:
    entity_uuid: str
    attribute: str
    value: str
    valid_from_event: Optional[str]
    valid_until_event: Optional[str]
    created_at: Optional[str]


@dataclass
class RelationshipSnapshot:
    source_uuid: str
    target_uuid: str
    nature: str
    weight: Optional[float]
    context: Optional[str]


@dataclass
class ActiveOntology:
    run_id: str
    timestamp: str
    entities: List[OntologyEntity]
    states: List[StateSnapshot]
    relationships: List[RelationshipSnapshot]
    retrieval: Dict[str, Any]
    event_types: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "entities": [entity.__dict__ for entity in self.entities],
            "states": [vars(state) for state in self.states],
            "relationships": [vars(rel) for rel in self.relationships],
            "retrieval": self.retrieval,
            "event_types": self.event_types,
        }


@dataclass
class ExtractedGraphPayload:
    data: Dict[str, Any]


@dataclass
class ResolutionPlan:
    run_id: str
    resolved_entities: List[Dict[str, Any]]
    new_entities: List[Dict[str, Any]]
    conflicts: List[Dict[str, Any]]
    warnings: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "resolved_entities": self.resolved_entities,
            "new_entities": self.new_entities,
            "conflicts": self.conflicts,
            "warnings": self.warnings,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ResolutionPlan":
        return cls(
            run_id=payload.get("run_id", ""),
            resolved_entities=payload.get("resolved_entities", []),
            new_entities=payload.get("new_entities", []),
            conflicts=payload.get("conflicts", []),
            warnings=payload.get("warnings", []),
        )


@dataclass
class DiffReport:
    run_id: str
    green: List[Dict[str, Any]]
    yellow: List[Dict[str, Any]]
    red: List[Dict[str, Any]]
    decision: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "green": self.green,
            "yellow": self.yellow,
            "red": self.red,
            "decision": self.decision,
        }


@dataclass
class CommitReport:
    run_id: str
    status: str
    metrics: Dict[str, Any]
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "metrics": self.metrics,
            "notes": self.notes,
        }
