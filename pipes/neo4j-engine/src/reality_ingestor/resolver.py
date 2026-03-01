from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from rapidfuzz import fuzz

from .errors import ResolutionConflictError, ResolutionError
from .models import ActiveOntology, ResolutionPlan


@dataclass
class ResolvedEntity:
    temp_id: str
    uuid: str
    match_source: str


@dataclass
class NewEntity:
    temp_id: str
    uuid: str
    name: str
    type: str


class Resolver:
    def __init__(self, ontology: ActiveOntology, config):
        self.ontology = ontology
        self.config = config
        self.entity_index = self._build_index(ontology.entities)

    def _build_index(self, entities: Iterable) -> Dict[str, str]:
        index: Dict[str, str] = {}
        for entity in entities:
            for candidate in {entity.name, *(entity.aliases or [])}:
                if candidate:
                    index[candidate.lower()] = entity.uuid
        return index

    def resolve_entities(self, payload: dict[str, Any]) -> ResolutionPlan:
        resolved: List[Dict[str, str]] = []
        new_entities: List[Dict[str, str]] = []
        conflicts: List[Dict[str, str]] = []
        warnings: List[str] = []
        threshold = self.config.conflict_threshold

        for entity in payload.get("entities", []):
            temp_id = entity["temp_id"]
            normalized = entity["name"].strip().lower()
            matched_uuid = self.entity_index.get(normalized)
            if not entity.get("is_new") and matched_uuid:
                resolved.append({"temp_id": temp_id, "uuid": matched_uuid, "match_source": "ontology"})
                continue
            if matched_uuid:
                conflicts.append(
                    {
                        "temp_id": temp_id,
                        "reason": "High similarity to existing ontology entity",
                        "candidate_uuid": matched_uuid,
                    }
                )
                continue
            best_score = 0.0
            best_uuid = None
            for known in self.ontology.entities:
                score = fuzz.ratio(entity["name"].lower(), known.name.lower())
                if score > best_score:
                    best_score = score
                    best_uuid = known.uuid
            if best_score >= threshold * 100:
                conflicts.append(
                    {
                        "temp_id": temp_id,
                        "reason": f"Fuzzy match score {best_score:.1f}% exceeds threshold",
                        "candidate_uuid": best_uuid,
                    }
                )
                continue
            new_uuid = str(uuid.uuid4())
            new_entities.append(
                {
                    "temp_id": temp_id,
                    "uuid": new_uuid,
                    "name": entity["name"],
                    "type": entity["type"],
                }
            )
        plan = ResolutionPlan(
            run_id=self.config.run_id,
            resolved_entities=resolved,
            new_entities=new_entities,
            conflicts=conflicts,
            warnings=warnings,
        )
        if plan.conflicts:
            raise ResolutionConflictError("Conflict detected during resolution")
        return plan
