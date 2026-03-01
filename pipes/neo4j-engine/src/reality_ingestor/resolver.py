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
        self.entity_by_uuid = {entity.uuid: entity for entity in ontology.entities}

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
        warnings: List[Dict[str, Any]] = []
        metrics = {
            "exact_resolved": 0,
            "auto_corrected_is_new": 0,
            "fuzzy_conflicts": 0,
            "created_new": 0,
            "name_promotions": 0,
        }
        threshold = self.config.conflict_threshold

        for entity in payload.get("entities", []):
            temp_id = entity["temp_id"]
            normalized = entity["name"].strip().lower()
            matched_uuid = self.entity_index.get(normalized)
            if matched_uuid:
                resolved.append({"temp_id": temp_id, "uuid": matched_uuid, "match_source": "ontology"})
                metrics["exact_resolved"] += 1
                known = self.entity_by_uuid.get(matched_uuid)
                if known and self._should_promote_name(known.name, entity["name"]):
                    warnings.append(
                        {
                            "type": "name_promotion",
                            "temp_id": temp_id,
                            "entity_name": entity["name"],
                            "candidate_uuid": matched_uuid,
                            "old_name": known.name,
                            "new_name": entity["name"],
                            "details": f"Promote canonical name from '{known.name}' to '{entity['name']}'.",
                        }
                    )
                    metrics["name_promotions"] += 1
                if entity.get("is_new"):
                    warnings.append(
                        {
                            "type": "is_new_mismatch",
                            "temp_id": temp_id,
                            "entity_name": entity["name"],
                            "candidate_uuid": matched_uuid,
                            "details": "Extractor flagged is_new=true but ontology had an exact match.",
                        }
                    )
                    metrics["auto_corrected_is_new"] += 1
                continue

            # Heuristic for pronoun/generic placeholders: if exactly one generic entity
            # exists for the same type, treat a new specific name as a promotion candidate.
            generic_candidates = self._generic_candidates_for_type(entity.get("type", "Entity"))
            if len(generic_candidates) == 1 and not self._is_generic_name(entity["name"]):
                known = generic_candidates[0]
                resolved.append(
                    {"temp_id": temp_id, "uuid": known.uuid, "match_source": "generic_placeholder_upgrade"}
                )
                warnings.append(
                    {
                        "type": "name_promotion",
                        "temp_id": temp_id,
                        "entity_name": entity["name"],
                        "candidate_uuid": known.uuid,
                        "old_name": known.name,
                        "new_name": entity["name"],
                        "details": (
                            f"Upgrading generic placeholder '{known.name}' to specific name '{entity['name']}'."
                        ),
                    }
                )
                metrics["name_promotions"] += 1
                continue
            best_score = 0.0
            best_uuid = None
            for known in self.ontology.entities:
                score = fuzz.ratio(entity["name"].lower(), known.name.lower())
                if score > best_score:
                    best_score = score
                    best_uuid = known.uuid
            if best_score >= threshold * 100:
                known = self.entity_by_uuid.get(best_uuid) if isinstance(best_uuid, str) else None
                if known and self._should_promote_name(known.name, entity["name"]):
                    resolved.append(
                        {"temp_id": temp_id, "uuid": best_uuid, "match_source": "fuzzy_generic_upgrade"}
                    )
                    warnings.append(
                        {
                            "type": "name_promotion",
                            "temp_id": temp_id,
                            "entity_name": entity["name"],
                            "candidate_uuid": best_uuid,
                            "old_name": known.name,
                            "new_name": entity["name"],
                            "details": (
                                f"Fuzzy match {best_score:.1f}% from generic '{known.name}' "
                                f"to specific '{entity['name']}'."
                            ),
                        }
                    )
                    metrics["name_promotions"] += 1
                    continue
                conflicts.append(
                    {
                        "temp_id": temp_id,
                        "reason": f"Fuzzy match score {best_score:.1f}% exceeds threshold",
                        "candidate_uuid": best_uuid,
                    }
                )
                metrics["fuzzy_conflicts"] += 1
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
            metrics["created_new"] += 1
        plan = ResolutionPlan(
            run_id=self.config.run_id,
            resolved_entities=resolved,
            new_entities=new_entities,
            conflicts=conflicts,
            warnings=warnings,
            metrics=metrics,
        )
        if plan.conflicts:
            raise ResolutionConflictError("Conflict detected during resolution", conflicts=plan.conflicts)
        return plan

    def _should_promote_name(self, current_name: str, proposed_name: str) -> bool:
        current = (current_name or "").strip()
        proposed = (proposed_name or "").strip()
        if not current or not proposed:
            return False
        if current.lower() == proposed.lower():
            return False
        return self._is_generic_name(current) and not self._is_generic_name(proposed)

    def _is_generic_name(self, name: str) -> bool:
        value = (name or "").strip().lower()
        generic_tokens = {
            "he",
            "she",
            "they",
            "them",
            "him",
            "her",
            "person",
            "man",
            "woman",
            "boy",
            "girl",
            "stranger",
        }
        if value in generic_tokens:
            return True
        if value.startswith("the "):
            return True
        return False

    def _generic_candidates_for_type(self, entity_type: str) -> list[Any]:
        candidates: list[Any] = []
        wanted = (entity_type or "").strip().lower()
        for item in self.ontology.entities:
            if (item.type or "").strip().lower() != wanted:
                continue
            if self._is_generic_name(item.name):
                candidates.append(item)
        return candidates
