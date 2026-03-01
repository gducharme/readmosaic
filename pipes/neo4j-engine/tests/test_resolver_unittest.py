from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from reality_ingestor.errors import ResolutionConflictError
from reality_ingestor.models import ActiveOntology, OntologyEntity
from reality_ingestor.resolver import Resolver


@dataclass
class _Config:
    run_id: str = "test-run"
    conflict_threshold: float = 0.88


def _ontology() -> ActiveOntology:
    return ActiveOntology(
        run_id="r0",
        timestamp="2026-01-01T00:00:00Z",
        entities=[
            OntologyEntity(
                uuid="uuid-narrator",
                name="Narrator",
                type="Character",
                aliases=["The Narrator"],
                baseline_state=None,
            )
        ],
        states=[],
        relationships=[],
        retrieval={},
        event_types=[],
    )


def _payload(*entities):
    return {"entities": list(entities)}


class ResolverTests(unittest.TestCase):
    def test_exact_match_not_new_resolves_without_warning(self) -> None:
        resolver = Resolver(_ontology(), _Config())
        plan = resolver.resolve_entities(
            _payload({"temp_id": "e1", "name": "Narrator", "type": "Character", "is_new": False})
        )
        self.assertEqual(len(plan.resolved_entities), 1)
        self.assertEqual(plan.conflicts, [])
        self.assertEqual(plan.warnings, [])
        self.assertEqual(plan.metrics["exact_resolved"], 1)

    def test_exact_match_marked_new_auto_corrects_and_warns(self) -> None:
        resolver = Resolver(_ontology(), _Config())
        plan = resolver.resolve_entities(
            _payload({"temp_id": "e1", "name": "Narrator", "type": "Character", "is_new": True})
        )
        self.assertEqual(len(plan.resolved_entities), 1)
        self.assertEqual(plan.conflicts, [])
        self.assertEqual(plan.warnings[0]["type"], "is_new_mismatch")
        self.assertEqual(plan.metrics["auto_corrected_is_new"], 1)

    def test_fuzzy_match_above_threshold_raises_conflict(self) -> None:
        resolver = Resolver(_ontology(), _Config(conflict_threshold=0.6))
        with self.assertRaises(ResolutionConflictError) as ctx:
            resolver.resolve_entities(
                _payload({"temp_id": "e1", "name": "Narrat0r", "type": "Character", "is_new": True})
            )
        self.assertEqual(len(ctx.exception.conflicts), 1)
        self.assertIn("Fuzzy match score", ctx.exception.conflicts[0]["reason"])

    def test_low_fuzzy_match_creates_new_entity(self) -> None:
        resolver = Resolver(_ontology(), _Config(conflict_threshold=0.95))
        plan = resolver.resolve_entities(
            _payload({"temp_id": "e1", "name": "Harbor Master", "type": "Character", "is_new": True})
        )
        self.assertEqual(len(plan.new_entities), 1)
        self.assertEqual(plan.new_entities[0]["name"], "Harbor Master")
        self.assertEqual(plan.metrics["created_new"], 1)

    def test_mixed_payload_collects_conflicts_then_raises_once(self) -> None:
        resolver = Resolver(_ontology(), _Config(conflict_threshold=0.6))
        with self.assertRaises(ResolutionConflictError) as ctx:
            resolver.resolve_entities(
                _payload(
                    {"temp_id": "e1", "name": "Narrator", "type": "Character", "is_new": True},
                    {"temp_id": "e2", "name": "Narrat0r", "type": "Character", "is_new": True},
                )
            )
        self.assertEqual(len(ctx.exception.conflicts), 1)
        self.assertEqual(ctx.exception.conflicts[0]["temp_id"], "e2")

    def test_generic_name_can_promote_to_specific_name(self) -> None:
        ontology = ActiveOntology(
            run_id="r0",
            timestamp="2026-01-01T00:00:00Z",
            entities=[
                OntologyEntity(
                    uuid="uuid-she",
                    name="She",
                    type="Character",
                    aliases=[],
                    baseline_state=None,
                )
            ],
            states=[],
            relationships=[],
            retrieval={},
            event_types=[],
        )
        resolver = Resolver(ontology, _Config(conflict_threshold=0.6))
        plan = resolver.resolve_entities(
            _payload({"temp_id": "e1", "name": "Elara", "type": "Character", "is_new": True})
        )
        self.assertEqual(len(plan.conflicts), 0)
        self.assertEqual(len(plan.resolved_entities), 1)
        self.assertEqual(plan.resolved_entities[0]["uuid"], "uuid-she")
        promotion = [w for w in plan.warnings if w.get("type") == "name_promotion"]
        self.assertEqual(len(promotion), 1)
        self.assertEqual(promotion[0]["old_name"], "She")
        self.assertEqual(promotion[0]["new_name"], "Elara")


if __name__ == "__main__":
    unittest.main()
