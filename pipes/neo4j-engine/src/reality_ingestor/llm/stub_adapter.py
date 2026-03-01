from __future__ import annotations

from typing import Any

from .base import LLMAdapter


class StubAdapter(LLMAdapter):
    def structured_extract(
        self,
        *,
        model: str,
        prompt: str,
        json_schema: dict[str, Any],
        temperature: float = 0.0,
        timeout_s: int = 90,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if not prompt:
            raise ValueError("StubAdapter requires prompt text")
        chunk = prompt.strip().split("\n\n")[0]
        primary_entity = {
            "temp_id": "ent_primary",
            "name": "Narrator",
            "type": "Character",
            "is_new": True,
        }
        location_entity = {
            "temp_id": "ent_location",
            "name": "ChapterLocation",
            "type": "Location",
            "is_new": True,
        }
        event = {
            "event_id": "evt_1",
            "description": chunk[:280],
            "location_temp_id": "ent_location",
            "participants": [
                {"entity_temp_id": "ent_primary", "role": "Initiator"},
            ],
        }
        state_change = {
            "entity_temp_id": "ent_primary",
            "attribute": "Epistemic_Knowledge",
            "new_value": "Awakened",
            "triggered_by_event_id": "evt_1",
        }
        relationship = {
            "source_temp_id": "ent_primary",
            "target_temp_id": "ent_location",
            "nature": "Located_At",
            "weight": 0.3,
        }
        return {
            "entities": [primary_entity, location_entity],
            "events": [event],
            "state_changes": [state_change],
            "relationships": [relationship],
        }
