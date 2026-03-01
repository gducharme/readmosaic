from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema

from .errors import ExtractionSchemaError
from .llm.base import LLMAdapter
from .schemas import EXTRACTION_SCHEMA
from .models import ActiveOntology, ExtractedGraphPayload, ParsedChapter


class Extractor:
    def __init__(self, adapter: LLMAdapter):
        self.adapter = adapter
        self.schema = EXTRACTION_SCHEMA

    def extract_graph_json(
        self,
        parsed: ParsedChapter,
        ontology: ActiveOntology,
        config,
    ) -> ExtractedGraphPayload:
        prompt = self._build_prompt(parsed, ontology)
        attempts = 0
        validation_failures = 0
        prompt_to_use = prompt
        last_errors: list[str] = []
        raw: str | dict[str, Any] = ""
        while attempts < 2:
            attempts += 1
            raw = self.adapter.structured_extract(
                model=config.model,
                prompt=prompt_to_use,
                json_schema=self.schema,
                temperature=0.0,
            )
            self._print_llm_proposal(raw=raw, attempt=attempts)
            payload, errors = self._parse_once(raw)
            if payload is not None and not errors:
                self._write_artifacts(payload, raw, config, attempts, validation_failures, [])
                return ExtractedGraphPayload(payload)
            validation_failures += 1
            last_errors = errors
            prompt_to_use = (
                f"{prompt}\n\nValidation errors from previous output:\n- "
                + "\n- ".join(errors)
                + "\nReturn corrected JSON only."
            )
        self._write_artifacts({}, raw, config, attempts, validation_failures, last_errors)
        raise ExtractionSchemaError(
            "Extractor returned invalid JSON/semantics after retry: " + "; ".join(last_errors)
        )

    def _parse_once(self, raw: str | dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        errors: list[str] = []
        try:
            payload = self._deserialize(raw)
        except (json.JSONDecodeError, ExtractionSchemaError):
            repaired = self._repair(raw)
            try:
                payload = self._deserialize(repaired)
            except (json.JSONDecodeError, ExtractionSchemaError):
                return None, ["Invalid JSON returned by extractor."]
        try:
            jsonschema.validate(instance=payload, schema=self.schema)
        except jsonschema.ValidationError as exc:
            errors.append(f"Schema error at {list(exc.absolute_path) or '$'}: {exc.message}")
            return None, errors
        errors.extend(self._semantic_checks(payload))
        if errors:
            return None, errors
        return payload, []

    def _write_artifacts(
        self,
        payload: dict[str, Any],
        raw: str | dict[str, Any],
        config,
        attempts: int,
        validation_failures: int,
        errors: list[str],
    ) -> None:
        artifact_dir = Path(config.artifact_dir) / "extraction"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        raw_path = artifact_dir / "llm_raw_output.txt"
        with raw_path.open("w", encoding="utf-8") as fh:
            fh.write(isinstance(raw, str) and raw or json.dumps(raw))
        final_path = artifact_dir / "extracted_graph_payload.json"
        with final_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        meta_path = artifact_dir / "extraction_meta.json"
        meta = {
            "adapter": getattr(config, "adapter", "unknown"),
            "model": getattr(config, "model", "unknown"),
            "base_url": getattr(config, "llm_base_url", None),
            "attempts": attempts,
            "validation_failures": validation_failures,
            "errors": errors,
        }
        with meta_path.open("w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2)

    def _semantic_checks(self, payload: dict[str, Any]) -> list[str]:
        errors: list[str] = []
        entities = payload.get("entities", [])
        events = payload.get("events", [])
        entity_by_id = {item.get("temp_id"): item for item in entities if isinstance(item, dict)}
        event_ids = {item.get("event_id") for item in events if isinstance(item, dict)}
        for event in events:
            if not isinstance(event, dict):
                continue
            location_id = event.get("location_temp_id")
            location = entity_by_id.get(location_id)
            if not location:
                errors.append(f"Event {event.get('event_id')} references missing location_temp_id '{location_id}'.")
            elif location.get("type") != "Location":
                errors.append(f"Event {event.get('event_id')} location '{location_id}' is not type Location.")
            for participant in event.get("participants", []):
                part_id = participant.get("entity_temp_id")
                if part_id not in entity_by_id:
                    errors.append(
                        f"Event {event.get('event_id')} participant references missing entity_temp_id '{part_id}'."
                    )
        for state in payload.get("state_changes", []):
            if not isinstance(state, dict):
                continue
            if state.get("entity_temp_id") not in entity_by_id:
                errors.append(
                    f"State change references missing entity_temp_id '{state.get('entity_temp_id')}'."
                )
            if state.get("triggered_by_event_id") not in event_ids:
                errors.append(
                    f"State change references missing triggered_by_event_id '{state.get('triggered_by_event_id')}'."
                )
        for rel in payload.get("relationships", []):
            if not isinstance(rel, dict):
                continue
            if rel.get("source_temp_id") not in entity_by_id:
                errors.append(f"Relationship source missing entity '{rel.get('source_temp_id')}'.")
            if rel.get("target_temp_id") not in entity_by_id:
                errors.append(f"Relationship target missing entity '{rel.get('target_temp_id')}'.")
            weight = rel.get("weight")
            if not isinstance(weight, (int, float)) or weight < -1.0 or weight > 1.0:
                errors.append("Relationship weight must be between -1.0 and 1.0.")
        return errors

    def _deserialize(self, raw: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str):
            raise ExtractionSchemaError("Unexpected extractor payload type")
        return json.loads(raw.strip())

    def _repair(self, raw: Any) -> str:
        text = raw if isinstance(raw, str) else ""
        cleaned = text.strip()
        if cleaned.startswith("```") and cleaned.endswith("```"):
            cleaned = cleaned.strip("`\n")
        if not cleaned.startswith("{") and "{" in cleaned:
            cleaned = cleaned[cleaned.index("{") :]
        while cleaned.endswith(","):
            cleaned = cleaned[:-1]
        return cleaned

    def _print_llm_proposal(self, *, raw: str | dict[str, Any], attempt: int) -> None:
        print(f"\n[extractor] LLM proposal (attempt {attempt}):")
        try:
            if isinstance(raw, dict):
                print(json.dumps(raw, ensure_ascii=False, indent=2))
                return
            text = raw if isinstance(raw, str) else str(raw)
            parsed = json.loads(text)
            print(json.dumps(parsed, ensure_ascii=False, indent=2))
        except Exception:
            preview = raw if isinstance(raw, str) else str(raw)
            preview = preview[:4000]
            print(preview)

    def _build_prompt(self, parsed: ParsedChapter, ontology: ActiveOntology) -> str:
        summary = (
            f"Active ontology contains {len(ontology.entities)} entities, "
            f"{len(ontology.states)} tracked states, and {len(ontology.relationships)} relationships."
        )
        return (
            "Extract a knowledge graph from the following chapter. "
            "Return JSON strictly matching the schema.\n\n"
            f"Chapter text:\n{parsed.full_text}\n\n"
            f"{summary}\n\n"
            f"Required JSON schema:\n{json.dumps(self.schema, ensure_ascii=False, indent=2)}\n\n"
            "Set is_new=false when an entity clearly matches the active ontology by name/alias; "
            "set is_new=true only for truly unseen entities.\n"
            "Extract all distinct characters present in chapter text; "
            "do not collapse multi-actor scenes into narrator-only output.\n"
            "Refer to actors by consistent temp_ids, identify their roles, "
            "anchor events to locations, and describe state changes and relationships."
        )
