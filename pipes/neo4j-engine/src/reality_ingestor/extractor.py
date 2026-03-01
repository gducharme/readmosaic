from __future__ import annotations

import json
import os
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
        raw = self.adapter.structured_extract(
            model=config.model,
            prompt=prompt,
            json_schema=self.schema,
            temperature=0.0,
        )
        payload = self._parse(raw, prompt, config)
        return ExtractedGraphPayload(payload)

    def _parse(self, raw: str | dict[str, Any], prompt: str, config) -> dict[str, Any]:
        artifact_dir = Path(config.artifact_dir) / "extraction"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        raw_path = artifact_dir / "llm_raw_output.txt"
        with raw_path.open("w", encoding="utf-8") as fh:
            fh.write(isinstance(raw, str) and raw or json.dumps(raw))

        payload = self._deserialize(raw)
        try:
            jsonschema.validate(instance=payload, schema=self.schema)
        except jsonschema.ValidationError as exc:
            repaired = self._repair(raw)
            try:
                payload = self._deserialize(repaired)
                jsonschema.validate(instance=payload, schema=self.schema)
            except (json.JSONDecodeError, jsonschema.ValidationError) as exc2:
                raise ExtractionSchemaError("Extractor returned invalid JSON") from exc2
        final_path = artifact_dir / "extracted_graph_payload.json"
        with final_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        return payload

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
            "Refer to actors by consistent temp_ids, identify their roles, "
            "anchor events to locations, and describe state changes and relationships."
        )
