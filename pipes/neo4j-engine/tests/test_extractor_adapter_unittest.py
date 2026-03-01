from __future__ import annotations

import json
import sys
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from reality_ingestor.errors import ExtractionAdapterError, ExtractionSchemaError
from reality_ingestor.extractor import Extractor
from reality_ingestor.llm.llm_adapter import LiteLLMAdapter
from reality_ingestor.models import ActiveOntology, ParsedChapter
from reality_ingestor.reality_ingestor import RealityIngestor


@dataclass
class _Config:
    model: str = "lfm2-24b-a2b"
    adapter: str = "litellm"
    llm_base_url: str = "http://127.0.0.1:1234/v1"
    artifact_dir: Path = Path(".")


class _Adapter:
    def __init__(self, responses):
        self.responses = list(responses)

    def structured_extract(self, **kwargs):
        return self.responses.pop(0)


def _parsed() -> ParsedChapter:
    return ParsedChapter(
        chapter_id="chapter",
        chapter_hash="hash",
        source_path="sample.md",
        full_text="Alice meets Bob in the corridor.",
        chunks=[],
    )


def _ontology() -> ActiveOntology:
    return ActiveOntology(
        run_id="r1",
        timestamp="t",
        entities=[],
        states=[],
        relationships=[],
        retrieval={},
        event_types=[],
    )


class ExtractorAdapterTests(unittest.TestCase):
    def test_semantic_validation_fails_on_missing_participant(self) -> None:
        bad_payload = {
            "entities": [{"temp_id": "loc1", "name": "Hall", "type": "Location", "is_new": True}],
            "events": [
                {
                    "event_id": "evt_1",
                    "description": "test",
                    "location_temp_id": "loc1",
                    "participants": [{"entity_temp_id": "char1", "role": "Observer"}],
                }
            ],
            "state_changes": [],
            "relationships": [],
        }
        extractor = Extractor(_Adapter([bad_payload, bad_payload]))
        with tempfile.TemporaryDirectory() as td:
            cfg = _Config(artifact_dir=Path(td))
            with self.assertRaises(ExtractionSchemaError):
                extractor.extract_graph_json(_parsed(), _ontology(), cfg)
            meta = json.loads((Path(td) / "extraction" / "extraction_meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["validation_failures"], 2)

    def test_retry_recovers_from_first_invalid_output(self) -> None:
        bad_payload = {
            "entities": [],
            "events": [{"event_id": "evt_1", "description": "test", "location_temp_id": "loc1", "participants": []}],
            "state_changes": [],
            "relationships": [],
        }
        good_payload = {
            "entities": [
                {"temp_id": "loc1", "name": "Hall", "type": "Location", "is_new": True},
                {"temp_id": "c1", "name": "Alice", "type": "Character", "is_new": True},
            ],
            "events": [
                {
                    "event_id": "evt_1",
                    "description": "test",
                    "location_temp_id": "loc1",
                    "participants": [{"entity_temp_id": "c1", "role": "Observer"}],
                }
            ],
            "state_changes": [],
            "relationships": [],
        }
        extractor = Extractor(_Adapter([bad_payload, good_payload]))
        with tempfile.TemporaryDirectory() as td:
            cfg = _Config(artifact_dir=Path(td))
            payload = extractor.extract_graph_json(_parsed(), _ontology(), cfg)
            self.assertEqual(len(payload.data["entities"]), 2)
            meta = json.loads((Path(td) / "extraction" / "extraction_meta.json").read_text(encoding="utf-8"))
            self.assertEqual(meta["attempts"], 2)
            self.assertEqual(meta["validation_failures"], 1)

    def test_reality_ingestor_hard_fails_on_unknown_adapter(self) -> None:
        class _Cfg:
            adapter = "unknown"
            llm_base_url = "http://127.0.0.1:1234/v1"
            llm_api_key = "lm-studio"

            def ensure_artifact_dir(self):
                return Path(".")

            def driver(self):
                raise RuntimeError("not needed")

        with self.assertRaises(ExtractionAdapterError):
            RealityIngestor(_Cfg())

    def test_lmstudio_adapter_connection_error_hard_fails(self) -> None:
        adapter = LiteLLMAdapter(base_url="http://127.0.0.1:1234/v1", api_key="lm-studio")
        with patch("reality_ingestor.llm.llm_adapter.request.urlopen", side_effect=OSError("refused")):
            with self.assertRaises(ExtractionAdapterError):
                adapter.structured_extract(model="lfm2-24b-a2b", prompt="{}", json_schema={})

    def test_lmstudio_adapter_parses_openai_content_json(self) -> None:
        adapter = LiteLLMAdapter(base_url="http://127.0.0.1:1234/v1", api_key="lm-studio")

        class _Resp:
            def __enter__(self):
                payload = {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "entities": [],
                                        "events": [],
                                        "state_changes": [],
                                        "relationships": [],
                                    }
                                )
                            }
                        }
                    ]
                }
                self._body = json.dumps(payload).encode("utf-8")
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return self._body

        with patch("reality_ingestor.llm.llm_adapter.request.urlopen", return_value=_Resp()):
            out = adapter.structured_extract(model="lfm2-24b-a2b", prompt="{}", json_schema={})
        self.assertIn("entities", out)


if __name__ == "__main__":
    unittest.main()
