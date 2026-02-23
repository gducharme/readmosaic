from __future__ import annotations

import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, RefResolver
from lib.paragraph_state_machine import ALLOWED_STATUS_EVOLUTION, KNOWN_STATES

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = REPO_ROOT / "schemas" / "translation_toolchain"
FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "translation_toolchain" / "schema_validation"


def _schema_store() -> dict[str, dict]:
    store: dict[str, dict] = {}
    for schema_path in SCHEMA_ROOT.glob("*.schema.json"):
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
        schema_id = payload.get("$id")
        if isinstance(schema_id, str):
            store[schema_id] = payload
        store[schema_path.name] = payload
    return store


def _validate(payload: dict, schema_name: str, store: dict[str, dict]) -> None:
    schema = store[schema_name]
    resolver = RefResolver.from_schema(schema, store=store)
    Draft202012Validator(schema, resolver=resolver).validate(payload)


class TranslationToolchainSchemaContractTests(unittest.TestCase):
    def test_status_evolution_contract_is_known_and_ordered(self) -> None:
        store = _schema_store()
        defs_schema = store["defs.schema.json"]
        status_enum = set(defs_schema["$defs"]["paragraph_status"]["enum"])
        self.assertEqual(status_enum, set(ALLOWED_STATUS_EVOLUTION.keys()))
        self.assertEqual(status_enum, KNOWN_STATES)

        for prior, next_states in ALLOWED_STATUS_EVOLUTION.items():
            for next_state in next_states:
                with self.subTest(prior=prior, next_state=next_state):
                    self.assertIn(next_state, status_enum)

    def test_representative_contract_fixtures(self) -> None:
        store = _schema_store()
        fixture_names = [
            "happy_path.json",
            "rework_path.json",
            "mapping_error_path.json",
            "ingested_seed_path.json",
        ]

        for fixture_name in fixture_names:
            fixture_path = FIXTURE_ROOT / fixture_name
            fixture = json.loads(fixture_path.read_text(encoding="utf-8"))

            with self.subTest(fixture=fixture_name, schema="paragraph_state_row"):
                state_row = fixture["paragraph_state_row"]
                _validate(state_row, "paragraph_state_row.schema.json", store)

                self.assertGreaterEqual(int(state_row["attempt"]), 0)
                for entry in state_row.get("failure_history", []):
                    attempt_value = entry["attempt"]
                    if attempt_value is not None:
                        self.assertGreaterEqual(int(attempt_value), 1)

                if fixture_name == "ingested_seed_path.json":
                    self.assertNotIn("failure_history", state_row)

            with self.subTest(fixture=fixture_name, schema="normalized_review_row"):
                _validate(fixture["normalized_review_row"], "normalized_review_row.schema.json", store)

            with self.subTest(fixture=fixture_name, schema="paragraph_scores_row"):
                _validate(fixture["paragraph_scores_row"], "paragraph_scores_row.schema.json", store)

            with self.subTest(fixture=fixture_name, schema="candidate_map_row"):
                candidate_map_row = fixture["candidate_map_row"]
                _validate(candidate_map_row, "candidate_map_row.schema.json", store)
                self.assertGreaterEqual(candidate_map_row["end_line"], candidate_map_row["start_line"])

            if "rework_queue_row" in fixture:
                with self.subTest(fixture=fixture_name, schema="rework_queue_row"):
                    _validate(fixture["rework_queue_row"], "rework_queue_row.schema.json", store)

            with self.subTest(fixture=fixture_name, schema="manifest"):
                _validate(fixture["manifest"], "manifest.schema.json", store)

    def test_failure_history_oneof_modern_or_legacy_sentinel(self) -> None:
        store = _schema_store()
        row = {
            "paragraph_id": "p_9000",
            "status": "rework_queued",
            "attempt": 2,
            "failure_history": [
                {"issues": ["mapping_error"], "attempt": 2, "timestamp": "2026-03-01T10:00:00Z"},
                {"issues": ["legacy_unknown"], "attempt": None, "timestamp": None},
            ],
            "excluded_by_policy": False,
            "content_hash": "sha256:" + "f" * 64,
        }
        _validate(row, "paragraph_state_row.schema.json", store)

        bad_row = {
            **row,
            "failure_history": [
                {"issues": ["mixed_null"], "attempt": None, "timestamp": "2026-03-01T10:00:00Z"}
            ],
        }

        with self.assertRaises(Exception):
            _validate(bad_row, "paragraph_state_row.schema.json", store)

    def test_manifest_contract_requires_rich_metadata(self) -> None:
        store = _schema_store()
        fixture = json.loads((FIXTURE_ROOT / "happy_path.json").read_text(encoding="utf-8"))
        manifest = dict(fixture["manifest"])

        _validate(manifest, "manifest.schema.json", store)

        missing_policy = dict(manifest)
        missing_policy.pop("aggregation_policy_snapshot", None)
        with self.assertRaises(Exception):
            _validate(missing_policy, "manifest.schema.json", store)

        bad_checkpoint = dict(manifest)
        bad_checkpoint["status_checkpoint"] = {
            "phase": "D",
            "phase_state": "paused",
            "updated_at": manifest["updated_at"],
        }
        with self.assertRaises(Exception):
            _validate(bad_checkpoint, "manifest.schema.json", store)

        bad_review_dir = dict(manifest)
        bad_review_dir["review_pre_dir"] = "source_pre"
        with self.assertRaises(Exception):
            _validate(bad_review_dir, "manifest.schema.json", store)



if __name__ == "__main__":
    unittest.main()
