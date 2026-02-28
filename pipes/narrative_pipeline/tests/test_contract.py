from __future__ import annotations

import json
from pathlib import Path

from src.stages.contract import validate_bundle


def test_validate_bundle_fixture() -> None:
    fixture = Path(__file__).parent / "fixtures" / "sample_bundle.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    validate_bundle(payload)

