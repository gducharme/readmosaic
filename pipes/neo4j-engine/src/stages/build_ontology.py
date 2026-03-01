from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.reality_ingestor import RealityIngestor


def run_whole(ctx) -> None:  # noqa: ARG001
    ingestor = RealityIngestor.from_env()
    parsed_path = Path("artifacts/parsed_chapter.json")
    parsed_payload = json.loads(parsed_path.read_text(encoding="utf-8"))
    parsed = ingestor.parse_markdown(parsed_payload["source_path"])
    ontology = ingestor.build_ontology_context(parsed)
    output_path = Path("artifacts/active_ontology.json")
    output_path.write_text(json.dumps(ontology.to_dict(), indent=2), encoding="utf-8")
