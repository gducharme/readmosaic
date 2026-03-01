from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.reality_ingestor import RealityIngestor
from stages.helpers import load_artifact, write_json_artifact


def run_whole(ctx) -> None:
    ingestor = RealityIngestor.from_env()
    parsed_payload = load_artifact(ctx, "parsed_chapter.json")
    parsed = ingestor.parse_markdown(parsed_payload["source_path"])
    ontology = ingestor.build_ontology_context(parsed)
    write_json_artifact(ctx, "active_ontology.json", ontology.to_dict())
