from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.reality_ingestor import RealityIngestor
from stages.helpers import hydrate_ontology, load_artifact, write_json_artifact


def run_whole(ctx) -> None:
    ingestor = RealityIngestor.from_env()
    parsed_payload = load_artifact(ctx, "parsed_chapter.json")
    parsed = ingestor.parse_markdown(parsed_payload["source_path"])
    ontology_payload = load_artifact(ctx, "active_ontology.json")
    ontology = hydrate_ontology(ontology_payload)
    extraction = ingestor.extract_graph_json(parsed, ontology)
    write_json_artifact(ctx, "extracted_graph_payload.json", extraction.data)
