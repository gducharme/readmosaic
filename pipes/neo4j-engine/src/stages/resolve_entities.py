from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.reality_ingestor import RealityIngestor
from stages.helpers import hydrate_ontology, load_artifact


def run_whole(ctx) -> None:  # noqa: ARG001
    ingestor = RealityIngestor.from_env()
    extraction = load_artifact(Path("artifacts/extracted_graph_payload.json"))
    ontology_payload = load_artifact(Path("artifacts/active_ontology.json"))
    ontology = hydrate_ontology(ontology_payload)
    plan = ingestor.resolve_entities(extraction, ontology)
    output_path = Path("artifacts/resolution_plan.json")
    output_path.write_text(json.dumps(plan.to_dict(), indent=2), encoding="utf-8")
