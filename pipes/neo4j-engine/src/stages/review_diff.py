from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.models import ResolutionPlan
from reality_ingestor.reality_ingestor import RealityIngestor
from stages.helpers import hydrate_ontology, load_artifact, write_json_artifact


def run_whole(ctx) -> None:
    ingestor = RealityIngestor.from_env()
    extraction = load_artifact(ctx, "extracted_graph_payload.json")
    ontology_payload = load_artifact(ctx, "active_ontology.json")
    ontology = hydrate_ontology(ontology_payload)
    plan_dict = load_artifact(ctx, "resolution_plan.json")
    plan = ResolutionPlan.from_dict(plan_dict)
    diff = ingestor.diff_validator.summarize(plan, extraction)
    write_json_artifact(ctx, "diff_report.json", diff.to_dict())
