from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.errors import CommitRejected
from reality_ingestor.models import ResolutionPlan
from reality_ingestor.reality_ingestor import RealityIngestor
from stages.helpers import load_artifact


def run_whole(ctx) -> None:  # noqa: ARG001
    ingestor = RealityIngestor.from_env()
    parsed_payload = load_artifact(Path("artifacts/parsed_chapter.json"))
    parsed = ingestor.parse_markdown(parsed_payload["source_path"])
    extraction = load_artifact(Path("artifacts/extracted_graph_payload.json"))
    plan_dict = load_artifact(Path("artifacts/resolution_plan.json"))
    plan = ResolutionPlan.from_dict(plan_dict)
    diff = load_artifact(Path("artifacts/diff_report.json"))
    if diff.get("decision", {}).get("status") != "accepted":
        raise CommitRejected("Commit rejected by user")
    report = ingestor.commit_to_graph(parsed, extraction, plan)
    output_path = Path("artifacts/commit_report.json")
    output_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
