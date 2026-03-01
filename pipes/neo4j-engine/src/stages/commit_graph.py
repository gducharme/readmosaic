from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.errors import CommitRejected
from reality_ingestor.models import ResolutionPlan
from reality_ingestor.reality_ingestor import RealityIngestor
from reality_ingestor.schemas import EXTRACTION_SCHEMA
from stages.edit_json import EditCancelled, edit_json_artifact_with_schema
from stages.helpers import load_artifact, write_json_artifact


def run_whole(ctx) -> None:
    ingestor = RealityIngestor.from_env()
    parsed_payload = load_artifact(ctx, "parsed_chapter.json")
    parsed = ingestor.parse_markdown(parsed_payload["source_path"])
    extraction = load_artifact(ctx, "extracted_graph_payload.json")
    plan_dict = load_artifact(ctx, "resolution_plan.json")
    plan = ResolutionPlan.from_dict(plan_dict)
    diff = load_artifact(ctx, "diff_report.json")
    decision = diff.get("decision", {})
    status = decision.get("status")
    edit_stats = {"edit_attempts": 0, "validation_failures": 0}
    edited_before_commit = False

    if status == "rejected":
        raise CommitRejected("Commit rejected by user")

    if status == "edited":
        if not sys.stdin.isatty():
            raise CommitRejected(
                "Edit mode requires TTY/editor; rerun interactively or set decision to accepted/rejected."
            )
        edit_target = decision.get("edit_target") or "extracted_graph_payload.json"
        if edit_target != "extracted_graph_payload.json":
            raise CommitRejected(f"Unsupported edit target: {edit_target}")
        try:
            extraction, edit_stats = edit_json_artifact_with_schema(
                ctx,
                artifact_name="extracted_graph_payload.json",
                schema=EXTRACTION_SCHEMA,
            )
        except EditCancelled as exc:
            raise CommitRejected(str(exc)) from exc
        edited_before_commit = True

    if status not in {"accepted", "edited"}:
        raise CommitRejected(f"Unknown decision status: {status}")

    report = ingestor.commit_to_graph(parsed, extraction, plan)
    report.metrics["edited_before_commit"] = edited_before_commit
    report.metrics["edit_attempts"] = edit_stats["edit_attempts"]
    report.metrics["validation_failures"] = edit_stats["validation_failures"]
    write_json_artifact(ctx, "commit_report.json", report.to_dict())
