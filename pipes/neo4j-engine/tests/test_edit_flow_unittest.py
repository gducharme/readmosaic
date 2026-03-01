from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from reality_ingestor.errors import CommitRejected
from reality_ingestor.models import CommitReport, ResolutionPlan
from reality_ingestor.schemas import EXTRACTION_SCHEMA
from reality_ingestor.diff_validator import DiffValidator
from stages import commit_graph
from stages.edit_json import EditCancelled, edit_json_artifact_with_schema


class _Config:
    run_id = "run-test"
    diff_decision = "edited"


class _Ctx:
    def __init__(self, mapping: dict[str, Path], output: Path):
        self.inputs = [{"path": str(v)} for v in mapping.values()]
        self.outputs = [{"path": str(output)}]


class EditFlowTests(unittest.TestCase):
    def test_diff_validator_sets_edit_target(self) -> None:
        validator = DiffValidator(_Config())
        plan = ResolutionPlan(
            run_id="r",
            resolved_entities=[],
            new_entities=[],
            conflicts=[],
            warnings=[],
        )
        report = validator.summarize(plan, {"events": [], "state_changes": [], "relationships": []})
        self.assertEqual(report.decision["status"], "edited")
        self.assertEqual(report.decision["edit_target"], "extracted_graph_payload.json")

    def test_edit_json_invalid_then_valid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            target = base / "extracted_graph_payload.json"
            target.write_text(json.dumps({"entities": [], "events": [], "state_changes": [], "relationships": []}))
            ctx = SimpleNamespace(inputs=[{"path": str(target)}])
            calls = {"n": 0}

            def _fake_editor(cmd, check):
                calls["n"] += 1
                path = Path(cmd[1])
                if calls["n"] == 1:
                    path.write_text("{invalid", encoding="utf-8")
                else:
                    path.write_text(
                        json.dumps(
                            {"entities": [], "events": [], "state_changes": [], "relationships": []}
                        ),
                        encoding="utf-8",
                    )
                return SimpleNamespace(returncode=0)

            with patch("stages.edit_json.subprocess.run", side_effect=_fake_editor):
                with patch("builtins.input", return_value="n"):
                    payload, stats = edit_json_artifact_with_schema(
                        ctx, artifact_name="extracted_graph_payload.json", schema=EXTRACTION_SCHEMA
                    )
            self.assertIn("entities", payload)
            self.assertEqual(stats["edit_attempts"], 2)
            self.assertEqual(stats["validation_failures"], 1)

    def test_edit_json_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            target = base / "extracted_graph_payload.json"
            target.write_text(json.dumps({"entities": [], "events": [], "state_changes": [], "relationships": []}))
            ctx = SimpleNamespace(inputs=[{"path": str(target)}])
            with patch("stages.edit_json.subprocess.run", return_value=SimpleNamespace(returncode=0)):
                with patch("builtins.input", return_value="y"):
                    with self.assertRaises(EditCancelled):
                        edit_json_artifact_with_schema(
                            ctx, artifact_name="extracted_graph_payload.json", schema=EXTRACTION_SCHEMA
                        )

    def test_commit_stage_edited_routes_to_editor(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            paths = {
                "parsed_chapter.json": base / "parsed_chapter.json",
                "extracted_graph_payload.json": base / "extracted_graph_payload.json",
                "resolution_plan.json": base / "resolution_plan.json",
                "diff_report.json": base / "diff_report.json",
            }
            paths["parsed_chapter.json"].write_text(json.dumps({"source_path": "dummy.md"}), encoding="utf-8")
            paths["extracted_graph_payload.json"].write_text(
                json.dumps({"entities": [], "events": [], "state_changes": [], "relationships": []}),
                encoding="utf-8",
            )
            paths["resolution_plan.json"].write_text(
                json.dumps(
                    {
                        "run_id": "r",
                        "resolved_entities": [],
                        "new_entities": [],
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                encoding="utf-8",
            )
            paths["diff_report.json"].write_text(
                json.dumps({"decision": {"status": "edited", "edit_target": "extracted_graph_payload.json"}}),
                encoding="utf-8",
            )
            output = base / "commit_report.json"
            ctx = _Ctx(paths, output)

            class _Ingestor:
                @staticmethod
                def from_env():
                    return _Ingestor()

                def parse_markdown(self, source_path):
                    return SimpleNamespace(chapter_id="c1", source_path=source_path, chunks=[])

                def commit_to_graph(self, parsed, payload, plan):
                    return CommitReport(run_id="r", status="success", metrics={})

            with patch.object(commit_graph, "RealityIngestor", _Ingestor):
                with patch.object(
                    commit_graph,
                    "edit_json_artifact_with_schema",
                    return_value=(
                        {"entities": [], "events": [], "state_changes": [], "relationships": []},
                        {"edit_attempts": 2, "validation_failures": 1},
                    ),
                ):
                    with patch("sys.stdin.isatty", return_value=True):
                        commit_graph.run_whole(ctx)

            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "success")
            self.assertTrue(report["metrics"]["edited_before_commit"])
            self.assertEqual(report["metrics"]["edit_attempts"], 2)
            self.assertEqual(report["metrics"]["validation_failures"], 1)

    def test_commit_stage_non_tty_edited_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            paths = {
                "parsed_chapter.json": base / "parsed_chapter.json",
                "extracted_graph_payload.json": base / "extracted_graph_payload.json",
                "resolution_plan.json": base / "resolution_plan.json",
                "diff_report.json": base / "diff_report.json",
            }
            paths["parsed_chapter.json"].write_text(json.dumps({"source_path": "dummy.md"}), encoding="utf-8")
            paths["extracted_graph_payload.json"].write_text(
                json.dumps({"entities": [], "events": [], "state_changes": [], "relationships": []}),
                encoding="utf-8",
            )
            paths["resolution_plan.json"].write_text(
                json.dumps(
                    {
                        "run_id": "r",
                        "resolved_entities": [],
                        "new_entities": [],
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                encoding="utf-8",
            )
            paths["diff_report.json"].write_text(
                json.dumps({"decision": {"status": "edited", "edit_target": "extracted_graph_payload.json"}}),
                encoding="utf-8",
            )
            output = base / "commit_report.json"
            ctx = _Ctx(paths, output)

            class _Ingestor:
                @staticmethod
                def from_env():
                    return _Ingestor()

                def parse_markdown(self, source_path):
                    return SimpleNamespace(chapter_id="c1", source_path=source_path, chunks=[])

            with patch.object(commit_graph, "RealityIngestor", _Ingestor):
                with patch("sys.stdin.isatty", return_value=False):
                    with self.assertRaises(CommitRejected):
                        commit_graph.run_whole(ctx)


if __name__ == "__main__":
    unittest.main()
