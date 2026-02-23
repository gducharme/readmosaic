from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.map_review_to_paragraphs import _extract_issues
from scripts.normalize_critics_runner_output import normalize_critics_payload
from scripts.translation_toolchain import atomic_write_jsonl, run_phase_d
from scripts.typographic_precision_review import parse_args as parse_typography_args


class _PhaseDStopAfterIntermediate(RuntimeError):
    """Internal sentinel used to stop run_phase_d after contract assertions."""


class TranslationToolchainPhaseDContractsTests(unittest.TestCase):
    def _build_paths(self, root: Path) -> dict[str, Path]:
        state_path = root / "state" / "paragraph_state.jsonl"
        candidate_map = root / "final" / "candidate_map.jsonl"
        final_candidate = root / "final" / "candidate.md"

        atomic_write_jsonl(
            state_path,
            [
                {
                    "paragraph_id": "p_1",
                    "status": "candidate_assembled",
                    "attempt": 0,
                    "excluded_by_policy": False,
                    "failure_history": [],
                    "content_hash": "sha256:" + "a" * 64,
                }
            ],
        )
        atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_1", "paragraph_index": 1, "start_line": 1, "end_line": 2}])
        final_candidate.parent.mkdir(parents=True, exist_ok=True)
        final_candidate.write_text("First line.\nSecond line.", encoding="utf-8")

        manifest = root / "manifest.json"
        manifest.write_text('{"pipeline_profile":"tamazight_two_pass","model":"stub-model"}', encoding="utf-8")

        paths = {
            "run_root": root,
            "manifest": manifest,
            "paragraph_state": state_path,
            "final_candidate": final_candidate,
            "candidate_map": candidate_map,
            "review_normalized": root / "review" / "normalized",
            "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
            "rework_queue": root / "state" / "rework_queue.jsonl",
            "review_blockers": root / "gate" / "review_blockers.json",
            "pass1_pre": root / "pass1_pre",
            "pass2_pre": root / "pass2_pre",
        }
        paths["pass1_pre"].mkdir(parents=True, exist_ok=True)
        paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
        (paths["pass1_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")
        (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")
        return paths

    def test_typography_cli_accepts_phase_d_assembled_command_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp))
            commands: list[list[str]] = []

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text("[]", encoding="utf-8")
                    return
                if any("typographic_precision_review.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text('{"issues":[]}', encoding="utf-8")
                    return
                if any("critics_runner.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text('{"issues":[]}', encoding="utf-8")
                    return
                if any("normalize_critics_runner_output.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text('{"issues":[]}', encoding="utf-8")
                    return
                if any("map_review_to_paragraphs.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text("", encoding="utf-8")
                    return
                if any("normalize_review_output.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text("", encoding="utf-8")
                    return
                if any("aggregate_paragraph_reviews.py" in part for part in command):
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])
                    return
                raise AssertionError(f"Unexpected command: {command}")

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_contract", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            typography_cmd = next(cmd for cmd in commands if any("typographic_precision_review.py" in part for part in cmd))
            argv = ["typographic_precision_review.py", *typography_cmd[2:]]
            with patch("sys.argv", argv):
                args = parse_typography_args()

            self.assertEqual(args.manuscript, paths["final_candidate"])
            self.assertEqual(args.output_dir, paths["run_root"] / "review" / "typography")
            self.assertEqual(args.output, paths["run_root"] / "review" / "typography" / "typography_review.json")

    def test_critics_fixture_normalizes_to_mapper_ready_shape(self) -> None:
        fixture_root = Path("tests/fixtures/translation_toolchain")
        raw_payload = json.loads((fixture_root / "phase_d_contract_critics_runner_output.json").read_text(encoding="utf-8"))
        expected_payload = json.loads((fixture_root / "phase_d_expected_mapped_ready.json").read_text(encoding="utf-8"))

        normalized = normalize_critics_payload(raw_payload)
        self.assertEqual(normalized, expected_payload)

        extracted = _extract_issues(normalized)
        self.assertEqual(len(extracted), 2)
        for issue in extracted:
            self.assertTrue(any(anchor in issue for anchor in ("line", "start_line", "quote")))
            self.assertIn("reviewer", issue)
            self.assertIn("critic_name", issue)

    def test_phase_d_contract_validates_intermediate_artifacts_before_normalize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp))
            commands: list[list[str]] = []
            fixture_payload = json.loads(
                Path("tests/fixtures/translation_toolchain/phase_d_contract_critics_runner_output.json").read_text(
                    encoding="utf-8"
                )
            )

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text("[]", encoding="utf-8")
                    return

                if any("typographic_precision_review.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(
                        json.dumps({"issues": [{"issue_id": "typo_1", "line": 1, "description": "double space"}]}),
                        encoding="utf-8",
                    )
                    return

                if any("critics_runner.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(json.dumps(fixture_payload), encoding="utf-8")
                    return

                if any("normalize_critics_runner_output.py" in part for part in command):
                    input_path = Path(command[command.index("--input") + 1])
                    output_path = Path(command[command.index("--output") + 1])
                    normalized = normalize_critics_payload(json.loads(input_path.read_text(encoding="utf-8")))
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(json.dumps(normalized), encoding="utf-8")
                    return

                if any("map_review_to_paragraphs.py" in part for part in command):
                    review_input = Path(command[command.index("--review-input") + 1])
                    payload = json.loads(review_input.read_text(encoding="utf-8"))
                    extracted = _extract_issues(payload)
                    self.assertGreaterEqual(len(extracted), 1)
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    mapped_rows = [
                        {
                            "issue_id": issue.get("issue_id", "issue"),
                            "mapping_status": "mapped",
                            "paragraph_id": "p_1",
                            "paragraph_index": 1,
                        }
                        for issue in extracted
                    ]
                    output_path.write_text(
                        "".join(json.dumps(row) + "\n" for row in mapped_rows),
                        encoding="utf-8",
                    )
                    return

                if any("normalize_review_output.py" in part for part in command):
                    normalized_rows = paths["review_normalized"] / "all_reviews.jsonl"
                    typography_rows = paths["run_root"] / "review" / "normalized" / "typography_paragraph_rows.jsonl"
                    critics_rows = paths["run_root"] / "review" / "normalized" / "critics_paragraph_rows.jsonl"
                    critics_normalized = paths["run_root"] / "review" / "critics" / "critics_review_normalized.json"

                    self.assertTrue(typography_rows.exists())
                    self.assertTrue(critics_rows.exists())
                    self.assertTrue(critics_normalized.exists())

                    critics_payload = json.loads(critics_normalized.read_text(encoding="utf-8"))
                    self.assertEqual(critics_payload, json.loads(Path("tests/fixtures/translation_toolchain/phase_d_expected_mapped_ready.json").read_text(encoding="utf-8")))

                    self.assertFalse(normalized_rows.exists())
                    raise _PhaseDStopAfterIntermediate()

                raise AssertionError(f"Unexpected command: {command}")

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                with self.assertRaises(_PhaseDStopAfterIntermediate):
                    run_phase_d(paths, run_id="tx_contract", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            self.assertFalse(any(any("aggregate_paragraph_reviews.py" in part for part in cmd) for cmd in commands))


if __name__ == "__main__":
    unittest.main()
