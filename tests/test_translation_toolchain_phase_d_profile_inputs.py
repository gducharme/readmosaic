from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.translation_toolchain import atomic_write_jsonl, run_phase_d


class TranslationToolchainPhaseDProfileInputsTests(unittest.TestCase):
    def _build_paths(self, root: Path, profile: str) -> dict[str, Path]:
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
        atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_1", "paragraph_index": 1, "start_line": 1, "end_line": 1}])
        final_candidate.parent.mkdir(parents=True, exist_ok=True)
        final_candidate.write_text("candidate", encoding="utf-8")

        manifest = root / "manifest.json"
        manifest.write_text(
            '{"pipeline_profile":"%s","model":"stub-model"}' % profile,
            encoding="utf-8",
        )

        paths = {
            "run_root": root,
            "manifest": manifest,
            "paragraph_state": state_path,
            "final_candidate": final_candidate,
            "candidate_map": candidate_map,
            "review_normalized": root / "review" / "normalized",
            "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
            "rework_queue": root / "state" / "rework_queue.jsonl",
            "pass1_pre": root / "pass1_pre",
            "pass2_pre": root / "pass2_pre",
        }
        paths["pass1_pre"].mkdir(parents=True, exist_ok=True)
        paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
        (paths["pass1_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")
        (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")
        return paths

    def test_phase_d_uses_pass2_pre_for_tamazight_two_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            commands: list[list[str]] = []

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if "aggregate_paragraph_reviews.py" in command:
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            grammar_cmd = next(command for command in commands if "scripts/grammar_auditor.py" in command)
            self.assertIn(str(paths["pass2_pre"]), grammar_cmd)

    def test_phase_d_uses_pass1_pre_for_standard_single_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "standard_single_pass")
            commands: list[list[str]] = []

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if "aggregate_paragraph_reviews.py" in command:
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            grammar_cmd = next(command for command in commands if "scripts/grammar_auditor.py" in command)
            self.assertIn(str(paths["pass1_pre"]), grammar_cmd)

    def test_phase_d_rejects_empty_profile_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            with self.assertRaises(ValueError) as ctx:
                run_phase_d(
                    paths,
                    run_id="tx_001",
                    max_paragraph_attempts=4,
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                    pipeline_profile="   ",
                )
            self.assertIn("Missing pipeline profile", str(ctx.exception))

    def test_phase_d_override_profile_takes_precedence_over_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            commands: list[list[str]] = []

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if "aggregate_paragraph_reviews.py" in command:
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(
                    paths,
                    run_id="tx_001",
                    max_paragraph_attempts=4,
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                    pipeline_profile="standard_single_pass",
                )

            grammar_cmd = next(command for command in commands if "scripts/grammar_auditor.py" in command)
            self.assertIn(str(paths["pass1_pre"]), grammar_cmd)

    def test_phase_d_rejects_missing_manifest_profile_with_remediation_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            paths["manifest"].write_text('{"model":"stub-model"}', encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)
            self.assertIn("Missing pipeline profile", str(ctx.exception))
            self.assertIn("review_pre_dir", str(ctx.exception))

    def test_phase_d_rejects_missing_review_paragraphs_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            (paths["pass2_pre"] / "paragraphs.jsonl").unlink(missing_ok=True)
            with self.assertRaises(FileNotFoundError) as ctx:
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)
            self.assertIn("missing required artifact", str(ctx.exception))

    def test_phase_d_rejects_unknown_profile_with_remediation_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "mystery_profile")
            with self.assertRaises(ValueError) as ctx:
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)
            self.assertIn("Unknown pipeline profile", str(ctx.exception))
            self.assertIn("tamazight_two_pass", str(ctx.exception))
            self.assertIn("standard_single_pass", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
