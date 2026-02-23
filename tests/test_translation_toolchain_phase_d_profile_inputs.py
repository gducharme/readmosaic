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
            "review_blockers": root / "gate" / "review_blockers.json",
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
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text('[{"paragraph_id":"p_1","scores":{},"issues":[],"blocking_issues":[],"hard_fail":false}]', encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
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
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text('[{"paragraph_id":"p_1","scores":{},"issues":[],"blocking_issues":[],"hard_fail":false}]', encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
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
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text('[{"paragraph_id":"p_1","scores":{},"issues":[],"blocking_issues":[],"hard_fail":false}]', encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
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

    def test_phase_d_normalizes_using_latest_grammar_artifact_before_aggregation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            commands: list[list[str]] = []

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20240101T000000Z.json").write_text("[]", encoding="utf-8")
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text("[]", encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            normalize_cmd = next(command for command in commands if any("normalize_review_output.py" in part for part in command))
            grammar_input = normalize_cmd[normalize_cmd.index("--grammar-input") + 1]
            self.assertIn("grammar_audit_issues_20260101T000000Z.json", grammar_input)

            normalize_index = commands.index(normalize_cmd)
            aggregate_index = next(
                idx for idx, command in enumerate(commands) if any("aggregate_paragraph_reviews.py" in part for part in command)
            )
            self.assertLess(normalize_index, aggregate_index)

    def test_phase_d_passes_existing_mapped_inputs_to_normalizer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
            commands: list[list[str]] = []
            normalized_dir = paths["run_root"] / "review" / "normalized"
            normalized_dir.mkdir(parents=True, exist_ok=True)
            typography_rows = normalized_dir / "typography_paragraph_rows.jsonl"
            critics_rows = normalized_dir / "critics_paragraph_rows.jsonl"
            typography_rows.write_text('{"paragraph_id":"p_1","mapping_status":"mapped"}\n', encoding="utf-8")
            critics_rows.write_text('{"paragraph_id":"p_1","mapping_status":"mapped"}\n', encoding="utf-8")

            def _stub_exec(command: list[str], **_: object) -> None:
                commands.append(command)
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text("[]", encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            normalize_cmd = next(command for command in commands if any("normalize_review_output.py" in part for part in command))
            mapped_inputs = [
                normalize_cmd[index + 1]
                for index, token in enumerate(normalize_cmd)
                if token == "--mapped-input"
            ]
            self.assertEqual(mapped_inputs, [str(typography_rows), str(critics_rows)])

    def test_phase_d_inserts_critics_adapter_before_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
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
                    output_path.write_text('{"critic_a":{"issues":[{"issue_id":"crit_1","line":1}]}}', encoding="utf-8")
                    return
                if any("normalize_critics_runner_output.py" in part for part in command):
                    import subprocess

                    subprocess.run(command, check=True)
                    return
                if any("map_review_to_paragraphs.py" in part for part in command):
                    mapped_output = Path(command[command.index("--output") + 1])
                    mapped_output.parent.mkdir(parents=True, exist_ok=True)
                    mapped_output.write_text('', encoding="utf-8")
                    return
                if any("normalize_review_output.py" in part for part in command) or any("aggregate_paragraph_reviews.py" in part for part in command):
                    import subprocess

                    subprocess.run(command, check=True)
                    return
                raise AssertionError(f"Unexpected command: {command}")

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            adapter_cmd = next(command for command in commands if any("normalize_critics_runner_output.py" in part for part in command))
            critics_map_cmd = next(
                command
                for command in commands
                if any("map_review_to_paragraphs.py" in part for part in command) and command[command.index("--reviewer") + 1] == "critics"
            )

            self.assertEqual(
                critics_map_cmd[critics_map_cmd.index("--review-input") + 1],
                adapter_cmd[adapter_cmd.index("--output") + 1],
            )
            self.assertLess(commands.index(adapter_cmd), commands.index(critics_map_cmd))

    def test_phase_d_mapping_errors_become_run_level_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")

            def _stub_exec(command: list[str], **_: object) -> None:
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = paths["run_root"] / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text("[]", encoding="utf-8")
                    return
                if any("typographic_precision_review.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text('{"issues":[{"issue_id":"typo_1","description":"bad anchor"}]}', encoding="utf-8")
                    return
                if any("critics_runner.py" in part for part in command):
                    output_path = Path(command[command.index("--output") + 1])
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text('{"critic_a":{"issues":[{"issue_id":"crit_1","description":"bad anchor"}]}}', encoding="utf-8")
                    return
                if any("normalize_critics_runner_output.py" in part for part in command):
                    import subprocess

                    subprocess.run(command, check=True)
                    return
                if any("map_review_to_paragraphs.py" in part for part in command):
                    mapped_output = Path(command[command.index("--output") + 1])
                    mapped_output.parent.mkdir(parents=True, exist_ok=True)
                    mapped_output.write_text(
                        '{"issue_id":"issue_0001","mapping_status":"mapping_error","anchor_type":"none","paragraph_id":null,"reason":"missing_anchor","issue":{"issue_id":"issue_0001"}}\n',
                        encoding="utf-8",
                    )
                    return
                if any("normalize_review_output.py" in part for part in command) or any("aggregate_paragraph_reviews.py" in part for part in command):
                    import subprocess

                    subprocess.run(command, check=True)
                    return
                raise AssertionError(f"Unexpected command: {command}")

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            blockers_payload = paths["review_blockers"].read_text(encoding="utf-8")
            self.assertIn("run_level_blockers", blockers_payload)
            self.assertIn("mapping_error_unresolved", blockers_payload)
            self.assertIn("missing_anchor", blockers_payload)


    def test_phase_d_typography_command_matches_cli_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = self._build_paths(Path(tmp), "tamazight_two_pass")
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
                    import subprocess

                    subprocess.run(command, check=True)
                    return
                if any("map_review_to_paragraphs.py" in part for part in command):
                    mapped_output = Path(command[command.index("--output") + 1])
                    mapped_output.parent.mkdir(parents=True, exist_ok=True)
                    mapped_output.write_text("", encoding="utf-8")
                    return
                if any("normalize_review_output.py" in part for part in command) or any("aggregate_paragraph_reviews.py" in part for part in command):
                    import subprocess

                    subprocess.run(command, check=True)
                    return
                raise AssertionError(f"Unexpected command: {command}")

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            typography_cmd = next(command for command in commands if any("typographic_precision_review.py" in part for part in command))
            self.assertEqual(
                typography_cmd,
                [
                    typography_cmd[0],
                    "scripts/typographic_precision_review.py",
                    "--manuscript",
                    str(paths["final_candidate"]),
                    "--output-dir",
                    str(paths["run_root"] / "review" / "typography"),
                    "--output",
                    str(paths["run_root"] / "review" / "typography" / "typography_review.json"),
                ],
            )


if __name__ == "__main__":
    unittest.main()
