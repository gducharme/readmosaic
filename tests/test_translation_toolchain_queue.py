from __future__ import annotations

import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from scripts.translation_toolchain import (
    atomic_write_jsonl,
    build_rework_queue_packet,
    build_rework_queue_rows,
    read_jsonl,
    _compute_status_report,
    _materialize_preprocessed_from_translation,
    run_phase_b,
    run_phase_c,
    run_phase_c5,
    run_phase_d,
    run_phase_e,
    run_phase_a,
    _ensure_manifest,
    _language_output_dir_name,
    resolve_paragraph_review_state,
    _run_rework_only,
    _is_stale,
    _resolve_pipeline_languages,
    _resolve_subset_paragraph_ids,
    run_rework_translation_stage,
)


class TranslationToolchainQueueTests(unittest.TestCase):

    _REWORK_FIXTURE_ROOT = (
        Path(__file__).parent / "fixtures" / "translation_toolchain" / "rework_only_targeted_loop"
    )
    _REWORK_PARTIAL_BATCH_FIXTURE_ROOT = (
        Path(__file__).parent / "fixtures" / "translation_toolchain" / "rework_only_partial_batch"
    )

    def _write_fixture_file(self, src_name: str, dest_path: Path, *, fixture_root: Path | None = None) -> None:
        root = fixture_root or self._REWORK_FIXTURE_ROOT
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text((root / src_name).read_text(encoding="utf-8"), encoding="utf-8")

    def test_ensure_manifest_raises_clear_error_for_corrupt_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "manifest": root / "manifest.json",
                "run_root": root,
            }
            paths["manifest"].write_text("{not-json", encoding="utf-8")

            with self.assertRaises(ValueError):
                _ensure_manifest(
                    paths,
                    run_id="run_1",
                    pipeline_profile="standard_single_pass",
                    source="book.md",
                    model="gpt-4o",
                    pass1_language="Tamazight",
                    pass2_language=None,
                    exclusion_policy=None,
                )

    def test_phase_a_materializes_exclusion_policy_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "manifest": root / "manifest.json",
                "source_pre": root / "source_pre",
                "state_dir": root / "state",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
            }

            def _stub_exec_phase_command(command, **kwargs):
                del command, kwargs
                paths["source_pre"].mkdir(parents=True, exist_ok=True)
                atomic_write_jsonl(
                    paths["source_pre"] / "paragraphs.jsonl",
                    [
                        {"paragraph_id": "p_0001", "text": "Keep this paragraph"},
                        {"paragraph_id": "p_0002", "text": "SKIP: policy excluded paragraph"},
                    ],
                )

            policy = {
                "metadata": {"policy_version": "2026-02-01"},
                "templates": [{"pattern": "SKIP", "reason": "policy_template_match", "ignore_case": True}],
            }

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec_phase_command):
                run_phase_a(
                    paths,
                    run_id="run_policy_seed",
                    pipeline_profile="tamazight_two_pass",
                    pass1_language="Tamazight",
                    pass2_language="Tifinagh",
                    source="source.md",
                    model="gpt-4o",
                    exclusion_policy=policy,
                    allow_exclusion_policy_reauthorization=False,
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

            rows = {row["paragraph_id"]: row for row in read_jsonl(paths["paragraph_state"])}
            self.assertFalse(rows["p_0001"]["excluded_by_policy"])
            self.assertNotIn("exclude_reason", rows["p_0001"])
            self.assertTrue(rows["p_0002"]["excluded_by_policy"])
            self.assertEqual(rows["p_0002"]["exclude_reason"], "policy_template_match")
            self.assertEqual(rows["p_0002"]["status"], "ingested")

    def test_phase_a_rejects_exclusion_policy_drift_without_reauthorization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "manifest": root / "manifest.json",
                "source_pre": root / "source_pre",
                "state_dir": root / "state",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
            }

            def _stub_exec_phase_command(command, **kwargs):
                del command, kwargs
                paths["source_pre"].mkdir(parents=True, exist_ok=True)
                atomic_write_jsonl(
                    paths["source_pre"] / "paragraphs.jsonl",
                    [{"paragraph_id": "p_0001", "text": "skip me"}],
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec_phase_command):
                run_phase_a(
                    paths,
                    run_id="run_policy_immutable",
                    pipeline_profile="tamazight_two_pass",
                    pass1_language="Tamazight",
                    pass2_language="Tifinagh",
                    source="source.md",
                    model="gpt-4o",
                    exclusion_policy={"templates": [{"pattern": "skip", "reason": "skip_reason", "ignore_case": True}]},
                    allow_exclusion_policy_reauthorization=False,
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

                with self.assertRaises(ValueError):
                    run_phase_a(
                        paths,
                        run_id="run_policy_immutable",
                        pipeline_profile="tamazight_two_pass",
                        pass1_language="Tamazight",
                        pass2_language="Tifinagh",
                        source="source.md",
                        model="gpt-4o",
                        exclusion_policy={"templates": [{"pattern": "different", "reason": "changed", "ignore_case": True}]},
                        allow_exclusion_policy_reauthorization=False,
                        phase_timeout_seconds=0,
                        should_abort=lambda: None,
                    )

    def test_status_and_queue_ignore_excluded_blocker_rows(self) -> None:
        rows = [
            {"paragraph_id": "p_1", "status": "rework_queued", "excluded_by_policy": True, "content_hash": "sha256:" + "a" * 64},
            {"paragraph_id": "p_2", "status": "manual_review_required", "excluded_by_policy": False, "content_hash": "sha256:" + "b" * 64},
            {"paragraph_id": "p_3", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "c" * 64},
        ]

        report = _compute_status_report(rows)
        self.assertEqual(report["required_total"], 2)
        self.assertEqual(report["required_merge_blockers"], 1)

        queue_rows = build_rework_queue_rows(
            [
                {"paragraph_id": "p_1", "status": "ingested", "excluded_by_policy": True, "content_hash": "sha256:" + "a" * 64},
                {"paragraph_id": "p_2", "status": "rework_queued", "excluded_by_policy": False, "content_hash": "sha256:" + "b" * 64},
            ],
            [],
        )
        self.assertEqual([row["paragraph_id"] for row in queue_rows], ["p_2"])

    def test_packet_contains_full_rework_fields(self) -> None:
        row = {
            "paragraph_id": "p_0002",
            "status": "rework_queued",
            "content_hash": "sha256:" + "a" * 64,
            "attempt": 2,
            "failure_history": [{"attempt": 1, "issues": ["critical_grammar"], "timestamp": "2026-01-01T00:00:00Z"}],
            "blocking_issues": ["critical_grammar"],
            "required_fixes": ["fix grammar"],
        }

        packet = build_rework_queue_packet(row)
        self.assertIsNotNone(packet)
        self.assertEqual(packet["paragraph_id"], "p_0002")
        self.assertEqual(packet["failure_reasons"], ["critical_grammar"])
        self.assertEqual(packet["required_fixes"], ["fix grammar"])
        self.assertEqual(packet["attempt"], 2)
        self.assertEqual(packet["content_hash"], row["content_hash"])
        self.assertIsNone(packet["source_text"])
        self.assertIsNone(packet["current_text"])

    def test_packet_skips_non_rework_status(self) -> None:
        self.assertIsNone(build_rework_queue_packet({"paragraph_id": "p_0001", "status": "ready_to_merge"}))

    def test_build_queue_rows_deduplicates_unchanged_existing_rows(self) -> None:
        state_rows = [
            {
                "paragraph_id": "p_0002",
                "status": "rework_queued",
                "content_hash": "sha256:" + "b" * 64,
                "attempt": 1,
                "blocking_issues": ["voice_below_threshold"],
                "failure_history": [],
                "required_fixes": ["raise voice fidelity"],
            },
            {"paragraph_id": "p_0001", "status": "ready_to_merge"},
        ]
        existing = [
            {
                "paragraph_id": "p_0002",
                "content_hash": "sha256:" + "b" * 64,
                "attempt": 1,
                "failure_reasons": ["voice_below_threshold"],
                "failure_history": [],
                "required_fixes": ["raise voice fidelity"],
            },
            {
                "paragraph_id": "p_9999",
                "content_hash": "sha256:" + "c" * 64,
                "attempt": 3,
                "failure_reasons": ["other"],
                "failure_history": [],
                "required_fixes": ["other"],
            },
        ]

        out = build_rework_queue_rows(state_rows, existing)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["paragraph_id"], existing[0]["paragraph_id"])
        self.assertIsNone(out[0]["source_text"])
        self.assertIsNone(out[0]["current_text"])

    def test_atomic_write_jsonl_replaces_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state" / "paragraph_state.jsonl"
            atomic_write_jsonl(path, [{"paragraph_id": "p_1", "status": "ingested"}])
            atomic_write_jsonl(path, [{"paragraph_id": "p_1", "status": "rework_queued"}])
            rows = read_jsonl(path)
            self.assertEqual(rows, [{"paragraph_id": "p_1", "status": "rework_queued"}])



    def test_aggregate_reviews_records_review_failed_before_routing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "paragraph_state.jsonl"
            review_rows_path = root / "review_rows.jsonl"
            scores_out = root / "paragraph_scores.jsonl"
            queue_out = root / "rework_queue.jsonl"

            atomic_write_jsonl(
                state_path,
                [
                    {
                        "paragraph_id": "p_0001",
                        "status": "review_in_progress",
                        "excluded_by_policy": False,
                        "attempt": 1,
                        "content_hash": "sha256:" + "1" * 64,
                        "failure_history": [],
                    }
                ],
            )
            atomic_write_jsonl(
                review_rows_path,
                [
                    {
                        "paragraph_id": "p_0001",
                        "hard_fail": True,
                        "blocking_issues": ["critical_grammar"],
                        "scores": {"semantic": 0.42},
                    }
                ],
            )

            subprocess.run(
                [
                    sys.executable,
                    "scripts/aggregate_paragraph_reviews.py",
                    "--state",
                    str(state_path),
                    "--review-rows",
                    str(review_rows_path),
                    "--scores-out",
                    str(scores_out),
                    "--queue-out",
                    str(queue_out),
                    "--max-attempts",
                    "4",
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
            )

            state_rows = read_jsonl(state_path)
            self.assertEqual(state_rows[0]["status"], "rework_queued")
            self.assertEqual(state_rows[0]["failure_history"][0]["state"], "review_failed")
            self.assertEqual(state_rows[0]["review_state"], "review_failed")
            self.assertEqual(state_rows[0]["routing_state"], "rework_queued")
            self.assertEqual(state_rows[0]["review_transition_trace"], ["review_failed", "rework_queued"])

            score_rows = read_jsonl(scores_out)
            self.assertEqual(score_rows[0]["status"], "rework_queued")
            self.assertEqual(score_rows[0]["review_state"], "review_failed")
            self.assertEqual(score_rows[0]["routing_state"], "rework_queued")
            self.assertEqual(score_rows[0]["transition_trace"], ["review_failed", "rework_queued"])
            self.assertEqual(score_rows[0]["scores"], {"semantic": 0.42})

    def test_packet_raises_for_missing_identity_fields(self) -> None:
        with self.assertRaises(ValueError):
            build_rework_queue_packet({"status": "rework_queued", "content_hash": "sha256:" + "d" * 64})

        with self.assertRaises(ValueError):
            build_rework_queue_packet({"status": "rework_queued", "paragraph_id": "p_01"})

    def test_empty_explicit_failure_fields_fallback_to_unspecified(self) -> None:
        packet = build_rework_queue_packet(
            {
                "paragraph_id": "p_003",
                "status": "rework_queued",
                "content_hash": "sha256:" + "e" * 64,
                "blocking_issues": ["critical_grammar"],
                "failure_reasons": [],
                "required_fixes": [],
            }
        )
        assert packet is not None
        self.assertEqual(packet["failure_reasons"], ["unspecified_failure"])
        self.assertEqual(packet["required_fixes"], ["unspecified_failure"])


    def test_packet_raises_for_non_list_failure_fields(self) -> None:
        with self.assertRaises(ValueError):
            build_rework_queue_packet(
                {
                    "paragraph_id": "p_010",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "5" * 64,
                    "failure_reasons": "critical_grammar",
                }
            )

        with self.assertRaises(ValueError):
            build_rework_queue_packet(
                {
                    "paragraph_id": "p_011",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "6" * 64,
                    "required_fixes": "fix grammar",
                }
            )

        with self.assertRaises(ValueError):
            build_rework_queue_packet(
                {
                    "paragraph_id": "p_012",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "7" * 64,
                    "failure_history": {"attempt": 1},
                }
            )


    def test_status_report_treats_string_false_excluded_as_not_excluded(self) -> None:
        report = _compute_status_report(
            [
                {"paragraph_id": "p_1", "status": "ready_to_merge", "excluded_by_policy": "false"},
                {"paragraph_id": "p_2", "status": "manual_review_required", "excluded_by_policy": False},
                {"paragraph_id": "p_3", "status": "ready_to_merge", "excluded_by_policy": True},
            ]
        )

        self.assertEqual(report["required_total"], 2)
        self.assertEqual(report["done"], 1)
        self.assertEqual(report["required_merge_blockers"], 1)


    def test_packet_raises_for_null_list_field(self) -> None:
        with self.assertRaises(ValueError):
            build_rework_queue_packet(
                {
                    "paragraph_id": "p_777",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "8" * 64,
                    "failure_reasons": None,
                }
            )

    def test_materialize_raises_for_non_object_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            (source_pre / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1","text":"a"}\n', encoding="utf-8")

            translation_json = root / "translation.json"
            translation_json.write_text(json.dumps(["bad"]), encoding="utf-8")

            with self.assertRaises(ValueError):
                _materialize_preprocessed_from_translation(source_pre, translation_json, root / "out")

    def test_materialize_raises_when_records_empty_and_no_paragraph_translations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            (source_pre / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1","text":"a"}\n', encoding="utf-8")

            translation_json = root / "translation.json"
            translation_json.write_text(json.dumps({"records": []}), encoding="utf-8")

            with self.assertRaises(ValueError):
                _materialize_preprocessed_from_translation(source_pre, translation_json, root / "out")


    def test_materialize_raises_on_duplicate_record_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            (source_pre / "paragraphs.jsonl").write_text(
                "\n".join([
                    '{"paragraph_id":"p_1","text":"a"}',
                    '{"paragraph_id":"p_2","text":"b"}',
                ]) + "\n",
                encoding="utf-8",
            )

            translation_json = root / "translation.json"
            translation_json.write_text(
                json.dumps(
                    {
                        "records": [
                            {"paragraph_index": 0, "translation": "A"},
                            {"paragraph_index": 0, "translation": "A2"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                _materialize_preprocessed_from_translation(source_pre, translation_json, root / "out")

    def test_phase_c_copies_pass1_when_no_pass2_language(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pass1_pre = root / "pass1_pre"
            pass1_pre.mkdir(parents=True, exist_ok=True)
            (pass1_pre / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1","text":"a"}\n', encoding="utf-8")
            (pass1_pre / "sentences.jsonl").write_text('{"sentence_id":"s_1"}\n', encoding="utf-8")

            paths = {
                "run_root": root,
                "pass1_pre": pass1_pre,
                "pass2_pre": root / "pass2_pre",
            }
            run_phase_c(
                paths,
                pass2_language=None,
                model="dummy",
                phase_timeout_seconds=0,
                should_abort=lambda: None,
            )

            self.assertTrue((paths["pass2_pre"] / "paragraphs.jsonl").exists())
            self.assertTrue((paths["pass2_pre"] / "sentences.jsonl").exists())

    def test_phase_b_marks_non_excluded_rows_translated_pass1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [
                    {
                        "paragraph_id": "p_1",
                        "status": "ingested",
                        "attempt": 0,
                        "excluded_by_policy": False,
                        "failure_history": [],
                        "content_hash": "sha256:" + "a" * 64,
                    },
                    {
                        "paragraph_id": "p_2",
                        "status": "ingested",
                        "attempt": 0,
                        "excluded_by_policy": True,
                        "exclude_reason": "policy",
                        "failure_history": [],
                        "content_hash": "sha256:" + "b" * 64,
                    },
                ],
            )
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            (source_pre / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1","paragraph_index":1,"text":"a"}\n', encoding="utf-8")
            paths = {
                "run_root": root,
                "source_pre": source_pre,
                "pass1_pre": root / "pass1_pre",
                "paragraph_state": state_path,
            }

            with patch("scripts.translation_toolchain._exec_phase_command", return_value=None), patch(
                "scripts.translation_toolchain._materialize_preprocessed_from_translation", return_value=None
            ):
                run_phase_b(
                    paths,
                    pass1_language="French",
                    model="dummy",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

            rows = read_jsonl(state_path)
            self.assertEqual(rows[0]["status"], "translated_pass1")
            self.assertEqual(rows[1]["status"], "ingested")

    def test_phase_c_sets_pass2_status_or_preserves_pass1_in_single_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [
                    {
                        "paragraph_id": "p_1",
                        "status": "translated_pass1",
                        "attempt": 0,
                        "excluded_by_policy": False,
                        "failure_history": [],
                        "content_hash": "sha256:" + "c" * 64,
                    }
                ],
            )
            pass1_pre = root / "pass1_pre"
            pass1_pre.mkdir(parents=True, exist_ok=True)
            (pass1_pre / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1","text":"a"}\n', encoding="utf-8")

            paths = {
                "run_root": root,
                "pass1_pre": pass1_pre,
                "pass2_pre": root / "pass2_pre",
                "paragraph_state": state_path,
            }

            with patch("scripts.translation_toolchain._exec_phase_command", return_value=None), patch(
                "scripts.translation_toolchain._materialize_preprocessed_from_translation", return_value=None
            ):
                run_phase_c(paths, pass2_language="Tifinagh", model="dummy", phase_timeout_seconds=0, should_abort=lambda: None)
            self.assertEqual(read_jsonl(state_path)[0]["status"], "translated_pass2")

            atomic_write_jsonl(state_path, [{**read_jsonl(state_path)[0], "status": "translated_pass1"}])
            run_phase_c(paths, pass2_language=None, model="dummy", phase_timeout_seconds=0, should_abort=lambda: None)
            self.assertEqual(read_jsonl(state_path)[0]["status"], "translated_pass1")


    def test_phase_b_subset_translation_merges_only_selected_paragraphs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                source_pre / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "source one", "content_hash": "sha256:" + "1" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "source two", "content_hash": "sha256:" + "2" * 64},
                ],
            )
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [
                    {"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "1" * 64},
                    {"paragraph_id": "p_2", "status": "ingested", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "2" * 64},
                ],
            )
            paths = {
                "run_root": root,
                "source_pre": source_pre,
                "pass1_pre": root / "pass1_pre",
                "paragraph_state": state_path,
            }

            def _stub_exec(cmd, **kwargs):
                preprocessed_arg = cmd[cmd.index("--preprocessed") + 1]
                self.assertNotEqual(preprocessed_arg, str(source_pre))

            def _stub_materialize(source_path: Path, _translation_json: Path, out_path: Path):
                rows = read_jsonl(source_path / "paragraphs.jsonl")
                updated = []
                for row in rows:
                    next_row = dict(row)
                    next_row["text"] = "translated " + str(row.get("paragraph_id"))
                    updated.append(next_row)
                atomic_write_jsonl(out_path / "paragraphs.jsonl", updated)

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec), patch(
                "scripts.translation_toolchain._materialize_preprocessed_from_translation", side_effect=_stub_materialize
            ):
                run_phase_b(
                    paths,
                    pass1_language="French",
                    model="dummy",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                    subset_paragraph_ids={"p_2"},
                )

            merged = read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl")
            self.assertEqual([row["paragraph_id"] for row in merged], ["p_1", "p_2"])
            self.assertEqual(merged[0]["text"], "source one")
            self.assertEqual(merged[1]["text"], "translated p_2")
            self.assertEqual(merged[1]["paragraph_index"], 2)
            self.assertEqual(merged[1]["content_hash"], "sha256:" + "2" * 64)

            state_rows = {row["paragraph_id"]: row for row in read_jsonl(state_path)}
            self.assertEqual(state_rows["p_1"]["status"], "ingested")
            self.assertEqual(state_rows["p_2"]["status"], "translated_pass1")

    def test_phase_b_subset_from_empty_queue_noops(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                source_pre / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": 1, "text": "source", "content_hash": "sha256:" + "1" * 64}],
            )
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "1" * 64}],
            )
            paths = {
                "run_root": root,
                "source_pre": source_pre,
                "pass1_pre": root / "pass1_pre",
                "paragraph_state": state_path,
            }

            with patch("scripts.translation_toolchain._exec_phase_command") as exec_mock:
                run_phase_b(
                    paths,
                    pass1_language="French",
                    model="dummy",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                    subset_paragraph_ids=set(),
                )
            exec_mock.assert_not_called()
            self.assertFalse((paths["pass1_pre"] / "paragraphs.jsonl").exists())
            self.assertEqual(read_jsonl(state_path)[0]["status"], "ingested")

    def test_phase_c5_marks_candidate_assembled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "translated_pass2", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "d" * 64}],
            )
            paths = {
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": state_path,
            }
            with patch("scripts.translation_toolchain.assemble_candidate", return_value=None):
                run_phase_c5(paths)
            self.assertEqual(read_jsonl(state_path)[0]["status"], "candidate_assembled")


    def test_phase_c5_raises_for_missing_status_in_paragraph_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "d" * 64}],
            )
            paths = {
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": state_path,
            }
            with patch("scripts.translation_toolchain.assemble_candidate", return_value=None):
                with self.assertRaises(ValueError):
                    run_phase_c5(paths)

    def test_phase_d_marks_review_in_progress_before_aggregation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "candidate_assembled", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "e" * 64}],
            )
            candidate_map = root / "final" / "candidate_map.jsonl"
            candidate_map.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_1", "paragraph_index": 1, "start_line": 1, "end_line": 1}])
            final_candidate = root / "final" / "candidate.md"
            final_candidate.write_text("text", encoding="utf-8")
            paths = {
                "paragraph_state": state_path,
                "final_candidate": final_candidate,
                "candidate_map": candidate_map,
                "run_root": root,
                "manifest": root / "manifest.json",
                "review_normalized": root / "review_normalized",
                "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
                "review_blockers": root / "gate" / "review_blockers.json",
            }
            paths["manifest"].write_text('{"pipeline_profile":"tamazight_two_pass","model":"stub-model"}', encoding="utf-8")
            paths["pass2_pre"] = root / "pass2_pre"
            paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
            (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")

            def _stub_exec(command, **kwargs):
                del kwargs
                self.assertEqual(read_jsonl(state_path)[0]["status"], "review_in_progress")
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = root / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text('[{"paragraph_id":"p_1","scores":{},"issues":[],"blocking_issues":[],"hard_fail":false}]', encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)


    def test_phase_d_transitions_rows_out_of_review_in_progress_when_reviews_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "candidate_assembled", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "e" * 64}],
            )
            candidate_map = root / "final" / "candidate_map.jsonl"
            candidate_map.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_1", "paragraph_index": 1, "start_line": 1, "end_line": 1}])
            final_candidate = root / "final" / "candidate.md"
            final_candidate.write_text("text", encoding="utf-8")
            paths = {
                "paragraph_state": state_path,
                "final_candidate": final_candidate,
                "candidate_map": candidate_map,
                "run_root": root,
                "manifest": root / "manifest.json",
                "review_normalized": root / "review_normalized",
                "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
                "review_blockers": root / "gate" / "review_blockers.json",
            }
            paths["manifest"].write_text('{"pipeline_profile":"tamazight_two_pass","model":"stub-model"}', encoding="utf-8")
            paths["pass2_pre"] = root / "pass2_pre"
            paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
            (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")
            (paths["pass2_pre"] / "sentences.jsonl").write_text('{"id":"s_1","order":0,"text":"Sentence one."}\n', encoding="utf-8")

            def _stub_exec(command: list[str], **_: object) -> None:
                if "scripts/grammar_auditor.py" in command:
                    out_dir = Path(command[command.index("--output-dir") + 1])
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text(
                        json.dumps(
                            [
                                {
                                    "paragraph_id": "p_1",
                                    "scores": {"grammar": 0.99},
                                    "issues": [],
                                    "blocking_issues": [],
                                    "hard_fail": False,
                                }
                            ]
                        ),
                        encoding="utf-8",
                    )
                    return
                if any("typographic_precision_review.py" in part for part in command):
                    return
                if any("critics_runner.py" in part for part in command):
                    return
                if any("map_review_to_paragraphs.py" in part for part in command):
                    mapped_output = Path(command[command.index("--output") + 1])
                    mapped_output.parent.mkdir(parents=True, exist_ok=True)
                    mapped_output.write_text("", encoding="utf-8")
                    return
                if any("normalize_review_output.py" in part for part in command) or any("aggregate_paragraph_reviews.py" in part for part in command):
                    subprocess.run(command, check=True)
                    return
                raise AssertionError(f"Unexpected command: {command}")

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            rows = read_jsonl(state_path)
            self.assertNotEqual(rows[0]["status"], "review_in_progress")
            review_rows = read_jsonl(paths["review_normalized"] / "all_reviews.jsonl")
            self.assertEqual(len(review_rows), 1)
            self.assertEqual(review_rows[0]["paragraph_id"], "p_1")

    def test_phase_d_only_marks_candidate_map_rows_review_in_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [
                    {"paragraph_id": "p_1", "status": "candidate_assembled", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "1" * 64},
                    {"paragraph_id": "p_2", "status": "candidate_assembled", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "2" * 64},
                ],
            )
            candidate_map = root / "final" / "candidate_map.jsonl"
            candidate_map.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_1", "paragraph_index": 1, "start_line": 1, "end_line": 1}])
            final_candidate = root / "final" / "candidate.md"
            final_candidate.write_text("text", encoding="utf-8")
            paths = {
                "paragraph_state": state_path,
                "final_candidate": final_candidate,
                "candidate_map": candidate_map,
                "run_root": root,
                "manifest": root / "manifest.json",
                "review_normalized": root / "review_normalized",
                "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
                "review_blockers": root / "gate" / "review_blockers.json",
            }
            paths["manifest"].write_text('{"pipeline_profile":"tamazight_two_pass","model":"stub-model"}', encoding="utf-8")
            paths["pass2_pre"] = root / "pass2_pre"
            paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
            (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")

            def _stub_exec(command, **kwargs):
                del kwargs
                if any("grammar_auditor.py" in part for part in command):
                    out_dir = root / "review" / "grammar"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "grammar_audit_issues_20260101T000000Z.json").write_text('[{"paragraph_id":"p_1","scores":{},"issues":[],"blocking_issues":[],"hard_fail":false}]', encoding="utf-8")
                if any("aggregate_paragraph_reviews.py" in part for part in command):
                    atomic_write_jsonl(paths["paragraph_scores"], [])
                    atomic_write_jsonl(paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

            rows = {row["paragraph_id"]: row for row in read_jsonl(state_path)}
            self.assertEqual(rows["p_1"]["status"], "review_in_progress")
            self.assertEqual(rows["p_2"]["status"], "candidate_assembled")


    def test_phase_d_rejects_missing_paragraph_id_for_targeted_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"status": "candidate_assembled", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "1" * 64}],
            )
            candidate_map = root / "final" / "candidate_map.jsonl"
            candidate_map.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_1", "paragraph_index": 1, "start_line": 1, "end_line": 1}])
            final_candidate = root / "final" / "candidate.md"
            final_candidate.write_text("text", encoding="utf-8")
            paths = {
                "paragraph_state": state_path,
                "final_candidate": final_candidate,
                "candidate_map": candidate_map,
                "run_root": root,
                "manifest": root / "manifest.json",
                "review_normalized": root / "review_normalized",
                "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
                "review_blockers": root / "gate" / "review_blockers.json",
            }
            paths["manifest"].write_text('{"pipeline_profile":"tamazight_two_pass","model":"stub-model"}', encoding="utf-8")
            paths["pass2_pre"] = root / "pass2_pre"
            paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
            (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")

            with self.assertRaises(ValueError):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

    def test_phase_d_rejects_candidate_map_ids_missing_in_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "candidate_assembled", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "1" * 64}],
            )
            candidate_map = root / "final" / "candidate_map.jsonl"
            candidate_map.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(candidate_map, [{"paragraph_id": "p_9999", "paragraph_index": 1, "start_line": 1, "end_line": 1}])
            final_candidate = root / "final" / "candidate.md"
            final_candidate.write_text("text", encoding="utf-8")
            paths = {
                "paragraph_state": state_path,
                "final_candidate": final_candidate,
                "candidate_map": candidate_map,
                "run_root": root,
                "manifest": root / "manifest.json",
                "review_normalized": root / "review_normalized",
                "paragraph_scores": root / "state" / "paragraph_scores.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
                "review_blockers": root / "gate" / "review_blockers.json",
            }
            paths["manifest"].write_text('{"pipeline_profile":"tamazight_two_pass","model":"stub-model"}', encoding="utf-8")
            paths["pass2_pre"] = root / "pass2_pre"
            paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
            (paths["pass2_pre"] / "paragraphs.jsonl").write_text('{"paragraph_id":"p_1"}\n', encoding="utf-8")

            with self.assertRaises(ValueError):
                run_phase_d(paths, run_id="tx_001", max_paragraph_attempts=4, phase_timeout_seconds=0, should_abort=lambda: None)

    def test_phase_c5_rerun_does_not_backslide_review_in_progress_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "review_in_progress", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "d" * 64}],
            )
            paths = {
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": state_path,
            }
            with patch("scripts.translation_toolchain.assemble_candidate", return_value=None):
                run_phase_c5(paths)
            self.assertEqual(read_jsonl(state_path)[0]["status"], "review_in_progress")

    def test_phase_e_applies_single_batch_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [
                    {"paragraph_id": "p_1", "status": "rework_queued", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "f" * 64},
                    {"paragraph_id": "p_2", "status": "rework_queued", "attempt": 1, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "e" * 64},
                ],
            )
            paths = {
                "paragraph_state": state_path,
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }

            run_phase_e(paths, max_paragraph_attempts=4, bump_attempts=True, should_abort=lambda: None)
            rows = read_jsonl(state_path)
            self.assertEqual(rows[0]["updated_at"], rows[1]["updated_at"])

    def test_phase_e_marks_reworked_for_rows_leaving_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_jsonl(
                state_path,
                [{"paragraph_id": "p_1", "status": "rework_queued", "attempt": 0, "excluded_by_policy": False, "failure_history": [], "content_hash": "sha256:" + "f" * 64}],
            )
            paths = {
                "paragraph_state": state_path,
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }

            run_phase_e(paths, max_paragraph_attempts=4, bump_attempts=True, should_abort=lambda: None)
            self.assertEqual(read_jsonl(state_path)[0]["status"], "reworked")




    def test_resolve_paragraph_review_state_exposes_review_trace_fields(self) -> None:
        result = resolve_paragraph_review_state(
            {"paragraph_id": "p_1", "status": "review_in_progress", "attempt": 0, "excluded_by_policy": False, "failure_history": []},
            {"blocking_issues": ["critical_grammar"], "scores": {"semantic": 0.5}, "hard_fail": True},
            max_attempts=4,
        )
        self.assertEqual(result["status"], "rework_queued")
        self.assertEqual(result["review_state"], "review_failed")
        self.assertEqual(result["routing_state"], "rework_queued")
        self.assertEqual(result["review_transition_trace"], ["review_failed", "rework_queued"])

    def test_resolve_paragraph_review_state_rejects_non_list_blocking_issues(self) -> None:
        with self.assertRaises(ValueError):
            resolve_paragraph_review_state(
                {"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False},
                {"blocking_issues": "not-a-list", "scores": {}, "hard_fail": False},
                max_attempts=4,
            )

    def test_resolve_paragraph_review_state_rejects_non_string_blocking_issue_item(self) -> None:
        with self.assertRaises(ValueError):
            resolve_paragraph_review_state(
                {"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False},
                {"blocking_issues": ["ok", 123], "scores": {}, "hard_fail": False},
                max_attempts=4,
            )

    def test_resolve_paragraph_review_state_rejects_non_dict_scores(self) -> None:
        with self.assertRaises(ValueError):
            resolve_paragraph_review_state(
                {"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False},
                {"blocking_issues": [], "scores": [0.9], "hard_fail": False},
                max_attempts=4,
            )

    def test_resolve_paragraph_review_state_rejects_non_numeric_score_values(self) -> None:
        with self.assertRaises(ValueError):
            resolve_paragraph_review_state(
                {"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False},
                {"blocking_issues": [], "scores": {"semantic": "high"}, "hard_fail": False},
                max_attempts=4,
            )

    def test_resolve_paragraph_review_state_rejects_boolean_score_values(self) -> None:
        with self.assertRaises(ValueError):
            resolve_paragraph_review_state(
                {"paragraph_id": "p_1", "status": "ingested", "attempt": 0, "excluded_by_policy": False},
                {"blocking_issues": [], "scores": {"semantic": True}, "hard_fail": False},
                max_attempts=4,
            )

    def test_language_output_dir_slug_is_safe_for_arbitrary_input(self) -> None:
        self.assertEqual(_language_output_dir_name("French"), "french")
        self.assertEqual(_language_output_dir_name("Русский язык"), "русский_язык")
        self.assertEqual(_language_output_dir_name("Arabic/RTL"), "arabic_rtl")

    def test_queue_rows_are_sorted_by_paragraph_id(self) -> None:
        out = build_rework_queue_rows(
            [
                {
                    "paragraph_id": "p_0009",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "f" * 64,
                },
                {
                    "paragraph_id": "p_0002",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "1" * 64,
                },
            ]
        )
        self.assertEqual([row["paragraph_id"] for row in out], ["p_0002", "p_0009"])


    def test_build_queue_rows_projects_source_and_current_text(self) -> None:
        out = build_rework_queue_rows(
            [
                {
                    "paragraph_id": "p_0002",
                    "status": "rework_queued",
                    "content_hash": "sha256:" + "1" * 64,
                    "attempt": 1,
                    "blocking_issues": ["critical_grammar"],
                }
            ],
            source_lookup_by_id={"p_0002": {"paragraph_id": "p_0002", "text": "source text"}},
            current_lookup_by_id={"p_0002": {"paragraph_id": "p_0002", "text": "current text"}},
        )
        self.assertEqual(out[0]["source_text"], "source text")
        self.assertEqual(out[0]["current_text"], "current text")

    def test_read_jsonl_soft_skips_invalid_rows_when_non_strict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rework_queue.jsonl"
            path.write_text('\n'.join([
                '{"paragraph_id":"p_1"}',
                'not-json',
                '[]',
                '{"paragraph_id":"p_2"}',
                '',
            ]), encoding="utf-8")
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rows = read_jsonl(path, strict=False)
            self.assertEqual(rows, [{"paragraph_id": "p_1"}, {"paragraph_id": "p_2"}])
            self.assertIn("row skipped", stderr.getvalue())

    def test_read_jsonl_strict_raises_for_invalid_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "paragraph_state.jsonl"
            path.write_text('\n'.join([
                '{"paragraph_id":"p_1"}',
                'not-json',
                '',
            ]), encoding="utf-8")
            with self.assertRaises(ValueError):
                read_jsonl(path)


    def test_read_jsonl_empty_file_returns_no_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.jsonl"
            path.write_text("", encoding="utf-8")
            self.assertEqual(read_jsonl(path), [])

    def test_duplicate_existing_rows_reuse_matching_candidate(self) -> None:
        state_rows = [
            {
                "paragraph_id": "p_0002",
                "status": "rework_queued",
                "content_hash": "sha256:" + "2" * 64,
                "attempt": 3,
                "failure_reasons": ["voice_below_threshold"],
                "failure_history": [],
                "required_fixes": ["raise voice fidelity"],
            }
        ]
        existing = [
            {
                "paragraph_id": "p_0002",
                "content_hash": "sha256:" + "2" * 64,
                "attempt": 1,
                "failure_reasons": ["old_reason"],
                "failure_history": [],
                "required_fixes": ["old_fix"],
            },
            {
                "paragraph_id": "p_0002",
                "content_hash": "sha256:" + "2" * 64,
                "attempt": 3,
                "failure_reasons": ["voice_below_threshold"],
                "failure_history": [],
                "required_fixes": ["raise voice fidelity"],
                "source_text": None,
                "current_text": None,
            },
        ]

        out = build_rework_queue_rows(state_rows, existing)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0], existing[1])

    def test_duplicate_existing_rows_do_not_override_changed_state_packet(self) -> None:
        state_rows = [
            {
                "paragraph_id": "p_0002",
                "status": "rework_queued",
                "content_hash": "sha256:" + "3" * 64,
                "attempt": 4,
                "failure_reasons": ["critical_grammar"],
                "failure_history": [],
                "required_fixes": ["fix grammar"],
            }
        ]
        existing = [
            {
                "paragraph_id": "p_0002",
                "content_hash": "sha256:" + "3" * 64,
                "attempt": 1,
                "failure_reasons": ["old_reason"],
                "failure_history": [],
                "required_fixes": ["old_fix"],
            },
            {
                "paragraph_id": "p_0002",
                "content_hash": "sha256:" + "3" * 64,
                "attempt": 2,
                "failure_reasons": ["older_reason"],
                "failure_history": [],
                "required_fixes": ["older_fix"],
            },
        ]

        out = build_rework_queue_rows(state_rows, existing)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["attempt"], 4)
        self.assertEqual(out[0]["failure_reasons"], ["critical_grammar"])

    def test_invalid_existing_rows_are_not_reused(self) -> None:
        state_rows = [
            {
                "paragraph_id": "p_0002",
                "status": "rework_queued",
                "content_hash": "sha256:" + "4" * 64,
                "attempt": 2,
                "blocking_issues": ["issue"],
            }
        ]
        existing = [
            {"paragraph_id": "p_0002", "attempt": 1},
            {"content_hash": "sha256:" + "4" * 64},
        ]

        out = build_rework_queue_rows(state_rows, existing)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["attempt"], 2)



    def test_resolve_subset_from_queue_empty_returns_empty_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {"rework_queue": root / "state" / "rework_queue.jsonl"}
            atomic_write_jsonl(paths["rework_queue"], [])
            subset_ids = _resolve_subset_paragraph_ids(paths, subset_from_queue=True, subset_paragraph_ids_raw=None)
            self.assertEqual(subset_ids, set())

    def test_run_rework_only_executes_phase_r_d_e(self) -> None:
        phase_calls: list[str] = []

        def run_phase(name: str, runner):
            phase_calls.append(name)

        from argparse import Namespace

        args = Namespace(
            run_id="run_1",
            max_paragraph_attempts=4,
            phase_timeout_seconds=0,
            no_bump_attempts=False,
            pass1_language="Tamazight",
            pass2_language="Tifinagh",
            model="gpt-4o-mini",
            rework_run_phase_f=False,
        )
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(json.dumps({"pass1_language": "Tamazight", "pass2_language": "Tifinagh", "model": "gpt-4o-mini"}), encoding="utf-8")
            with patch("scripts.translation_toolchain.run_rework_translation_stage", return_value=set()):
                _run_rework_only(paths={"manifest": manifest}, args=args, run_phase=run_phase, should_abort=lambda: None)
        self.assertEqual(phase_calls, ["R", "D", "E"])

    def test_run_rework_only_optionally_runs_phase_f(self) -> None:
        phase_calls: list[str] = []

        def run_phase(name: str, runner):
            phase_calls.append(name)

        from argparse import Namespace

        args = Namespace(
            run_id="run_1",
            max_paragraph_attempts=4,
            phase_timeout_seconds=0,
            no_bump_attempts=False,
            pass1_language="Tamazight",
            pass2_language="Tifinagh",
            model="gpt-4o-mini",
            rework_run_phase_f=True,
        )
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(json.dumps({"pass1_language": "Tamazight", "pass2_language": "Tifinagh", "model": "gpt-4o-mini"}), encoding="utf-8")
            with patch("scripts.translation_toolchain.run_rework_translation_stage", return_value=set()):
                _run_rework_only(paths={"manifest": manifest}, args=args, run_phase=run_phase, should_abort=lambda: None)
        self.assertEqual(phase_calls, ["R", "D", "E", "F"])

    def test_rework_translation_stage_rewrites_only_queued_paragraphs_from_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass2_pre_before.jsonl", paths["pass2_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            self._write_fixture_file("rework_queue.jsonl", paths["rework_queue"])

            def _stub_exec(command, **kwargs):
                preprocessed = Path(command[command.index("--preprocessed") + 1])
                output_root = Path(command[command.index("--output-root") + 1])
                language = command[command.index("--language") + 1]
                paragraphs = read_jsonl(preprocessed / "paragraphs.jsonl")
                if language == "Tamazight":
                    out_rows = [f"RW1:{row['text']}" for row in paragraphs]
                else:
                    out_rows = [f"RW2:{row['text']}" for row in paragraphs]
                output_path = output_root / _language_output_dir_name(language)
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "translation.json").write_text(
                    json.dumps({"paragraph_translations": out_rows}),
                    encoding="utf-8",
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                queued_ids = run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language="Tifinagh",
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

            self.assertEqual(queued_ids, {"p_0002"})
            self.assertEqual(read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl"), read_jsonl(self._REWORK_FIXTURE_ROOT / "expected_pass1_after.jsonl"))
            self.assertEqual(read_jsonl(paths["pass2_pre"] / "paragraphs.jsonl"), read_jsonl(self._REWORK_FIXTURE_ROOT / "expected_pass2_after.jsonl"))
            self.assertEqual(read_jsonl(paths["candidate_map"]), read_jsonl(self._REWORK_FIXTURE_ROOT / "expected_candidate_map.jsonl"))
            self.assertEqual(paths["final_candidate"].read_text(encoding="utf-8"), (self._REWORK_FIXTURE_ROOT / "expected_candidate.md").read_text(encoding="utf-8"))

            actual_state = read_jsonl(paths["paragraph_state"])
            expected_state = read_jsonl(self._REWORK_FIXTURE_ROOT / "expected_state_after_stage.jsonl")
            self.assertEqual(len(actual_state), len(expected_state))
            self.assertEqual(actual_state[0]["paragraph_id"], expected_state[0]["paragraph_id"])
            self.assertEqual(actual_state[0]["status"], expected_state[0]["status"])
            self.assertEqual(actual_state[0]["attempt"], expected_state[0]["attempt"])
            self.assertEqual(actual_state[1]["paragraph_id"], "p_0002")
            self.assertEqual(actual_state[1]["status"], "reworked")
            self.assertIsInstance(actual_state[1].get("updated_at"), str)

    def test_rework_translation_stage_single_pass_uses_pass1_for_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            self._write_fixture_file("rework_queue.jsonl", paths["rework_queue"])

            def _stub_exec(command, **kwargs):
                preprocessed = Path(command[command.index("--preprocessed") + 1])
                output_root = Path(command[command.index("--output-root") + 1])
                language = command[command.index("--language") + 1]
                paragraphs = read_jsonl(preprocessed / "paragraphs.jsonl")
                out_rows = [f"RW1:{row['text']}" for row in paragraphs]
                output_path = output_root / _language_output_dir_name(language)
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "translation.json").write_text(
                    json.dumps({"paragraph_translations": out_rows}),
                    encoding="utf-8",
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language=None,
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

            self.assertIn("RW1:beta source", paths["final_candidate"].read_text(encoding="utf-8"))
            self.assertFalse((paths["pass2_pre"] / "paragraphs.jsonl").exists())

    def test_rework_translation_stage_honors_partial_batch_progression(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file(
                "source_pre_paragraphs.jsonl",
                paths["source_pre"] / "paragraphs.jsonl",
                fixture_root=self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT,
            )
            self._write_fixture_file(
                "pass1_pre_before.jsonl",
                paths["pass1_pre"] / "paragraphs.jsonl",
                fixture_root=self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT,
            )
            self._write_fixture_file(
                "pass2_pre_before.jsonl",
                paths["pass2_pre"] / "paragraphs.jsonl",
                fixture_root=self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT,
            )
            self._write_fixture_file(
                "paragraph_state_before.jsonl",
                paths["paragraph_state"],
                fixture_root=self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT,
            )
            self._write_fixture_file(
                "rework_queue.jsonl",
                paths["rework_queue"],
                fixture_root=self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT,
            )

            def _stub_exec(command, **kwargs):
                preprocessed = Path(command[command.index("--preprocessed") + 1])
                output_root = Path(command[command.index("--output-root") + 1])
                language = command[command.index("--language") + 1]
                paragraphs = read_jsonl(preprocessed / "paragraphs.jsonl")
                if language == "Tamazight":
                    out_rows = [f"RW1:{row['text']}" for row in paragraphs]
                else:
                    out_rows = [f"RW2:{row['text']}" for row in paragraphs]
                output_path = output_root / _language_output_dir_name(language)
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "translation.json").write_text(
                    json.dumps({"paragraph_translations": out_rows}),
                    encoding="utf-8",
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                queued_ids = run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language="Tifinagh",
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                    rework_batch_size=1,
                    rework_max_batches=2,
                )

            self.assertEqual(queued_ids, {"p_0001", "p_0002"})
            self.assertEqual(
                read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl"),
                read_jsonl(self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT / "expected_pass1_after_partial.jsonl"),
            )
            self.assertEqual(
                read_jsonl(paths["pass2_pre"] / "paragraphs.jsonl"),
                read_jsonl(self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT / "expected_pass2_after_partial.jsonl"),
            )
            self.assertEqual(
                read_jsonl(paths["rework_queue"]),
                read_jsonl(self._REWORK_PARTIAL_BATCH_FIXTURE_ROOT / "expected_queue_after_partial.jsonl"),
            )

            state_by_id = {row["paragraph_id"]: row for row in read_jsonl(paths["paragraph_state"])}
            self.assertEqual(state_by_id["p_0001"]["status"], "reworked")
            self.assertEqual(state_by_id["p_0002"]["status"], "reworked")
            self.assertEqual(state_by_id["p_0003"]["status"], "rework_queued")

    def test_rework_translation_stage_single_pass_does_not_mutate_existing_pass2(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass2_pre_before.jsonl", paths["pass2_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            self._write_fixture_file("rework_queue.jsonl", paths["rework_queue"])

            before_pass2 = read_jsonl(paths["pass2_pre"] / "paragraphs.jsonl")

            def _stub_exec(command, **kwargs):
                preprocessed = Path(command[command.index("--preprocessed") + 1])
                output_root = Path(command[command.index("--output-root") + 1])
                language = command[command.index("--language") + 1]
                paragraphs = read_jsonl(preprocessed / "paragraphs.jsonl")
                out_rows = [f"RW1:{row['text']}" for row in paragraphs]
                output_path = output_root / _language_output_dir_name(language)
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "translation.json").write_text(
                    json.dumps({"paragraph_translations": out_rows}),
                    encoding="utf-8",
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language=None,
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

            self.assertEqual(read_jsonl(paths["pass2_pre"] / "paragraphs.jsonl"), before_pass2)

    def test_build_merged_rows_rejects_invalid_paragraph_id(self) -> None:
        from scripts.translation_toolchain import _build_merged_paragraph_rows

        with self.assertRaises(ValueError):
            _build_merged_paragraph_rows(
                [{"paragraph_id": None, "text": "bad"}],
                {"p_1": {"paragraph_id": "p_1", "text": "ok"}},
            )

    def test_rework_translation_stage_fails_for_content_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            atomic_write_jsonl(
                paths["rework_queue"],
                [{"paragraph_id": "p_0002", "content_hash": "sha256:" + "9" * 64, "attempt": 1, "failure_reasons": [], "failure_history": [], "required_fixes": []}],
            )

            with self.assertRaises(ValueError):
                run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language=None,
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

    def test_rework_translation_stage_pass2_failure_keeps_canonical_unmodified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass2_pre_before.jsonl", paths["pass2_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            self._write_fixture_file("rework_queue.jsonl", paths["rework_queue"])

            before_pass1 = read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl")
            before_pass2 = read_jsonl(paths["pass2_pre"] / "paragraphs.jsonl")

            def _stub_exec(command, **kwargs):
                output_root = Path(command[command.index("--output-root") + 1])
                language = command[command.index("--language") + 1]
                if language == "Tifinagh":
                    raise RuntimeError("pass2 failed")
                output_path = output_root / _language_output_dir_name(language)
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "translation.json").write_text(
                    json.dumps({"paragraph_translations": ["RW1:beta source"]}),
                    encoding="utf-8",
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                with self.assertRaises(RuntimeError):
                    run_rework_translation_stage(
                        paths,
                        pass1_language="Tamazight",
                        pass2_language="Tifinagh",
                        model="gpt-4o-mini",
                        phase_timeout_seconds=0,
                        should_abort=lambda: None,
                    )

            self.assertEqual(read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl"), before_pass1)
            self.assertEqual(read_jsonl(paths["pass2_pre"] / "paragraphs.jsonl"), before_pass2)

    def test_rework_translation_stage_rejects_duplicate_queue_paragraphs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            atomic_write_jsonl(
                paths["rework_queue"],
                [
                    {"paragraph_id": "p_0002", "content_hash": "sha256:" + "2" * 64, "attempt": 1, "failure_reasons": [], "failure_history": [], "required_fixes": []},
                    {"paragraph_id": "p_0002", "content_hash": "sha256:" + "2" * 64, "attempt": 1, "failure_reasons": [], "failure_history": [], "required_fixes": []},
                ],
            )

            with self.assertRaises(ValueError):
                run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language=None,
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

    def test_rework_translation_stage_rejects_active_pipeline_states(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            self._write_fixture_file("pass1_pre_before.jsonl", paths["pass1_pre"] / "paragraphs.jsonl")
            atomic_write_jsonl(
                paths["paragraph_state"],
                [
                    {"paragraph_id": "p_0001", "status": "translated_pass1", "attempt": 0, "excluded_by_policy": False, "content_hash": "sha256:" + "1" * 64},
                    {"paragraph_id": "p_0002", "status": "rework_queued", "attempt": 1, "excluded_by_policy": False, "content_hash": "sha256:" + "2" * 64},
                ],
            )
            self._write_fixture_file("rework_queue.jsonl", paths["rework_queue"])

            with self.assertRaises(ValueError):
                run_rework_translation_stage(
                    paths,
                    pass1_language="Tamazight",
                    pass2_language=None,
                    model="gpt-4o-mini",
                    phase_timeout_seconds=0,
                    should_abort=lambda: None,
                )

    def test_rework_translation_stage_rejects_duplicate_candidate_indices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "run_root": root,
                "source_pre": root / "source_pre",
                "pass1_pre": root / "pass1_pre",
                "pass2_pre": root / "pass2_pre",
                "final_candidate": root / "final" / "candidate.md",
                "candidate_map": root / "final" / "candidate_map.jsonl",
                "paragraph_state": root / "state" / "paragraph_state.jsonl",
                "rework_queue": root / "state" / "rework_queue.jsonl",
            }
            self._write_fixture_file("source_pre_paragraphs.jsonl", paths["source_pre"] / "paragraphs.jsonl")
            atomic_write_jsonl(
                paths["pass1_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_0001", "paragraph_index": 2, "text": "alpha", "content_hash": "sha256:" + "1" * 64},
                    {"paragraph_id": "p_0002", "paragraph_index": 1, "text": "beta", "content_hash": "sha256:" + "2" * 64},
                ],
            )
            self._write_fixture_file("paragraph_state_before.jsonl", paths["paragraph_state"])
            self._write_fixture_file("rework_queue.jsonl", paths["rework_queue"])

            def _stub_exec(command, **kwargs):
                preprocessed = Path(command[command.index("--preprocessed") + 1])
                output_root = Path(command[command.index("--output-root") + 1])
                language = command[command.index("--language") + 1]
                paragraphs = read_jsonl(preprocessed / "paragraphs.jsonl")
                output_path = output_root / _language_output_dir_name(language)
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "translation.json").write_text(
                    json.dumps({"paragraph_translations": [f"RW1:{row['text']}" for row in paragraphs]}),
                    encoding="utf-8",
                )

            with patch("scripts.translation_toolchain._exec_phase_command", side_effect=_stub_exec):
                with self.assertRaises(ValueError):
                    run_rework_translation_stage(
                        paths,
                        pass1_language="Tamazight",
                        pass2_language=None,
                        model="gpt-4o-mini",
                        phase_timeout_seconds=0,
                        should_abort=lambda: None,
                    )

    def test_phase_e_raises_for_missing_content_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "paragraph_state.jsonl"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                '{"paragraph_id":"p_1","status":"rework_queued","attempt":0,"content_hash":""}\n',
                encoding="utf-8",
            )
            paths = {
                "paragraph_state": state_path,
                "rework_queue": root / "rework_queue.jsonl",
            }
            from scripts.translation_toolchain import run_phase_e
            with self.assertRaises(ValueError):
                run_phase_e(paths, max_paragraph_attempts=4, bump_attempts=True, should_abort=lambda: None)

    def test_is_stale_treats_future_heartbeat_as_not_stale(self) -> None:
        import time
        future = time.time() + 3600
        payload = {"last_heartbeat_at": __import__("datetime").datetime.fromtimestamp(future, __import__("datetime").timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")}
        self.assertFalse(_is_stale(payload))

    def test_resolve_pipeline_languages_allows_explicit_languages_without_profile(self) -> None:
        from argparse import Namespace

        pass1, pass2 = _resolve_pipeline_languages(
            Namespace(pipeline_profile=None, pass1_language="French", pass2_language="none")
        )
        self.assertEqual(pass1, "French")
        self.assertIsNone(pass2)

    def test_resolve_pipeline_languages_uses_profile_defaults_when_present(self) -> None:
        from argparse import Namespace

        pass1, pass2 = _resolve_pipeline_languages(
            Namespace(pipeline_profile="tamazight_two_pass", pass1_language=None, pass2_language=None)
        )
        self.assertEqual(pass1, "Tamazight")
        self.assertEqual(pass2, "Tifinagh")

    def test_resolve_pipeline_languages_rejects_unknown_profile(self) -> None:
        from argparse import Namespace

        with self.assertRaises(SystemExit) as exc:
            _resolve_pipeline_languages(
                Namespace(pipeline_profile="unknown_profile", pass1_language=None, pass2_language=None)
            )
        self.assertEqual(exc.exception.code, 5)

    def test_resolve_pipeline_languages_requires_pass1_with_usage_exit_code(self) -> None:
        from argparse import Namespace

        with self.assertRaises(SystemExit) as exc:
            _resolve_pipeline_languages(
                Namespace(pipeline_profile=None, pass1_language=None, pass2_language=None)
            )
        self.assertEqual(exc.exception.code, 5)


if __name__ == "__main__":
    unittest.main()
