from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from scripts.translation_toolchain import (
    atomic_write_jsonl,
    build_rework_queue_packet,
    build_rework_queue_rows,
    read_jsonl,
    _compute_status_report,
    _materialize_preprocessed_from_translation,
    run_phase_c,
    _ensure_manifest,
    _language_output_dir_name,
)


class TranslationToolchainQueueTests(unittest.TestCase):

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
                )

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
        self.assertEqual(out[0], existing[0])

    def test_atomic_write_jsonl_replaces_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state" / "paragraph_state.jsonl"
            atomic_write_jsonl(path, [{"paragraph_id": "p_1", "status": "ingested"}])
            atomic_write_jsonl(path, [{"paragraph_id": "p_1", "status": "rework_queued"}])
            rows = read_jsonl(path)
            self.assertEqual(rows, [{"paragraph_id": "p_1", "status": "rework_queued"}])


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


if __name__ == "__main__":
    unittest.main()
