from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from scripts.translation_toolchain import (
    atomic_write_jsonl,
    build_rework_queue_packet,
    build_rework_queue_rows,
    read_jsonl,
)


class TranslationToolchainQueueTests(unittest.TestCase):
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

    def test_empty_explicit_failure_fields_do_not_fallback(self) -> None:
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
        self.assertEqual(packet["failure_reasons"], [])
        self.assertEqual(packet["required_fixes"], [])


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
