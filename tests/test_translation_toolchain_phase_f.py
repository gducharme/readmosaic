from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.translation_toolchain import atomic_write_jsonl, read_jsonl, run_phase_f


class TranslationToolchainPhaseFTests(unittest.TestCase):
    def test_phase_f_blocks_when_required_paragraphs_not_merge_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "tx_001"
            paths = {
                "run_root": run_root,
                "source_pre": run_root / "source_pre",
                "pass1_pre": run_root / "pass1_pre",
                "pass2_pre": run_root / "pass2_pre",
                "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
                "final_dir": run_root / "final",
                "final_output": run_root / "final" / "final.md",
                "final_pre": run_root / "final" / "final_pre",
                "gate_dir": run_root / "gate",
                "gate_report": run_root / "gate" / "gate_report.json",
            }

            atomic_write_jsonl(
                paths["source_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "s1", "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "s2", "content_hash": "sha256:" + "b" * 64},
                ],
            )
            atomic_write_jsonl(
                paths["pass2_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "t1", "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "t2", "content_hash": "sha256:" + "b" * 64},
                ],
            )
            atomic_write_jsonl(
                paths["paragraph_state"],
                [
                    {"paragraph_id": "p_1", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "status": "manual_review_required", "excluded_by_policy": False, "content_hash": "sha256:" + "b" * 64},
                ],
            )

            run_phase_f(paths, run_id="tx_001")

            gate_report = json.loads(paths["gate_report"].read_text(encoding="utf-8"))
            self.assertFalse(gate_report["can_merge"])
            self.assertEqual(gate_report["blocking_paragraphs"], [{"paragraph_id": "p_2", "reason": "status:manual_review_required"}])
            self.assertFalse(paths["final_output"].exists())



    def test_phase_f_blocks_merge_when_run_level_mapping_blocker_exists(self) -> None:
        fixture_dir = Path(__file__).parent / "fixtures" / "translation_toolchain" / "mapping_error_unresolved"
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "tx_001"
            paths = {
                "run_root": run_root,
                "source_pre": run_root / "source_pre",
                "pass1_pre": run_root / "pass1_pre",
                "pass2_pre": run_root / "pass2_pre",
                "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
                "final_dir": run_root / "final",
                "final_output": run_root / "final" / "final.md",
                "final_pre": run_root / "final" / "final_pre",
                "gate_dir": run_root / "gate",
                "gate_report": run_root / "gate" / "gate_report.json",
                "review_blockers": run_root / "gate" / "review_blockers.json",
            }

            atomic_write_jsonl(
                paths["source_pre"] / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": 1, "text": "s1", "content_hash": "sha256:" + "a" * 64}],
            )
            atomic_write_jsonl(
                paths["pass2_pre"] / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": 1, "text": "t1", "content_hash": "sha256:" + "a" * 64}],
            )
            atomic_write_jsonl(
                paths["paragraph_state"],
                [{"paragraph_id": "p_1", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "a" * 64}],
            )

            paths["review_blockers"].parent.mkdir(parents=True, exist_ok=True)
            paths["review_blockers"].write_text((fixture_dir / "review_blockers.json").read_text(encoding="utf-8"), encoding="utf-8")

            run_phase_f(paths, run_id="tx_001")

            gate_report = json.loads(paths["gate_report"].read_text(encoding="utf-8"))
            expected = json.loads((fixture_dir / "expected_phase_f_gate_report.json").read_text(encoding="utf-8"))
            self.assertFalse(gate_report["can_merge"])
            self.assertEqual(gate_report["blocking_paragraphs"], expected["blocking_paragraphs"])
            self.assertEqual(gate_report["run_level_blockers"], expected["run_level_blockers"])
            self.assertFalse(paths["final_output"].exists())

    def test_phase_f_merges_ready_rows_preserving_source_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "tx_001"
            paths = {
                "run_root": run_root,
                "source_pre": run_root / "source_pre",
                "pass1_pre": run_root / "pass1_pre",
                "pass2_pre": run_root / "pass2_pre",
                "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
                "final_dir": run_root / "final",
                "final_output": run_root / "final" / "final.md",
                "final_pre": run_root / "final" / "final_pre",
                "gate_dir": run_root / "gate",
                "gate_report": run_root / "gate" / "gate_report.json",
            }

            atomic_write_jsonl(
                paths["source_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "s1", "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "s2", "content_hash": "sha256:" + "b" * 64},
                    {"paragraph_id": "p_3", "paragraph_index": 3, "text": "s3", "content_hash": "sha256:" + "c" * 64},
                ],
            )
            # Deliberately shuffled to prove merge uses source paragraph_index ordering.
            atomic_write_jsonl(
                paths["pass2_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_3", "paragraph_index": 3, "text": "t3", "content_hash": "sha256:" + "c" * 64},
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "t1", "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "t2", "content_hash": "sha256:" + "b" * 64},
                ],
            )
            atomic_write_jsonl(
                paths["paragraph_state"],
                [
                    {"paragraph_id": "p_1", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "b" * 64},
                    {"paragraph_id": "p_3", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "c" * 64},
                ],
            )

            run_phase_f(paths, run_id="tx_001")

            self.assertEqual(paths["final_output"].read_text(encoding="utf-8"), "t1\n\nt2\n\nt3")
            self.assertTrue((paths["final_pre"] / "paragraphs.jsonl").exists())
            self.assertTrue((paths["final_pre"] / "sentences.jsonl").exists())
            self.assertTrue((paths["final_pre"] / "words.jsonl").exists())
            self.assertTrue((paths["final_pre"] / "manuscript_tokens.json").exists())

            updated_rows = read_jsonl(paths["paragraph_state"], strict=True)
            self.assertEqual({row["status"] for row in updated_rows}, {"merged"})

            gate_report = json.loads(paths["gate_report"].read_text(encoding="utf-8"))
            self.assertTrue(gate_report["can_merge"])
            self.assertEqual(gate_report["blocking_paragraphs"], [])

    def test_phase_f_is_idempotent_when_rows_already_merged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "tx_001"
            paths = {
                "run_root": run_root,
                "source_pre": run_root / "source_pre",
                "pass1_pre": run_root / "pass1_pre",
                "pass2_pre": run_root / "pass2_pre",
                "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
                "final_dir": run_root / "final",
                "final_output": run_root / "final" / "final.md",
                "final_pre": run_root / "final" / "final_pre",
                "gate_dir": run_root / "gate",
                "gate_report": run_root / "gate" / "gate_report.json",
            }

            atomic_write_jsonl(
                paths["source_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "s1", "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "s2", "content_hash": "sha256:" + "b" * 64},
                ],
            )
            atomic_write_jsonl(
                paths["pass2_pre"] / "paragraphs.jsonl",
                [
                    {"paragraph_id": "p_1", "paragraph_index": 1, "text": "t1", "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "paragraph_index": 2, "text": "t2", "content_hash": "sha256:" + "b" * 64},
                ],
            )
            atomic_write_jsonl(
                paths["paragraph_state"],
                [
                    {"paragraph_id": "p_1", "status": "merged", "excluded_by_policy": False, "content_hash": "sha256:" + "a" * 64},
                    {"paragraph_id": "p_2", "status": "merged", "excluded_by_policy": False, "content_hash": "sha256:" + "b" * 64},
                ],
            )

            run_phase_f(paths, run_id="tx_001")
            self.assertEqual(paths["final_output"].read_text(encoding="utf-8"), "t1\n\nt2")

    def test_phase_f_falls_back_to_pass1_for_single_pass_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "tx_001"
            paths = {
                "run_root": run_root,
                "source_pre": run_root / "source_pre",
                "pass1_pre": run_root / "pass1_pre",
                "pass2_pre": run_root / "pass2_pre",
                "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
                "final_dir": run_root / "final",
                "final_output": run_root / "final" / "final.md",
                "final_pre": run_root / "final" / "final_pre",
                "gate_dir": run_root / "gate",
                "gate_report": run_root / "gate" / "gate_report.json",
            }

            atomic_write_jsonl(
                paths["source_pre"] / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": 1, "text": "s1", "content_hash": "sha256:" + "a" * 64}],
            )
            atomic_write_jsonl(
                paths["pass1_pre"] / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": "1", "text": "t1", "content_hash": "sha256:" + "a" * 64}],
            )
            atomic_write_jsonl(
                paths["paragraph_state"],
                [{"paragraph_id": "p_1", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "a" * 64}],
            )

            run_phase_f(paths, run_id="tx_001")
            self.assertEqual(paths["final_output"].read_text(encoding="utf-8"), "t1")

    def test_final_pre_sentence_ids_increment_per_sentence_not_per_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_root = Path(tmpdir) / "runs" / "tx_001"
            paths = {
                "run_root": run_root,
                "source_pre": run_root / "source_pre",
                "pass1_pre": run_root / "pass1_pre",
                "pass2_pre": run_root / "pass2_pre",
                "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
                "final_dir": run_root / "final",
                "final_output": run_root / "final" / "final.md",
                "final_pre": run_root / "final" / "final_pre",
                "gate_dir": run_root / "gate",
                "gate_report": run_root / "gate" / "gate_report.json",
            }

            atomic_write_jsonl(
                paths["source_pre"] / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": 1, "text": "s1", "content_hash": "sha256:" + "a" * 64}],
            )
            atomic_write_jsonl(
                paths["pass2_pre"] / "paragraphs.jsonl",
                [{"paragraph_id": "p_1", "paragraph_index": 1, "text": "One two. Three four.", "content_hash": "sha256:" + "a" * 64}],
            )
            atomic_write_jsonl(
                paths["paragraph_state"],
                [{"paragraph_id": "p_1", "status": "ready_to_merge", "excluded_by_policy": False, "content_hash": "sha256:" + "a" * 64}],
            )

            run_phase_f(paths, run_id="tx_001")

            sentences = read_jsonl(paths["final_pre"] / "sentences.jsonl", strict=True)
            self.assertEqual(len(sentences), 2)
            self.assertEqual(sentences[0]["id"], "final-s000001")
            self.assertEqual(sentences[1]["id"], "final-s000002")


if __name__ == "__main__":
    unittest.main()
