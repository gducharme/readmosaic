from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.aggregate_paragraph_reviews import (
    _apply_threshold_failures,
    _collect_run_level_blockers,
    _merge_reviews,
    _resolve_score_thresholds,
)
from lib.paragraph_state_machine import ParagraphPolicyConfig


class AggregateParagraphReviewsTests(unittest.TestCase):
    def test_collect_run_level_blockers_filters_and_deduplicates(self) -> None:
        rows = [
            {
                "paragraph_id": "__unmapped__",
                "run_level_blocker": True,
                "run_level_blocker_reason": "mapping_error_unresolved",
                "run_level_blocker_detail": "quote_not_found",
                "issues": [{"code": "mapping_error"}],
            },
            {
                "paragraph_id": "__unmapped__",
                "run_level_blocker": True,
                "run_level_blocker_reason": "mapping_error_unresolved",
                "run_level_blocker_detail": "quote_not_found",
                "issues": [{"code": "mapping_error"}],
            },
            {"paragraph_id": "p_0001", "run_level_blocker": False},
        ]
        blockers = _collect_run_level_blockers(rows)
        self.assertEqual(
            blockers,
            [
                {
                    "reason": "mapping_error_unresolved",
                    "paragraph_id": "__unmapped__",
                    "detail": "quote_not_found",
                    "paragraph_ids": ["__unmapped__"],
                    "issues": [{"code": "mapping_error"}],
                }
            ],
        )


    def test_collect_run_level_blockers_deduplicates_same_reason_across_paragraphs(self) -> None:
        rows = [
            {
                "paragraph_id": "p_0002",
                "run_level_blocker": True,
                "run_level_blocker_reason": "mapping_error_unresolved",
                "run_level_blocker_detail": "ambiguous_line_membership",
                "issues": [{"code": "mapping_error", "line": 11}],
            },
            {
                "paragraph_id": "p_0003",
                "run_level_blocker": True,
                "run_level_blocker_reason": "mapping_error_unresolved",
                "run_level_blocker_detail": "ambiguous_line_membership",
                "issues": [{"line": 11, "code": "mapping_error"}],
            },
        ]

        blockers = _collect_run_level_blockers(rows)
        self.assertEqual(
            blockers,
            [
                {
                    "reason": "mapping_error_unresolved",
                    "paragraph_id": "p_0002",
                    "detail": "ambiguous_line_membership",
                    "paragraph_ids": ["p_0002", "p_0003"],
                    "issues": [{"code": "mapping_error", "line": 11}],
                }
            ],
        )

    def test_cli_does_not_warn_for_unmapped_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            state = tmp / "paragraph_state.jsonl"
            review_rows = tmp / "review_rows.jsonl"
            scores = tmp / "paragraph_scores.jsonl"
            queue = tmp / "rework_queue.jsonl"

            state.write_text(
                json.dumps(
                    {
                        "paragraph_id": "p_0001",
                        "status": "review_in_progress",
                        "attempt": 0,
                        "excluded_by_policy": False,
                        "content_hash": "sha256:" + "a" * 64,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            review_rows.write_text(
                "\n".join(
                    [
                        json.dumps({"paragraph_id": "__unmapped__", "hard_fail": True, "blocking_issues": ["mapping_error"], "scores": {}}),
                        json.dumps({"paragraph_id": "p_missing", "hard_fail": True, "blocking_issues": ["mapping_error"], "scores": {}}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    "python",
                    "scripts/aggregate_paragraph_reviews.py",
                    "--state",
                    str(state),
                    "--review-rows",
                    str(review_rows),
                    "--scores-out",
                    str(scores),
                    "--queue-out",
                    str(queue),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("paragraph_id='p_missing'", proc.stderr)
            self.assertNotIn("paragraph_id='__unmapped__'", proc.stderr)


    def test_threshold_failures_append_deterministic_blocking_issue_codes(self) -> None:
        review_rows = [
            {
                "paragraph_id": "p_0001",
                "hard_fail": False,
                "blocking_issues": [],
                "scores": {
                    "grammar": 0.92,
                    "vocabulary": 0.87,
                    "style": 0.88,
                    "voice": 0.86,
                    "semantic_fidelity": 0.9,
                },
            },
            {
                "paragraph_id": "p_0002",
                "hard_fail": False,
                "blocking_issues": ["critical_grammar"],
                "scores": {"grammar": 0.75},
            },
        ]
        merged = _merge_reviews(review_rows)
        thresholds = _resolve_score_thresholds(ParagraphPolicyConfig(), review_rows)
        _apply_threshold_failures(merged, thresholds)

        self.assertFalse(merged["p_0001"]["hard_fail"])
        self.assertEqual(merged["p_0001"]["blocking_issues"], [])

        self.assertTrue(merged["p_0002"]["hard_fail"])
        self.assertIn("critical_grammar", merged["p_0002"]["blocking_issues"])
        self.assertIn("score_below_threshold:grammar", merged["p_0002"]["blocking_issues"])
        self.assertIn("score_below_threshold:vocabulary", merged["p_0002"]["blocking_issues"])


if __name__ == "__main__":
    unittest.main()
