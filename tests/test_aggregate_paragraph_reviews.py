from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.aggregate_paragraph_reviews import _collect_run_level_blockers


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
                    "issues": [{"code": "mapping_error"}],
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


if __name__ == "__main__":
    unittest.main()
