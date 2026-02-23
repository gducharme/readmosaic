from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.normalize_review_output import (
    RUN_LEVEL_BLOCKER_REASON,
    UNMAPPED_PARAGRAPH_ID,
    _normalize_grammar_rows,
    _normalize_mapped_rows,
)


class NormalizeReviewOutputTests(unittest.TestCase):
    def test_normalize_grammar_rows_blockers_critical_only_and_counters(self) -> None:
        rows = [
            {
                "paragraph_id": "p_0001",
                "scores": {"grammar": 0.92},
                "issues": [
                    {"category": "style", "severity": "minor", "description": "Nit."},
                    {"category": "named_entity", "severity": "major", "description": "Name mismatch."},
                    {"category": "grammar", "severity": "critical", "description": "Breaks meaning."},
                ],
            }
        ]

        result = _normalize_grammar_rows(rows)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["paragraph_id"], "p_0001")
        self.assertEqual(result[0]["scores"], {"grammar": 0.92})
        self.assertEqual(result[0]["blocking_issues"], ["named_entity", "grammar"])
        self.assertTrue(result[0]["hard_fail"])
        self.assertEqual(result[0]["issue_count"], 3)
        self.assertEqual(result[0]["critical_count"], 1)
        self.assertEqual(result[0]["blocker_count"], 2)

    def test_grammar_explicit_blocking_issues_preserved(self) -> None:
        rows = [
            {
                "paragraph_id": "p_0002",
                "issues": [{"category": "style", "severity": "minor"}],
                "blocking_issues": ["upstream_blocker"],
                "hard_fail": False,
            }
        ]
        result = _normalize_grammar_rows(rows)
        self.assertEqual(result[0]["blocking_issues"], ["upstream_blocker"])
        self.assertTrue(result[0]["hard_fail"])

    def test_mapping_error_candidates_force_hard_fail(self) -> None:
        mapped_rows = [
            {
                "issue_id": "issue_0001",
                "mapping_status": "mapping_error",
                "reason": "ambiguous_line_membership",
                "paragraph_id": None,
                "issue": {"line": 12, "severity": "critical"},
                "candidates": [
                    {"paragraph_id": "p_0002", "paragraph_index": 2},
                    {"paragraph_id": "p_0003", "paragraph_index": 3},
                ],
            }
        ]

        result = _normalize_mapped_rows(mapped_rows, reviewer_name="typography")
        by_id = {row["paragraph_id"]: row for row in result}

        self.assertEqual(set(by_id), {"p_0002", "p_0003"})
        for row in by_id.values():
            self.assertTrue(row["hard_fail"])
            self.assertIn("mapping_error", row["blocking_issues"])
            self.assertEqual(row["issues"][0]["mapping_status"], "mapping_error")

    def test_mapping_error_without_candidates_is_preserved(self) -> None:
        mapped_rows = [
            {
                "issue_id": "issue_0002",
                "mapping_status": "mapping_error",
                "reason": "quote_not_found",
                "issue": {"quote": "missing text"},
                "candidates": [],
            }
        ]

        result = _normalize_mapped_rows(mapped_rows, reviewer_name="critics")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["paragraph_id"], UNMAPPED_PARAGRAPH_ID)
        self.assertTrue(result[0]["hard_fail"])
        self.assertIn("mapping_error", result[0]["blocking_issues"])
        self.assertTrue(result[0]["run_level_blocker"])
        self.assertEqual(result[0]["run_level_blocker_reason"], RUN_LEVEL_BLOCKER_REASON)
        self.assertEqual(result[0]["run_level_blocker_detail"], "quote_not_found")


    def test_mapping_error_sets_code_and_category_without_reason(self) -> None:
        mapped_rows = [
            {
                "issue_id": "issue_0009",
                "mapping_status": "mapping_error",
                "paragraph_id": "p_0010",
                "issue": {"line": 22},
            }
        ]
        result = _normalize_mapped_rows(mapped_rows, reviewer_name="critics")
        self.assertEqual(len(result), 1)
        issue = result[0]["issues"][0]
        self.assertEqual(issue.get("code"), "mapping_error")
        self.assertEqual(issue.get("category"), "mapping_error")

    def test_mapped_rows_record_issues_without_hard_fail(self) -> None:
        mapped_rows = [
            {
                "issue_id": "issue_0003",
                "mapping_status": "mapped",
                "paragraph_id": "p_0004",
                "issue": {"line": 10, "severity": "minor", "category": "style"},
            }
        ]
        result = _normalize_mapped_rows(mapped_rows, reviewer_name="critics")
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0]["hard_fail"])
        self.assertEqual(result[0]["blocking_issues"], [])
        self.assertEqual(result[0]["issue_count"], 1)
        self.assertEqual(result[0]["blocker_count"], 0)


    def test_mapped_issue_without_issue_id_does_not_emit_null_issue_id(self) -> None:
        mapped_rows = [
            {
                "mapping_status": "mapped",
                "paragraph_id": "p_0005",
                "issue": {"line": 2, "category": "style", "severity": "minor"},
            }
        ]
        result = _normalize_mapped_rows(mapped_rows, reviewer_name="critics")
        self.assertEqual(len(result), 1)
        self.assertNotIn("issue_id", result[0]["issues"][0])

    def test_cli_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            grammar = tmp / "grammar.jsonl"
            mapped = tmp / "mapped.jsonl"
            output = tmp / "normalized" / "all_reviews.jsonl"

            grammar.write_text(
                json.dumps(
                    {
                        "paragraph_id": "p_0001",
                        "scores": {"grammar": 0.9},
                        "issues": [{"severity": "critical", "category": "grammar"}],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            mapped.write_text(
                json.dumps(
                    {
                        "issue_id": "i1",
                        "mapping_status": "mapped",
                        "paragraph_id": "p_0001",
                        "issue": {"line": 5, "category": "style", "severity": "minor"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            subprocess.run(
                [
                    "python",
                    "scripts/normalize_review_output.py",
                    "--grammar-input",
                    str(grammar),
                    "--mapped-input",
                    str(mapped),
                    "--output",
                    str(output),
                ],
                check=True,
            )

            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(rows), 2)


if __name__ == "__main__":
    unittest.main()
