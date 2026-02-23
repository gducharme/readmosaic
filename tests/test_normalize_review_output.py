from __future__ import annotations

import unittest

from scripts.normalize_review_output import _normalize_grammar_rows, _normalize_mapped_rows


class NormalizeReviewOutputTests(unittest.TestCase):
    def test_normalize_grammar_rows_outputs_required_keys(self) -> None:
        rows = [
            {
                "paragraph_id": "p_0001",
                "scores": {"grammar": 0.92},
                "issues": [{"category": "critical_grammar", "description": "Bad agreement."}],
            }
        ]

        result = _normalize_grammar_rows(rows)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["paragraph_id"], "p_0001")
        self.assertEqual(result[0]["scores"], {"grammar": 0.92})
        self.assertEqual(result[0]["blocking_issues"], ["critical_grammar"])
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


if __name__ == "__main__":
    unittest.main()
