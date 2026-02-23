from __future__ import annotations

import unittest

from scripts.normalize_critics_runner_output import normalize_critics_payload


class NormalizeCriticsRunnerOutputTests(unittest.TestCase):
    def test_flattens_critic_keyed_payload_and_preserves_provenance(self) -> None:
        payload = {
            "line_editor": {
                "issues": [
                    {"issue_id": "le_1", "description": "Agreement error", "line": 12},
                    {"issue_id": "le_2", "description": "Quote mismatch", "quote": "foo"},
                ],
                "summary": "x",
            },
            "tone_guard": {
                "issues": [
                    {"issue_id": "tg_1", "description": "Voice drift", "start_line": 3, "end_line": 5}
                ]
            },
        }

        normalized = normalize_critics_payload(payload)
        issues = normalized["issues"]

        self.assertEqual(len(issues), 3)
        by_id = {issue["issue_id"]: issue for issue in issues}
        self.assertEqual(by_id["le_1"]["reviewer"], "line_editor")
        self.assertEqual(by_id["le_1"]["critic_name"], "line_editor")
        self.assertEqual(by_id["tg_1"]["reviewer"], "tone_guard")
        self.assertEqual(by_id["tg_1"]["critic_name"], "tone_guard")

    def test_passes_through_already_flat_issues_payload(self) -> None:
        payload = {"issues": [{"issue_id": "i_1", "line": 4}]}
        normalized = normalize_critics_payload(payload)
        self.assertEqual(normalized, payload)


if __name__ == "__main__":
    unittest.main()
