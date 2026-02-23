from __future__ import annotations

import unittest

from lib.paragraph_state_machine import (
    ParagraphPolicyConfig,
    ParagraphReviewAggregate,
    assert_pipeline_state_allowed,
    resolve_review_transition,
)


class ParagraphStateMachineTests(unittest.TestCase):
    def test_excluded_clears_scores_and_timestamps_without_touching_updated_at(self) -> None:
        prior = {
            "status": "review_in_progress",
            "excluded_by_policy": True,
            "attempt": 3,
            "failure_history": ["critical_grammar"],
            "scores": {"grammar": 0.2},
            "reviewed_at": "2020-01-01T00:00:00Z",
            "last_failed_at": "2020-01-01T00:00:00Z",
            "last_success_at": "2020-01-01T00:00:00Z",
            "updated_at": "2020-01-02T00:00:00Z",
        }
        review = ParagraphReviewAggregate(hard_fail=False, blocking_issues=(), scores={"grammar": 1.0})
        result = resolve_review_transition(
            prior,
            review,
            ParagraphPolicyConfig(max_attempts=4),
            now_iso="2026-02-23T00:00:00Z",
        )

        self.assertEqual(result.next_state, "ingested")
        self.assertEqual(result.metadata_updates["scores"], {})
        self.assertEqual(result.metadata_updates["blocking_issues"], [])
        self.assertIsNone(result.metadata_updates["reviewed_at"])
        self.assertIsNone(result.metadata_updates["last_failed_at"])
        self.assertIsNone(result.metadata_updates["last_success_at"])
        self.assertEqual(result.metadata_updates["updated_at"], "2020-01-02T00:00:00Z")

    def test_fail_before_max_goes_to_rework(self) -> None:
        prior = {"status": "review_in_progress", "attempt": 1, "failure_history": [], "excluded_by_policy": False}
        review = ParagraphReviewAggregate(hard_fail=True, blocking_issues=("critical_grammar",), scores={})
        result = resolve_review_transition(prior, review, ParagraphPolicyConfig(max_attempts=4), now_iso="2026-02-23T00:00:00Z")
        self.assertEqual(result.next_state, "rework_queued")
        self.assertEqual(result.metadata_updates["attempt"], 2)
        self.assertEqual(result.metadata_updates["failure_history"][0]["issues"], ["critical_grammar"])

    def test_fail_at_max_goes_manual(self) -> None:
        prior = {"status": "review_in_progress", "attempt": 3, "failure_history": [], "excluded_by_policy": False}
        review = ParagraphReviewAggregate(hard_fail=True, blocking_issues=("critical_grammar",), scores={})
        result = resolve_review_transition(prior, review, ParagraphPolicyConfig(max_attempts=4), now_iso="2026-02-23T00:00:00Z")
        self.assertEqual(result.next_state, "manual_review_required")
        self.assertEqual(result.metadata_updates["attempt"], 4)

    def test_immediate_reason_goes_manual(self) -> None:
        prior = {"status": "review_in_progress", "attempt": 1, "failure_history": [], "excluded_by_policy": False}
        review = ParagraphReviewAggregate(hard_fail=True, blocking_issues=("mapping_error",), scores={})
        result = resolve_review_transition(prior, review, ParagraphPolicyConfig(max_attempts=10), now_iso="2026-02-23T00:00:00Z")
        self.assertEqual(result.next_state, "manual_review_required")

    def test_pass_goes_ready_to_merge_and_increments_attempt(self) -> None:
        prior = {"status": "review_in_progress", "attempt": 0, "failure_history": [], "excluded_by_policy": False}
        review = ParagraphReviewAggregate(hard_fail=False, blocking_issues=(), scores={"grammar": 0.95})
        result = resolve_review_transition(prior, review, ParagraphPolicyConfig(max_attempts=4), now_iso="2026-02-23T00:00:00Z")
        self.assertEqual(result.next_state, "ready_to_merge")
        self.assertEqual(result.metadata_updates["attempt"], 1)

    def test_excluded_merged_state_is_preserved(self) -> None:
        prior = {"status": "merged", "attempt": 1, "failure_history": [], "excluded_by_policy": True}
        review = ParagraphReviewAggregate(hard_fail=False, blocking_issues=(), scores={})
        result = resolve_review_transition(prior, review, ParagraphPolicyConfig(max_attempts=4), now_iso="2026-02-23T00:00:00Z")
        self.assertEqual(result.next_state, "merged")

    def test_exclusion_disallowed_state_guard(self) -> None:
        with self.assertRaises(ValueError):
            assert_pipeline_state_allowed("review_in_progress", excluded_by_policy=True)


if __name__ == "__main__":
    unittest.main()
