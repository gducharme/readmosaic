from __future__ import annotations

import json
import unittest
from pathlib import Path

from lib.paragraph_state_machine import (
    ParagraphPolicyConfig,
    ParagraphReviewAggregate,
    assert_pipeline_state_allowed,
    resolve_review_transition,
)
from scripts.aggregate_paragraph_reviews import (
    _apply_threshold_failures,
    _merge_reviews,
    _resolve_score_thresholds,
)
from scripts.translation_toolchain import build_rework_queue_rows, read_jsonl

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "translation_toolchain"
REQUIRED_FIXTURE_FILES = {
    "fixture_meta.json",
    "input_state.jsonl",
    "review_rows.jsonl",
    "expected_state.jsonl",
    "expected_scores.jsonl",
    "expected_queue.jsonl",
    "expected_summary.json",
}
EXPECTED_FIXTURE_DIRS = {
    "excluded_paragraph_handling",
    "happy_path_tamazight_two_pass",
    "mapping_error_unresolved",
    "mixed_required_excluded_manual_review",
    "repeated_identical_hard_fail",
    "rework_queue_population",
    "semantic_fidelity_hard_floor",
}
EXPECTED_PIPELINE_PROFILE = "tamazight_two_pass"


class TranslationToolchainFixtureTests(unittest.TestCase):
    _STATE_OPTIONAL_DEFAULTS: dict[str, object] = {
        "scores": {},
        "blocking_issues": [],
        "reviewed_at": None,
        "updated_at": None,
        "last_failed_at": None,
        "last_success_at": None,
    }

    def _normalized_state_rows(self, rows: list[dict]) -> list[dict]:
        normalized: list[dict] = []
        for row in rows:
            item = dict(row)
            for key, default in self._STATE_OPTIONAL_DEFAULTS.items():
                item.setdefault(key, default)
            normalized.append(item)
        return normalized

    def _sorted_rows(self, rows: list[dict]) -> list[dict]:
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("paragraph_id", "")),
                int(row.get("attempt", -1)),
                str(row.get("status", "")),
            ),
        )

    def _run_fixture(self, fixture_dir: Path) -> tuple[list[dict], list[dict], list[dict], dict]:
        meta = json.loads((fixture_dir / "fixture_meta.json").read_text(encoding="utf-8"))
        fixed_now = meta["fixed_now"]
        self.assertEqual(meta.get("pipeline_profile"), EXPECTED_PIPELINE_PROFILE)
        policy = ParagraphPolicyConfig(max_attempts=meta["max_attempts"])

        state_rows = read_jsonl(fixture_dir / "input_state.jsonl")
        review_rows = read_jsonl(fixture_dir / "review_rows.jsonl")
        merged_reviews = _merge_reviews(review_rows)
        score_thresholds = _resolve_score_thresholds(policy, review_rows)
        _apply_threshold_failures(merged_reviews, score_thresholds, policy.semantic_fidelity_hard_floor)

        updated_state_rows: list[dict] = []
        score_rows: list[dict] = []

        for row in state_rows:
            paragraph_id = row.get("paragraph_id")
            aggregate = merged_reviews.get(paragraph_id)
            if not aggregate:
                updated_state_rows.append(row)
                continue

            review = ParagraphReviewAggregate(
                hard_fail=bool(aggregate.get("hard_fail", False)),
                blocking_issues=tuple(aggregate.get("blocking_issues", [])),
                scores=dict(aggregate.get("scores", {})),
            )
            transition = resolve_review_transition(row, review, policy, now_iso=fixed_now)
            next_row = dict(row)
            next_row["status"] = transition.next_state
            next_row.update(transition.metadata_updates)
            assert_pipeline_state_allowed(next_row["status"], bool(next_row.get("excluded_by_policy", False)))

            if bool(row.get("excluded_by_policy", False)):
                # Excluded rows remain out of active pipeline flow even when review rows exist.
                self.assertIn(next_row["status"], {"ingested", "merged"})

            updated_state_rows.append(next_row)
            score_rows.append(
                {
                    "paragraph_id": paragraph_id,
                    "status": next_row["status"],
                    "attempt": next_row.get("attempt", 0),
                    "scores": next_row.get("scores", {}),
                    "blocking_issues": next_row.get("blocking_issues", []),
                    "updated_at": next_row.get("updated_at"),
                }
            )

        queue_rows = build_rework_queue_rows(updated_state_rows, existing_queue_rows=[])

        counts = {
            "ready_to_merge": sum(1 for row in updated_state_rows if row.get("status") == "ready_to_merge"),
            "manual_review_required": sum(1 for row in updated_state_rows if row.get("status") == "manual_review_required"),
            "merged": sum(1 for row in updated_state_rows if row.get("status") == "merged"),
            "rework_queued": sum(1 for row in updated_state_rows if row.get("status") == "rework_queued"),
            "ingested": sum(1 for row in updated_state_rows if row.get("status") == "ingested"),
        }
        self.assertEqual(sum(counts.values()), len(updated_state_rows))

        # In this suite, required paragraphs are defined as non-excluded rows.
        blocking_required_ids = [
            row["paragraph_id"]
            for row in updated_state_rows
            if not bool(row.get("excluded_by_policy", False))
            and row.get("status") not in {"ready_to_merge", "merged"}
        ]

        summary = {
            "counts": counts,
            "required_merge": {
                "can_merge": len(blocking_required_ids) == 0,
                "blocking_required_ids": blocking_required_ids,
            },
        }
        return updated_state_rows, score_rows, queue_rows, summary

    def test_translation_toolchain_fixtures(self) -> None:
        fixture_dirs = sorted(
            path
            for path in FIXTURES_ROOT.iterdir()
            if path.is_dir() and REQUIRED_FIXTURE_FILES.issubset({item.name for item in path.iterdir()})
        )
        self.assertEqual({path.name for path in fixture_dirs}, EXPECTED_FIXTURE_DIRS)

        for fixture_dir in fixture_dirs:
            with self.subTest(fixture=fixture_dir.name):
                actual_state, actual_scores, actual_queue, actual_summary = self._run_fixture(fixture_dir)

                expected_state = read_jsonl(fixture_dir / "expected_state.jsonl")
                expected_scores = read_jsonl(fixture_dir / "expected_scores.jsonl")
                expected_queue = read_jsonl(fixture_dir / "expected_queue.jsonl")
                expected_summary = json.loads((fixture_dir / "expected_summary.json").read_text(encoding="utf-8"))

                self.assertEqual(
                    self._sorted_rows(self._normalized_state_rows(actual_state)),
                    self._sorted_rows(self._normalized_state_rows(expected_state)),
                )
                self.assertEqual(self._sorted_rows(actual_scores), self._sorted_rows(expected_scores))
                self.assertEqual(self._sorted_rows(actual_queue), self._sorted_rows(expected_queue))
                self.assertEqual(actual_summary, expected_summary)


if __name__ == "__main__":
    unittest.main()
