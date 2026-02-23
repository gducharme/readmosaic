from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

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
from scripts.translation_toolchain import (
    _run_converge_pipeline,
    atomic_write_jsonl,
    build_rework_queue_rows,
    read_jsonl,
)

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
        _apply_threshold_failures(merged_reviews, score_thresholds)

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


class TranslationToolchainConvergeFixtureTests(unittest.TestCase):
    def _make_paths(self, root: Path) -> dict[str, Path]:
        return {
            "manifest": root / "manifest.json",
            "paragraph_state": root / "state" / "paragraph_state.jsonl",
            "rework_queue": root / "state" / "rework_queue.jsonl",
        }

    def _read_orchestration_status(self, manifest_path: Path) -> dict:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return payload["orchestration_status"]

    def test_converge_fixture_converges_to_mergeable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = self._make_paths(root)
            paths["manifest"].parent.mkdir(parents=True, exist_ok=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            atomic_write_jsonl(paths["paragraph_state"], [{"paragraph_id": "p1", "content_hash": "sha256:" + "a" * 64, "status": "rework_queued", "excluded_by_policy": False}])
            atomic_write_jsonl(paths["rework_queue"], [{"paragraph_id": "p1", "content_hash": "sha256:" + "a" * 64, "attempt": 1}])

            args = Namespace(
                run_id="run_conv",
                max_rework_cycles=3,
                run_phase_f_each_cycle=False,
                rework_run_phase_f=False,
            )
            calls: list[str] = []

            def run_phase(name: str, runner):
                calls.append(name)
                runner()

            def stub_full(_paths, _args, _run_phase, _should_abort):
                return None

            def stub_rework(_paths, _args, _run_phase, _should_abort):
                atomic_write_jsonl(_paths["paragraph_state"], [{"paragraph_id": "p1", "content_hash": "sha256:" + "a" * 64, "status": "ready_to_merge", "excluded_by_policy": False}])
                atomic_write_jsonl(_paths["rework_queue"], [])

            with patch("scripts.translation_toolchain._run_full_pipeline", side_effect=stub_full), patch(
                "scripts.translation_toolchain._run_rework_only", side_effect=stub_rework
            ), patch("scripts.translation_toolchain.run_phase_f", return_value=None):
                _run_converge_pipeline(paths, args, run_phase, lambda: None, run_initial_full=True)

            self.assertEqual(calls, ["F"])
            status = self._read_orchestration_status(paths["manifest"])
            self.assertEqual(status["rework_cycles_completed"], 1)
            self.assertEqual(status["stop_reason"], "converged_no_blockers")

    def test_converge_fixture_stops_manual_review_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = self._make_paths(root)
            paths["manifest"].parent.mkdir(parents=True, exist_ok=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            atomic_write_jsonl(paths["paragraph_state"], [{"paragraph_id": "p1", "content_hash": "sha256:" + "b" * 64, "status": "manual_review_required", "excluded_by_policy": False}])
            atomic_write_jsonl(paths["rework_queue"], [])

            args = Namespace(
                run_id="run_manual",
                max_rework_cycles=3,
                run_phase_f_each_cycle=False,
                rework_run_phase_f=False,
            )

            with patch("scripts.translation_toolchain._run_full_pipeline", return_value=None), patch(
                "scripts.translation_toolchain._run_rework_only", return_value=None
            ), patch("scripts.translation_toolchain.run_phase_f", return_value=None):
                _run_converge_pipeline(paths, args, lambda name, runner: runner(), lambda: None, run_initial_full=True)

            status = self._read_orchestration_status(paths["manifest"])
            self.assertEqual(status["rework_cycles_completed"], 0)
            self.assertEqual(status["stop_reason"], "manual_review_only")

    def test_converge_fixture_stops_at_max_cycle_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = self._make_paths(root)
            paths["manifest"].parent.mkdir(parents=True, exist_ok=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            atomic_write_jsonl(paths["paragraph_state"], [{"paragraph_id": "p1", "content_hash": "sha256:" + "c" * 64, "status": "rework_queued", "excluded_by_policy": False}])
            atomic_write_jsonl(paths["rework_queue"], [{"paragraph_id": "p1", "content_hash": "sha256:" + "c" * 64, "attempt": 1}])

            args = Namespace(
                run_id="run_cap",
                max_rework_cycles=2,
                run_phase_f_each_cycle=False,
                rework_run_phase_f=False,
            )
            cycle_calls = {"count": 0}

            def stub_rework(_paths, _args, _run_phase, _should_abort):
                cycle_calls["count"] += 1

            with patch("scripts.translation_toolchain._run_full_pipeline", return_value=None), patch(
                "scripts.translation_toolchain._run_rework_only", side_effect=stub_rework
            ), patch("scripts.translation_toolchain.run_phase_f", return_value=None):
                _run_converge_pipeline(paths, args, lambda name, runner: runner(), lambda: None, run_initial_full=True)

            status = self._read_orchestration_status(paths["manifest"])
            self.assertEqual(cycle_calls["count"], 2)
            self.assertEqual(status["rework_cycles_completed"], 2)
            self.assertEqual(status["stop_reason"], "max_rework_cycles_reached")


if __name__ == "__main__":
    unittest.main()
