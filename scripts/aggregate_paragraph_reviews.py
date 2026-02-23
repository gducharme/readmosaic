#!/usr/bin/env python3
"""Aggregate normalized paragraph reviews into canonical paragraph state rows."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.paragraph_state_machine import (
    ParagraphPolicyConfig,
    ParagraphReviewAggregate,
    assert_pipeline_state_allowed,
    evaluate_score_threshold_issues,
    resolve_review_transition,
)
from scripts.translation_toolchain import (
    atomic_write_jsonl,
    build_rework_queue_rows,
    read_jsonl,
    _load_paragraph_lookup,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate normalized paragraph reviews into paragraph state.")
    parser.add_argument("--state", type=Path, required=True, help="Path to paragraph_state.jsonl")
    parser.add_argument("--review-rows", type=Path, required=True, help="Path to normalized review rows JSONL")
    parser.add_argument("--scores-out", type=Path, required=True, help="Path to paragraph_scores.jsonl output")
    parser.add_argument("--queue-out", type=Path, required=True, help="Path to rework_queue.jsonl output")
    parser.add_argument("--max-attempts", type=int, default=4, help="Maximum paragraph attempts before manual review")
    parser.add_argument("--source-paragraphs", type=Path, default=None, help="Optional source_pre/paragraphs.jsonl for queue text projection")
    parser.add_argument("--current-paragraphs", type=Path, default=None, help="Optional active review stage paragraphs.jsonl for queue text projection")
    parser.add_argument(
        "--review-blockers-out",
        type=Path,
        default=None,
        help="Optional run-level blocker artifact path (e.g., runs/<run_id>/gate/review_blockers.json)",
    )
    return parser.parse_args()


def _resolve_score_thresholds(policy: ParagraphPolicyConfig, review_rows: list[dict[str, Any]]) -> dict[str, float]:
    thresholds = dict(policy.score_thresholds)
    for row in review_rows:
        candidate = row.get("score_thresholds")
        if not isinstance(candidate, dict):
            continue
        for metric, threshold in candidate.items():
            try:
                thresholds[str(metric)] = float(threshold)
            except (TypeError, ValueError):
                continue
    return thresholds


def _apply_threshold_failures(
    merged_reviews: dict[str, dict[str, Any]],
    score_thresholds: dict[str, float],
) -> None:
    for aggregate in merged_reviews.values():
        threshold_issues = evaluate_score_threshold_issues(
            dict(aggregate.get("scores", {})),
            score_thresholds,
        )
        if not threshold_issues:
            continue
        aggregate["hard_fail"] = True
        blocking_issues = list(aggregate.get("blocking_issues", []))
        for issue in threshold_issues:
            if issue not in blocking_issues:
                blocking_issues.append(issue)
        aggregate["blocking_issues"] = blocking_issues


def _collect_run_level_blockers(review_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for row in review_rows:
        if row.get("run_level_blocker") is not True:
            continue

        blocker: dict[str, Any] = {
            "reason": str(row.get("run_level_blocker_reason") or "mapping_error_unresolved"),
            "paragraph_id": str(row.get("paragraph_id") or ""),
        }

        detail = row.get("run_level_blocker_detail")
        if isinstance(detail, str) and detail.strip():
            blocker["detail"] = detail.strip()

        issues = row.get("issues")
        if isinstance(issues, list):
            blocker["issues"] = [issue for issue in issues if isinstance(issue, dict)]

        blockers.append(blocker)

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for blocker in blockers:
        key = json.dumps(blocker, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(blocker)
    return deduped



def _merge_reviews(review_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in review_rows:
        paragraph_id = row["paragraph_id"]
        target = merged.setdefault(
            paragraph_id,
            {"hard_fail": False, "blocking_issues": [], "scores": {}},
        )

        target["hard_fail"] = bool(target["hard_fail"] or row.get("hard_fail", False))
        target["scores"].update(row.get("scores", {}))


        for issue in row.get("blocking_issues", []):
            if issue not in target["blocking_issues"]:
                target["blocking_issues"].append(issue)

    for aggregate in merged.values():
        if aggregate["hard_fail"] and not aggregate["blocking_issues"]:
            aggregate["blocking_issues"] = ["hard_fail"]
    return merged


def main() -> None:
    args = parse_args()
    policy = ParagraphPolicyConfig(max_attempts=args.max_attempts)

    state_rows = read_jsonl(args.state)
    review_rows = read_jsonl(args.review_rows)
    merged_reviews = _merge_reviews(review_rows)
    score_thresholds = _resolve_score_thresholds(policy, review_rows)
    _apply_threshold_failures(merged_reviews, score_thresholds)
    run_level_blockers = _collect_run_level_blockers(review_rows)

    score_rows: list[dict[str, Any]] = []
    updated_state_rows: list[dict[str, Any]] = []

    seen_paragraph_ids: set[str] = set()

    for row in state_rows:
        paragraph_id = row.get("paragraph_id")
        if paragraph_id is not None:
            seen_paragraph_ids.add(paragraph_id)
        aggregate = merged_reviews.get(paragraph_id)
        if not aggregate:
            updated_state_rows.append(row)
            continue

        review = ParagraphReviewAggregate(
            hard_fail=bool(aggregate.get("hard_fail", False)),
            blocking_issues=tuple(aggregate.get("blocking_issues", [])),
            scores=dict(aggregate.get("scores", {})),
        )
        transition = resolve_review_transition(row, review, policy)
        next_row = dict(row)
        next_row.update(transition.metadata_updates)

        # Record deterministic review lineage moment, then apply routing (single persisted row; status is final).
        transition_trace: list[str] = [transition.immediate_state]
        next_row["status"] = transition.immediate_state
        assert_pipeline_state_allowed(next_row["status"], bool(next_row.get("excluded_by_policy", False)))

        if transition.follow_up_state is not None:
            transition_trace.append(transition.follow_up_state)
            next_row["status"] = transition.follow_up_state
            assert_pipeline_state_allowed(next_row["status"], bool(next_row.get("excluded_by_policy", False)))

        next_row["review_state"] = transition.immediate_state
        next_row["routing_state"] = transition.follow_up_state
        next_row["review_transition_trace"] = transition_trace

        updated_state_rows.append(next_row)
        score_rows.append(
            {
                "paragraph_id": paragraph_id,
                "status": next_row["status"],
                "review_state": transition.immediate_state,
                "routing_state": transition.follow_up_state,
                "attempt": next_row.get("attempt", 0),
                "scores": next_row.get("scores", {}),
                "blocking_issues": next_row.get("blocking_issues", []),
                "updated_at": next_row.get("updated_at"),
                "transition_trace": transition_trace,
            }
        )


    unknown_review_rows = sorted(set(merged_reviews) - seen_paragraph_ids)
    for paragraph_id in unknown_review_rows:
        if paragraph_id == "__unmapped__":
            continue
        print(
            f"Warning: review row for unknown paragraph_id='{paragraph_id}' was ignored.",
            file=sys.stderr,
        )

    existing_queue_rows = read_jsonl(args.queue_out, strict=False)
    source_lookup_by_id = _load_paragraph_lookup(args.source_paragraphs, label="source_pre") if args.source_paragraphs else {}
    current_lookup_by_id = _load_paragraph_lookup(args.current_paragraphs, label="review_pre") if args.current_paragraphs else {}
    rework_queue_rows = build_rework_queue_rows(
        updated_state_rows,
        existing_queue_rows,
        source_lookup_by_id=source_lookup_by_id,
        current_lookup_by_id=current_lookup_by_id,
    )

    atomic_write_jsonl(args.state, updated_state_rows)
    atomic_write_jsonl(args.scores_out, score_rows)
    atomic_write_jsonl(args.queue_out, rework_queue_rows)

    if args.review_blockers_out is not None:
        args.review_blockers_out.parent.mkdir(parents=True, exist_ok=True)
        payload = {"run_level_blockers": run_level_blockers}
        args.review_blockers_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
