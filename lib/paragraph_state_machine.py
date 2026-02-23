#!/usr/bin/env python3
"""Canonical paragraph review state transitions."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

KNOWN_STATES = {
    "ingested",
    "translated_pass1",
    "translated_pass2",
    "candidate_assembled",
    "review_in_progress",
    "review_failed",
    "rework_queued",
    "reworked",
    "ready_to_merge",
    "manual_review_required",
    "merged",
}

REQUIRED_SCORE_METRICS: tuple[str, ...] = (
    "grammar",
    "vocabulary",
    "style",
    "voice",
    "semantic_fidelity",
)

DEFAULT_REVIEW_SCORE_THRESHOLDS: dict[str, float] = {
    "grammar": 0.8,
    "vocabulary": 0.8,
    "style": 0.8,
    "voice": 0.8,
    "semantic_fidelity": 0.85,
}

EXCLUSION_DISALLOWED_STATES = {
    "translated_pass1",
    "translated_pass2",
    "candidate_assembled",
    "review_in_progress",
    "review_failed",
    "rework_queued",
    "reworked",
    "ready_to_merge",
    "manual_review_required",
}

PRESERVED_EXCLUDED_STATES = {"merged"}

ALLOWED_STATUS_EVOLUTION: dict[str, set[str]] = {
    "ingested": {"translated_pass1"},
    "translated_pass1": {"translated_pass2", "candidate_assembled"},
    "translated_pass2": {"candidate_assembled"},
    "candidate_assembled": {"review_in_progress"},
    "review_in_progress": {"ready_to_merge", "rework_queued", "manual_review_required"},
    # review_failed is emitted as the immediate review outcome by resolve_review_transition
    # before routing to rework_queued/manual_review_required in persisted status.
    "review_failed": {"rework_queued", "manual_review_required"},
    "rework_queued": {"reworked", "manual_review_required"},
    "reworked": {"translated_pass1", "review_in_progress"},
    "ready_to_merge": {"merged"},
    "manual_review_required": set(),
    "merged": set(),
}


@dataclass(frozen=True)
class ParagraphPolicyConfig:
    max_attempts: int = 4
    score_thresholds: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_REVIEW_SCORE_THRESHOLDS))
    immediate_manual_review_reasons: frozenset[str] = frozenset(
        {
            "mapping_error",
            "semantic_fidelity_hard_floor",
            "content_hash_mismatch",
            "artifact_corrupt",
        }
    )


def evaluate_score_threshold_issues(
    scores: dict[str, Any],
    thresholds: dict[str, float],
    *,
    required_metrics: tuple[str, ...] = REQUIRED_SCORE_METRICS,
) -> list[str]:
    """Return deterministic issue codes for missing/below-threshold required score metrics."""
    issues: list[str] = []
    for metric in required_metrics:
        threshold = thresholds.get(metric)
        if threshold is None:
            continue
        raw_value = scores.get(metric)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = None
        if value is None or value < float(threshold):
            issues.append(f"score_below_threshold:{metric}")
    return issues


@dataclass(frozen=True)
class ParagraphReviewAggregate:
    hard_fail: bool
    blocking_issues: tuple[str, ...]
    scores: dict[str, float]


@dataclass(frozen=True)
class ParagraphTransitionResult:
    immediate_state: str
    follow_up_state: str | None
    metadata_updates: dict[str, Any]

    @property
    def next_state(self) -> str:
        """Compatibility view of the durable routed status (never transient-only events)."""
        return self.follow_up_state or self.immediate_state


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_state(state: str) -> None:
    if state not in KNOWN_STATES:
        raise ValueError(f"Unknown paragraph state '{state}'.")


def _normalize_failure_history_entry(entry: Any) -> dict[str, Any]:
    if isinstance(entry, dict):
        normalized = dict(entry)
        normalized.setdefault("state", None)
        return normalized
    return {"issues": [str(entry)], "attempt": None, "timestamp": None, "state": None}


def _resolve_excluded_state(prior_status: str) -> str:
    if prior_status in PRESERVED_EXCLUDED_STATES:
        return prior_status
    return "ingested"


def _top_level_issue_code(issue: Any) -> str:
    return str(issue).split(":", 1)[0]


def _has_repeated_identical_hard_fail(
    prior_failure_history: list[dict[str, Any]],
    blocking_issues: list[str],
) -> bool:
    if not blocking_issues:
        return False

    current_issue_codes = {_top_level_issue_code(issue) for issue in blocking_issues}
    for issue_code in current_issue_codes:
        prior_matches = 0
        for failure in prior_failure_history:
            historical_issues = failure.get("issues") or []
            if any(_top_level_issue_code(historical_issue) == issue_code for historical_issue in historical_issues):
                prior_matches += 1
            if prior_matches >= 1:
                return True
    return False


def resolve_review_transition(
    prior_state: dict[str, Any],
    review: ParagraphReviewAggregate,
    policy: ParagraphPolicyConfig,
    now_iso: str | None = None,
) -> ParagraphTransitionResult:
    timestamp = now_iso or utc_now_iso()
    excluded = bool(prior_state.get("excluded_by_policy", False))
    prior_status = str(prior_state.get("status", "ingested"))
    _validate_state(prior_status)
    prior_attempt = int(prior_state.get("attempt", 0))
    prior_failure_history = [
        _normalize_failure_history_entry(entry) for entry in (prior_state.get("failure_history") or [])
    ]

    if excluded:
        return ParagraphTransitionResult(
            immediate_state=_resolve_excluded_state(prior_status),
            follow_up_state=None,
            metadata_updates={
                "attempt": prior_attempt,
                "failure_history": prior_failure_history,
                "scores": {},
                "blocking_issues": [],
                "reviewed_at": None,
                "last_failed_at": None,
                "last_success_at": None,
                "updated_at": prior_state.get("updated_at"),
            },
        )

    next_attempt = prior_attempt + 1
    blocking_issues = list(dict.fromkeys(review.blocking_issues))
    has_repeated_identical_hard_fail = _has_repeated_identical_hard_fail(prior_failure_history, blocking_issues)
    if has_repeated_identical_hard_fail:
        blocking_issues = list(dict.fromkeys(blocking_issues + ["repeated_identical_hard_fail"]))
    metadata_updates: dict[str, Any] = {
        "attempt": next_attempt,
        "scores": dict(review.scores),
        "blocking_issues": blocking_issues,
        "reviewed_at": timestamp,
        "updated_at": timestamp,
    }

    if not review.hard_fail and not blocking_issues:
        metadata_updates.update(
            {
                "failure_history": prior_failure_history,
                "last_failed_at": None,
                "last_success_at": timestamp,
            }
        )
        return ParagraphTransitionResult(
            immediate_state="ready_to_merge",
            follow_up_state=None,
            metadata_updates=metadata_updates,
        )

    failure_history = prior_failure_history + [
        {"attempt": next_attempt, "issues": blocking_issues, "timestamp": timestamp, "state": "review_failed"}
    ]

    requires_manual = (
        next_attempt >= policy.max_attempts
        or has_repeated_identical_hard_fail
        or any(issue in policy.immediate_manual_review_reasons for issue in blocking_issues)
    )

    metadata_updates.update(
        {
            "failure_history": failure_history,
            "last_failed_at": timestamp,
            "last_success_at": None,
        }
    )

    return ParagraphTransitionResult(
        immediate_state="review_failed",
        follow_up_state="manual_review_required" if requires_manual else "rework_queued",
        metadata_updates=metadata_updates,
    )


def assert_pipeline_state_allowed(state: str, excluded_by_policy: bool) -> None:
    _validate_state(state)
    if excluded_by_policy and state in EXCLUSION_DISALLOWED_STATES:
        raise ValueError(f"Excluded paragraph cannot transition into active pipeline state '{state}'.")


def assert_pipeline_transition_allowed(
    current_state: str,
    next_state: str,
    excluded_by_policy: bool,
) -> None:
    assert_pipeline_state_allowed(current_state, excluded_by_policy)
    assert_pipeline_state_allowed(next_state, excluded_by_policy)

    if current_state == next_state:
        return

    allowed_next_states = ALLOWED_STATUS_EVOLUTION.get(current_state)
    if allowed_next_states is None:
        raise ValueError(f"Unknown transition source state '{current_state}'.")

    if next_state not in allowed_next_states:
        raise ValueError(f"Disallowed paragraph transition '{current_state}' -> '{next_state}'.")
