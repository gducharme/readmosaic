#!/usr/bin/env python3
"""Translation toolchain orchestrator with lock-safe run lifecycle management."""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import tempfile
import random
import socket
import subprocess
import sys
import threading
import time
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.paragraph_state_machine import (
    ParagraphPolicyConfig,
    ParagraphReviewAggregate,
    assert_pipeline_state_allowed,
    assert_pipeline_transition_allowed,
    resolve_review_transition,
)
from scripts.assemble_candidate import assemble_candidate
from scripts.normalize_translation_output import PARAGRAPH_SEPARATOR_LEN, normalize_translation_output

LOCK_FILE_NAME = "RUNNING.lock"
LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS = 10
LOCK_STALE_TTL_SECONDS = 120
LOCK_STALE_RETRY_BASE_SLEEP_SECONDS = 0.05
LOCK_READ_RETRY_ATTEMPTS = 3
LOCK_READ_RETRY_SLEEP_SECONDS = 0.05
LOCK_TIMESTAMP_SKEW_ALLOWANCE_SECONDS = 300
LOCK_HEARTBEAT_MAX_CONSECUTIVE_FAILURES = 3
LOCK_HEARTBEAT_STALE_ABORT_SECONDS = max(30, LOCK_STALE_TTL_SECONDS - 30)
LOCK_FILE_FIELDS = (
    "pid",
    "host",
    "started_at",
    "last_heartbeat_at",
    "run_id",
)

# Convenience presets only (non-exhaustive): callers may provide explicit --pass1-language/--pass2-language
# for arbitrary language flows without using presets.
PIPELINE_PRESETS: dict[str, dict[str, str | None]] = {
    "tamazight_two_pass": {"pass1_language": "Tamazight", "pass2_language": "Tifinagh"},
    "standard_single_pass": {"pass1_language": "Tamazight", "pass2_language": None},
}

EXIT_ACTIVE_LOCK = 2
EXIT_INVALID_LOCK = 3
EXIT_LOCK_RACE = 4
EXIT_USAGE_ERROR = 5
EXIT_CORRUPT_STATE = 6
EXIT_PHASE_FAILURE = 7

LockIdentity = tuple[int, int]
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class ActiveRunLockError(RuntimeError):
    """Raised when a fresh run lock already exists."""


class InvalidRunLockError(RuntimeError):
    """Raised when a lock file cannot be parsed or validated."""


if LOCK_STALE_TTL_SECONDS <= LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS * 2:
    raise RuntimeError(
        "LOCK_STALE_TTL_SECONDS must be greater than 2x LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS"
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso8601(timestamp: str) -> float:
    normalized = timestamp.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized).timestamp()


def _build_lock_payload(run_id: str, started_at: str | None = None) -> dict[str, Any]:
    started = started_at or _utc_now_iso()
    return {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "started_at": started,
        "last_heartbeat_at": _utc_now_iso(),
        "run_id": run_id,
    }


def _validate_lock_payload(payload: dict[str, Any], lock_path: Path) -> dict[str, Any]:
    missing = [key for key in LOCK_FILE_FIELDS if key not in payload]
    if missing:
        raise InvalidRunLockError(f"{lock_path} missing required fields: {', '.join(missing)}")

    if not isinstance(payload["pid"], int):
        raise InvalidRunLockError(f"{lock_path} field 'pid' must be an integer")
    for key in ("host", "started_at", "last_heartbeat_at", "run_id"):
        if not isinstance(payload[key], str) or not payload[key].strip():
            raise InvalidRunLockError(f"{lock_path} field '{key}' must be a non-empty string")

    try:
        started_at = _parse_iso8601(payload["started_at"])
        last_heartbeat_at = _parse_iso8601(payload["last_heartbeat_at"])
    except ValueError as exc:
        raise InvalidRunLockError(f"{lock_path} contains invalid timestamp: {exc}") from exc

    if last_heartbeat_at < started_at:
        raise InvalidRunLockError(f"{lock_path} last_heartbeat_at must be >= started_at")
    if last_heartbeat_at > time.time() + LOCK_TIMESTAMP_SKEW_ALLOWANCE_SECONDS:
        raise InvalidRunLockError(
            f"{lock_path} last_heartbeat_at is too far in the future "
            f"(>{LOCK_TIMESTAMP_SKEW_ALLOWANCE_SECONDS}s skew allowance)"
        )

    return payload


def _read_lock(lock_path: Path) -> dict[str, Any]:
    try:
        raw = lock_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise InvalidRunLockError(f"Unable to read lock file {lock_path}: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InvalidRunLockError(f"{lock_path} is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise InvalidRunLockError(f"{lock_path} must contain a JSON object")

    return _validate_lock_payload(payload, lock_path)


def _lock_identity(path: Path) -> LockIdentity:
    stat = path.stat()
    return (stat.st_dev, stat.st_ino)


def _fsync_directory(directory: Path) -> None:
    try:
        dir_fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def read_jsonl(path: Path, *, strict: bool = True) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                message = f"Invalid JSONL row in {path} at line {line_number}: {exc}"
                if strict:
                    raise ValueError(message) from exc
                print(f"Warning: {message}; row skipped.", file=sys.stderr)
                continue

            if not isinstance(payload, dict):
                message = f"JSONL row in {path} at line {line_number} must be an object"
                if strict:
                    raise ValueError(message)
                print(f"Warning: {message}; row skipped.", file=sys.stderr)
                continue

            rows.append(payload)
    return rows


def atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")

    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

    _fsync_directory(path.parent)


def _mutate_paragraph_statuses(
    paths: dict[str, Path],
    *,
    next_status: str,
    eligible_statuses: set[str] | None = None,
    paragraph_ids: set[str] | None = None,
) -> None:
    if "paragraph_state" not in paths:
        print("Warning: paragraph_state path missing; skipping phase status mutation.", file=sys.stderr)
        return

    rows = read_jsonl(paths["paragraph_state"], strict=True)
    now_iso = _utc_now_iso()
    updated_rows: list[dict[str, Any]] = []
    did_change = False

    for row in rows:
        paragraph_id = str(row.get("paragraph_id", ""))
        if paragraph_ids is not None and not paragraph_id.strip():
            raise ValueError("Invalid paragraph_state row: missing non-empty paragraph_id for targeted mutation")
        excluded = row.get("excluded_by_policy", False) is True
        if "status" not in row:
            raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': missing required status")
        current_status = str(row["status"])
        assert_pipeline_state_allowed(current_status, excluded)

        should_update = (
            not excluded
            and (eligible_statuses is None or current_status in eligible_statuses)
            and (paragraph_ids is None or paragraph_id in paragraph_ids)
        )

        if should_update and current_status != next_status:
            assert_pipeline_transition_allowed(current_status, next_status, excluded)
            next_row = dict(row)
            next_row["status"] = next_status
            next_row["updated_at"] = now_iso
            updated_rows.append(next_row)
            did_change = True
        else:
            updated_rows.append(row)

    if did_change:
        atomic_write_jsonl(paths["paragraph_state"], updated_rows)


def _coerce_optional_list_field(
    paragraph_state_row: dict[str, Any],
    field_name: str,
    *,
    paragraph_id: str,
) -> list[Any] | None:
    if field_name not in paragraph_state_row:
        return None

    value = paragraph_state_row.get(field_name)
    if value is None:
        raise ValueError(
            f"rework_queued paragraph '{paragraph_id}' field '{field_name}' must not be null; use []"
        )
    if not isinstance(value, list):
        raise ValueError(
            f"rework_queued paragraph '{paragraph_id}' field '{field_name}' must be a list when provided"
        )
    return list(value)




def resolve_paragraph_review_state(
    prior_state: dict[str, Any],
    review_result: dict[str, Any],
    *,
    max_attempts: int,
) -> dict[str, Any]:
    blocking_issues = review_result.get("blocking_issues", [])
    if not isinstance(blocking_issues, list):
        raise ValueError("review_result.blocking_issues must be a list")
    if any(not isinstance(issue, str) for issue in blocking_issues):
        raise ValueError("review_result.blocking_issues items must be strings")

    scores = review_result.get("scores", {})
    if not isinstance(scores, dict):
        raise ValueError("review_result.scores must be a dict")
    for key, value in scores.items():
        if not isinstance(key, str):
            raise ValueError("review_result.scores keys must be strings")
        if isinstance(value, bool):
            raise ValueError("review_result.scores values must be numeric (bool is not allowed)")
        if not isinstance(value, (int, float)):
            raise ValueError("review_result.scores values must be numeric")

    review = ParagraphReviewAggregate(
        hard_fail=bool(review_result.get("hard_fail", False)),
        blocking_issues=tuple(blocking_issues),
        scores={k: float(v) for k, v in scores.items()},
    )
    transition = resolve_review_transition(prior_state, review, ParagraphPolicyConfig(max_attempts=max_attempts))
    next_row = dict(prior_state)
    next_row.update(transition.metadata_updates)

    transition_trace: list[str] = [transition.immediate_state]
    next_row["status"] = transition.immediate_state
    if transition.follow_up_state is not None:
        transition_trace.append(transition.follow_up_state)
        next_row["status"] = transition.follow_up_state

    next_row["review_state"] = transition.immediate_state
    next_row["routing_state"] = transition.follow_up_state
    next_row["review_transition_trace"] = transition_trace
    return next_row

def build_rework_queue_packet(paragraph_state_row: dict[str, Any]) -> dict[str, Any] | None:
    if paragraph_state_row.get("status") != "rework_queued":
        return None

    paragraph_id = paragraph_state_row.get("paragraph_id")
    if not isinstance(paragraph_id, str) or not paragraph_id.strip():
        raise ValueError("rework_queued paragraph is missing required 'paragraph_id'")

    content_hash = paragraph_state_row.get("content_hash")
    if not isinstance(content_hash, str) or not content_hash.strip():
        raise ValueError(f"rework_queued paragraph '{paragraph_id}' is missing required 'content_hash'")

    failure_reasons = _coerce_optional_list_field(
        paragraph_state_row,
        "failure_reasons",
        paragraph_id=paragraph_id,
    )
    if failure_reasons is None:
        failure_reasons = _coerce_optional_list_field(
            paragraph_state_row,
            "blocking_issues",
            paragraph_id=paragraph_id,
        ) or []

    required_fixes = _coerce_optional_list_field(
        paragraph_state_row,
        "required_fixes",
        paragraph_id=paragraph_id,
    )
    if required_fixes is None:
        required_fixes = list(failure_reasons)
    if not failure_reasons and not required_fixes:
        failure_reasons = ["unspecified_failure"]
        required_fixes = ["unspecified_failure"]

    failure_history = _coerce_optional_list_field(
        paragraph_state_row,
        "failure_history",
        paragraph_id=paragraph_id,
    ) or []

    return {
        "paragraph_id": paragraph_id,
        "content_hash": content_hash,
        "attempt": _coerce_attempt(paragraph_state_row.get("attempt", 0), paragraph_id=paragraph_id),
        "failure_reasons": failure_reasons,
        "failure_history": failure_history,
        "required_fixes": required_fixes,
    }


def _atomic_write_json(path: Path, payload: dict[str, Any], expected_identity: LockIdentity | None = None) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())

        if expected_identity is not None:
            try:
                current_identity = _lock_identity(path)
            except FileNotFoundError as exc:
                raise InvalidRunLockError(f"Refusing to heartbeat: {path} no longer exists") from exc
            if current_identity != expected_identity:
                raise InvalidRunLockError(
                    f"Refusing to heartbeat: {path} identity changed before write"
                )

        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

    _fsync_directory(path.parent)


def _canonical_packet_json(packet: dict[str, Any]) -> str:
    return json.dumps(packet, ensure_ascii=False, sort_keys=True)


def build_rework_queue_rows(
    paragraph_state_rows: list[dict[str, Any]],
    existing_queue_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return deduplicated rework queue rows for the current paragraph state snapshot.

    Only paragraphs currently in ``rework_queued`` are emitted (queue is a projection of
    current paragraph state, not an append-only work log). Existing queue rows are reused
    only when they are byte-for-byte equivalent after canonical JSON normalization,
    which guarantees reruns do not create duplicate rows for unchanged paragraph state.
    """

    existing_by_id: dict[str, list[dict[str, Any]]] = {}
    if existing_queue_rows:
        for row in existing_queue_rows:
            if not isinstance(row, dict):
                continue
            paragraph_id = row.get("paragraph_id")
            content_hash = row.get("content_hash")
            if not isinstance(paragraph_id, str) or not paragraph_id.strip():
                continue
            if not isinstance(content_hash, str) or not content_hash.strip():
                continue
            existing_by_id.setdefault(paragraph_id, []).append(row)

    queue_rows: list[dict[str, Any]] = []
    for state_row in paragraph_state_rows:
        packet = build_rework_queue_packet(state_row)
        if packet is None:
            continue

        paragraph_id = packet["paragraph_id"]
        existing_candidates = existing_by_id.get(paragraph_id, [])
        packet_json = _canonical_packet_json(packet)

        matching_existing = next(
            (candidate for candidate in existing_candidates if _canonical_packet_json(candidate) == packet_json),
            None,
        )
        if matching_existing is not None:
            queue_rows.append(matching_existing)
            continue

        queue_rows.append(packet)

    return sorted(queue_rows, key=lambda row: row["paragraph_id"])


def _cleanup_stale_temp_files(run_dir: Path) -> None:
    """Best-effort cleanup for abandoned atomic-write temp files under a run directory."""
    now = time.time()
    max_age = LOCK_STALE_TTL_SECONDS * 2

    try:
        candidates = list(run_dir.rglob(".*.tmp.*"))
    except OSError:
        return

    for candidate in candidates:
        if not candidate.is_file():
            continue
        try:
            age = now - candidate.stat().st_mtime
        except OSError:
            continue
        if age <= max_age:
            continue
        try:
            candidate.unlink()
        except OSError:
            continue


def _is_stale(payload: dict[str, Any]) -> bool:
    heartbeat_ts = _parse_iso8601(payload["last_heartbeat_at"])
    age_seconds = max(0.0, time.time() - heartbeat_ts)
    return age_seconds > LOCK_STALE_TTL_SECONDS


def _archive_stale_lock(
    lock_path: Path,
    payload: dict[str, Any],
    *,
    expected_identity: LockIdentity,
) -> Path | None:
    heartbeat_ts = int(_parse_iso8601(payload["last_heartbeat_at"]))
    suffix = 0

    while True:
        if suffix == 0:
            stale_name = f"RUNNING.stale.{heartbeat_ts}.lock"
        else:
            stale_name = f"RUNNING.stale.{heartbeat_ts}.{suffix}.lock"
        stale_path = lock_path.with_name(stale_name)

        if stale_path.exists():
            suffix += 1
            continue

        try:
            if _lock_identity(lock_path) != expected_identity:
                return None
            lock_path.replace(stale_path)
            _fsync_directory(lock_path.parent)
            return stale_path
        except FileNotFoundError:
            return None
        except FileExistsError:
            suffix += 1
            continue
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                suffix += 1
                continue
            if exc.errno == errno.ENOENT:
                return None
            raise


def acquire_run_lock(run_dir: Path, run_id: str) -> tuple[Path, dict[str, Any], LockIdentity]:
    run_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_stale_temp_files(run_dir)
    lock_path = run_dir / LOCK_FILE_NAME

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                existing_identity = _lock_identity(lock_path)
            except FileNotFoundError:
                time.sleep(LOCK_STALE_RETRY_BASE_SLEEP_SECONDS + random.uniform(0, 0.05))
                continue
            try:
                existing = _read_lock(lock_path)
            except InvalidRunLockError as exc:
                if not lock_path.exists():
                    time.sleep(LOCK_STALE_RETRY_BASE_SLEEP_SECONDS + random.uniform(0, 0.05))
                    continue
                existing = None
                last_read_exc: InvalidRunLockError = exc
                for _ in range(LOCK_READ_RETRY_ATTEMPTS):
                    time.sleep(LOCK_READ_RETRY_SLEEP_SECONDS)
                    try:
                        existing = _read_lock(lock_path)
                        break
                    except InvalidRunLockError as retry_exc:
                        last_read_exc = retry_exc
                        if not lock_path.exists():
                            break
                if existing is None:
                    raise InvalidRunLockError(f"Invalid active lock file at {lock_path}: {last_read_exc}") from last_read_exc
            if not _is_stale(existing):
                raise ActiveRunLockError(
                    "Run already active: fresh RUNNING.lock exists "
                    f"(run_id={existing['run_id']}, host={existing['host']}, pid={existing['pid']})."
                )
            stale_path = _archive_stale_lock(lock_path, existing, expected_identity=existing_identity)
            if stale_path is not None:
                print(
                    "Archived stale lock to "
                    f"{stale_path} (run_id={existing['run_id']}, host={existing['host']}, pid={existing['pid']}).",
                    file=sys.stderr,
                )
            time.sleep(LOCK_STALE_RETRY_BASE_SLEEP_SECONDS + random.uniform(0, 0.05))
            continue

        payload = _build_lock_payload(run_id)
        lock_blob = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(lock_blob)
            handle.flush()
            os.fsync(handle.fileno())

        _fsync_directory(run_dir)
        return lock_path, payload, _lock_identity(lock_path)


def write_lock_heartbeat(
    lock_path: Path,
    current_payload: dict[str, Any],
    run_id: str,
    expected_identity: LockIdentity,
) -> tuple[dict[str, Any], LockIdentity]:
    if not lock_path.exists():
        raise InvalidRunLockError(f"Refusing to heartbeat: {lock_path} does not exist")

    identity_before = _lock_identity(lock_path)
    if identity_before != expected_identity:
        raise InvalidRunLockError(f"Refusing to heartbeat: {lock_path} identity changed")

    on_disk = _read_lock(lock_path)
    if on_disk["run_id"] != run_id:
        raise InvalidRunLockError(
            f"Refusing to heartbeat lock for different run_id: expected {run_id}, found {on_disk['run_id']}"
        )

    payload = dict(current_payload)
    payload["last_heartbeat_at"] = _utc_now_iso()
    _atomic_write_json(lock_path, payload, expected_identity=identity_before)
    return payload, _lock_identity(lock_path)


def release_run_lock(lock_path: Path, run_id: str) -> bool:
    if not lock_path.exists():
        return False

    try:
        stat_before = lock_path.stat()
    except FileNotFoundError:
        return False

    payload = _read_lock(lock_path)
    if payload["run_id"] != run_id:
        raise InvalidRunLockError(
            f"Refusing to remove lock for different run_id: expected {run_id}, found {payload['run_id']}"
        )

    try:
        stat_after = lock_path.stat()
    except FileNotFoundError:
        return False

    if (stat_before.st_ino != stat_after.st_ino) or (stat_before.st_dev != stat_after.st_dev):
        raise InvalidRunLockError("Refusing to remove lock because file identity changed during release")

    try:
        lock_path.unlink()
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return False
        raise
    _fsync_directory(lock_path.parent)
    return True


def _coerce_attempt(value: Any, *, paragraph_id: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"paragraph '{paragraph_id}' has invalid boolean attempt value")
    if isinstance(value, int):
        return max(0, value)
    if value is None:
        return 0
    raise ValueError(f"paragraph '{paragraph_id}' has non-integer attempt value: {value!r}")


def _run_paths(run_id: str) -> dict[str, Path]:
    run_root = Path("runs") / run_id
    return {
        "run_root": run_root,
        "manifest": run_root / "manifest.json",
        "state_dir": run_root / "state",
        "phase_markers_dir": run_root / "state" / "phase_markers",
        "progress": run_root / "state" / "progress.json",
        "paragraph_state": run_root / "state" / "paragraph_state.jsonl",
        "rework_queue": run_root / "state" / "rework_queue.jsonl",
        "paragraph_scores": run_root / "state" / "paragraph_scores.jsonl",
        "source_pre": run_root / "source_pre",
        "pass1_pre": run_root / "pass1_pre",
        "pass2_pre": run_root / "pass2_pre",
        "final_dir": run_root / "final",
        "final_candidate": run_root / "final" / "candidate.md",
        "candidate_map": run_root / "final" / "candidate_map.jsonl",
        "final_output": run_root / "final" / "final.md",
        "final_pre": run_root / "final" / "final_pre",
        "review_normalized": run_root / "review" / "normalized",
        "review_grammar": run_root / "review" / "grammar",
        "gate_dir": run_root / "gate",
        "gate_report": run_root / "gate" / "gate_report.json",
        "review_blockers": run_root / "gate" / "review_blockers.json",
    }


def _ensure_manifest(
    paths: dict[str, Path],
    *,
    run_id: str,
    pipeline_profile: str,
    source: str,
    model: str,
    pass1_language: str,
    pass2_language: str | None,
    exclusion_policy: dict[str, Any] | None,
) -> None:
    desired = {
        "run_id": run_id,
        "pipeline_profile": pipeline_profile,
        "source": source,
        "model": model,
        "pass1_language": pass1_language,
        "pass2_language": pass2_language,
        "exclusion_policy": exclusion_policy,
    }
    if paths["manifest"].exists():
        try:
            existing = json.loads(paths["manifest"].read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest is not valid JSON for run '{run_id}': {paths['manifest']}") from exc
        if not isinstance(existing, dict):
            raise ValueError(f"manifest must be a JSON object for run '{run_id}': {paths['manifest']}")
        drift_fields = [field for field, expected in desired.items() if existing.get(field) != expected]
        if drift_fields:
            mismatches = ", ".join(
                f"{field}: existing={existing.get(field)!r} expected={desired[field]!r}" for field in drift_fields
            )
            raise ValueError(f"manifest drift detected for run '{run_id}': {mismatches}")
        return
    paths["run_root"].mkdir(parents=True, exist_ok=True)
    payload = {**desired, "created_at": _utc_now_iso()}
    _atomic_write_json(paths["manifest"], payload)


def _match_exclusion_policy(
    paragraph_row: dict[str, Any], exclusion_policy: dict[str, Any] | None
) -> tuple[bool, str | None]:
    if not exclusion_policy:
        return False, None

    paragraph_id = str(paragraph_row.get("paragraph_id") or paragraph_row.get("id") or "").strip()
    paragraph_text = str(paragraph_row.get("text", ""))

    templates = exclusion_policy.get("templates", [])
    if isinstance(templates, list):
        for template in templates:
            if not isinstance(template, dict):
                continue
            pattern = template.get("pattern")
            if not isinstance(pattern, str) or not pattern:
                continue
            flags = 0
            if template.get("ignore_case") is True:
                flags |= re.IGNORECASE
            if re.search(pattern, paragraph_text, flags=flags):
                reason = template.get("reason")
                if isinstance(reason, str) and reason.strip():
                    return True, reason.strip()
                return True, f"template:{pattern}"

    rules = exclusion_policy.get("rules", [])
    if isinstance(rules, list):
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            rule_paragraph_id = rule.get("paragraph_id")
            if not isinstance(rule_paragraph_id, str) or not rule_paragraph_id.strip():
                continue
            if paragraph_id != rule_paragraph_id.strip():
                continue
            reason = rule.get("reason")
            if isinstance(reason, str) and reason.strip():
                return True, reason.strip()
            return True, f"rule:{rule_paragraph_id.strip()}"

    return False, None


def _exec_phase_command(
    command: list[str],
    *,
    timeout_seconds: int | None = None,
    should_abort: Callable[[], Exception | None] | None = None,
) -> None:
    process = subprocess.Popen(command)
    start_monotonic = time.monotonic()

    try:
        while True:
            code = process.poll()
            if code is not None:
                if code != 0:
                    raise subprocess.CalledProcessError(code, command)
                return

            if should_abort is not None:
                abort_error = should_abort()
                if abort_error is not None:
                    raise abort_error

            if timeout_seconds is not None and timeout_seconds > 0:
                if time.monotonic() - start_monotonic > timeout_seconds:
                    raise TimeoutError(f"Phase command timed out after {timeout_seconds}s: {' '.join(command)}")

            time.sleep(0.2)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def _materialize_preprocessed_from_translation(source_pre: Path, translation_json: Path, output_pre: Path) -> None:
    normalize_translation_output(source_pre, translation_json, output_pre)


def _hash_content(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _language_output_dir_name(language: str) -> str:
    normalized = unicodedata.normalize("NFKC", language).strip().lower()
    sanitized_chars: list[str] = []
    previous_was_separator = False

    for char in normalized:
        category = unicodedata.category(char)
        if category.startswith("C"):
            continue

        if char.isspace() or char in {"/", "\\"}:
            if not previous_was_separator:
                sanitized_chars.append("_")
                previous_was_separator = True
            continue

        if char.isalnum() or char in {"_", "-"}:
            sanitized_chars.append(char)
            previous_was_separator = False
            continue

        if not previous_was_separator:
            sanitized_chars.append("_")
            previous_was_separator = True

    slug = "".join(sanitized_chars).strip("._-")
    slug = re.sub(r"_+", "_", slug)
    return slug or "language"


def _build_seed_state_rows(
    paragraph_rows: list[dict[str, Any]], *, exclusion_policy: dict[str, Any] | None
) -> tuple[list[dict[str, Any]], int]:
    state_rows: list[dict[str, Any]] = []
    skipped_rows = 0
    for row in paragraph_rows:
        paragraph_id = row.get("paragraph_id") or row.get("id")
        text = str(row.get("text", ""))
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            skipped_rows += 1
            continue
        excluded_by_policy, exclude_reason = _match_exclusion_policy(row, exclusion_policy)
        state_row: dict[str, Any] = {
            "paragraph_id": paragraph_id,
            "status": "ingested",
            "attempt": 0,
            "content_hash": _hash_content(text),
            "excluded_by_policy": excluded_by_policy,
        }
        if exclude_reason is not None:
            state_row["exclude_reason"] = exclude_reason

        state_rows.append(
            {
                **state_row,
            }
        )
    return state_rows, skipped_rows


def _phase_marker_path(paths: dict[str, Path], phase_name: str) -> Path:
    return paths["phase_markers_dir"] / f"phase_{phase_name}.done"


def _format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _write_progress(
    paths: dict[str, Path],
    *,
    run_id: str,
    mode: str,
    current_phase: str,
    phase_state: str,
    phase_started_at: str,
    phase_finished_at: str | None,
    last_heartbeat_at: str | None,
) -> None:
    payload = {
        "run_id": run_id,
        "mode": mode,
        "current_phase": current_phase,
        "phase_state": phase_state,
        "phase_started_at": phase_started_at,
        "phase_finished_at": phase_finished_at,
        "last_heartbeat_at": last_heartbeat_at,
        "updated_at": _utc_now_iso(),
    }
    _atomic_write_json(paths["progress"], payload)


def _read_progress(paths: dict[str, Path]) -> dict[str, Any] | None:
    progress_path = paths["progress"]
    if not progress_path.exists():
        return None
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _compute_status_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    required_merge_blockers = 0
    required_total = 0
    done = 0
    blocker_states = {"rework_queued", "manual_review_required"}

    for row in rows:
        status = row.get("status", "unknown")
        if isinstance(status, str):
            status_counts[status] = status_counts.get(status, 0) + 1

        excluded = row.get("excluded_by_policy", False) is True
        if excluded:
            continue
        required_total += 1
        if status == "ready_to_merge":
            done += 1
        if status in blocker_states:
            required_merge_blockers += 1

    in_flight = max(0, required_total - done - required_merge_blockers)
    progress_percent = 0.0 if required_total == 0 else (done / required_total) * 100

    return {
        "total": len(rows),
        "required_total": required_total,
        "required_merge_blockers": required_merge_blockers,
        "done": done,
        "in_flight": in_flight,
        "progress_percent": progress_percent,
        "status_counts": dict(sorted(status_counts.items())),
    }


def _sentence_spans_for_final(text: str) -> list[tuple[str, int, int]]:
    spans: list[tuple[str, int, int]] = []
    for match in re.finditer(r"[^.!?؟。！？\n]+(?:[.!?؟。！？]+|$)", text, flags=re.MULTILINE):
        raw_sentence = match.group(0)
        if not raw_sentence.strip():
            continue

        left_trim = len(raw_sentence) - len(raw_sentence.lstrip())
        right_trim = len(raw_sentence) - len(raw_sentence.rstrip())
        start = match.start() + left_trim
        end = match.end() - right_trim
        if end <= start:
            continue

        sentence = text[start:end]
        spans.append((sentence, start, end))

    if not spans and text.strip():
        clean = text.strip()
        start = text.find(clean)
        spans.append((clean, start, start + len(clean)))
    return spans


def _token_spans_for_final(text: str) -> list[tuple[str, int, int]]:
    return [(m.group(0), m.start(), m.end()) for m in re.finditer(r"\S+", text)]


def _coerce_paragraph_index(value: Any, *, paragraph_id: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Invalid paragraph_index for '{paragraph_id}': boolean values are not allowed")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if normalized and normalized.lstrip("-").isdigit():
            return int(normalized)
    raise ValueError(f"Invalid paragraph_index for '{paragraph_id}': {value!r}")


def _resolve_merge_paragraphs_path(paths: dict[str, Path]) -> Path:
    pass2_paragraphs = paths["pass2_pre"] / "paragraphs.jsonl"
    if pass2_paragraphs.exists():
        return pass2_paragraphs
    pass1_paragraphs = paths["pass1_pre"] / "paragraphs.jsonl"
    if pass1_paragraphs.exists():
        return pass1_paragraphs
    raise FileNotFoundError(
        "Missing merge input paragraphs.jsonl: expected one of "
        f"{pass2_paragraphs} or {pass1_paragraphs}"
    )


def _read_manifest(paths: dict[str, Path], *, run_id: str) -> dict[str, Any]:
    manifest_path = paths["manifest"]
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing manifest for run '{run_id}': {manifest_path}")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"manifest is not valid JSON for run '{run_id}': {manifest_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"manifest must be a JSON object for run '{run_id}': {manifest_path}")
    return payload


def _resolve_review_pre_dir(manifest: dict[str, Any], override_profile: str | None = None) -> str:
    if override_profile is not None:
        profile = override_profile.strip()
    else:
        configured_review_pre_dir = manifest.get("review_pre_dir")
        if isinstance(configured_review_pre_dir, str) and configured_review_pre_dir.strip():
            return configured_review_pre_dir.strip()
        raw_profile = manifest.get("pipeline_profile")
        profile = raw_profile.strip() if isinstance(raw_profile, str) else None

    if not profile:
        raise ValueError(
            "Missing pipeline profile for phase D review inputs. Remediation: set manifest pipeline_profile "
            "(or pass --pipeline-profile) to one of: tamazight_two_pass, standard_single_pass. "
            "Alternatively set manifest review_pre_dir directly (for example: pass1_pre or pass2_pre)."
        )

    if profile == "tamazight_two_pass":
        return "pass2_pre"
    if profile == "standard_single_pass":
        return "pass1_pre"

    raise ValueError(
        "Unknown pipeline profile for phase D review inputs: "
        f"{profile!r}. Remediation: set manifest pipeline_profile (or pass --pipeline-profile) "
        "to one of: tamazight_two_pass, standard_single_pass, or set manifest review_pre_dir directly."
    )
def _load_exclusion_policy(path: str | None) -> dict[str, Any] | None:
    if path is None:
        return None
    policy_path = Path(path)
    if not policy_path.exists():
        raise ValueError(f"exclusion policy file not found: {policy_path}")
    try:
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"exclusion policy file is not valid JSON: {policy_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"exclusion policy must be a JSON object: {policy_path}")
    return payload


def _resolve_rework_runtime_config(paths: dict[str, Path], args: argparse.Namespace) -> tuple[str, str | None, str]:
    manifest = _read_manifest(paths, run_id=args.run_id)

    pass1_language = _normalize_optional_language(getattr(args, "pass1_language", None))
    if pass1_language is None:
        pass1_language = _normalize_optional_language(manifest.get("pass1_language"))
    if pass1_language is None:
        raise ValueError("Missing pass1 language for rework-only mode; provide --pass1-language or a manifest value")

    pass2_language = _normalize_optional_language(getattr(args, "pass2_language", None))
    if getattr(args, "pass2_language", None) is None:
        pass2_language = _normalize_optional_language(manifest.get("pass2_language"))

    model = getattr(args, "model", None)
    if model is None:
        manifest_model = manifest.get("model")
        if isinstance(manifest_model, str) and manifest_model.strip():
            model = manifest_model.strip()
    if model is None:
        raise ValueError("Missing model for rework-only mode; provide --model or a manifest value")

    return pass1_language, pass2_language, model


def _build_rework_subset_rows(source_rows: list[dict[str, Any]], paragraph_ids: set[str]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        paragraph_id = row.get("paragraph_id")
        if isinstance(paragraph_id, str) and paragraph_id.strip():
            by_id[paragraph_id] = row

    missing = sorted(paragraph_ids - set(by_id))
    if missing:
        raise ValueError(f"Rework queue references unknown paragraph_id values: {', '.join(missing)}")
    return [by_id[paragraph_id] for paragraph_id in sorted(paragraph_ids)]


def _build_merged_paragraph_rows(
    rows: list[dict[str, Any]], replacements: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        paragraph_id_raw = row.get("paragraph_id")
        if not isinstance(paragraph_id_raw, str) or not paragraph_id_raw.strip():
            raise ValueError("Canonical paragraph row is missing non-empty string paragraph_id")
        paragraph_id = paragraph_id_raw.strip()
        replacement = replacements.get(paragraph_id)
        if replacement is None:
            out_rows.append(row)
            continue
        out_rows.append(replacement)
        seen.add(paragraph_id)
    missing = sorted(set(replacements) - seen)
    if missing:
        raise ValueError(f"Canonical paragraphs missing queued paragraph_id values: {', '.join(missing)}")
    return out_rows


def _validate_rework_queue_lineage(paths: dict[str, Path], queue_rows: list[dict[str, Any]], queued_ids: set[str]) -> None:
    queue_counts: dict[str, int] = {}
    for queue_row in queue_rows:
        paragraph_id = queue_row.get("paragraph_id")
        if isinstance(paragraph_id, str) and paragraph_id.strip():
            queue_counts[paragraph_id] = queue_counts.get(paragraph_id, 0) + 1
    duplicates = sorted(paragraph_id for paragraph_id, count in queue_counts.items() if count > 1)
    if duplicates:
        raise ValueError("Duplicate paragraph_id entries in rework_queue: " + ", ".join(duplicates))

    source_rows = read_jsonl(paths["source_pre"] / "paragraphs.jsonl", strict=True)
    source_hash_by_id: dict[str, str] = {}
    for row in source_rows:
        paragraph_id = row.get("paragraph_id")
        content_hash = row.get("content_hash")
        if isinstance(paragraph_id, str) and paragraph_id.strip() and isinstance(content_hash, str) and content_hash.strip():
            source_hash_by_id[paragraph_id] = content_hash

    for queue_row in queue_rows:
        paragraph_id = queue_row.get("paragraph_id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            continue
        if paragraph_id not in queued_ids:
            continue
        queue_hash = queue_row.get("content_hash")
        source_hash = source_hash_by_id.get(paragraph_id)
        if not isinstance(queue_hash, str) or not queue_hash.strip():
            raise ValueError(f"rework queue row for '{paragraph_id}' is missing non-empty content_hash")
        if source_hash is None:
            raise ValueError(f"Rework queue references unknown paragraph_id '{paragraph_id}' in source_pre")
        if queue_hash != source_hash:
            raise ValueError(
                f"Rework queue content_hash mismatch for '{paragraph_id}': queue={queue_hash!r} source_pre={source_hash!r}"
            )

    state_rows = [_normalize_paragraph_state_row(row) for row in read_jsonl(paths["paragraph_state"], strict=True)]
    state_by_id = {
        str(row.get("paragraph_id")): row
        for row in state_rows
        if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
    }
    missing_state = sorted(paragraph_id for paragraph_id in queued_ids if paragraph_id not in state_by_id)
    if missing_state:
        raise ValueError(f"Rework queue paragraph_id values missing from paragraph_state: {', '.join(missing_state)}")

    for paragraph_id in sorted(queued_ids):
        state_row = state_by_id[paragraph_id]
        if state_row.get("status") != "rework_queued":
            raise ValueError(
                f"Rework queue paragraph '{paragraph_id}' must be in status 'rework_queued' "
                f"before rework stage; got {state_row.get('status')!r}"
            )


def _normalize_paragraph_state_row(row: dict[str, Any]) -> dict[str, Any]:
    paragraph_id = row.get("paragraph_id")
    if not isinstance(paragraph_id, str) or not paragraph_id.strip():
        raise ValueError("Invalid paragraph_state row: missing non-empty paragraph_id")

    content_hash = row.get("content_hash")
    if not isinstance(content_hash, str) or not content_hash.strip():
        raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': missing non-empty content_hash")

    status = row.get("status")
    if not isinstance(status, str) or not status.strip():
        raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': missing non-empty status")

    normalized = dict(row)
    normalized["attempt"] = _coerce_attempt(row.get("attempt", 0), paragraph_id=paragraph_id)

    blocking_issues = row.get("blocking_issues", [])
    if blocking_issues is None:
        blocking_issues = []
    if not isinstance(blocking_issues, list):
        raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': blocking_issues must be a list")
    normalized["blocking_issues"] = list(blocking_issues)

    failure_history = row.get("failure_history", [])
    if failure_history is None:
        failure_history = []
    if not isinstance(failure_history, list):
        raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': failure_history must be a list")
    normalized["failure_history"] = list(failure_history)

    excluded = row.get("excluded_by_policy", False) is True
    normalized["excluded_by_policy"] = excluded
    assert_pipeline_state_allowed(status, excluded)
    return normalized


def _assert_rework_stage_pipeline_state(paths: dict[str, Path]) -> None:
    rows = [_normalize_paragraph_state_row(row) for row in read_jsonl(paths["paragraph_state"], strict=True)]
    disallowed_states = {
        "translated_pass1",
        "translated_pass2",
        "candidate_assembled",
        "review_in_progress",
    }
    blocking_ids = [
        str(row["paragraph_id"])
        for row in rows
        if not bool(row.get("excluded_by_policy", False)) and str(row.get("status")) in disallowed_states
    ]
    if blocking_ids:
        raise ValueError(
            "Rework stage cannot run while paragraphs are still in active translation/review states: "
            + ", ".join(sorted(blocking_ids))
        )


def _validate_candidate_paragraph_indices(rows: list[dict[str, Any]], *, paragraphs_path: Path) -> None:
    seen_indices: set[int] = set()
    for row in rows:
        paragraph_id = str(row.get("paragraph_id", "<unknown>"))
        index = _coerce_paragraph_index(row.get("paragraph_index"), paragraph_id=paragraph_id)
        if index in seen_indices:
            raise ValueError(
                f"Duplicate paragraph_index={index} while rebuilding candidate from {paragraphs_path}"
            )
        seen_indices.add(index)

    if seen_indices:
        expected = set(range(1, len(rows) + 1))
        if seen_indices != expected:
            raise ValueError(
                f"paragraph_index values must be contiguous 1..N in {paragraphs_path}; "
                f"got {sorted(seen_indices)}"
            )


def _mark_reworked_ready_for_review(paths: dict[str, Path], paragraph_ids: set[str]) -> None:
    rows = [_normalize_paragraph_state_row(row) for row in read_jsonl(paths["paragraph_state"], strict=True)]
    updated_rows: list[dict[str, Any]] = []
    now_iso = _utc_now_iso()
    transitioned_ids: set[str] = set()
    for row in rows:
        paragraph_id = str(row.get("paragraph_id", ""))
        if paragraph_id in paragraph_ids and row.get("status") == "rework_queued":
            excluded = row.get("excluded_by_policy", False) is True
            assert_pipeline_transition_allowed("rework_queued", "reworked", excluded)
            next_row = dict(row)
            next_row["status"] = "reworked"
            next_row["updated_at"] = now_iso
            updated_rows.append(next_row)
            transitioned_ids.add(paragraph_id)
        else:
            updated_rows.append(row)
    missing = sorted(paragraph_ids - transitioned_ids)
    if missing:
        raise ValueError(
            "Expected to transition queued paragraph(s) to reworked but did not: " + ", ".join(missing)
        )
    atomic_write_jsonl(paths["paragraph_state"], updated_rows)


def run_rework_translation_stage(
    paths: dict[str, Path],
    *,
    pass1_language: str,
    pass2_language: str | None,
    model: str,
    phase_timeout_seconds: int,
    should_abort: Callable[[], Exception | None],
) -> set[str]:
    _assert_rework_stage_pipeline_state(paths)

    if not paths["rework_queue"].exists():
        return set()

    queue_rows = read_jsonl(paths["rework_queue"], strict=True)
    queued_ids = {
        str(row.get("paragraph_id"))
        for row in queue_rows
        if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
    }
    if not queued_ids:
        return set()

    _validate_rework_queue_lineage(paths, queue_rows, queued_ids)

    source_rows = read_jsonl(paths["source_pre"] / "paragraphs.jsonl", strict=True)
    subset_rows = _build_rework_subset_rows(source_rows, queued_ids)

    canonical_pass1_rows = read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl", strict=True)
    canonical_pass2_path = paths["pass2_pre"] / "paragraphs.jsonl"
    canonical_pass2_rows = read_jsonl(canonical_pass2_path, strict=True) if canonical_pass2_path.exists() else []

    with tempfile.TemporaryDirectory(prefix="rework_loop_", dir=paths["run_root"]) as tmp_dir_raw:
        tmp_dir = Path(tmp_dir_raw)
        subset_source_pre = tmp_dir / "source_pre_subset"
        subset_source_pre.mkdir(parents=True, exist_ok=True)
        atomic_write_jsonl(subset_source_pre / "paragraphs.jsonl", subset_rows)

        pass1_translate_output = tmp_dir / "translate_pass1"
        _exec_phase_command(
            [
                sys.executable,
                "scripts/translate.py",
                "--language",
                pass1_language,
                "--model",
                model,
                "--preprocessed",
                str(subset_source_pre),
                "--output-root",
                str(pass1_translate_output),
            ],
            timeout_seconds=phase_timeout_seconds or None,
            should_abort=should_abort,
        )
        pass1_translation_json = pass1_translate_output / _language_output_dir_name(pass1_language) / "translation.json"
        pass1_pre_subset = tmp_dir / "pass1_pre_subset"
        _materialize_preprocessed_from_translation(subset_source_pre, pass1_translation_json, pass1_pre_subset)

        pass1_replacements = {
            str(row.get("paragraph_id")): row
            for row in read_jsonl(pass1_pre_subset / "paragraphs.jsonl", strict=True)
            if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
        }
        merged_pass1_rows = _build_merged_paragraph_rows(canonical_pass1_rows, pass1_replacements)

        final_replacements = pass1_replacements
        merged_pass2_rows: list[dict[str, Any]] | None = None
        if pass2_language:
            pass2_translate_output = tmp_dir / "translate_pass2"
            _exec_phase_command(
                [
                    sys.executable,
                    "scripts/translate.py",
                    "--language",
                    pass2_language,
                    "--model",
                    model,
                    "--preprocessed",
                    str(pass1_pre_subset),
                    "--output-root",
                    str(pass2_translate_output),
                ],
                timeout_seconds=phase_timeout_seconds or None,
                should_abort=should_abort,
            )
            pass2_translation_json = pass2_translate_output / _language_output_dir_name(pass2_language) / "translation.json"
            pass2_pre_subset = tmp_dir / "pass2_pre_subset"
            _materialize_preprocessed_from_translation(pass1_pre_subset, pass2_translation_json, pass2_pre_subset)
            final_replacements = {
                str(row.get("paragraph_id")): row
                for row in read_jsonl(pass2_pre_subset / "paragraphs.jsonl", strict=True)
                if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
            }
            merged_pass2_rows = _build_merged_paragraph_rows(canonical_pass2_rows, final_replacements)

    atomic_write_jsonl(paths["pass1_pre"] / "paragraphs.jsonl", merged_pass1_rows)
    if merged_pass2_rows is not None:
        atomic_write_jsonl(paths["pass2_pre"] / "paragraphs.jsonl", merged_pass2_rows)

    if pass2_language:
        merge_input_path = paths["pass2_pre"] / "paragraphs.jsonl"
    else:
        merge_input_path = paths["pass1_pre"] / "paragraphs.jsonl"
    _validate_candidate_paragraph_indices(read_jsonl(merge_input_path, strict=True), paragraphs_path=merge_input_path)
    assemble_candidate(merge_input_path, paths["final_candidate"], paths["candidate_map"])
    _mark_reworked_ready_for_review(paths, queued_ids)
    return queued_ids


def _build_final_pre_bundle(output_dir: Path, paragraph_rows: list[dict[str, Any]]) -> None:
    sentence_rows: list[dict[str, Any]] = []
    word_rows: list[dict[str, Any]] = []
    tokenized_paragraphs: list[dict[str, Any]] = []

    manuscript_id = str(paragraph_rows[0].get("manuscript_id", output_dir.parent.name)) if paragraph_rows else output_dir.parent.name
    source_value = str(paragraph_rows[0].get("source", str(output_dir.parent))) if paragraph_rows else str(output_dir.parent)

    paragraph_start = 0
    sentence_counter = 1
    word_counter = 1
    token_counter = 1

    for idx, paragraph_row in enumerate(paragraph_rows, start=1):
        paragraph_text = str(paragraph_row.get("text", ""))
        paragraph_id = str(paragraph_row.get("paragraph_id") or paragraph_row.get("id") or f"{manuscript_id}-p{idx:04d}")
        sentence_spans = _sentence_spans_for_final(paragraph_text)
        paragraph_tokens: list[dict[str, Any]] = []

        for sentence_text, sent_local_start, sent_local_end in sentence_spans:
            sentence_id = f"{manuscript_id}-s{sentence_counter:06d}"
            sentence_record = {
                "id": sentence_id,
                "order": len(sentence_rows),
                "prev_id": sentence_rows[-1]["id"] if sentence_rows else None,
                "next_id": None,
                "text": sentence_text,
                "start_char": paragraph_start + sent_local_start,
                "end_char": paragraph_start + sent_local_end,
                "paragraph_id": paragraph_id,
                "manuscript_id": manuscript_id,
                "source": source_value,
            }
            if sentence_rows:
                sentence_rows[-1]["next_id"] = sentence_id
            sentence_rows.append(sentence_record)

            for token, token_local_start, token_local_end in _token_spans_for_final(sentence_text):
                word_id = f"{manuscript_id}-w{word_counter:06d}"
                word_record = {
                    "id": word_id,
                    "order": len(word_rows),
                    "prev_id": word_rows[-1]["id"] if word_rows else None,
                    "next_id": None,
                    "text": token,
                    "start_char": paragraph_start + sent_local_start + token_local_start,
                    "end_char": paragraph_start + sent_local_start + token_local_end,
                    "sentence_id": sentence_id,
                    "paragraph_id": paragraph_id,
                    "manuscript_id": manuscript_id,
                    "source": source_value,
                }
                if word_rows:
                    word_rows[-1]["next_id"] = word_id
                word_rows.append(word_record)
                word_counter += 1

            # Increment exactly once per sentence (never per token).
            sentence_counter += 1

        for local_index, (token, start_char, end_char) in enumerate(_token_spans_for_final(paragraph_text)):
            paragraph_tokens.append(
                {
                    "token_id": f"{manuscript_id}-t{token_counter:06d}",
                    "text": token,
                    "start_char": paragraph_start + start_char,
                    "end_char": paragraph_start + end_char,
                    "global_index": token_counter,
                    "local_index": local_index,
                }
            )
            token_counter += 1

        tokenized_paragraphs.append(
            {
                "paragraph_id": paragraph_id,
                "order": idx - 1,
                "text": paragraph_text,
                "tokens": paragraph_tokens,
            }
        )
        paragraph_start += len(paragraph_text) + PARAGRAPH_SEPARATOR_LEN

    manuscript_tokens = {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "source": source_value,
        "created_at": _utc_now_iso(),
        "tokenization": {
            "method": "regex",
            "model": "whitespace+punctuation",
            "notes": "Canonical final_pre bundle generated from merge-eligible paragraph rows.",
        },
        "paragraphs": tokenized_paragraphs,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_jsonl(output_dir / "paragraphs.jsonl", paragraph_rows)
    atomic_write_jsonl(output_dir / "sentences.jsonl", sentence_rows)
    atomic_write_jsonl(output_dir / "words.jsonl", word_rows)
    _atomic_write_json(output_dir / "manuscript_tokens.json", manuscript_tokens)


def _print_status_report(report: dict[str, Any], *, run_id: str) -> None:
    print(f"run_id={run_id}")
    print(
        "progress="
        f"{report['progress_percent']:.1f}% "
        f"done={report['done']} "
        f"blocked={report['required_merge_blockers']} "
        f"inflight={report['in_flight']} "
        f"total_required={report['required_total']}"
    )
    print(f"total={report['total']}")
    print(f"required_total={report['required_total']}")
    print(f"required_merge_blockers={report['required_merge_blockers']}")
    for status, count in report["status_counts"].items():
        print(f"status[{status}]={count}")


def run_phase_a(
    paths: dict[str, Path],
    *,
    run_id: str,
    pipeline_profile: str,
    pass1_language: str,
    pass2_language: str | None,
    source: str,
    model: str,
    exclusion_policy: dict[str, Any] | None,
    allow_exclusion_policy_reauthorization: bool,
    phase_timeout_seconds: int,
    should_abort: Callable[[], Exception | None],
) -> None:
    paths["source_pre"].mkdir(parents=True, exist_ok=True)
    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    _ensure_manifest(
        paths,
        run_id=run_id,
        pipeline_profile=pipeline_profile,
        source=source,
        model=model,
        pass1_language=pass1_language,
        pass2_language=pass2_language,
        exclusion_policy=exclusion_policy,
    )

    _exec_phase_command(
        [sys.executable, "scripts/pre_processing.py", source, "--output-dir", str(paths["source_pre"])],
        timeout_seconds=phase_timeout_seconds or None,
        should_abort=should_abort,
    )

    paragraph_rows = read_jsonl(paths["source_pre"] / "paragraphs.jsonl", strict=True)
    state_rows, skipped_rows = _build_seed_state_rows(paragraph_rows, exclusion_policy=exclusion_policy)
    if not paths["paragraph_state"].exists():
        if skipped_rows:
            print(
                f"Warning: skipped {skipped_rows} paragraph row(s) without valid paragraph_id while seeding state.",
                file=sys.stderr,
            )
        if not state_rows and paragraph_rows:
            raise ValueError("Unable to seed paragraph state: no valid paragraph IDs found in source_pre/paragraphs.jsonl")
        atomic_write_jsonl(paths["paragraph_state"], state_rows)
    else:
        existing_rows = read_jsonl(paths["paragraph_state"], strict=True)
        existing_signature: dict[str, str] = {}
        for row in existing_rows:
            paragraph_id = row.get("paragraph_id")
            content_hash = row.get("content_hash")
            if not isinstance(paragraph_id, str) or not paragraph_id.strip():
                raise ValueError("Invalid paragraph_state row: missing non-empty paragraph_id")
            if not isinstance(content_hash, str) or not content_hash.strip():
                raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': missing non-empty content_hash")
            if paragraph_id in existing_signature:
                raise ValueError(f"Invalid paragraph_state.jsonl: duplicate paragraph_id '{paragraph_id}'")
            existing_signature[paragraph_id] = content_hash

        new_signature: dict[str, str] = {}
        for row in state_rows:
            paragraph_id = row["paragraph_id"]
            if paragraph_id in new_signature:
                raise ValueError(f"Invalid source_pre/paragraphs.jsonl: duplicate paragraph_id '{paragraph_id}'")
            new_signature[paragraph_id] = row["content_hash"]
        if existing_signature != new_signature:
            raise ValueError(
                "paragraph_state.jsonl drift detected against source_pre/paragraphs.jsonl for immutable run_id; "
                "use a new --run-id for changed inputs"
            )

        existing_policy_signature: dict[str, tuple[bool, str | None]] = {}
        for row in existing_rows:
            paragraph_id = str(row.get("paragraph_id", "")).strip()
            existing_policy_signature[paragraph_id] = (
                row.get("excluded_by_policy", False) is True,
                row.get("exclude_reason") if isinstance(row.get("exclude_reason"), str) else None,
            )

        new_policy_signature: dict[str, tuple[bool, str | None]] = {
            row["paragraph_id"]: (
                row.get("excluded_by_policy", False) is True,
                row.get("exclude_reason") if isinstance(row.get("exclude_reason"), str) else None,
            )
            for row in state_rows
        }

        if existing_policy_signature != new_policy_signature:
            if not allow_exclusion_policy_reauthorization:
                raise ValueError(
                    "exclusion policy drift detected for immutable run_id; "
                    "rerun with --allow-exclusion-policy-reauthorization to apply updated exclusions"
                )

            now_iso = _utc_now_iso()
            rewired_rows: list[dict[str, Any]] = []
            for row in existing_rows:
                paragraph_id = str(row.get("paragraph_id", "")).strip()
                next_policy = new_policy_signature.get(paragraph_id)
                if next_policy is None:
                    rewired_rows.append(row)
                    continue

                next_excluded, next_reason = next_policy
                updated = dict(row)
                updated["excluded_by_policy"] = next_excluded
                if next_reason is None:
                    updated.pop("exclude_reason", None)
                else:
                    updated["exclude_reason"] = next_reason
                updated["updated_at"] = now_iso
                rewired_rows.append(updated)

            atomic_write_jsonl(paths["paragraph_state"], rewired_rows)


def run_phase_b(
    paths: dict[str, Path],
    *,
    pass1_language: str,
    model: str,
    phase_timeout_seconds: int,
    should_abort: Callable[[], Exception | None],
    subset_paragraph_ids: set[str] | None = None,
) -> None:
    if subset_paragraph_ids is not None and not subset_paragraph_ids:
        return

    preprocessed_source = paths["source_pre"]
    canonical_rows: list[dict[str, Any]] | None = None
    selected_ids: set[str] | None = None
    tmp_dir_context: tempfile.TemporaryDirectory[str] | None = None
    if subset_paragraph_ids is not None:
        selected_ids = set(subset_paragraph_ids)
        canonical_rows = read_jsonl(paths["source_pre"] / "paragraphs.jsonl", strict=True)
        subset_rows = _build_rework_subset_rows(canonical_rows, selected_ids)
        tmp_dir_context = tempfile.TemporaryDirectory(prefix="phase_b_subset_", dir=paths["run_root"])
        subset_preprocessed = Path(tmp_dir_context.name) / "source_pre_subset"
        _build_final_pre_bundle(subset_preprocessed, subset_rows)
        preprocessed_source = subset_preprocessed

    translate_output = paths["run_root"] / "translate_pass1"
    try:
        _exec_phase_command(
            [
                sys.executable,
                "scripts/translate.py",
                "--language",
                pass1_language,
                "--model",
                model,
                "--preprocessed",
                str(preprocessed_source),
                "--output-root",
                str(translate_output),
            ],
            timeout_seconds=phase_timeout_seconds or None,
            should_abort=should_abort,
        )
        translation_json = translate_output / _language_output_dir_name(pass1_language) / "translation.json"
        if selected_ids is None:
            _materialize_preprocessed_from_translation(paths["source_pre"], translation_json, paths["pass1_pre"])
            _mutate_paragraph_statuses(paths, next_status="translated_pass1", eligible_statuses={"ingested", "reworked"})
            return

        subset_out_pre = Path(tmp_dir_context.name) / "pass1_pre_subset"
        _materialize_preprocessed_from_translation(preprocessed_source, translation_json, subset_out_pre)
        replacements = {
            str(row.get("paragraph_id")): row
            for row in read_jsonl(subset_out_pre / "paragraphs.jsonl", strict=True)
            if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
        }
        merged_rows = _build_merged_paragraph_rows(canonical_rows or [], replacements)
        _build_final_pre_bundle(paths["pass1_pre"], merged_rows)
        _mutate_paragraph_statuses(
            paths,
            next_status="translated_pass1",
            eligible_statuses={"ingested", "reworked"},
            paragraph_ids=selected_ids,
        )
    finally:
        if tmp_dir_context is not None:
            tmp_dir_context.cleanup()


def run_phase_c(
    paths: dict[str, Path], *, pass2_language: str | None, model: str, phase_timeout_seconds: int, should_abort: Callable[[], Exception | None], subset_paragraph_ids: set[str] | None = None
) -> None:
    if subset_paragraph_ids is not None and not subset_paragraph_ids:
        return

    if pass2_language:
        preprocessed_source = paths["pass1_pre"]
        canonical_pass1_rows: list[dict[str, Any]] | None = None
        selected_ids: set[str] | None = None
        tmp_dir_context: tempfile.TemporaryDirectory[str] | None = None
        if subset_paragraph_ids is not None:
            selected_ids = set(subset_paragraph_ids)
            canonical_pass1_rows = read_jsonl(paths["pass1_pre"] / "paragraphs.jsonl", strict=True)
            subset_rows = _build_rework_subset_rows(canonical_pass1_rows, selected_ids)
            tmp_dir_context = tempfile.TemporaryDirectory(prefix="phase_c_subset_", dir=paths["run_root"])
            subset_preprocessed = Path(tmp_dir_context.name) / "pass1_pre_subset"
            _build_final_pre_bundle(subset_preprocessed, subset_rows)
            preprocessed_source = subset_preprocessed

        translate_output = paths["run_root"] / "translate_pass2"
        try:
            _exec_phase_command(
                [
                    sys.executable,
                    "scripts/translate.py",
                    "--language",
                    pass2_language,
                    "--model",
                    model,
                    "--preprocessed",
                    str(preprocessed_source),
                    "--output-root",
                    str(translate_output),
                ],
                timeout_seconds=phase_timeout_seconds or None,
                should_abort=should_abort,
            )
            translation_json = translate_output / _language_output_dir_name(pass2_language) / "translation.json"
            if selected_ids is None:
                _materialize_preprocessed_from_translation(paths["pass1_pre"], translation_json, paths["pass2_pre"])
                _mutate_paragraph_statuses(paths, next_status="translated_pass2", eligible_statuses={"translated_pass1"})
                return

            subset_out_pre = Path(tmp_dir_context.name) / "pass2_pre_subset"
            _materialize_preprocessed_from_translation(preprocessed_source, translation_json, subset_out_pre)
            replacements = {
                str(row.get("paragraph_id")): row
                for row in read_jsonl(subset_out_pre / "paragraphs.jsonl", strict=True)
                if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
            }
            canonical_pass2_path = paths["pass2_pre"] / "paragraphs.jsonl"
            if canonical_pass2_path.exists():
                base_rows = read_jsonl(canonical_pass2_path, strict=True)
            else:
                base_rows = canonical_pass1_rows or []
            merged_rows = _build_merged_paragraph_rows(base_rows, replacements)
            _build_final_pre_bundle(paths["pass2_pre"], merged_rows)
            _mutate_paragraph_statuses(
                paths,
                next_status="translated_pass2",
                eligible_statuses={"translated_pass1"},
                paragraph_ids=selected_ids,
            )
            return
        finally:
            if tmp_dir_context is not None:
                tmp_dir_context.cleanup()

    paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
    for artifact_name in ("paragraphs.jsonl", "sentences.jsonl", "words.jsonl", "manuscript_tokens.json"):
        source_path = paths["pass1_pre"] / artifact_name
        if source_path.exists():
            shutil.copy2(source_path, paths["pass2_pre"] / artifact_name)
    # Single-pass mode preserves translated_pass1 status by design; no state mutation needed here.


def run_phase_c5(paths: dict[str, Path]) -> None:
    assemble_candidate(paths["pass2_pre"] / "paragraphs.jsonl", paths["final_candidate"], paths["candidate_map"])
    _mutate_paragraph_statuses(paths, next_status="candidate_assembled", eligible_statuses={"translated_pass1", "translated_pass2"})


def run_phase_d(
    paths: dict[str, Path],
    *,
    run_id: str,
    max_paragraph_attempts: int,
    phase_timeout_seconds: int,
    should_abort: Callable[[], Exception | None],
    pipeline_profile: str | None = None,
) -> None:
    if not paths["final_candidate"].exists() or not paths["candidate_map"].exists():
        raise FileNotFoundError(
            "Missing canonical assembler outputs required for manuscript-level review mapping: "
            f"{paths['final_candidate']} and {paths['candidate_map']}"
        )
    paths["review_normalized"].mkdir(parents=True, exist_ok=True)
    paths["paragraph_scores"].parent.mkdir(parents=True, exist_ok=True)
    normalized_rows = paths["review_normalized"] / "all_reviews.jsonl"

    # phase D profile-resolved paragraph review input (used by grammar and other paragraph-scoped reviewers)

    candidate_map_rows = read_jsonl(paths["candidate_map"], strict=True)
    reviewable_ids = {
        str(row.get("paragraph_id"))
        for row in candidate_map_rows
        if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
    }
    state_rows = read_jsonl(paths["paragraph_state"], strict=True)
    state_ids = {
        str(row.get("paragraph_id"))
        for row in state_rows
        if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
    }
    unknown_candidate_ids = sorted(reviewable_ids - state_ids)
    if unknown_candidate_ids:
        raise ValueError(
            "candidate_map.jsonl contains paragraph_id values not present in paragraph_state.jsonl: "
            + ", ".join(unknown_candidate_ids)
        )

    manifest = _read_manifest(paths, run_id=run_id)
    review_pre_dir = _resolve_review_pre_dir(manifest, pipeline_profile)
    review_preprocessed = paths[review_pre_dir]

    # Guardrail: paragraph-scoped reviewers (especially grammar) must consume profile-resolved preprocessed input.
    if not review_preprocessed.exists():
        raise FileNotFoundError(
            f"Resolved review input directory '{review_pre_dir}' does not exist for run '{run_id}': {review_preprocessed}"
        )
    review_paragraphs = review_preprocessed / "paragraphs.jsonl"
    if not review_paragraphs.exists():
        raise FileNotFoundError(
            "Resolved review input is incomplete for phase D grammar reviewer; missing required artifact "
            f"{review_paragraphs}. Remediation: complete phase B/C preprocessing before phase D."
        )

    manifest_model = manifest.get("model")
    if not isinstance(manifest_model, str) or not manifest_model.strip():
        raise ValueError(
            f"Missing model in manifest for run '{run_id}'; phase D grammar reviewer requires manifest['model']."
        )
    grammar_output_dir = paths.get("review_grammar", paths["run_root"] / "review" / "grammar")

    _mutate_paragraph_statuses(
        paths,
        next_status="review_in_progress",
        eligible_statuses={"candidate_assembled", "reworked"},
        paragraph_ids=reviewable_ids,
    )

    timeout_seconds = None if phase_timeout_seconds == 0 else phase_timeout_seconds

    _exec_phase_command(
        [
            sys.executable,
            "scripts/grammar_auditor.py",
            "--preprocessed",
            str(review_preprocessed),
            "--model",
            manifest_model.strip(),
            "--output-dir",
            str(grammar_output_dir),
        ],
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )

    grammar_artifacts = sorted(grammar_output_dir.glob("grammar_audit_issues_*.json"))
    if not grammar_artifacts:
        grammar_artifacts = sorted(grammar_output_dir.glob("*.json"))
    if not grammar_artifacts:
        raise FileNotFoundError(
            "Grammar auditor did not produce an output artifact in "
            f"{grammar_output_dir}; expected grammar_audit_issues_*.json"
        )
    grammar_input = grammar_artifacts[-1]

    review_typography_dir = paths.get("review_typography", paths["run_root"] / "review" / "typography")
    review_critics_dir = paths.get("review_critics", paths["run_root"] / "review" / "critics")
    review_typography_dir.mkdir(parents=True, exist_ok=True)
    review_critics_dir.mkdir(parents=True, exist_ok=True)

    typography_review_output = review_typography_dir / "typography_review.json"
    critics_review_output = review_critics_dir / "critics_review.json"

    _exec_phase_command(
        [
            sys.executable,
            "scripts/typographic_precision_review.py",
            "--manuscript",
            str(paths["final_candidate"]),
            "--output-dir",
            str(review_typography_dir),
            "--output",
            str(typography_review_output),
        ],
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )

    _exec_phase_command(
        [
            sys.executable,
            "scripts/critics_runner.py",
            "--manuscript",
            str(paths["final_candidate"]),
            "--model",
            manifest_model.strip(),
            "--output",
            str(critics_review_output),
        ],
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )

    normalized_review_dir = paths["run_root"] / "review" / "normalized"
    normalized_review_dir.mkdir(parents=True, exist_ok=True)
    typography_mapped_output = paths.get("review_typography_mapped", normalized_review_dir / "typography_paragraph_rows.jsonl")
    critics_mapped_output = paths.get("review_critics_mapped", normalized_review_dir / "critics_paragraph_rows.jsonl")

    _exec_phase_command(
        [
            sys.executable,
            "scripts/map_review_to_paragraphs.py",
            "--run-id",
            run_id,
            "--reviewer",
            "typography",
            "--review-input",
            str(typography_review_output),
            "--output",
            str(typography_mapped_output),
        ],
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )

    _exec_phase_command(
        [
            sys.executable,
            "scripts/map_review_to_paragraphs.py",
            "--run-id",
            run_id,
            "--reviewer",
            "critics",
            "--review-input",
            str(critics_review_output),
            "--output",
            str(critics_mapped_output),
        ],
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )

    mapped_inputs: list[Path] = [Path(typography_mapped_output), Path(critics_mapped_output)]

    normalize_command = [
        sys.executable,
        "scripts/normalize_review_output.py",
        "--grammar-input",
        str(grammar_input),
        "--output",
        str(normalized_rows),
    ]
    for mapped_input in mapped_inputs:
        normalize_command.extend(["--mapped-input", str(mapped_input)])

    _exec_phase_command(
        normalize_command,
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )

    _exec_phase_command(
        [
            sys.executable,
            "scripts/aggregate_paragraph_reviews.py",
            "--state",
            str(paths["paragraph_state"]),
            "--review-rows",
            str(normalized_rows),
            "--scores-out",
            str(paths["paragraph_scores"]),
            "--queue-out",
            str(paths["rework_queue"]),
            "--review-blockers-out",
            str(paths["review_blockers"]),
            "--max-attempts",
            str(max_paragraph_attempts),
        ],
        timeout_seconds=timeout_seconds,
        should_abort=should_abort,
    )
    if not paths["paragraph_scores"].exists():
        atomic_write_jsonl(paths["paragraph_scores"], [])


def run_phase_e(
    paths: dict[str, Path],
    *,
    max_paragraph_attempts: int,
    bump_attempts: bool = True,
    should_abort: Callable[[], Exception | None] | None = None,
) -> None:
    # Attempts are incremented only in phase E. Phase D computes review-state
    # transitions without mutating attempts so policy ownership stays centralized.
    rows = read_jsonl(paths["paragraph_state"], strict=True)
    now_iso = _utc_now_iso()
    did_change = False
    for row in rows:
        paragraph_id = str(row.get("paragraph_id", "<unknown>"))
        content_hash = row.get("content_hash")
        if not isinstance(content_hash, str) or not content_hash.strip():
            raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': missing non-empty content_hash")

    rework_rows = [row for row in rows if row.get("status") == "rework_queued"]
    for row in rework_rows:
        if should_abort is not None:
            abort_error = should_abort()
            if abort_error is not None:
                raise abort_error
        paragraph_id = str(row.get("paragraph_id", "<unknown>"))
        excluded = row.get("excluded_by_policy", False) is True
        row_changed = False
        current_attempt = _coerce_attempt(row.get("attempt", 0), paragraph_id=paragraph_id)
        next_attempt = current_attempt + 1 if bump_attempts else current_attempt
        if row.get("attempt") != next_attempt:
            row_changed = True
        row["attempt"] = next_attempt
        if row["attempt"] >= max_paragraph_attempts:
            current_status = str(row.get("status", ""))
            assert_pipeline_transition_allowed(current_status, "manual_review_required", excluded)
            row["status"] = "manual_review_required"
            row_changed = True
            blockers = list(row.get("blocking_issues", [])) if isinstance(row.get("blocking_issues"), list) else []
            if "max_attempts_reached" not in blockers:
                blockers.append("max_attempts_reached")
                row["blocking_issues"] = blockers
                row_changed = True
        assert_pipeline_state_allowed(row["status"], excluded)

        if row["status"] == "rework_queued":
            current_status = str(row["status"])
            assert_pipeline_transition_allowed(current_status, "reworked", excluded)
            row["status"] = "reworked"
            row_changed = True
            assert_pipeline_state_allowed(row["status"], excluded)

        if row_changed:
            row["updated_at"] = now_iso
            did_change = True

    if did_change:
        atomic_write_jsonl(paths["paragraph_state"], rows)
    existing_queue = read_jsonl(paths["rework_queue"], strict=False) if paths["rework_queue"].exists() else []
    queue_rows = build_rework_queue_rows(rows, existing_queue_rows=existing_queue)
    atomic_write_jsonl(paths["rework_queue"], queue_rows)


def run_phase_f(
    paths: dict[str, Path],
    *,
    run_id: str,
    should_abort: Callable[[], Exception | None] | None = None,
) -> None:
    if should_abort is not None:
        abort_error = should_abort()
        if abort_error is not None:
            raise abort_error

    state_rows = read_jsonl(paths["paragraph_state"], strict=True)
    status_report = _compute_status_report(state_rows)

    source_rows = read_jsonl(paths["source_pre"] / "paragraphs.jsonl", strict=True)
    source_hash_by_id: dict[str, str] = {}
    for row in source_rows:
        paragraph_id = row.get("paragraph_id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            raise ValueError("Invalid source_pre paragraph row: missing non-empty paragraph_id")
        content_hash = row.get("content_hash")
        if not isinstance(content_hash, str) or not content_hash.strip():
            raise ValueError(f"Invalid source_pre paragraph row for '{paragraph_id}': missing non-empty content_hash")
        source_hash_by_id[paragraph_id] = content_hash

    merge_input_path = _resolve_merge_paragraphs_path(paths)
    translated_rows = read_jsonl(merge_input_path, strict=True)
    translated_by_id: dict[str, dict[str, Any]] = {}
    for row in translated_rows:
        paragraph_id = row.get("paragraph_id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            raise ValueError(f"Invalid merge input paragraph row in {merge_input_path}: missing non-empty paragraph_id")
        if paragraph_id in translated_by_id:
            raise ValueError(f"Invalid merge input paragraphs: duplicate paragraph_id '{paragraph_id}' in {merge_input_path}")
        translated_by_id[paragraph_id] = row

    blocker_states = {"rework_queued", "manual_review_required"}
    required_ids: list[str] = []
    blocking_entries: list[dict[str, str]] = []
    merged_output_rows: list[dict[str, Any]] = []

    for row in state_rows:
        paragraph_id = row.get("paragraph_id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            raise ValueError("Invalid paragraph_state row: missing non-empty paragraph_id")
        excluded = row.get("excluded_by_policy", False) is True
        status = row.get("status")
        content_hash = row.get("content_hash")
        if not isinstance(content_hash, str) or not content_hash.strip():
            raise ValueError(f"Invalid paragraph_state row for '{paragraph_id}': missing non-empty content_hash")

        source_hash = source_hash_by_id.get(paragraph_id)
        if source_hash is None:
            raise ValueError(f"Missing source_pre lineage row for paragraph_id '{paragraph_id}'")
        if source_hash != content_hash:
            raise ValueError(
                f"Lineage mismatch for paragraph_id '{paragraph_id}': paragraph_state content_hash does not match source_pre"
            )

        if excluded:
            continue

        required_ids.append(paragraph_id)
        if status in blocker_states:
            blocking_entries.append({"paragraph_id": paragraph_id, "reason": f"status:{status}"})
            continue
        if status not in {"ready_to_merge", "merged"}:
            blocking_entries.append({"paragraph_id": paragraph_id, "reason": f"required_not_merge_eligible:{status}"})
            continue

        merge_row = translated_by_id.get(paragraph_id)
        if merge_row is None:
            blocking_entries.append({"paragraph_id": paragraph_id, "reason": "missing_merge_input"})
            continue
        translated_hash = merge_row.get("content_hash")
        if not isinstance(translated_hash, str) or not translated_hash.strip():
            blocking_entries.append({"paragraph_id": paragraph_id, "reason": "missing_merge_input_content_hash"})
            continue
        if translated_hash != source_hash:
            blocking_entries.append({"paragraph_id": paragraph_id, "reason": "content_hash_lineage_mismatch"})
            continue
        merged_output_rows.append(merge_row)

    run_level_blockers: list[dict[str, Any]] = []
    review_blockers_path = paths.get("review_blockers")
    if isinstance(review_blockers_path, Path) and review_blockers_path.exists():
        payload = json.loads(review_blockers_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid review blocker artifact in {review_blockers_path}: expected object")
        blockers_payload = payload.get("run_level_blockers", [])
        if not isinstance(blockers_payload, list):
            raise ValueError(
                f"Invalid review blocker artifact in {review_blockers_path}: 'run_level_blockers' must be a list"
            )
        for blocker in blockers_payload:
            if isinstance(blocker, dict):
                run_level_blockers.append(dict(blocker))

    if run_level_blockers:
        blocking_entries.append({"paragraph_id": "__run__", "reason": "mapping_error_unresolved"})

    can_merge = len(blocking_entries) == 0

    paths["gate_dir"].mkdir(parents=True, exist_ok=True)
    gate_report = {
        "run_id": run_id,
        **status_report,
        "required_paragraph_ids": required_ids,
        "can_merge": can_merge,
        "blocking_paragraphs": blocking_entries,
        "run_level_blockers": run_level_blockers,
    }
    _atomic_write_json(paths["gate_report"], gate_report)

    if not can_merge:
        return

    paths["final_dir"].mkdir(parents=True, exist_ok=True)
    ordered_rows = sorted(
        merged_output_rows,
        key=lambda row: _coerce_paragraph_index(row.get("paragraph_index"), paragraph_id=str(row.get("paragraph_id", "<unknown>"))),
    )
    final_text = "\n\n".join(str(row.get("text", "")) for row in ordered_rows)
    paths["final_output"].write_text(final_text, encoding="utf-8")
    _build_final_pre_bundle(paths["final_pre"], ordered_rows)

    updated_rows: list[dict[str, Any]] = []
    merged_ids = {str(row.get("paragraph_id")) for row in ordered_rows}
    now_iso = _utc_now_iso()
    for row in state_rows:
        paragraph_id = str(row.get("paragraph_id", ""))
        if paragraph_id in merged_ids and row.get("status") == "ready_to_merge":
            next_row = dict(row)
            next_row["status"] = "merged"
            next_row["updated_at"] = now_iso
            updated_rows.append(next_row)
        else:
            updated_rows.append(row)
    atomic_write_jsonl(paths["paragraph_state"], updated_rows)


def _run_full_pipeline(
    paths: dict[str, Path],
    args: argparse.Namespace,
    run_phase: Callable[[str, Callable[[], None]], None],
    should_abort: Callable[[], Exception | None],
) -> None:
    exclusion_policy = _load_exclusion_policy(args.exclusion_policy_file)
    subset_paragraph_ids = _resolve_subset_paragraph_ids(
        paths,
        subset_from_queue=args.subset_from_queue,
        subset_paragraph_ids_raw=args.subset_paragraph_ids,
    )
    run_phase(
        "A",
        lambda: run_phase_a(
            paths,
            run_id=args.run_id,
            pipeline_profile=args.pipeline_profile,
            pass1_language=args.pass1_language,
            pass2_language=args.pass2_language,
            source=args.source,
            model=args.model,
            exclusion_policy=exclusion_policy,
            allow_exclusion_policy_reauthorization=args.allow_exclusion_policy_reauthorization,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
        ),
    )
    run_phase(
        "B",
        lambda: run_phase_b(
            paths,
            pass1_language=args.pass1_language,
            model=args.model,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
            subset_paragraph_ids=subset_paragraph_ids,
        ),
    )
    run_phase(
        "C",
        lambda: run_phase_c(
            paths,
            pass2_language=args.pass2_language,
            model=args.model,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
            subset_paragraph_ids=subset_paragraph_ids,
        ),
    )
    run_phase("C5", lambda: run_phase_c5(paths))
    run_phase(
        "D",
        lambda: run_phase_d(
            paths,
            run_id=args.run_id,
            max_paragraph_attempts=args.max_paragraph_attempts,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
            pipeline_profile=args.pipeline_profile,
        ),
    )
    run_phase(
        "E",
        lambda: run_phase_e(
            paths,
            max_paragraph_attempts=args.max_paragraph_attempts,
            should_abort=should_abort,
        ),
    )
    run_phase("F", lambda: run_phase_f(paths, run_id=args.run_id, should_abort=should_abort))


def _run_rework_only(
    paths: dict[str, Path],
    args: argparse.Namespace,
    run_phase: Callable[[str, Callable[[], None]], None],
    should_abort: Callable[[], Exception | None],
) -> None:
    pass1_language, pass2_language, model = _resolve_rework_runtime_config(paths, args)
    run_phase(
        "R",
        lambda: run_rework_translation_stage(
            paths,
            pass1_language=pass1_language,
            pass2_language=pass2_language,
            model=model,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
        ),
    )
    run_phase(
        "D",
        lambda: run_phase_d(
            paths,
            run_id=args.run_id,
            max_paragraph_attempts=args.max_paragraph_attempts,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
            pipeline_profile=args.pipeline_profile,
        ),
    )
    run_phase(
        "E",
        lambda: run_phase_e(
            paths,
            max_paragraph_attempts=args.max_paragraph_attempts,
            bump_attempts=not args.no_bump_attempts,
            should_abort=should_abort,
        ),
    )
    if args.rework_run_phase_f:
        run_phase("F", lambda: run_phase_f(paths, run_id=args.run_id, should_abort=should_abort))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Translation toolchain mode orchestrator.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=("full", "rework-only", "status"),
        help=(
            "Execution mode. rework-only runs targeted queued-paragraph translation rework (phase R), "
            "then review aggregation (phase D) and queue/state projection (phase E)."
        ),
    )
    parser.add_argument("--run-id", required=True, help="Run identifier under runs/<run_id>.")
    parser.add_argument(
        "--pipeline-profile",
        "--pipeline-preset",
        dest="pipeline_profile",
        help="Optional preset defaults for --mode full; explicit language args can be used without a preset.",
    )
    parser.add_argument(
        "--pass1-language",
        help="Optional first-pass language override. Defaults to preset pass1 language.",
    )
    parser.add_argument(
        "--pass2-language",
        help="Optional second-pass language override. Use 'none' to disable pass2.",
    )
    parser.add_argument("--source", help="Source manuscript path for --mode full.")
    parser.add_argument("--model", help="Model identifier for active execution modes.")
    parser.add_argument(
        "--exclusion-policy-file",
        help="Optional JSON file describing exclusion policy rules/templates for phase-A seeding.",
    )
    parser.add_argument(
        "--allow-exclusion-policy-reauthorization",
        action="store_true",
        help="Allow exclusion flag mutation for existing runs when policy drift is detected.",
    )
    parser.add_argument(
        "--max-paragraph-attempts",
        type=int,
        default=4,
        help="Maximum attempts before manual review is required.",
    )
    parser.add_argument(
        "--no-bump-attempts",
        action="store_true",
        help="For --mode rework-only, rebuild queue/state projection without incrementing attempts.",
    )
    parser.add_argument(
        "--rework-run-phase-f",
        action="store_true",
        help="For --mode rework-only, run Phase F after rework loop and review/queue updates.",
    )
    parser.add_argument(
        "--phase-timeout-seconds",
        type=int,
        default=0,
        help="Optional per-phase subprocess timeout in seconds (0 disables timeout).",
    )
    parser.add_argument(
        "--subset-from-queue",
        action="store_true",
        help=(
            "For --mode full, run Phases B/C only for paragraph IDs present in state/rework_queue.jsonl. "
            "If the queue is empty, translation phases no-op."
        ),
    )
    parser.add_argument(
        "--subset-paragraph-ids",
        help="Optional comma-separated paragraph_id list for subset translation in Phases B/C.",
    )
    return parser.parse_args()




def _parse_subset_paragraph_ids(raw: str | None) -> set[str]:
    if raw is None:
        return set()
    out: set[str] = set()
    for item in raw.split(','):
        paragraph_id = item.strip()
        if paragraph_id:
            out.add(paragraph_id)
    return out


def _resolve_subset_paragraph_ids(
    paths: dict[str, Path],
    *,
    subset_from_queue: bool,
    subset_paragraph_ids_raw: str | None,
) -> set[str] | None:
    explicit_ids = _parse_subset_paragraph_ids(subset_paragraph_ids_raw)
    queue_ids: set[str] = set()
    if subset_from_queue:
        queue_rows = read_jsonl(paths["rework_queue"], strict=False) if paths["rework_queue"].exists() else []
        queue_ids = {
            str(row.get("paragraph_id"))
            for row in queue_rows
            if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
        }

    selected_ids = explicit_ids | queue_ids
    if not subset_from_queue and not explicit_ids:
        return None
    return selected_ids


def _normalize_optional_language(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.lower() == "none":
        return None
    return cleaned




def _raise_usage_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(EXIT_USAGE_ERROR)

def _resolve_pipeline_languages(args: argparse.Namespace) -> tuple[str, str | None]:
    preset_defaults: dict[str, str | None] = {}
    if args.pipeline_profile:
        if args.pipeline_profile not in PIPELINE_PRESETS:
            _raise_usage_error(f"Unknown pipeline preset: {args.pipeline_profile}")
        preset_defaults = PIPELINE_PRESETS[args.pipeline_profile]

    resolved_pass1_language = _normalize_optional_language(args.pass1_language)
    if resolved_pass1_language is None:
        resolved_pass1_language = _normalize_optional_language(preset_defaults.get("pass1_language"))

    resolved_pass2_language = _normalize_optional_language(args.pass2_language)
    if args.pass2_language is None:
        resolved_pass2_language = _normalize_optional_language(preset_defaults.get("pass2_language"))

    if resolved_pass1_language is None:
        _raise_usage_error("pass1 language is required; provide --pass1-language or a known preset with pass1 language")

    return resolved_pass1_language, resolved_pass2_language

def main() -> None:
    args = parse_args()
    if args.max_paragraph_attempts <= 0:
        raise SystemExit("--max-paragraph-attempts must be > 0")
    if args.phase_timeout_seconds < 0:
        raise SystemExit("--phase-timeout-seconds must be >= 0")
    if not RUN_ID_PATTERN.match(args.run_id):
        raise SystemExit("--run-id must start with a letter/number, use only letters, numbers, dot, underscore, or hyphen, and be at most 64 chars")

    if args.mode == "full":
        if not args.source:
            raise SystemExit("--source is required for --mode full")
        if not args.model:
            raise SystemExit("--model is required for --mode full")

        resolved_pass1_language, resolved_pass2_language = _resolve_pipeline_languages(args)
        args.pass1_language = resolved_pass1_language
        args.pass2_language = resolved_pass2_language
        if not args.pipeline_profile:
            args.pipeline_profile = "explicit_languages"
    paths = _run_paths(args.run_id)
    run_dir = paths["run_root"]

    if args.mode == "rework-only":
        if not run_dir.exists() or not paths["paragraph_state"].exists():
            print("status=run_not_initialized", file=sys.stderr)
            return

    if args.mode == "status":
        if not paths["run_root"].exists():
            print("status=run_not_initialized")
            _print_status_report(_compute_status_report([]), run_id=args.run_id)
            return
        state_path = paths["paragraph_state"]
        if not state_path.exists():
            print("status=run_not_initialized")
            rows: list[dict[str, Any]] = []
        else:
            try:
                rows = read_jsonl(state_path, strict=True)
            except ValueError as exc:
                print(f"status=corrupt_state error={exc}", file=sys.stderr)
                raise SystemExit(EXIT_CORRUPT_STATE) from exc
        progress = _read_progress(paths)
        if progress:
            phase = progress.get("current_phase", "unknown")
            phase_state = progress.get("phase_state", "unknown")
            started_at = progress.get("phase_started_at")
            last_heartbeat = progress.get("last_heartbeat_at")
            elapsed_display = "00:00:00"
            if isinstance(started_at, str) and started_at.strip():
                try:
                    elapsed_display = _format_duration(time.time() - _parse_iso8601(started_at))
                except ValueError:
                    elapsed_display = "invalid"
            print(
                f"phase={phase} "
                f"phase_state={phase_state} "
                f"phase_elapsed={elapsed_display} "
                f"last_heartbeat={last_heartbeat or 'n/a'}"
            )
        _print_status_report(_compute_status_report(rows), run_id=args.run_id)
        return

    lock_path: Path | None = None
    payload: dict[str, Any] | None = None
    lock_identity: LockIdentity | None = None
    last_heartbeat = time.monotonic()
    progress_state: dict[str, Any] = {
        "current_phase": "setup",
        "phase_state": "running",
        "phase_started_at": _utc_now_iso(),
        "phase_finished_at": None,
        "last_heartbeat_at": None,
    }
    current_phase_abort_checker: Callable[[], Exception | None] | None = None

    def current_abort_error() -> Exception | None:
        if current_phase_abort_checker is None:
            return None
        return current_phase_abort_checker()

    def maybe_heartbeat() -> bool:
        nonlocal payload, lock_identity, last_heartbeat, progress_state
        assert lock_path is not None and payload is not None and lock_identity is not None
        now = time.monotonic()
        if now - last_heartbeat < LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS:
            return False
        payload, lock_identity = write_lock_heartbeat(lock_path, payload, args.run_id, lock_identity)
        last_heartbeat = now
        progress_state["last_heartbeat_at"] = payload["last_heartbeat_at"]
        try:
            _write_progress(
                paths,
                run_id=args.run_id,
                mode=args.mode,
                current_phase=progress_state["current_phase"],
                phase_state=progress_state["phase_state"],
                phase_started_at=progress_state["phase_started_at"],
                phase_finished_at=progress_state["phase_finished_at"],
                last_heartbeat_at=progress_state["last_heartbeat_at"],
            )
        except Exception as exc:  # noqa: BLE001
            print(
                f"Warning: failed to update progress heartbeat metadata after lock heartbeat write: {exc}",
                file=sys.stderr,
            )
        return True

    def run_phase_with_progress(phase_name: str, runner: Callable[[], None]) -> None:
        nonlocal progress_state, current_phase_abort_checker
        phase_started_at = _utc_now_iso()
        progress_state = {
            "current_phase": phase_name,
            "phase_state": "running",
            "phase_started_at": phase_started_at,
            "phase_finished_at": None,
            "last_heartbeat_at": progress_state.get("last_heartbeat_at"),
        }
        _write_progress(
            paths,
            run_id=args.run_id,
            mode=args.mode,
            current_phase=phase_name,
            phase_state="running",
            phase_started_at=phase_started_at,
            phase_finished_at=None,
            last_heartbeat_at=progress_state["last_heartbeat_at"],
        )
        heartbeat_stop = threading.Event()
        fatal_heartbeat_error: InvalidRunLockError | None = None
        warning_heartbeat_error: Exception | None = None
        warning_heartbeat_consecutive_failures = 0
        heartbeat_degraded = False
        last_heartbeat_write_monotonic = time.monotonic() - LOCK_HEARTBEAT_STALE_ABORT_SECONDS

        def _safe_maybe_heartbeat() -> None:
            nonlocal fatal_heartbeat_error, warning_heartbeat_error, warning_heartbeat_consecutive_failures, heartbeat_degraded, last_heartbeat_write_monotonic
            try:
                wrote_heartbeat = maybe_heartbeat()
                if wrote_heartbeat:
                    warning_heartbeat_consecutive_failures = 0
                    last_heartbeat_write_monotonic = time.monotonic()
            except InvalidRunLockError as exc:
                fatal_heartbeat_error = exc
                raise
            except Exception as exc:  # noqa: BLE001
                warning_heartbeat_error = exc
                warning_heartbeat_consecutive_failures += 1
                if warning_heartbeat_consecutive_failures >= LOCK_HEARTBEAT_MAX_CONSECUTIVE_FAILURES:
                    heartbeat_degraded = True
                    fatal_heartbeat_error = RuntimeError("Heartbeat degraded after repeated failures; refusing to risk stale lock")
                    heartbeat_stop.set()

        def _heartbeat_loop() -> None:
            nonlocal fatal_heartbeat_error, warning_heartbeat_error
            while not heartbeat_stop.wait(1):
                if time.monotonic() - last_heartbeat_write_monotonic > LOCK_HEARTBEAT_STALE_ABORT_SECONDS:
                    fatal_heartbeat_error = RuntimeError("Heartbeat stalled near stale-lock threshold; aborting to preserve lock authority")
                    heartbeat_stop.set()
                    break
                try:
                    _safe_maybe_heartbeat()
                except InvalidRunLockError:
                    heartbeat_stop.set()
                    break
                except Exception as exc:  # noqa: BLE001
                    warning_heartbeat_error = exc
                    fatal_heartbeat_error = RuntimeError("Heartbeat thread crashed unexpectedly; aborting to preserve lock authority")
                    heartbeat_stop.set()
                    break

        heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        def _phase_abort_error() -> Exception | None:
            if fatal_heartbeat_error is not None:
                return fatal_heartbeat_error
            return None

        phase_succeeded = False
        try:
            _safe_maybe_heartbeat()
            if fatal_heartbeat_error is not None:
                raise fatal_heartbeat_error
            current_phase_abort_checker = _phase_abort_error
            # IMPORTANT: phase implementations must either call _exec_phase_command(... should_abort=...)
            # or periodically invoke should_abort() so heartbeat degradation can interrupt long-running work.
            runner()
            if fatal_heartbeat_error is not None:
                raise fatal_heartbeat_error
            phase_succeeded = True
        except Exception:  # noqa: BLE001
            _write_progress(
                paths,
                run_id=args.run_id,
                mode=args.mode,
                current_phase=phase_name,
                phase_state="error",
                phase_started_at=phase_started_at,
                phase_finished_at=_utc_now_iso(),
                last_heartbeat_at=progress_state["last_heartbeat_at"],
            )
            raise
        finally:
            heartbeat_stop.set()
            current_phase_abort_checker = None
            heartbeat_thread.join(timeout=2)
            if heartbeat_thread.is_alive():
                print(
                    f"Warning: heartbeat thread did not shut down cleanly for phase {phase_name}; it may still be blocked in I/O",
                    file=sys.stderr,
                )
            if warning_heartbeat_error is not None:
                if heartbeat_degraded:
                    print(
                        f"Error: heartbeat degraded during phase {phase_name} after repeated failures: {warning_heartbeat_error}",
                        file=sys.stderr,
                    )
                else:
                    print(f"Warning: heartbeat failed during phase {phase_name}: {warning_heartbeat_error}", file=sys.stderr)
        if not phase_succeeded:
            return

        if fatal_heartbeat_error is not None:
            _write_progress(
                paths,
                run_id=args.run_id,
                mode=args.mode,
                current_phase=phase_name,
                phase_state="error",
                phase_started_at=phase_started_at,
                phase_finished_at=_utc_now_iso(),
                last_heartbeat_at=progress_state["last_heartbeat_at"],
            )
            raise fatal_heartbeat_error

        _write_progress(
            paths,
            run_id=args.run_id,
            mode=args.mode,
            current_phase=phase_name,
            phase_state="done",
            phase_started_at=phase_started_at,
            phase_finished_at=_utc_now_iso(),
            last_heartbeat_at=progress_state["last_heartbeat_at"],
        )
        marker_path = _phase_marker_path(paths, phase_name)
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_tmp = marker_path.with_name(f".{marker_path.name}.tmp.{os.getpid()}")
        try:
            with marker_tmp.open("w", encoding="utf-8") as handle:
                handle.write(_utc_now_iso() + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(marker_tmp, marker_path)
            _fsync_directory(marker_path.parent)
        finally:
            if marker_tmp.exists():
                try:
                    marker_tmp.unlink()
                except OSError:
                    pass

    try:
        lock_path, payload, lock_identity = acquire_run_lock(run_dir, args.run_id)
    except ActiveRunLockError as exc:
        print(f"{exc}", file=sys.stderr)
        raise SystemExit(EXIT_ACTIVE_LOCK) from exc
    except InvalidRunLockError as exc:
        print(f"Invalid lock encountered: {exc}", file=sys.stderr)
        raise SystemExit(EXIT_INVALID_LOCK) from exc
    except OSError as exc:
        print(f"Failed to acquire RUNNING.lock: {exc}", file=sys.stderr)
        raise SystemExit(EXIT_LOCK_RACE) from exc

    print(f"Acquired lock: {lock_path}")
    progress_state["last_heartbeat_at"] = payload["last_heartbeat_at"]
    try:
        try:
            if args.mode == "full":
                _run_full_pipeline(paths, args, run_phase_with_progress, current_abort_error)
            elif args.mode == "rework-only":
                _run_rework_only(paths, args, run_phase_with_progress, current_abort_error)
        except (subprocess.CalledProcessError, TimeoutError, ValueError, FileNotFoundError) as exc:
            print(f"status=phase_failed error={exc}", file=sys.stderr)
            raise SystemExit(EXIT_PHASE_FAILURE) from exc
    finally:
        try:
            if lock_path is not None and release_run_lock(lock_path, args.run_id):
                print(f"Released lock: {lock_path}")
        except InvalidRunLockError as exc:
            print(f"Failed to release lock safely: {exc}", file=sys.stderr)
            raise SystemExit(EXIT_INVALID_LOCK) from exc


if __name__ == "__main__":
    main()
