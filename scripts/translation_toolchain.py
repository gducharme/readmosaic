#!/usr/bin/env python3
"""Translation toolchain lock handling primitives.

This module currently focuses on race-safe run lock acquisition/release semantics.
"""
from __future__ import annotations

import argparse
import errno
import json
import os
import random
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.paragraph_state_machine import (
    ParagraphPolicyConfig,
    ParagraphReviewAggregate,
    assert_pipeline_state_allowed,
    resolve_review_transition,
)

LOCK_FILE_NAME = "RUNNING.lock"
LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS = 10
LOCK_STALE_TTL_SECONDS = 120
LOCK_STALE_RETRY_BASE_SLEEP_SECONDS = 0.05
LOCK_FILE_FIELDS = (
    "pid",
    "host",
    "started_at",
    "last_heartbeat_at",
    "run_id",
)

EXIT_OK = 0
EXIT_ACTIVE_LOCK = 2
EXIT_INVALID_LOCK = 3
EXIT_LOCK_RACE = 4

LockIdentity = tuple[int, int]


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
    normalized = timestamp.strip().replace("Z", "+00:00")
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
        _parse_iso8601(payload["started_at"])
        _parse_iso8601(payload["last_heartbeat_at"])
    except ValueError as exc:
        raise InvalidRunLockError(f"{lock_path} contains invalid timestamp: {exc}") from exc

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
        return []
    if not isinstance(value, list):
        raise ValueError(
            f"rework_queued paragraph '{paragraph_id}' field '{field_name}' must be a list when provided"
        )
    return list(value)

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

    failure_history = _coerce_optional_list_field(
        paragraph_state_row,
        "failure_history",
        paragraph_id=paragraph_id,
    ) or []

    return {
        "paragraph_id": paragraph_id,
        "content_hash": content_hash,
        "attempt": paragraph_state_row.get("attempt", 0),
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


def _cleanup_stale_temp_lock_files(run_dir: Path) -> None:
    """Best-effort cleanup for abandoned atomic-write temp files."""
    prefix = f".{LOCK_FILE_NAME}.tmp."
    now = time.time()
    max_age = LOCK_STALE_TTL_SECONDS * 2

    try:
        candidates = list(run_dir.iterdir())
    except OSError:
        return

    for candidate in candidates:
        if not candidate.is_file() or not candidate.name.startswith(prefix):
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
    age_seconds = time.time() - heartbeat_ts
    return age_seconds > LOCK_STALE_TTL_SECONDS


def _archive_stale_lock(lock_path: Path, payload: dict[str, Any]) -> Path:
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
            lock_path.replace(stale_path)
            _fsync_directory(lock_path.parent)
            return stale_path
        except FileExistsError:
            suffix += 1
            continue
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                suffix += 1
                continue
            raise


def acquire_run_lock(run_dir: Path, run_id: str) -> tuple[Path, dict[str, Any], LockIdentity]:
    run_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_stale_temp_lock_files(run_dir)
    lock_path = run_dir / LOCK_FILE_NAME
    payload = _build_lock_payload(run_id)
    lock_blob = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            existing = _read_lock(lock_path)
            if not _is_stale(existing):
                raise ActiveRunLockError(
                    "Run already active: fresh RUNNING.lock exists "
                    f"(run_id={existing['run_id']}, host={existing['host']}, pid={existing['pid']})."
                )
            stale_path = _archive_stale_lock(lock_path, existing)
            print(
                "Archived stale lock to "
                f"{stale_path} (run_id={existing['run_id']}, host={existing['host']}, pid={existing['pid']}).",
                file=sys.stderr,
            )
            time.sleep(LOCK_STALE_RETRY_BASE_SLEEP_SECONDS + random.uniform(0, 0.05))
            continue

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


def resolve_paragraph_review_state(
    prior_state: dict[str, Any],
    review_aggregate: dict[str, Any],
    max_attempts: int,
) -> dict[str, Any]:
    """Apply canonical paragraph review transition policy.

    This helper keeps toolchain callers on the same state machine used by
    aggregate_paragraph_reviews.py.
    """

    review = ParagraphReviewAggregate(
        hard_fail=bool(review_aggregate.get("hard_fail", False)),
        blocking_issues=tuple(review_aggregate.get("blocking_issues", [])),
        scores=dict(review_aggregate.get("scores", {})),
    )
    policy = ParagraphPolicyConfig(max_attempts=max_attempts)
    transition = resolve_review_transition(prior_state, review, policy)
    next_state = dict(prior_state)
    next_state["status"] = transition.next_state
    next_state.update(transition.metadata_updates)
    assert_pipeline_state_allowed(next_state["status"], bool(next_state.get("excluded_by_policy", False)))
    return next_state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Acquire/release translation toolchain run lock.")
    parser.add_argument("--run-dir", type=Path, required=True, help="Run directory containing RUNNING.lock.")
    parser.add_argument("--run-id", required=True, help="Run identifier recorded in lock metadata.")
    parser.add_argument(
        "--hold-seconds",
        type=int,
        default=0,
        help="Optionally hold the lock and emit heartbeat updates before releasing.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.hold_seconds < 0:
        raise SystemExit("--hold-seconds must be >= 0")

    try:
        lock_path, payload, lock_identity = acquire_run_lock(args.run_dir, args.run_id)
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

    try:
        remaining = args.hold_seconds
        while remaining > 0:
            sleep_for = min(LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS, remaining)
            time.sleep(sleep_for)
            payload, lock_identity = write_lock_heartbeat(
                lock_path,
                payload,
                args.run_id,
                lock_identity,
            )
            remaining -= sleep_for
            print(f"Heartbeat written at {payload['last_heartbeat_at']}")
    finally:
        try:
            if release_run_lock(lock_path, args.run_id):
                print(f"Released lock: {lock_path}")
        except InvalidRunLockError as exc:
            print(f"Failed to release lock safely: {exc}", file=sys.stderr)
            raise SystemExit(EXIT_INVALID_LOCK) from exc


if __name__ == "__main__":
    main()
