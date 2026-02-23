#!/usr/bin/env python3
"""Translation toolchain orchestrator with lock-safe run lifecycle management."""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import random
import socket
import subprocess
import sys
import threading
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

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
LOCK_HEARTBEAT_MAX_CONSECUTIVE_FAILURES = 3
LOCK_HEARTBEAT_STALE_ABORT_SECONDS = max(30, LOCK_STALE_TTL_SECONDS - 30)
LOCK_FILE_FIELDS = (
    "pid",
    "host",
    "started_at",
    "last_heartbeat_at",
    "run_id",
)

PIPELINE_PROFILE_CONFIG: dict[str, dict[str, str | None]] = {
    "tamazight_two_pass": {"pass1_language": "Tamazight", "pass2_language": "Tifinagh"},
    "standard_single_pass": {"pass1_language": "Tamazight", "pass2_language": None},
}

EXIT_OK = 0
EXIT_ACTIVE_LOCK = 2
EXIT_INVALID_LOCK = 3
EXIT_LOCK_RACE = 4
EXIT_USAGE_ERROR = 5

LockIdentity = tuple[int, int]
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


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
        raise ValueError(
            f"rework_queued paragraph '{paragraph_id}' field '{field_name}' must not be null; use []"
        )
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


def _archive_stale_lock(lock_path: Path, payload: dict[str, Any]) -> Path | None:
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
    _cleanup_stale_temp_lock_files(run_dir)
    lock_path = run_dir / LOCK_FILE_NAME
    payload = _build_lock_payload(run_id)
    lock_blob = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            try:
                existing = _read_lock(lock_path)
            except InvalidRunLockError as exc:
                raise InvalidRunLockError(f"Invalid active lock file at {lock_path}: {exc}") from exc
            if not _is_stale(existing):
                raise ActiveRunLockError(
                    "Run already active: fresh RUNNING.lock exists "
                    f"(run_id={existing['run_id']}, host={existing['host']}, pid={existing['pid']})."
                )
            stale_path = _archive_stale_lock(lock_path, existing)
            if stale_path is not None:
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


def _coerce_attempt(value: Any, *, paragraph_id: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"paragraph '{paragraph_id}' has invalid boolean attempt value")
    if isinstance(value, int):
        return max(0, value)
    if value is None:
        return 0
    raise ValueError(f"paragraph '{paragraph_id}' has non-integer attempt value: {value!r}")


def _run_root(run_id: str) -> Path:
    return Path("runs") / run_id


def _run_paths(run_id: str) -> dict[str, Path]:
    run_root = _run_root(run_id)
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
        "review_normalized": run_root / "review" / "normalized",
        "gate_dir": run_root / "gate",
        "gate_report": run_root / "gate" / "gate_report.json",
    }


def _ensure_manifest(paths: dict[str, Path], *, run_id: str, pipeline_profile: str, source: str, model: str) -> None:
    desired = {
        "run_id": run_id,
        "pipeline_profile": pipeline_profile,
        "source": source,
        "model": model,
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
            if should_abort is not None:
                abort_error = should_abort()
                if abort_error is not None:
                    raise abort_error

            code = process.poll()
            if code is not None:
                if code != 0:
                    raise subprocess.CalledProcessError(code, command)
                return

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


def _materialize_preprocessed_from_translation(
    source_pre: Path,
    translation_json: Path,
    output_pre: Path,
) -> None:
    if not translation_json.exists():
        raise FileNotFoundError(f"Missing translation output: {translation_json}")

    payload = json.loads(translation_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid translation payload in {translation_json}: expected a JSON object")
    records = payload.get("records")

    source_rows = read_jsonl(source_pre / "paragraphs.jsonl", strict=True)

    translated: list[str]
    if isinstance(records, list):
        if not records:
            if "paragraph_translations" not in payload:
                raise ValueError(
                    f"Invalid translation payload in {translation_json}: records is empty and paragraph_translations is missing"
                )
            translated = payload.get("paragraph_translations", [])
        else:
            by_index: dict[int, str] = {}
            for record in records:
                if not isinstance(record, dict):
                    raise ValueError(f"Invalid translation record in {translation_json}: expected object rows")
                idx = record.get("paragraph_index")
                if not isinstance(idx, int):
                    raise ValueError(f"Invalid translation record in {translation_json}: missing integer paragraph_index")
                if idx in by_index:
                    raise ValueError(
                        f"Invalid translation records in {translation_json}: duplicate paragraph_index={idx}"
                    )
                translation_value = record.get("translation", "")
                by_index[idx] = "" if translation_value is None else str(translation_value)

            keys = set(by_index.keys())
            expected_len = len(source_rows)
            if len(by_index) != expected_len:
                raise ValueError(
                    f"Invalid translation records in {translation_json}: expected {expected_len} unique indices, got {len(by_index)}"
                )

            if keys == set(range(1, expected_len + 1)):
                translated = [by_index[i] for i in range(1, expected_len + 1)]
            elif keys == set(range(0, expected_len)):
                translated = [by_index[i] for i in range(0, expected_len)]
            else:
                raise ValueError(
                    "Invalid translation record indices in "
                    f"{translation_json}: expected contiguous 0-based or 1-based indices"
                )
    else:
        translated = payload.get("paragraph_translations", [])

    if not isinstance(translated, list):
        raise ValueError(f"Invalid translation payload in {translation_json}: paragraph_translations must be a list")

    if len(source_rows) != len(translated):
        raise ValueError(
            "Translated paragraph count mismatch: "
            f"source={len(source_rows)} translated={len(translated)} in {translation_json}"
        )

    output_pre.mkdir(parents=True, exist_ok=True)
    translated_rows: list[dict[str, Any]] = []
    for row, text in zip(source_rows, translated):
        next_row = dict(row)
        next_row["text"] = str(text)
        translated_rows.append(next_row)

    atomic_write_jsonl(output_pre / "paragraphs.jsonl", translated_rows)
    source_sentences = source_pre / "sentences.jsonl"
    if source_sentences.exists():
        shutil.copy2(source_sentences, output_pre / "sentences.jsonl")


def _hash_content(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _language_output_dir_name(language: str) -> str:
    return language.lower().replace(" ", "_")


def _build_seed_state_rows(paragraph_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    state_rows: list[dict[str, Any]] = []
    skipped_rows = 0
    for row in paragraph_rows:
        paragraph_id = row.get("paragraph_id") or row.get("id")
        text = str(row.get("text", ""))
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            skipped_rows += 1
            continue
        state_rows.append(
            {
                "paragraph_id": paragraph_id,
                "status": "ingested",
                "attempt": 0,
                "content_hash": _hash_content(text),
                "excluded_by_policy": False,
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
    source: str,
    model: str,
    phase_timeout_seconds: int,
    should_abort: Callable[[], Exception | None],
) -> None:
    paths["source_pre"].mkdir(parents=True, exist_ok=True)
    paths["state_dir"].mkdir(parents=True, exist_ok=True)
    _ensure_manifest(paths, run_id=run_id, pipeline_profile=pipeline_profile, source=source, model=model)

    _exec_phase_command(
        [sys.executable, "scripts/pre_processing.py", source, "--output-dir", str(paths["source_pre"])],
        timeout_seconds=phase_timeout_seconds or None,
        should_abort=should_abort,
    )

    paragraph_rows = read_jsonl(paths["source_pre"] / "paragraphs.jsonl", strict=True)
    state_rows, skipped_rows = _build_seed_state_rows(paragraph_rows)
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


def run_phase_b(
    paths: dict[str, Path],
    *,
    pipeline_profile: str,
    model: str,
    phase_timeout_seconds: int,
    should_abort: Callable[[], Exception | None],
) -> None:
    pass1_language = PIPELINE_PROFILE_CONFIG[pipeline_profile]["pass1_language"]
    if pass1_language is None:
        raise ValueError(f"pipeline_profile={pipeline_profile} missing pass1 language")

    translate_output = paths["run_root"] / "translate_pass1"
    _exec_phase_command(
        [
            sys.executable,
            "scripts/translate.py",
            "--language",
            pass1_language,
            "--model",
            model,
            "--preprocessed",
            str(paths["source_pre"]),
            "--output-root",
            str(translate_output),
        ],
        timeout_seconds=phase_timeout_seconds or None,
        should_abort=should_abort,
    )
    translation_json = translate_output / _language_output_dir_name(pass1_language) / "translation.json"
    _materialize_preprocessed_from_translation(paths["source_pre"], translation_json, paths["pass1_pre"])


def run_phase_c(
    paths: dict[str, Path], *, pipeline_profile: str, model: str, phase_timeout_seconds: int, should_abort: Callable[[], Exception | None]
) -> None:
    pass2_language = PIPELINE_PROFILE_CONFIG[pipeline_profile]["pass2_language"]
    if pass2_language:
        translate_output = paths["run_root"] / "translate_pass2"
        _exec_phase_command(
            [
                sys.executable,
                "scripts/translate.py",
                "--language",
                pass2_language,
                "--model",
                model,
                "--preprocessed",
                str(paths["pass1_pre"]),
                "--output-root",
                str(translate_output),
            ],
            timeout_seconds=phase_timeout_seconds or None,
            should_abort=should_abort,
        )
        translation_json = translate_output / _language_output_dir_name(pass2_language) / "translation.json"
        _materialize_preprocessed_from_translation(paths["pass1_pre"], translation_json, paths["pass2_pre"])
        return

    paths["pass2_pre"].mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths["pass1_pre"] / "paragraphs.jsonl", paths["pass2_pre"] / "paragraphs.jsonl")
    source_sentences = paths["pass1_pre"] / "sentences.jsonl"
    if source_sentences.exists():
        shutil.copy2(source_sentences, paths["pass2_pre"] / "sentences.jsonl")


def run_phase_d(
    paths: dict[str, Path], *, max_paragraph_attempts: int, phase_timeout_seconds: int, should_abort: Callable[[], Exception | None]
) -> None:
    paths["review_normalized"].mkdir(parents=True, exist_ok=True)
    paths["paragraph_scores"].parent.mkdir(parents=True, exist_ok=True)
    normalized_rows = paths["review_normalized"] / "all_reviews.jsonl"
    if not normalized_rows.exists():
        atomic_write_jsonl(normalized_rows, [])
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
            "--max-attempts",
            str(max_paragraph_attempts),
        ],
        timeout_seconds=phase_timeout_seconds or None,
        should_abort=should_abort,
    )
    if not paths["paragraph_scores"].exists():
        atomic_write_jsonl(paths["paragraph_scores"], [])


def run_phase_e(paths: dict[str, Path], *, max_paragraph_attempts: int, bump_attempts: bool = True) -> None:
    # Attempts are incremented only in phase E. Phase D computes review-state
    # transitions without mutating attempts so policy ownership stays centralized.
    rows = read_jsonl(paths["paragraph_state"], strict=True)
    rework_rows = [row for row in rows if row.get("status") == "rework_queued"]
    for row in rework_rows:
        paragraph_id = str(row.get("paragraph_id", "<unknown>"))
        current_attempt = _coerce_attempt(row.get("attempt", 0), paragraph_id=paragraph_id)
        row["attempt"] = current_attempt + 1 if bump_attempts else current_attempt
        row["updated_at"] = _utc_now_iso()
        if row["attempt"] >= max_paragraph_attempts:
            row["status"] = "manual_review_required"
            blockers = list(row.get("blocking_issues", [])) if isinstance(row.get("blocking_issues"), list) else []
            if "max_attempts_reached" not in blockers:
                blockers.append("max_attempts_reached")
            row["blocking_issues"] = blockers
        assert_pipeline_state_allowed(row["status"], row.get("excluded_by_policy", False) is True)

    atomic_write_jsonl(paths["paragraph_state"], rows)
    existing_queue = read_jsonl(paths["rework_queue"], strict=False) if paths["rework_queue"].exists() else []
    queue_rows = build_rework_queue_rows(rows, existing_queue_rows=existing_queue)
    atomic_write_jsonl(paths["rework_queue"], queue_rows)


def run_phase_f(paths: dict[str, Path], *, run_id: str) -> None:
    paths["final_dir"].mkdir(parents=True, exist_ok=True)
    report = _compute_status_report(read_jsonl(paths["paragraph_state"], strict=True))
    paths["gate_dir"].mkdir(parents=True, exist_ok=True)
    _atomic_write_json(paths["gate_report"], {"run_id": run_id, **report})


def _run_full_pipeline(
    paths: dict[str, Path],
    args: argparse.Namespace,
    run_phase: Callable[[str, Callable[[], None]], None],
    should_abort: Callable[[], Exception | None],
) -> None:
    run_phase(
        "A",
        lambda: run_phase_a(
            paths,
            run_id=args.run_id,
            pipeline_profile=args.pipeline_profile,
            source=args.source,
            model=args.model,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
        ),
    )
    run_phase(
        "B",
        lambda: run_phase_b(
            paths,
            pipeline_profile=args.pipeline_profile,
            model=args.model,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
        ),
    )
    run_phase(
        "C",
        lambda: run_phase_c(
            paths,
            pipeline_profile=args.pipeline_profile,
            model=args.model,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
        ),
    )
    run_phase(
        "D",
        lambda: run_phase_d(
            paths,
            max_paragraph_attempts=args.max_paragraph_attempts,
            phase_timeout_seconds=args.phase_timeout_seconds,
            should_abort=should_abort,
        ),
    )
    run_phase("E", lambda: run_phase_e(paths, max_paragraph_attempts=args.max_paragraph_attempts))
    run_phase("F", lambda: run_phase_f(paths, run_id=args.run_id))


def _run_rework_only(
    paths: dict[str, Path],
    args: argparse.Namespace,
    run_phase: Callable[[str, Callable[[], None]], None],
) -> None:
    run_phase(
        "E",
        lambda: run_phase_e(
            paths,
            max_paragraph_attempts=args.max_paragraph_attempts,
            bump_attempts=not args.no_bump_attempts,
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Translation toolchain mode orchestrator.")
    parser.add_argument("--mode", required=True, choices=("full", "rework-only", "status"))
    parser.add_argument("--run-id", required=True, help="Run identifier under runs/<run_id>.")
    parser.add_argument(
        "--pipeline-profile",
        choices=("tamazight_two_pass", "standard_single_pass"),
        help="Pipeline profile for --mode full.",
    )
    parser.add_argument("--source", help="Source manuscript path for --mode full.")
    parser.add_argument("--model", help="Model identifier for active execution modes.")
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
        "--phase-timeout-seconds",
        type=int,
        default=0,
        help="Optional per-phase subprocess timeout in seconds (0 disables timeout).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.max_paragraph_attempts <= 0:
        raise SystemExit("--max-paragraph-attempts must be > 0")
    if args.phase_timeout_seconds < 0:
        raise SystemExit("--phase-timeout-seconds must be >= 0")
    if not RUN_ID_PATTERN.match(args.run_id):
        raise SystemExit("--run-id may contain only letters, numbers, dot, underscore, and hyphen")

    if args.mode == "full":
        if not args.pipeline_profile:
            raise SystemExit("--pipeline-profile is required for --mode full")
        if not args.source:
            raise SystemExit("--source is required for --mode full")
        if not args.model:
            raise SystemExit("--model is required for --mode full")
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
                raise SystemExit(EXIT_INVALID_LOCK) from exc
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

    def maybe_heartbeat() -> None:
        nonlocal payload, lock_identity, last_heartbeat, progress_state
        assert lock_path is not None and payload is not None and lock_identity is not None
        now = time.monotonic()
        if now - last_heartbeat < LOCK_HEARTBEAT_WRITE_INTERVAL_SECONDS:
            return
        payload, lock_identity = write_lock_heartbeat(lock_path, payload, args.run_id, lock_identity)
        last_heartbeat = now
        progress_state["last_heartbeat_at"] = payload["last_heartbeat_at"]
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
        last_heartbeat_success_monotonic = time.monotonic()

        def _safe_maybe_heartbeat() -> None:
            nonlocal fatal_heartbeat_error, warning_heartbeat_error, warning_heartbeat_consecutive_failures, heartbeat_degraded, last_heartbeat_success_monotonic
            try:
                maybe_heartbeat()
                warning_heartbeat_consecutive_failures = 0
                last_heartbeat_success_monotonic = time.monotonic()
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
            nonlocal fatal_heartbeat_error
            while not heartbeat_stop.wait(1):
                if time.monotonic() - last_heartbeat_success_monotonic > LOCK_HEARTBEAT_STALE_ABORT_SECONDS:
                    fatal_heartbeat_error = RuntimeError("Heartbeat stalled near stale-lock threshold; aborting to preserve lock authority")
                    heartbeat_stop.set()
                    break
                try:
                    _safe_maybe_heartbeat()
                except InvalidRunLockError:
                    heartbeat_stop.set()
                    break

        heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        def _phase_abort_error() -> Exception | None:
            if fatal_heartbeat_error is not None:
                return fatal_heartbeat_error
            if time.monotonic() - last_heartbeat_success_monotonic > LOCK_HEARTBEAT_STALE_ABORT_SECONDS:
                return RuntimeError("Heartbeat stalled near stale-lock threshold; aborting to preserve lock authority")
            return None

        try:
            _safe_maybe_heartbeat()
            if fatal_heartbeat_error is not None:
                raise fatal_heartbeat_error
            current_phase_abort_checker = _phase_abort_error
            runner()
            if fatal_heartbeat_error is not None:
                raise fatal_heartbeat_error
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
            if warning_heartbeat_error is not None:
                if heartbeat_degraded:
                    print(
                        f"Warning: heartbeat degraded during phase {phase_name} after repeated failures: {warning_heartbeat_error}",
                        file=sys.stderr,
                    )
                else:
                    print(f"Warning: heartbeat failed during phase {phase_name}: {warning_heartbeat_error}", file=sys.stderr)
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
        if args.mode == "full":
            _run_full_pipeline(paths, args, run_phase_with_progress, current_abort_error)
        elif args.mode == "rework-only":
            _run_rework_only(paths, args, run_phase_with_progress)
        else:
            raise SystemExit(EXIT_USAGE_ERROR)
    finally:
        try:
            if lock_path is not None and release_run_lock(lock_path, args.run_id):
                print(f"Released lock: {lock_path}")
        except InvalidRunLockError as exc:
            print(f"Failed to release lock safely: {exc}", file=sys.stderr)
            raise SystemExit(EXIT_INVALID_LOCK) from exc


if __name__ == "__main__":
    main()
