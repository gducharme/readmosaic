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
