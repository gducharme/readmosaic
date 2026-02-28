from __future__ import annotations

from typing import Any


def validate_bundle(payload: dict[str, Any]) -> None:
    required_top_level = ("contract_version", "run", "metrics", "highlights")
    for key in required_top_level:
        if key not in payload:
            raise ValueError(f"Missing diagnostics bundle key: {key}")
    run = payload.get("run")
    if not isinstance(run, dict):
        raise ValueError("diagnostics bundle key 'run' must be an object")
    for key in ("run_id", "created_at"):
        if not isinstance(run.get(key), str) or not run.get(key):
            raise ValueError(f"diagnostics bundle run key '{key}' must be a non-empty string")
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("diagnostics bundle key 'metrics' must be an object")
    highlights = payload.get("highlights")
    if not isinstance(highlights, list):
        raise ValueError("diagnostics bundle key 'highlights' must be an array")


def validate_delta(payload: dict[str, Any]) -> None:
    required_top_level = ("contract_version", "comparison", "metric_deltas")
    for key in required_top_level:
        if key not in payload:
            raise ValueError(f"Missing diagnostics delta key: {key}")

