#!/usr/bin/env python3
"""Normalize critics runner payloads into map_review_to_paragraphs-compatible JSON."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Flatten critics_runner output keyed by critic into a single `{\"issues\": [...]}` payload "
            "consumable by map_review_to_paragraphs.py."
        )
    )
    parser.add_argument("--input", type=Path, required=True, help="Path to critics runner JSON output.")
    parser.add_argument("--output", type=Path, required=True, help="Path to normalized JSON output.")
    return parser.parse_args()


def _coerce_issue(issue: Any, *, critic_name: str) -> dict[str, Any] | None:
    if not isinstance(issue, dict):
        return None
    normalized = dict(issue)
    normalized.setdefault("reviewer", critic_name)
    normalized.setdefault("critic_name", critic_name)
    return normalized


def normalize_critics_payload(payload: Any) -> dict[str, list[dict[str, Any]]]:
    if isinstance(payload, list):
        return {"issues": [issue for issue in payload if isinstance(issue, dict)]}

    if not isinstance(payload, dict):
        raise SystemExit("Critics payload must be a JSON object or list.")

    # Already adapter-shaped or single-critic shaped payload.
    if isinstance(payload.get("issues"), list):
        return {"issues": [issue for issue in payload["issues"] if isinstance(issue, dict)]}

    issues: list[dict[str, Any]] = []
    for critic_name, critic_payload in payload.items():
        if not isinstance(critic_name, str) or not critic_name.strip():
            continue
        if not isinstance(critic_payload, dict):
            continue
        critic_issues = critic_payload.get("issues")
        if not isinstance(critic_issues, list):
            continue
        for issue in critic_issues:
            normalized_issue = _coerce_issue(issue, critic_name=critic_name.strip())
            if normalized_issue is not None:
                issues.append(normalized_issue)

    return {"issues": issues}


def main() -> None:
    args = parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    normalized = normalize_critics_payload(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
