#!/usr/bin/env python3
"""Normalize reviewer outputs into aggregator-ready paragraph rows.

This normalizer may emit multiple rows for the same paragraph_id when multiple
review sources are provided; downstream aggregation is responsible for merging.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

BLOCKING_CATEGORIES = {
    "meaning_change",
    "negation",
    "numbers_units",
    "named_entity",
    "timeline",
    "who_did_what",
}

UNMAPPED_PARAGRAPH_ID = "__unmapped__"
RUN_LEVEL_BLOCKER_REASON = "mapping_error_unresolved"


def _mapping_error_requires_run_blocker(reason: str | None) -> bool:
    if not isinstance(reason, str):
        return False
    normalized = reason.strip().lower()
    if not normalized:
        return False
    return (
        normalized.startswith("ambiguous_")
        or normalized.endswith("_not_found")
        or normalized.startswith("invalid_")
        or normalized == "missing_anchor"
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSONL at {path}:{line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            raise SystemExit(f"JSONL row must be object at {path}:{line_no}")
        rows.append(payload)
    return rows


def _score_dict(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, float] = {}
    for key, raw in value.items():
        if not isinstance(key, str) or not key.strip():
            continue
        try:
            out[key] = float(raw)
        except (TypeError, ValueError):
            continue
    return out


def _issue_code(issue: dict[str, Any]) -> str:
    for key in ("code", "issue_id", "category", "severity"):
        value = issue.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "issue"


def _is_blocker(issue: dict[str, Any]) -> bool:
    severity = str(issue.get("severity", "")).strip().lower()
    category = str(issue.get("category", "")).strip().lower()
    return severity == "critical" or category in BLOCKING_CATEGORIES


def _normalize_grammar_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        paragraph_id = row.get("paragraph_id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            continue

        issues_value = row.get("issues")
        if not isinstance(issues_value, list):
            issues_value = []

        issues: list[dict[str, Any]] = [issue for issue in issues_value if isinstance(issue, dict)]

        explicit_blocking = row.get("blocking_issues")
        if isinstance(explicit_blocking, list):
            blocking_issues = [str(item) for item in explicit_blocking if isinstance(item, str) and item.strip()]
        else:
            blocking_issues = list(dict.fromkeys(_issue_code(issue) for issue in issues if _is_blocker(issue)))

        critical_count = sum(1 for issue in issues if str(issue.get("severity", "")).strip().lower() == "critical")
        hard_fail = bool(row.get("hard_fail", False) or bool(blocking_issues))
        normalized.append(
            {
                "paragraph_id": paragraph_id,
                "scores": _score_dict(row.get("scores")),
                "issues": issues,
                "blocking_issues": blocking_issues,
                "hard_fail": hard_fail,
                "issue_count": len(issues),
                "critical_count": critical_count,
                # blocker_count reflects `blocking_issues` whether explicit upstream or derived here
                "blocker_count": len(blocking_issues),
            }
        )
    return normalized


def _normalize_mapped_rows(rows: list[dict[str, Any]], reviewer_name: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    def _add_issue(paragraph_id: str, issue: dict[str, Any], hard_fail: bool) -> None:
        bucket = grouped.setdefault(
            paragraph_id,
            {
                "paragraph_id": paragraph_id,
                "scores": {},
                "issues": [],
                "blocking_issues": [],
                "hard_fail": False,
                "issue_count": 0,
                "critical_count": 0,
                "blocker_count": 0,
            },
        )
        bucket["issues"].append(issue)
        bucket["issue_count"] += 1
        if str(issue.get("severity", "")).strip().lower() == "critical":
            bucket["critical_count"] += 1

        is_blocking_issue = bool(hard_fail or _is_blocker(issue))
        if is_blocking_issue:
            code = _issue_code(issue)
            if code not in bucket["blocking_issues"]:
                bucket["blocking_issues"].append(code)
                bucket["blocker_count"] = len(bucket["blocking_issues"])
        bucket["hard_fail"] = bool(bucket["hard_fail"] or hard_fail)

    for row in rows:
        status = str(row.get("mapping_status", "")).strip().lower()
        issue_payload = row.get("issue") if isinstance(row.get("issue"), dict) else {}

        issue_out = dict(issue_payload)
        issue_id = row.get("issue_id")
        if isinstance(issue_id, str) and issue_id.strip() and "issue_id" not in issue_out:
            issue_out["issue_id"] = issue_id
        issue_out.setdefault("reviewer", reviewer_name)
        issue_out.setdefault("mapping_status", status or "mapped")

        paragraph_id = row.get("paragraph_id")
        if isinstance(paragraph_id, str) and paragraph_id.strip() and status == "mapped":
            _add_issue(paragraph_id, issue_out, hard_fail=False)
            continue

        if status == "mapping_error":
            issue_out["category"] = "mapping_error"
            issue_out["code"] = "mapping_error"
            reason = row.get("reason")
            reason_detail: str | None = None
            if isinstance(reason, str) and reason.strip():
                reason_detail = reason.strip()
                issue_out.setdefault("reason", reason_detail)

            candidates = row.get("candidates") if isinstance(row.get("candidates"), list) else []
            candidate_ids = [
                candidate.get("paragraph_id")
                for candidate in candidates
                if isinstance(candidate, dict)
                and isinstance(candidate.get("paragraph_id"), str)
                and candidate.get("paragraph_id").strip()
            ]
            candidate_ids = list(dict.fromkeys(candidate_ids))

            blocker_paragraph_ids: list[str] = []
            if isinstance(paragraph_id, str) and paragraph_id.strip():
                _add_issue(paragraph_id, issue_out, hard_fail=True)
                blocker_paragraph_ids = [paragraph_id]
            elif candidate_ids:
                for candidate_id in candidate_ids:
                    _add_issue(candidate_id, issue_out, hard_fail=True)
                blocker_paragraph_ids = candidate_ids
            else:
                _add_issue(UNMAPPED_PARAGRAPH_ID, issue_out, hard_fail=True)
                blocker_paragraph_ids = [UNMAPPED_PARAGRAPH_ID]

            if _mapping_error_requires_run_blocker(reason_detail):
                for blocker_paragraph_id in blocker_paragraph_ids:
                    bucket = grouped[blocker_paragraph_id]
                    bucket["run_level_blocker"] = True
                    bucket["run_level_blocker_reason"] = RUN_LEVEL_BLOCKER_REASON
                    bucket["run_level_blocker_detail"] = reason_detail
            continue

    return list(grouped.values())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize review outputs into unified paragraph rows.")
    parser.add_argument("--grammar-input", type=Path, help="Paragraph-scoped grammar JSON/JSONL input.")
    parser.add_argument(
        "--mapped-input",
        action="append",
        default=[],
        type=Path,
        help="Mapped typography/critics JSONL output(s) from map_review_to_paragraphs.py.",
    )
    parser.add_argument("--output", type=Path, required=True, help="Output JSONL path under review/normalized/.")
    return parser.parse_args()


def _load_maybe_jsonl(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        return _read_jsonl(path)
    payload = _read_json(path)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    raise SystemExit(f"Unsupported JSON payload shape in {path}")


def main() -> None:
    args = parse_args()

    normalized_rows: list[dict[str, Any]] = []

    if args.grammar_input:
        grammar_rows = _load_maybe_jsonl(args.grammar_input)
        normalized_rows.extend(_normalize_grammar_rows(grammar_rows))

    for mapped_path in args.mapped_input:
        mapped_rows = _read_jsonl(mapped_path)
        normalized_rows.extend(_normalize_mapped_rows(mapped_rows, reviewer_name=mapped_path.stem))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in normalized_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
