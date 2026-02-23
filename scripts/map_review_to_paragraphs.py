#!/usr/bin/env python3
"""Map manuscript-level review issues to candidate paragraph indices.

Resolution order (deterministic):
1. `start_line`/`end_line` range overlap
2. single `line`
3. bounded `quote` lookup within candidate paragraphs, tie-break by earliest
   absolute line number then lowest `paragraph_index`

If an issue cannot be resolved, emit a `mapping_error` row with anchor metadata
(and deterministic candidate metadata for ambiguous mappings) for downstream
hard-fail policy gates.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ParagraphRange:
    paragraph_id: str | None
    paragraph_index: int
    start_line: int
    end_line: int


@dataclass(frozen=True)
class MapResult:
    status: str
    paragraph_index: int | None
    paragraph_id: str | None
    anchor_type: str
    reason: str | None
    candidates: list[dict[str, Any]] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map reviewer issues to paragraphs.")
    parser.add_argument("--run-id", required=True, help="Run identifier under runs/<run_id>.")
    parser.add_argument(
        "--review-input",
        type=Path,
        required=True,
        help="Path to reviewer output (JSON object/list or JSONL with issue-like rows).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output JSONL path for mapped rows (including mapping_error rows).",
    )
    parser.add_argument(
        "--reviewer",
        default="unknown",
        help="Reviewer identifier attached to output rows.",
    )
    return parser.parse_args()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_candidate_map(path: Path) -> list[ParagraphRange]:
    records: list[ParagraphRange] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON in candidate_map at {path}:{line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            raise SystemExit(f"Invalid JSONL object at {path}:{line_no}.")

        required = ("paragraph_index", "start_line", "end_line")
        missing = [key for key in required if key not in payload]
        if missing:
            missing_csv = ", ".join(missing)
            raise SystemExit(
                f"candidate_map row missing required mapping fields ({missing_csv}) "
                f"at {path}:{line_no}."
            )

        try:
            start_line = int(payload["start_line"])
            end_line = int(payload["end_line"])
            paragraph_index = int(payload["paragraph_index"])
        except (TypeError, ValueError) as exc:
            raise SystemExit(
                f"candidate_map row has non-integer line/index fields at {path}:{line_no}."
            ) from exc

        if start_line < 1 or end_line < start_line:
            raise SystemExit(
                f"Invalid candidate_map range for paragraph_index={paragraph_index} "
                f"at {path}:{line_no} (start_line={start_line}, end_line={end_line})."
            )

        records.append(
            ParagraphRange(
                paragraph_id=(str(payload.get("paragraph_id")) if payload.get("paragraph_id") else None),
                paragraph_index=paragraph_index,
                start_line=start_line,
                end_line=end_line,
            )
        )

    if not records:
        raise SystemExit(f"candidate_map is empty: {path}")

    records.sort(key=lambda row: (row.paragraph_index, row.start_line, row.end_line))
    return records


def _validate_candidate_map_bounds(ranges: list[ParagraphRange], candidate_line_count: int, path: Path) -> None:
    for row in ranges:
        if row.start_line > candidate_line_count:
            raise SystemExit(
                "candidate_map range starts beyond candidate.md bounds: "
                f"paragraph_index={row.paragraph_index}, start_line={row.start_line}, "
                f"candidate_line_count={candidate_line_count} ({path})."
            )
        if row.end_line > candidate_line_count:
            raise SystemExit(
                "candidate_map range exceeds candidate.md bounds: "
                f"paragraph_index={row.paragraph_index}, end_line={row.end_line}, "
                f"candidate_line_count={candidate_line_count} ({path})."
            )


def _extract_issues(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("issues"), list):
            return [item for item in payload["issues"] if isinstance(item, dict)]
        if isinstance(payload.get("items"), list):
            return [item for item in payload["items"] if isinstance(item, dict)]
        if any(key in payload for key in ("issue_id", "line", "start_line", "end_line", "quote")):
            return [payload]
        raise SystemExit(
            "Review object does not contain an `issues`/`items` list and does not "
            "look like a single issue object."
        )

    raise SystemExit("Review input must be a JSON object/list or JSONL of objects.")


def _extract_issues_from_path(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        rows: list[dict[str, Any]] = []
        for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"Invalid JSON in review input at {path}:{line_no}: {exc}") from exc
            try:
                rows.extend(_extract_issues(payload))
            except SystemExit as exc:
                raise SystemExit(f"Invalid review row at {path}:{line_no}: {exc}") from exc
        return rows
    return _extract_issues(_load_json(path))


def _line_anchor(issue: dict[str, Any], key: str) -> int | None:
    value = issue.get(key)
    if value is None:
        return None
    try:
        line = int(value)
    except (TypeError, ValueError):
        return None
    return line if line >= 1 else None


def _candidate_meta(row: ParagraphRange, hit_line: int | None = None) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "paragraph_index": row.paragraph_index,
        "paragraph_id": row.paragraph_id,
        "start_line": row.start_line,
        "end_line": row.end_line,
    }
    if hit_line is not None:
        meta["hit_line"] = hit_line
    return meta


def _map_by_range(start_line: int, end_line: int, ranges: list[ParagraphRange]) -> MapResult:
    overlaps = [
        row
        for row in ranges
        if not (end_line < row.start_line or start_line > row.end_line)
    ]
    if len(overlaps) == 1:
        match = overlaps[0]
        return MapResult("mapped", match.paragraph_index, match.paragraph_id, "range", None)
    if not overlaps:
        return MapResult("mapping_error", None, None, "range", "range_not_found", [])
    candidates = [_candidate_meta(row) for row in overlaps]
    return MapResult("mapping_error", None, None, "range", "ambiguous_range_overlap", candidates)


def _map_by_line(line: int, ranges: list[ParagraphRange]) -> MapResult:
    matches = [row for row in ranges if row.start_line <= line <= row.end_line]
    if len(matches) == 1:
        match = matches[0]
        return MapResult("mapped", match.paragraph_index, match.paragraph_id, "line", None)
    if not matches:
        return MapResult("mapping_error", None, None, "line", "line_not_found", [])
    candidates = [_candidate_meta(row) for row in matches]
    return MapResult("mapping_error", None, None, "line", "ambiguous_line_membership", candidates)


def _find_quote_hits(candidate_lines: list[str], quote: str, row: ParagraphRange) -> list[tuple[int, int]]:
    paragraph_text = "\n".join(candidate_lines[row.start_line - 1 : row.end_line])
    hits: list[tuple[int, int]] = []
    start = 0
    while True:
        offset = paragraph_text.find(quote, start)
        if offset == -1:
            break
        line_offset = paragraph_text[:offset].count("\n")
        absolute_line = row.start_line + line_offset
        hits.append((absolute_line, row.paragraph_index))
        start = offset + 1
    return hits


def _map_by_quote(quote: str, candidate_lines: list[str], ranges: list[ParagraphRange]) -> MapResult:
    if not quote:
        return MapResult("mapping_error", None, None, "quote", "empty_quote", [])

    candidates: list[tuple[int, int, ParagraphRange]] = []
    for row in ranges:
        for hit_line, para_index in _find_quote_hits(candidate_lines, quote, row):
            candidates.append((hit_line, para_index, row))

    if not candidates:
        return MapResult("mapping_error", None, None, "quote", "quote_not_found", [])

    candidates.sort(key=lambda item: (item[0], item[1]))
    chosen = candidates[0][2]
    return MapResult("mapped", chosen.paragraph_index, chosen.paragraph_id, "quote", None)


def _resolve_issue(issue: dict[str, Any], candidate_lines: list[str], ranges: list[ParagraphRange]) -> MapResult:
    start_present = "start_line" in issue
    end_present = "end_line" in issue
    start_line = _line_anchor(issue, "start_line")
    end_line = _line_anchor(issue, "end_line")

    if start_present ^ end_present:
        return MapResult(
            "mapping_error",
            None,
            None,
            "range",
            "incomplete_range_anchor",
            [],
        )

    if start_present and end_present and (start_line is None or end_line is None):
        return MapResult(
            "mapping_error",
            None,
            None,
            "range",
            "invalid_range_anchor",
            [],
        )

    if start_line is not None and end_line is not None:
        normalized_start = min(start_line, end_line)
        normalized_end = max(start_line, end_line)
        return _map_by_range(normalized_start, normalized_end, ranges)

    line_present = "line" in issue
    line = _line_anchor(issue, "line")
    if line_present and line is None:
        return MapResult("mapping_error", None, None, "line", "invalid_line_anchor", [])
    if line is not None:
        return _map_by_line(line, ranges)

    quote_raw = issue.get("quote")
    quote = quote_raw.strip() if isinstance(quote_raw, str) else ""
    if isinstance(quote_raw, str) and not quote:
        return MapResult("mapping_error", None, None, "quote", "empty_quote", [])
    if quote:
        return _map_by_quote(quote, candidate_lines, ranges)

    return MapResult("mapping_error", None, None, "none", "missing_anchor", [])


def _issue_id(issue: dict[str, Any], fallback_index: int) -> str:
    value = issue.get("issue_id")
    if isinstance(value, str) and value.strip():
        return value
    return f"issue_{fallback_index:04d}"


def _emit_rows(
    run_id: str,
    reviewer: str,
    issues: Iterable[dict[str, Any]],
    candidate_lines: list[str],
    ranges: list[ParagraphRange],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, issue in enumerate(issues, start=1):
        issue_id = _issue_id(issue, index)
        result = _resolve_issue(issue, candidate_lines, ranges)

        base: dict[str, Any] = {
            "run_id": run_id,
            "reviewer": reviewer,
            "issue_id": issue_id,
            "mapping_status": result.status,
            "anchor_type": result.anchor_type,
            "paragraph_index": result.paragraph_index,
            "paragraph_id": result.paragraph_id,
            "issue": issue,
        }
        if result.status == "mapping_error":
            base["reason"] = result.reason
            base["candidates"] = result.candidates or []
        rows.append(base)
    return rows


def main() -> None:
    args = parse_args()

    run_root = Path("runs") / args.run_id
    candidate_map_path = run_root / "final" / "candidate_map.jsonl"
    candidate_md_path = run_root / "final" / "candidate.md"

    if not candidate_map_path.exists():
        raise SystemExit(f"Canonical mapping source missing: {candidate_map_path}")
    if not candidate_md_path.exists():
        raise SystemExit(f"Canonical mapping source missing: {candidate_md_path}")
    if not args.review_input.exists():
        raise SystemExit(f"Review input not found: {args.review_input}")

    ranges = _read_candidate_map(candidate_map_path)
    candidate_lines = candidate_md_path.read_text(encoding="utf-8").splitlines()
    _validate_candidate_map_bounds(ranges, len(candidate_lines), candidate_map_path)
    issues = _extract_issues_from_path(args.review_input)

    rows = _emit_rows(args.run_id, args.reviewer, issues, candidate_lines, ranges)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


if __name__ == "__main__":
    main()
