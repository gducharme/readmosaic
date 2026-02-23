#!/usr/bin/env python3
"""Assemble canonical candidate markdown and candidate_map from normalized paragraphs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid JSONL object at {path}:{line_no}")
        rows.append(payload)
    return rows


def assemble_candidate(paragraphs_path: Path, candidate_md: Path, candidate_map: Path) -> None:
    rows = _read_jsonl(paragraphs_path)
    if not rows:
        raise ValueError(f"No paragraph rows found in {paragraphs_path}")

    blocks: list[str] = []
    map_rows: list[dict[str, Any]] = []
    current_line = 1

    for index, row in enumerate(rows, start=1):
        paragraph_id = row.get("paragraph_id") or row.get("id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            raise ValueError(f"Row {index} missing paragraph_id/id in {paragraphs_path}")

        text = str(row.get("text", "")).strip("\n")
        block_lines = text.splitlines() if text else [""]
        start_line = current_line
        end_line = start_line + len(block_lines) - 1

        blocks.append("\n".join(block_lines))
        map_rows.append(
            {
                "paragraph_id": paragraph_id,
                "paragraph_index": index,
                "start_line": start_line,
                "end_line": end_line,
            }
        )

        current_line = end_line + 2

    candidate_text = "\n\n".join(blocks)
    candidate_lines = candidate_text.splitlines()

    for i, row in enumerate(map_rows, start=1):
        if row["paragraph_index"] != i:
            raise ValueError("paragraph_index must be 1-based and contiguous")
        if row["start_line"] < 1 or row["end_line"] < row["start_line"]:
            raise ValueError(f"Invalid range for paragraph_index={row['paragraph_index']}")
        if row["end_line"] > len(candidate_lines):
            raise ValueError(f"Range exceeds candidate.md bounds for paragraph_index={row['paragraph_index']}")

    for i in range(len(map_rows) - 1):
        left = map_rows[i]
        right = map_rows[i + 1]
        if right["start_line"] != left["end_line"] + 2:
            raise ValueError("Candidate map invariant violated: expected exactly one blank line between blocks")
        separator_line = left["end_line"]
        if separator_line >= len(candidate_lines) or candidate_lines[separator_line] != "":
            raise ValueError("Candidate markdown invariant violated: paragraph blocks must be separated by one blank line")

    candidate_md.parent.mkdir(parents=True, exist_ok=True)
    candidate_md.write_text(candidate_text + "\n", encoding="utf-8")
    candidate_map.parent.mkdir(parents=True, exist_ok=True)
    with candidate_map.open("w", encoding="utf-8") as handle:
        for row in map_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assemble candidate.md and candidate_map.jsonl from preprocessed paragraphs.")
    parser.add_argument("--paragraphs", type=Path, required=True)
    parser.add_argument("--candidate-md", type=Path, required=True)
    parser.add_argument("--candidate-map", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    assemble_candidate(args.paragraphs, args.candidate_md, args.candidate_map)


if __name__ == "__main__":
    main()
