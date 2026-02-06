#!/usr/bin/env python3
"""Aggregate paragraph text and detector issues into a unified bundle artifact."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a paragraph-level bundle containing source text, issues, "
            "detector evidence/signals, and routing hints."
        )
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        required=True,
        help="Path to preprocessing output directory (expects paragraphs.jsonl).",
    )
    parser.add_argument(
        "--tool-results",
        type=Path,
        required=True,
        help="Path to JSON file containing the orchestrator tool results list.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output path for bundle artifact (.json or .jsonl).",
    )
    parser.add_argument(
        "--manuscript-id",
        help="Optional manuscript identifier override.",
    )
    return parser.parse_args()


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def _to_issue_bundle_item(issue: Dict[str, Any], source_tool: str | None) -> Dict[str, Any]:
    return {
        "issue_id": issue.get("issue_id"),
        "type": issue.get("type"),
        "status": issue.get("status"),
        "location": issue.get("location", {}),
        "evidence": {
            "summary": issue.get("evidence", {}).get("summary"),
            "signals": issue.get("evidence", {}).get("signals", []),
            "detector": issue.get("evidence", {}).get("detector") or source_tool,
        },
        "suggested_actions": issue.get("suggested_actions", []),
        "routing": issue.get("routing", {}),
        "impact": issue.get("impact"),
        "source_tool": source_tool,
    }


def build_bundle(
    paragraphs: List[Dict[str, Any]],
    tool_results: List[Dict[str, Any]],
    manuscript_id: str,
) -> Dict[str, Any]:
    by_paragraph: Dict[str, List[Dict[str, Any]]] = {
        paragraph.get("id", ""): [] for paragraph in paragraphs if paragraph.get("id")
    }

    for result in tool_results:
        edits_path = result.get("edits_path")
        if not edits_path:
            continue
        path = Path(edits_path)
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue

        for issue in payload.get("items", []):
            if not isinstance(issue, dict):
                continue
            location = issue.get("location", {})
            paragraph_id = location.get("paragraph_id")
            if not paragraph_id or paragraph_id not in by_paragraph:
                continue
            by_paragraph[paragraph_id].append(
                _to_issue_bundle_item(issue, result.get("code"))
            )

    records: List[Dict[str, Any]] = []
    for paragraph in paragraphs:
        paragraph_id = paragraph.get("id")
        if not paragraph_id:
            continue
        records.append(
            {
                "manuscript_id": manuscript_id,
                "paragraph_id": paragraph_id,
                "paragraph_order": paragraph.get("order"),
                "paragraph_text": paragraph.get("text", ""),
                "issues": by_paragraph.get(paragraph_id, []),
            }
        )

    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": records,
    }


def main() -> None:
    args = parse_args()
    paragraphs_path = args.preprocessing / "paragraphs.jsonl"
    if not paragraphs_path.exists():
        raise SystemExit(f"Expected preprocessing paragraph artifact not found: {paragraphs_path}")
    if not args.tool_results.exists():
        raise SystemExit(f"Tool results file not found: {args.tool_results}")

    paragraphs = _read_jsonl(paragraphs_path)
    tool_results_payload = json.loads(args.tool_results.read_text(encoding="utf-8"))
    if not isinstance(tool_results_payload, list):
        raise SystemExit("Tool results payload must be a JSON list.")

    inferred_manuscript_id = paragraphs[0].get("manuscript_id") if paragraphs else None
    manuscript_id = args.manuscript_id or inferred_manuscript_id
    if not manuscript_id:
        raise SystemExit("Unable to infer manuscript_id; pass --manuscript-id.")

    bundle_payload = build_bundle(paragraphs, tool_results_payload, manuscript_id)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.suffix.lower() == ".jsonl":
        with args.output.open("w", encoding="utf-8") as handle:
            for item in bundle_payload["items"]:
                handle.write(json.dumps(item, ensure_ascii=False))
                handle.write("\n")
    else:
        args.output.write_text(
            json.dumps(bundle_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
