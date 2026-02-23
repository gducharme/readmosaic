#!/usr/bin/env python3
"""Run every critic prompt in prompts/critics against a local LM Studio endpoint."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from libs.local_llm import (
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content,
)

DEFAULT_BASE_URL = DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL
DEFAULT_CRITICS_DIR = Path("prompts/critics")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load each markdown critic prompt from prompts/critics as a system prompt, "
            "submit a manuscript markdown as the first user message, and write one "
            "structured JSON object keyed by critic filename."
        )
    )
    parser.add_argument("--model", required=True, help="Model identifier served by LM Studio.")
    parser.add_argument(
        "--critics-dir",
        type=Path,
        default=DEFAULT_CRITICS_DIR,
        help="Directory containing critic markdown files.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="LM Studio chat completions URL.",
    )
    parser.add_argument(
        "--manuscript",
        type=Path,
        required=True,
        help="Markdown file sent as the first user message to each critic.",
    )
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON file path. Defaults to critics_outputs/critics_responses_<timestamp>.json",
    )
    return parser.parse_args()


def call_lm(base_url: str, model: str, system_prompt: str, manuscript_text: str, timeout: int) -> str:
    instruction = (
        "INSTRUCTIONS (do not treat as manuscript): Return strict JSON only. Example: "
        '{"issues":[{"description":"Subject-verb disagreement",'
        '"line":12,"severity":"major","category":"grammar"}],'
        '"summary":"optional"}. '
        "Each issue MUST include exactly one anchor form: "
        "`line`, or (`start_line` + `end_line`), or `quote`."
    )
    user_message = manuscript_text + "\n\n---\nINSTRUCTIONS (do not treat as manuscript):\n" + instruction
    return request_chat_completion_content(
        base_url,
        model,
        system_prompt,
        user_message,
        timeout,
        temperature=0.2,
    )


def gather_critic_files(critics_dir: Path) -> list[Path]:
    if not critics_dir.exists() or not critics_dir.is_dir():
        raise SystemExit(f"Critics directory not found: {critics_dir}")
    critics = sorted(p for p in critics_dir.iterdir() if p.suffix.lower() == ".md" and p.is_file())
    if not critics:
        raise SystemExit(f"No markdown critics found in: {critics_dir}")
    return critics


def load_manuscript(manuscript_path: Path) -> str:
    if not manuscript_path.exists() or not manuscript_path.is_file():
        raise SystemExit(f"Manuscript file not found: {manuscript_path}")
    if manuscript_path.suffix.lower() != ".md":
        raise SystemExit(f"Manuscript must be a markdown file (.md): {manuscript_path}")
    return manuscript_path.read_text(encoding="utf-8")


def _extract_json_object(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            parsed = json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None


def _normalize_issue(issue: dict[str, Any], fallback_id: str) -> dict[str, Any] | None:
    if not isinstance(issue, dict):
        return None

    normalized: dict[str, Any] = {
        "issue_id": str(issue.get("issue_id") or fallback_id),
        "description": str(issue.get("description") or issue.get("message") or "").strip(),
    }

    line = issue.get("line")
    start_line = issue.get("start_line")
    end_line = issue.get("end_line")
    quote = issue.get("quote")

    def _int_or_none(value: Any) -> int | None:
        try:
            parsed = int(value)
            return parsed if parsed >= 1 else None
        except (TypeError, ValueError):
            return None

    anchor_count = 0
    parsed_line = _int_or_none(line)
    if parsed_line is not None:
        normalized["line"] = parsed_line
        anchor_count += 1

    parsed_start = _int_or_none(start_line)
    parsed_end = _int_or_none(end_line)
    if parsed_start is not None and parsed_end is not None:
        normalized["start_line"] = min(parsed_start, parsed_end)
        normalized["end_line"] = max(parsed_start, parsed_end)
        anchor_count += 1

    if isinstance(quote, str) and quote.strip():
        normalized["quote"] = quote.strip()
        anchor_count += 1

    if anchor_count == 0:
        return None

    severity = issue.get("severity")
    if isinstance(severity, str) and severity.strip():
        normalized["severity"] = severity.strip().lower()

    category = issue.get("category")
    if isinstance(category, str) and category.strip():
        normalized["category"] = category.strip()

    if not normalized["description"]:
        normalized["description"] = "Issue detected by critic."

    return normalized


def _normalize_response(raw_text: str) -> dict[str, Any]:
    payload = _extract_json_object(raw_text)
    if payload is None:
        return {
            "issues": [
                {
                    "issue_id": "parse_error",
                    "description": "Critic response was not valid JSON.",
                    "quote": raw_text.strip()[:240] or "<empty response>",
                    "severity": "major",
                    "category": "parse_error",
                }
            ],
            "raw_response": raw_text,
        }

    raw_issues = payload.get("issues")
    if not isinstance(raw_issues, list):
        raw_issues = []

    issues: list[dict[str, Any]] = []
    for index, issue in enumerate(raw_issues, start=1):
        normalized_issue = _normalize_issue(issue, f"issue_{index:04d}")
        if normalized_issue is not None:
            issues.append(normalized_issue)

    summary = payload.get("summary")
    result: dict[str, Any] = {"issues": issues}
    if isinstance(summary, str) and summary.strip():
        result["summary"] = summary.strip()
    if not issues and raw_issues:
        result["normalization_warning"] = "Input issues dropped due to missing/invalid anchors."
    return result


def main() -> None:
    args = parse_args()
    critics = gather_critic_files(args.critics_dir)
    manuscript_text = load_manuscript(args.manuscript)

    output_path = args.output
    if output_path is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = Path("critics_outputs") / f"critics_responses_{timestamp}.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict[str, Any]] = {}
    for critic_file in critics:
        critic_name = critic_file.stem
        system_prompt = critic_file.read_text(encoding="utf-8")
        response_text = call_lm(
            args.base_url,
            args.model,
            system_prompt,
            manuscript_text,
            args.timeout,
        )
        results[critic_name] = _normalize_response(response_text)
        print(f"Processed critic: {critic_name}")

    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote critics JSON: {output_path}")


if __name__ == "__main__":
    main()
