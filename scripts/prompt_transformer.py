#!/usr/bin/env python3
"""Prompt-driven line/paragraph transformation against a local LM Studio endpoint."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
import json
from datetime import datetime, timezone
import time
from typing import Dict, List

from libs.local_llm import (
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    extract_message_content,
    post_chat_completion,
)



DEFAULT_BASE_URL = DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL
DEFAULT_PROMPTS_DIR = Path("prompts")


class ProgressBar:
    """Simple terminal progress bar with count and ETA."""

    def __init__(self, total: int, width: int = 30) -> None:
        self.total = total
        self.width = width
        self.start = time.monotonic()

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        seconds = max(0, int(seconds))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{sec:02d}"
        return f"{minutes:02d}:{sec:02d}"

    def render(self, completed: int) -> str:
        ratio = completed / self.total if self.total else 1.0
        filled = min(self.width, int(ratio * self.width))
        bar = "#" * filled + "-" * (self.width - filled)

        elapsed = time.monotonic() - self.start
        if completed and completed < self.total:
            eta_seconds = (elapsed / completed) * (self.total - completed)
            eta = self._format_seconds(eta_seconds)
        elif completed >= self.total:
            eta = "00:00"
        else:
            eta = "--:--"

        return (
            f"\r[{bar}] {completed}/{self.total} "
            f"({ratio * 100:5.1f}%) ETA {eta}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Apply a selected prompt to each line or paragraph of a manuscript using a local LLM."
        )
    )
    parser.add_argument("--file", required=True, type=Path, help="Input .txt or .md manuscript path.")
    parser.add_argument(
        "--prompt",
        required=True,
        help=(
            "Prompt filename or path. If not an existing path, it is resolved inside ./prompts "
            "(for example: Revision_Assistant_Template.txt)."
        ),
    )
    parser.add_argument("--model", required=True, help="Model identifier served by LM Studio.")
    parser.add_argument(
        "--resolution",
        required=True,
        choices=["line", "paragraph"],
        help="Processing granularity.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("prompt_outputs"),
        help="Directory where output artifacts are written.",
    )
    parser.add_argument(
        "--preprocessed",
        type=Path,
        help=(
            "Optional pre-processing artifact directory. For paragraph mode, "
            "paragraphs.jsonl is preferred when available."
        ),
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="LM Studio chat completions URL.")
    parser.add_argument("--timeout", type=int, default=180, help="HTTP request timeout in seconds.")
    parser.add_argument(
        "--max-items",
        type=int,
        help="Optional cap on the number of lines/paragraphs to process.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Build artifacts without calling the model (useful for sanity checks).",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.file.exists():
        raise SystemExit(f"Input file not found: {args.file}")
    if args.preprocessed and not args.preprocessed.exists():
        raise SystemExit(f"Pre-processed directory not found: {args.preprocessed}")


def resolve_prompt_path(prompt_value: str) -> Path:
    direct = Path(prompt_value)
    if direct.exists():
        return direct
    from_prompts = DEFAULT_PROMPTS_DIR / prompt_value
    if from_prompts.exists():
        return from_prompts
    raise SystemExit(
        f"Prompt not found: {prompt_value}. Provide a valid path or a filename in {DEFAULT_PROMPTS_DIR}/"
    )


def load_lines(input_path: Path) -> List[Dict[str, object]]:
    units: List[Dict[str, object]] = []
    for idx, raw in enumerate(input_path.read_text(encoding="utf-8").splitlines(), start=1):
        text = raw.strip()
        if not text:
            continue
        units.append({"unit_index": len(units) + 1, "line_number": idx, "text": text})
    return units


def load_paragraphs(input_path: Path, preprocessed_dir: Path | None) -> List[Dict[str, object]]:
    if preprocessed_dir:
        paragraphs_jsonl = preprocessed_dir / "paragraphs.jsonl"
        if paragraphs_jsonl.exists():
            units: List[Dict[str, object]] = []
            for raw in paragraphs_jsonl.read_text(encoding="utf-8").splitlines():
                if not raw.strip():
                    continue
                row = json.loads(raw)
                text = str(row.get("text", "")).strip()
                if not text:
                    continue
                units.append(
                    {
                        "unit_index": len(units) + 1,
                        "paragraph_id": row.get("paragraph_id"),
                        "order": row.get("order"),
                        "text": text,
                    }
                )
            return units

    raw = input_path.read_text(encoding="utf-8")
    blocks = [chunk.strip() for chunk in raw.split("\n\n")]
    units = []
    for block in blocks:
        if not block:
            continue
        units.append({"unit_index": len(units) + 1, "text": block})
    return units


def call_lm(base_url: str, model: str, system_prompt: str, text: str, timeout: int, resolution: str) -> str:
    user_prompt = (
        f"You are transforming one {resolution} from a manuscript. "
        "Return only the revised text for this unit without commentary.\n\n"
        f"SOURCE:\n{text}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    parsed = post_chat_completion(base_url, payload, timeout)
    return extract_message_content(parsed)


def main() -> None:
    args = parse_args()
    validate_args(args)

    prompt_path = resolve_prompt_path(args.prompt)
    prompt_text = prompt_path.read_text(encoding="utf-8")

    if args.resolution == "line":
        units = load_lines(args.file)
    else:
        units = load_paragraphs(args.file, args.preprocessed)

    if args.max_items is not None:
        units = units[: args.max_items]

    if not units:
        raise SystemExit("No units found to process.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_slug = args.model.replace("/", "_").replace(":", "_")
    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = out_dir / f"{args.resolution}_{model_slug}_{timestamp}.jsonl"
    md_path = out_dir / f"{args.resolution}_{model_slug}_{timestamp}.md"

    progress = ProgressBar(total=len(units))

    with jsonl_path.open("w", encoding="utf-8") as jsonl_file, md_path.open("w", encoding="utf-8") as md_file:
        md_file.write(f"# Prompt Transformation Output\n\n")
        md_file.write(f"- input: `{args.file}`\n")
        md_file.write(f"- prompt: `{prompt_path}`\n")
        md_file.write(f"- model: `{args.model}`\n")
        md_file.write(f"- resolution: `{args.resolution}`\n")
        md_file.write(f"- preview: `{args.preview}`\n\n")

        print(progress.render(0), end="", flush=True)
        for index, unit in enumerate(units, start=1):
            source_text = str(unit["text"])
            rewritten = "[preview mode: no model call]"
            if not args.preview:
                rewritten = call_lm(
                    args.base_url,
                    args.model,
                    prompt_text,
                    source_text,
                    args.timeout,
                    args.resolution,
                )

            output_row = {
                "unit": unit,
                "model": args.model,
                "resolution": args.resolution,
                "prompt": str(prompt_path),
                "source_text": source_text,
                "rewritten_text": rewritten,
            }
            jsonl_file.write(json.dumps(output_row, ensure_ascii=False) + "\n")

            md_file.write(f"## Unit {unit['unit_index']}\n\n")
            md_file.write("### Source\n")
            md_file.write(f"{source_text}\n\n")
            md_file.write("### Rewrite\n")
            md_file.write(f"{rewritten}\n\n")

            print(progress.render(index), end="", flush=True)

    print()

    print(f"Processed {len(units)} {args.resolution}(s)")
    print(f"JSONL output: {jsonl_path}")
    print(f"Markdown output: {md_path}")


if __name__ == "__main__":
    main()
