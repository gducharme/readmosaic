#!/usr/bin/env python3
"""Translate manuscript text paragraph-by-paragraph using language-specific prompts."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from typing import Any

from libs.local_llm import (
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content,
)

DEFAULT_BASE_URL = DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL
DEFAULT_PROMPT_ROOTS = [Path("prompt/translate"), Path("prompts/translate")]
DEFAULT_OUTPUT_ROOT = Path("output/translate")


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

    def render(self, completed: int, failed: int = 0) -> str:
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

        status = f"{completed}/{self.total}"
        if failed:
            status += f" | failed: {failed}"

        return (
            f"\r[{bar}] {completed}/{self.total} "
            f"({ratio * 100:5.1f}%) ETA {eta} [{status}]"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Translate manuscript content paragraph-by-paragraph using a language-specific prompt "
            "from prompt/translate or prompts/translate."
        )
    )
    parser.add_argument("--file", type=Path, help="Optional input manuscript (.txt/.md).")
    parser.add_argument("--language", required=True, help="Target language (e.g., French).")
    parser.add_argument("--model", required=True, help="Model identifier served by LM Studio.")
    parser.add_argument(
        "--preprocessed",
        type=Path,
        help="Optional pre-processing directory or JSONL file used as the source of paragraphs.",
    )
    parser.add_argument(
        "--prompt-root",
        type=Path,
        help="Override root directory for language prompts.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root output directory (default: output/translate).",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="LM Studio chat completions URL.")
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallel request count.")
    parser.add_argument(
        "--retry",
        type=int,
        default=1,
        help="Retry count per paragraph after an initial failure (default: 1).",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.file and not args.preprocessed:
        raise SystemExit("Provide --file or --preprocessed as a translation source.")
    if args.file and not args.file.exists():
        raise SystemExit(f"Input file not found: {args.file}")
    if args.preprocessed and not args.preprocessed.exists():
        raise SystemExit(f"Preprocessed path not found: {args.preprocessed}")
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be at least 1")
    if args.retry < 0:
        raise SystemExit("--retry must be 0 or greater")


def normalize_language(language: str) -> str:
    return language.strip().strip("/")


def _candidate_prompt_paths(prompt_root: Path, language: str) -> list[Path]:
    variants = [language, language.lower(), language.replace(" ", "_"), language.lower().replace(" ", "_")]
    exts = ["", ".txt", ".md"]
    candidates: list[Path] = []
    for variant in variants:
        for ext in exts:
            candidates.append(prompt_root / f"{variant}{ext}")
    return candidates


def resolve_prompt_path(language: str, prompt_root: Path | None) -> Path:
    roots = [prompt_root] if prompt_root else DEFAULT_PROMPT_ROOTS
    for root in roots:
        if root is None or not root.exists() or not root.is_dir():
            continue
        for candidate in _candidate_prompt_paths(root, language):
            if candidate.exists() and candidate.is_file():
                return candidate

    roots_display = ", ".join(str(r) for r in roots if r is not None)
    raise SystemExit(
        f"No translation prompt found for language '{language}'. Checked: {roots_display}. "
        "Create a matching prompt file (e.g., French.txt)."
    )


def _load_paragraphs_from_jsonl(path: Path) -> list[str]:
    paragraphs: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        row = raw.strip()
        if not row:
            continue
        payload = json.loads(row)
        if isinstance(payload, dict) and "text" in payload:
            text = str(payload["text"]).strip()
            if text:
                paragraphs.append(text)
    return paragraphs


def load_source_paragraphs(input_path: Path | None, preprocessed: Path | None) -> list[str]:
    if preprocessed:
        if preprocessed.is_file() and preprocessed.suffix.lower() == ".jsonl":
            paragraphs = _load_paragraphs_from_jsonl(preprocessed)
            if paragraphs:
                return paragraphs
        elif preprocessed.is_dir():
            preferred = [preprocessed / "paragraphs.jsonl", preprocessed / "lines.jsonl", preprocessed / "sentences.jsonl"]
            for candidate in preferred:
                if candidate.exists() and candidate.is_file():
                    paragraphs = _load_paragraphs_from_jsonl(candidate)
                    if paragraphs:
                        return paragraphs

    if input_path is None:
        return []

    raw = input_path.read_text(encoding="utf-8")
    return [block.strip() for block in raw.split("\n\n") if block.strip()]


def call_lm(base_url: str, model: str, system_prompt: str, language: str, text: str, timeout: int) -> str:
    user_prompt = (
        f"Translate this single source paragraph into {language}. Return only translated text.\\n\\n"
        f"SOURCE PARAGRAPH:\n{text}"
    )
    return request_chat_completion_content(
        base_url,
        model,
        system_prompt,
        user_prompt,
        timeout,
        temperature=0.2,
    )


def main() -> None:
    args = parse_args()
    validate_args(args)

    normalized_language = normalize_language(args.language)
    if not normalized_language:
        raise SystemExit("--language must contain at least one non-slash character")

    prompt_path = resolve_prompt_path(normalized_language, args.prompt_root)
    prompt_text = prompt_path.read_text(encoding="utf-8")

    source_paragraphs = load_source_paragraphs(args.file, args.preprocessed)
    if not source_paragraphs:
        raise SystemExit("No source paragraphs found to translate.")

    progress = ProgressBar(total=len(source_paragraphs))
    print(progress.render(0, failed=0), end="", flush=True)

    failures = 0
    results: list[dict[str, Any]] = []

    def process_paragraph(index: int, text: str) -> dict[str, Any]:
        translation = ""
        error: str | None = None
        for attempt in range(args.retry + 1):
            try:
                translation = call_lm(
                    args.base_url,
                    args.model,
                    prompt_text,
                    normalized_language,
                    text,
                    args.timeout,
                )
                error = None
                break
            except (Exception, SystemExit) as exc:  # noqa: BLE001
                error = str(exc)
                if attempt >= args.retry:
                    break
        return {
            "paragraph_index": index,
            "source": text,
            "translation": translation,
            "error": error,
        }

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(process_paragraph, idx, paragraph)
            for idx, paragraph in enumerate(source_paragraphs, start=1)
        ]
        for completed, future in enumerate(as_completed(futures), start=1):
            row = future.result()
            results.append(row)
            if row.get("error"):
                failures += 1
            print(progress.render(completed, failed=failures), end="", flush=True)

    print()

    results.sort(key=lambda row: int(row["paragraph_index"]))
    paragraph_translations = [str(row["translation"]) for row in results]
    full_translation = "\n\n".join(paragraph_translations)
    whole_translation = full_translation

    language_dir = args.output_root / normalized_language.lower().replace(" ", "_")
    language_dir.mkdir(parents=True, exist_ok=True)
    output_path = language_dir / "translation.json"

    payload = {
        "language": normalized_language,
        "model": args.model,
        "input_file": str(args.file) if args.file else None,
        "preprocessed": str(args.preprocessed) if args.preprocessed else None,
        "prompt": str(prompt_path),
        "paragraph_translations": paragraph_translations,
        "full_translation": full_translation,
        "whole_translation": whole_translation,
        "records": results,
        "failures": failures,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Translated paragraphs: {len(source_paragraphs)}")
    if failures:
        print(f"Completed with {failures} failed paragraph(s).")
    print(f"Output JSON: {output_path}")


if __name__ == "__main__":
    main()
