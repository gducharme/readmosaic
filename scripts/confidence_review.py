#!/usr/bin/env python3
"""Render manuscript text with confidence coloring from Mosaic edits.

The script scans *_edits.json outputs, maps issues back to token IDs produced by
scripts/pre_processing.py, and prints a colorized manuscript with confidence
levels that mirror whisper.cpp's 5-band display.

Run with --help for full options.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from rich.console import Console
from rich.text import Text


CONFIDENCE_LEVELS = (
    (0.0, 1.0, "green4"),
    (0.25, 0.8, "green3"),
    (0.5, 0.6, "yellow1"),
    (0.75, 0.4, "orange3"),
    (1.0, 0.2, "red1"),
)

PUNCTUATION_CLOSERS = {
    ".",
    ",",
    "!",
    "?",
    ";",
    ":",
    "%",
    ")",
    "]",
    "}",
    "''",
    "”",
    "’",
}
PUNCTUATION_OPENERS = {"(", "[", "{", "``", "“", "‘"}
NO_SPACE_BEFORE = PUNCTUATION_CLOSERS | {
    "'s",
    "n't",
    "'re",
    "'ve",
    "'m",
    "'ll",
    "'d",
}


@dataclass(frozen=True)
class WordRecord:
    word_id: str
    sentence_id: str
    paragraph_id: str
    text: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render a confidence-colored manuscript using Mosaic *_edits.json outputs."
        ),
        epilog=(
            "Example: scripts/confidence_review.py --preprocessed /preprocessed "
            "--edits-root /mosaic/outputs"
        ),
    )
    parser.add_argument(
        "--preprocessed",
        default=Path("/preprocessed"),
        type=Path,
        help="Directory containing manuscript_tokens.json and words.jsonl.",
    )
    parser.add_argument(
        "--edits-root",
        default=Path("/mosaic/outputs"),
        type=Path,
        help="Root directory containing tool outputs with *_edits.json files.",
    )
    parser.add_argument(
        "--show-word-ids",
        action="store_true",
        help="Append word IDs after each token for debugging alignment.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional cap on the number of edits files to process.",
    )
    return parser.parse_args()


def load_words(path: Path) -> List[WordRecord]:
    if not path.exists():
        raise FileNotFoundError(f"words.jsonl not found: {path}")
    records: List[WordRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            records.append(
                WordRecord(
                    word_id=payload["id"],
                    sentence_id=payload["sentence_id"],
                    paragraph_id=payload["paragraph_id"],
                    text=payload["text"],
                )
            )
    return records


def load_token_index(path: Path) -> Dict[str, int]:
    if not path.exists():
        raise FileNotFoundError(f"manuscript_tokens.json not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    index: Dict[str, int] = {}
    for paragraph in payload.get("paragraphs", []):
        for token in paragraph.get("tokens", []):
            token_id = token.get("token_id")
            global_index = token.get("global_index")
            if token_id is None or global_index is None:
                continue
            index[token_id] = int(global_index)
    return index


def find_edits_files(root: Path, max_files: Optional[int]) -> List[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Edits root not found: {root}")
    files = sorted(root.glob("*/*_edits.json"))
    if max_files is not None:
        files = files[: max_files]
    return files


def iter_token_ids(edits_payload: dict) -> Iterable[str]:
    for item in edits_payload.get("items", []):
        location = item.get("location", {})
        for token_id in location.get("token_ids", []) or []:
            yield token_id


def build_issue_counts(
    edits_files: Iterable[Path], token_index: Dict[str, int], total_tokens: int
) -> List[int]:
    counts = [0 for _ in range(total_tokens)]
    missing_tokens: Dict[str, int] = {}
    for edits_file in edits_files:
        payload = json.loads(edits_file.read_text(encoding="utf-8"))
        for token_id in iter_token_ids(payload):
            if token_id not in token_index:
                missing_tokens[token_id] = missing_tokens.get(token_id, 0) + 1
                continue
            index = token_index[token_id]
            if 0 <= index < total_tokens:
                counts[index] += 1
    if missing_tokens:
        missing_sample = ", ".join(list(missing_tokens.keys())[:5])
        raise ValueError(
            "Some token IDs referenced in edits were not found in manuscript_tokens.json: "
            f"{missing_sample}"
        )
    return counts


def confidence_for_count(normalized_count: float) -> tuple[float, str]:
    for threshold, score, color in CONFIDENCE_LEVELS:
        if normalized_count <= threshold:
            return score, color
    return CONFIDENCE_LEVELS[-1][1], CONFIDENCE_LEVELS[-1][2]


def should_prefix_space(token: str, prev_token: Optional[str]) -> bool:
    if prev_token is None:
        return False
    if token in NO_SPACE_BEFORE:
        return False
    if prev_token in PUNCTUATION_OPENERS:
        return False
    return True


def render_text(
    words: List[WordRecord],
    normalized_counts: List[float],
    show_word_ids: bool,
    num_sources: int,
) -> None:
    console = Console()
    total_score = 0.0
    total_tokens = len(words)
    for idx, normalized_count in enumerate(normalized_counts):
        score, _ = confidence_for_count(normalized_count)
        total_score += score
    avg_confidence = total_score / total_tokens if total_tokens else 1.0

    console.print(
        f"Overall confidence: {avg_confidence:.2%} across {total_tokens} tokens",
        style="bold",
    )
    console.print(
        "Legend: deep green (clean) → light green → yellow → orange → red "
        f"(confidence based on normalized issue rate, count / {num_sources} source(s))",
        style="dim",
    )

    current_paragraph = None
    current_sentence = None
    buffer = Text()
    prev_token: Optional[str] = None

    def flush_buffer() -> None:
        nonlocal buffer, prev_token
        if buffer:
            console.print(buffer)
            buffer = Text()
            prev_token = None

    for index, word in enumerate(words):
        if word.paragraph_id != current_paragraph:
            flush_buffer()
            if current_paragraph is not None:
                console.print()
            console.print(f"Paragraph {word.paragraph_id}", style="bold dim")
            current_paragraph = word.paragraph_id
            current_sentence = None

        if word.sentence_id != current_sentence:
            flush_buffer()
            console.print(f"Sentence {word.sentence_id}", style="dim")
            current_sentence = word.sentence_id

        score, color = confidence_for_count(normalized_counts[index])
        token_text = word.text
        if show_word_ids:
            token_text = f"{token_text}[{word.word_id}]"

        if should_prefix_space(word.text, prev_token):
            buffer.append(" ")
        buffer.append(token_text, style=color)
        prev_token = word.text

    flush_buffer()


def main() -> None:
    args = parse_args()
    preprocessed_dir = args.preprocessed
    words_path = preprocessed_dir / "words.jsonl"
    tokens_path = preprocessed_dir / "manuscript_tokens.json"

    words = load_words(words_path)
    token_index = load_token_index(tokens_path)
    edits_files = find_edits_files(args.edits_root, args.max_files)

    if not edits_files:
        raise SystemExit(
            f"No *_edits.json files found under {args.edits_root}. "
            "Run the Mosaic tools to generate edits outputs first."
        )

    issue_counts = build_issue_counts(edits_files, token_index, len(words))
    tool_sources = {edits_file.parent.name for edits_file in edits_files}
    num_sources = len(tool_sources) or len(edits_files)
    if num_sources == 0:
        num_sources = 1
    normalized_counts = [count / num_sources for count in issue_counts]
    render_text(words, normalized_counts, args.show_word_ids, num_sources)


if __name__ == "__main__":
    main()
