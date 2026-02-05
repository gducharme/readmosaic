#!/usr/bin/env python3
"""Pre-process manuscript text into paragraph, sentence, and word JSONL files.

Also emits a single JSON artifact aligned to schemas/manuscript_tokens.schema.json.

Usage examples:
  python scripts/pre_processing.py path/to/manuscript.md
  python scripts/pre_processing.py path/to/manuscript.txt --output-dir /preprocessed
  python scripts/pre_processing.py path/to/manuscript.txt --manuscript-id draft-01

Run with --help to see all options.
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import nltk

try:
    from rich.console import Console
except ImportError:  # pragma: no cover - optional dependency
    Console = None


@dataclass
class ParagraphRecord:
    text: str
    line_numbers: List[int]
    paragraph_index: int
    line_offsets: List[int]


def ensure_nltk() -> None:
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)


def clean_markdown_line(line: str) -> str:
    line = re.sub(r"^\s{0,3}#{1,6}\s+", "", line)
    line = re.sub(r"^\s*>\s?", "", line)
    line = re.sub(r"^\s*[-*+]\s+", "", line)
    line = re.sub(r"^\s*\d+\.\s+", "", line)
    line = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", line)
    line = re.sub(r"\[([^\]]+)\]\([^\)]*\)", r"\1", line)
    line = re.sub(r"`[^`]+`", "", line)
    line = re.sub(r"[*_]{1,3}", "", line)
    line = re.sub(r"\s+", " ", line)
    return line.strip()


def strip_code_blocks(text: str) -> str:
    return re.sub(r"```.*?```", "", text, flags=re.DOTALL)


def parse_paragraphs(raw_text: str) -> List[ParagraphRecord]:
    raw_text = strip_code_blocks(raw_text)
    lines = raw_text.splitlines()

    cleaned_lines = []
    for idx, raw_line in enumerate(lines, start=1):
        cleaned_line = clean_markdown_line(raw_line)
        cleaned_lines.append((idx, cleaned_line))

    paragraphs: List[ParagraphRecord] = []
    buffer: List[tuple[int, str]] = []
    paragraph_index = 0

    def flush_buffer() -> None:
        nonlocal paragraph_index
        if not buffer:
            return
        paragraph_index += 1
        line_numbers = [item[0] for item in buffer]
        line_offsets: List[int] = []
        running = 0
        for _, line_text in buffer:
            line_offsets.append(running)
            running += len(line_text) + 1
        paragraph_text = "\n".join(item[1] for item in buffer)
        paragraphs.append(
            ParagraphRecord(
                text=paragraph_text,
                line_numbers=line_numbers,
                paragraph_index=paragraph_index,
                line_offsets=line_offsets,
            )
        )

    for item in cleaned_lines:
        if item[1].strip() == "":
            flush_buffer()
            buffer = []
            continue
        buffer.append(item)
    flush_buffer()

    return paragraphs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pre-process manuscripts into paragraph, sentence, and word JSONL files.",
        epilog="Example: scripts/pre_processing.py manuscript.md --output-dir /preprocessed",
    )
    parser.add_argument("input_path", help="Path to a .txt/.md manuscript file.")
    parser.add_argument(
        "--output-dir",
        default="/preprocessed",
        help="Directory where JSONL outputs are written (default: /preprocessed).",
    )
    parser.add_argument(
        "--manuscript-id",
        help="Optional manuscript identifier to store in each record.",
    )
    parser.add_argument(
        "--skip-jsonl",
        action="store_true",
        help="Skip legacy JSONL outputs (paragraphs/sentences/words).",
    )
    return parser.parse_args()


def build_paragraph_id(manuscript_id: str, index: int) -> str:
    return f"{manuscript_id}-p{index:04d}"


def build_token_id(manuscript_id: str, global_index: int) -> str:
    return f"{manuscript_id}-t{global_index:06d}"


def locate_tokens(text: str, tokens: Iterable[str]) -> List[tuple[str, int, int]]:
    spans: List[tuple[str, int, int]] = []
    cursor = 0
    for token in tokens:
        position = text.find(token, cursor)
        if position == -1:
            position = cursor
        start = position
        end = position + len(token)
        spans.append((token, start, end))
        cursor = end
    return spans


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def build_prev_next(ids: List[int]) -> List[tuple[Optional[int], Optional[int]]]:
    pairs: List[tuple[Optional[int], Optional[int]]] = []
    for idx, current in enumerate(ids):
        prev_id = ids[idx - 1] if idx > 0 else None
        next_id = ids[idx + 1] if idx + 1 < len(ids) else None
        pairs.append((prev_id, next_id))
    return pairs


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_path)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manuscript_id = args.manuscript_id or input_path.stem
    source = str(input_path)

    raw_text = input_path.read_text(encoding="utf-8")
    paragraphs = parse_paragraphs(raw_text)

    ensure_nltk()

    paragraph_records: List[dict] = []
    sentence_records: List[dict] = []
    word_records: List[dict] = []
    tokenized_paragraphs: List[dict] = []

    paragraph_ids = list(range(1, len(paragraphs) + 1))
    paragraph_links = build_prev_next(paragraph_ids)

    paragraph_start = 0
    sentence_id = 1
    word_id = 1
    global_token_index = 0

    for idx, (paragraph, (prev_id, next_id)) in enumerate(
        zip(paragraphs, paragraph_links)
    ):
        paragraph_text = paragraph.text
        paragraph_end = paragraph_start + len(paragraph_text)
        paragraph_record = {
            "id": paragraph_ids[idx],
            "order": idx,
            "prev_id": prev_id,
            "next_id": next_id,
            "text": paragraph_text,
            "start_char": paragraph_start,
            "end_char": paragraph_end,
            "manuscript_id": manuscript_id,
            "source": source,
        }
        paragraph_records.append(paragraph_record)

        paragraph_id = build_paragraph_id(manuscript_id, idx + 1)
        tokens = nltk.word_tokenize(paragraph_text)
        token_spans = locate_tokens(paragraph_text, tokens)
        token_records: List[dict] = []
        for local_index, (token, token_start, token_end) in enumerate(token_spans):
            token_records.append(
                {
                    "token_id": build_token_id(manuscript_id, global_token_index),
                    "text": token,
                    "start_char": token_start,
                    "end_char": token_end,
                    "global_index": global_token_index,
                    "local_index": local_index,
                }
            )
            global_token_index += 1

        tokenized_paragraphs.append(
            {
                "paragraph_id": paragraph_id,
                "order": idx,
                "text": paragraph_text,
                "tokens": token_records,
            }
        )

        sentences = nltk.sent_tokenize(paragraph_text)
        search_start = 0
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            position = paragraph_text.find(sentence, search_start)
            if position == -1:
                position = search_start
            search_start = position + len(sentence)
            sentence_start = paragraph_start + position
            sentence_end = sentence_start + len(sentence)

            sentence_record = {
                "id": sentence_id,
                "order": len(sentence_records),
                "prev_id": sentence_records[-1]["id"] if sentence_records else None,
                "next_id": None,
                "text": sentence,
                "start_char": sentence_start,
                "end_char": sentence_end,
                "paragraph_id": paragraph_ids[idx],
                "manuscript_id": manuscript_id,
                "source": source,
            }
            if sentence_records:
                sentence_records[-1]["next_id"] = sentence_id
            sentence_records.append(sentence_record)

            tokens = nltk.word_tokenize(sentence)
            token_spans = locate_tokens(sentence, tokens)
            for token, token_start, token_end in token_spans:
                word_start = sentence_start + token_start
                word_end = sentence_start + token_end
                word_record = {
                    "id": word_id,
                    "order": len(word_records),
                    "prev_id": word_records[-1]["id"] if word_records else None,
                    "next_id": None,
                    "text": token,
                    "start_char": word_start,
                    "end_char": word_end,
                    "sentence_id": sentence_id,
                    "paragraph_id": paragraph_ids[idx],
                    "manuscript_id": manuscript_id,
                    "source": source,
                }
                if word_records:
                    word_records[-1]["next_id"] = word_id
                word_records.append(word_record)
                word_id += 1

            sentence_id += 1

        paragraph_start = paragraph_end + 2

    manuscript_tokens_artifact = {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "source": source,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tokenization": {
            "method": "nltk",
            "model": "punkt",
            "notes": "Paragraph-level token offsets generated via nltk.word_tokenize.",
        },
        "paragraphs": tokenized_paragraphs,
    }

    if not args.skip_jsonl:
        write_jsonl(output_dir / "paragraphs.jsonl", paragraph_records)
        write_jsonl(output_dir / "sentences.jsonl", sentence_records)
        write_jsonl(output_dir / "words.jsonl", word_records)
    (output_dir / "manuscript_tokens.json").write_text(
        json.dumps(manuscript_tokens_artifact, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    console = Console() if Console else None
    summary_lines = [
        f"Paragraphs: {len(paragraph_records)}",
        f"Sentences: {len(sentence_records)}",
        f"Words: {len(word_records)}",
        f"Tokens artifact: {output_dir / 'manuscript_tokens.json'}",
        f"Output directory: {output_dir}",
    ]
    if args.skip_jsonl:
        summary_lines.insert(3, "Legacy JSONL outputs skipped.")
    if console:
        for line in summary_lines:
            console.print(line)
    else:
        for line in summary_lines:
            print(line)


if __name__ == "__main__":
    main()
