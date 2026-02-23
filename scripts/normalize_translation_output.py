#!/usr/bin/env python3
"""Normalize translate.py output into canonical preprocessed artifacts."""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PARAGRAPH_SEPARATOR = "\n\n"
PARAGRAPH_SEPARATOR_LEN = len(PARAGRAPH_SEPARATOR)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Required source artifact missing: {path}")

    rows: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL at {path}:{line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid JSONL object at {path}:{line_no}")
        rows.append(payload)

    if not rows:
        raise ValueError(f"Required source artifact is empty: {path}")
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _extract_translations(payload: dict[str, Any], expected_len: int, source: Path) -> list[str]:
    records = payload.get("records")
    translated: list[str]
    if isinstance(records, list) and records:
        by_index: dict[int, str] = {}
        for record in records:
            if not isinstance(record, dict):
                raise ValueError(f"Invalid translation record in {source}: expected object")
            idx = record.get("paragraph_index")
            if not isinstance(idx, int):
                raise ValueError(f"Invalid translation record in {source}: missing integer paragraph_index")
            if idx in by_index:
                raise ValueError(f"Duplicate paragraph_index={idx} in {source}")
            value = record.get("translation", "")
            by_index[idx] = "" if value is None else str(value)

        keys = set(by_index)
        if keys == set(range(1, expected_len + 1)):
            translated = [by_index[i] for i in range(1, expected_len + 1)]
        elif keys == set(range(0, expected_len)):
            translated = [by_index[i] for i in range(0, expected_len)]
        else:
            raise ValueError(
                f"Translation record indices in {source} must be contiguous 0-based or 1-based and match source length"
            )
        return translated

    translated = payload.get("paragraph_translations", [])
    if not isinstance(translated, list):
        raise ValueError(f"paragraph_translations must be a list in {source}")
    return ["" if row is None else str(row) for row in translated]


def _sentence_spans(text: str) -> list[tuple[str, int, int]]:
    spans: list[tuple[str, int, int]] = []
    for match in re.finditer(r"[^.!?؟。！？\n]+(?:[.!?؟。！？]+|$)", text, flags=re.MULTILINE):
        raw_sentence = match.group(0)
        if not raw_sentence.strip():
            continue

        left_trim = len(raw_sentence) - len(raw_sentence.lstrip())
        right_trim = len(raw_sentence) - len(raw_sentence.rstrip())
        start = match.start() + left_trim
        end = match.end() - right_trim
        if end <= start:
            continue

        sentence = text[start:end]
        spans.append((sentence, start, end))

    if not spans and text.strip():
        clean = text.strip()
        start = text.find(clean)
        spans.append((clean, start, start + len(clean)))
    return spans


def _token_spans(text: str) -> list[tuple[str, int, int]]:
    return [(m.group(0), m.start(), m.end()) for m in re.finditer(r"\S+", text)]


def normalize_translation_output(source_pre: Path, translation_json: Path, output_pre: Path) -> None:
    if not translation_json.exists():
        raise FileNotFoundError(f"Missing translation output: {translation_json}")

    source_rows = read_jsonl(source_pre / "paragraphs.jsonl")
    payload = json.loads(translation_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid translation payload in {translation_json}: expected object")

    translated = _extract_translations(payload, len(source_rows), translation_json)
    if len(source_rows) != len(translated):
        raise ValueError(
            f"Translated paragraph count mismatch for {translation_json}: source={len(source_rows)} translated={len(translated)}"
        )

    paragraph_rows: list[dict[str, Any]] = []
    sentence_rows: list[dict[str, Any]] = []
    word_rows: list[dict[str, Any]] = []
    tokenized_paragraphs: list[dict[str, Any]] = []

    manuscript_id = str(source_rows[0].get("manuscript_id", source_pre.name)) if source_rows else source_pre.name
    source_value = str(source_rows[0].get("source", str(source_pre))) if source_rows else str(source_pre)

    paragraph_start = 0
    sentence_counter = 1
    word_counter = 1
    token_counter = 1

    for idx, (source_row, translated_text) in enumerate(zip(source_rows, translated), start=1):
        paragraph_text = str(translated_text)
        row = dict(source_row)
        row["text"] = paragraph_text
        row.setdefault("paragraph_index", idx)
        paragraph_rows.append(row)

        paragraph_id = str(row.get("paragraph_id") or row.get("id") or f"{manuscript_id}-p{idx:04d}")
        sentence_spans = _sentence_spans(paragraph_text)
        paragraph_tokens: list[dict[str, Any]] = []

        for sentence_text, sent_local_start, sent_local_end in sentence_spans:
            sentence_id = f"{manuscript_id}-s{sentence_counter:06d}"
            sentence_record = {
                "id": sentence_id,
                # order is 0-based append order within manuscript sequence
                "order": len(sentence_rows),
                "prev_id": sentence_rows[-1]["id"] if sentence_rows else None,
                "next_id": None,
                "text": sentence_text,
                "start_char": paragraph_start + sent_local_start,
                "end_char": paragraph_start + sent_local_end,
                "paragraph_id": paragraph_id,
                "manuscript_id": manuscript_id,
                "source": source_value,
            }
            if sentence_rows:
                sentence_rows[-1]["next_id"] = sentence_id
            sentence_rows.append(sentence_record)

            for token, token_local_start, token_local_end in _token_spans(sentence_text):
                word_id = f"{manuscript_id}-w{word_counter:06d}"
                word_record = {
                    "id": word_id,
                    # order is 0-based append order within manuscript sequence
                    "order": len(word_rows),
                    "prev_id": word_rows[-1]["id"] if word_rows else None,
                    "next_id": None,
                    "text": token,
                    "start_char": paragraph_start + sent_local_start + token_local_start,
                    "end_char": paragraph_start + sent_local_start + token_local_end,
                    "sentence_id": sentence_id,
                    "paragraph_id": paragraph_id,
                    "manuscript_id": manuscript_id,
                    "source": source_value,
                }
                if word_rows:
                    word_rows[-1]["next_id"] = word_id
                word_rows.append(word_record)
                word_counter += 1

            sentence_counter += 1

        for local_index, (token, start_char, end_char) in enumerate(_token_spans(paragraph_text)):
            paragraph_tokens.append(
                {
                    "token_id": f"{manuscript_id}-t{token_counter:06d}",
                    "text": token,
                    "start_char": paragraph_start + start_char,
                    "end_char": paragraph_start + end_char,
                    "global_index": token_counter,
                    "local_index": local_index,
                }
            )
            token_counter += 1

        tokenized_paragraphs.append(
            {
                "paragraph_id": paragraph_id,
                "order": idx - 1,
                "text": paragraph_text,
                "tokens": paragraph_tokens,
            }
        )
        paragraph_start += len(paragraph_text) + PARAGRAPH_SEPARATOR_LEN

    manuscript_tokens = {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "source": source_value,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tokenization": {
            "method": "regex",
            "model": "whitespace+punctuation",
            "notes": "Normalized from translate.py output while preserving paragraph_id/content_hash lineage.",
        },
        "paragraphs": tokenized_paragraphs,
    }

    output_pre.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_pre / "paragraphs.jsonl", paragraph_rows)
    write_jsonl(output_pre / "sentences.jsonl", sentence_rows)
    write_jsonl(output_pre / "words.jsonl", word_rows)
    (output_pre / "manuscript_tokens.json").write_text(
        json.dumps(manuscript_tokens, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize translate.py output into canonical preprocessed artifacts.")
    parser.add_argument("--source-pre", type=Path, required=True)
    parser.add_argument("--translation-json", type=Path, required=True)
    parser.add_argument("--output-pre", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    normalize_translation_output(args.source_pre, args.translation_json, args.output_pre)


if __name__ == "__main__":
    main()
