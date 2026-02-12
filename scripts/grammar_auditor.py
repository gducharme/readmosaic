#!/usr/bin/env python3
"""Ultra-precision grammar auditing CLI with strict JSON output aggregation."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
import json
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List

from libs.local_llm import (
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content,
)
from schema_validator import validate_payload

DEFAULT_BASE_URL = DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL
DEFAULT_PROMPT_PATH = Path("prompts/Ultra_Precision_Grammar_Auditor.txt")
DEFAULT_SCHEMA_NAME = "grammar_audit_report.schema.json"
DEFAULT_OUTPUT_DIR = Path("grammar_outputs")


@dataclass
class SentenceUnit:
    text: str
    order: int
    sentence_id: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ultra-precision grammar analysis and collect all issues into one JSON file."
    )
    parser.add_argument("--file", type=Path, help="Input .txt or .md file path (fallback mode).")
    parser.add_argument(
        "--preprocessed",
        type=Path,
        help="Optional pre-processing directory that contains sentences.jsonl (preferred).",
    )
    parser.add_argument("--model", required=True, help="Model identifier served by LM Studio.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Chat completions URL.")
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT_PATH, help="System prompt file path.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for output report.")
    parser.add_argument("--timeout", type=int, default=240, help="Request timeout in seconds.")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Words per chunk (recommended 800-1200).")
    parser.add_argument("--preview", action="store_true", help="Do not call model; emit a stub report.")

    parser.add_argument("--include-optional-preferences", action="store_true")
    parser.add_argument("--include-style-choices", action="store_true")
    parser.add_argument("--include-register-consistency", action="store_true")
    parser.add_argument("--include-ambiguities", action="store_true")
    parser.add_argument("--strict-cmos", action="store_true")
    parser.add_argument("--clause-level-parse", action="store_true")

    return parser.parse_args()


def load_sentence_units(args: argparse.Namespace) -> List[SentenceUnit]:
    if args.preprocessed:
        sentences_path = args.preprocessed / "sentences.jsonl"
        if not sentences_path.exists():
            raise SystemExit(f"Preprocessed sentences artifact not found: {sentences_path}")

        units: List[SentenceUnit] = []
        for raw in sentences_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            row = json.loads(raw)
            text = str(row.get("text", "")).strip()
            if not text:
                continue
            units.append(
                SentenceUnit(
                    text=text,
                    order=int(row.get("order", len(units))),
                    sentence_id=str(row["id"]) if "id" in row and row["id"] is not None else None,
                )
            )
        units.sort(key=lambda item: item.order)
        return units

    if not args.file:
        raise SystemExit("Provide either --preprocessed (preferred) or --file.")
    if not args.file.exists():
        raise SystemExit(f"Input file not found: {args.file}")

    text = args.file.read_text(encoding="utf-8").strip()
    if not text:
        return []

    rough_sentences = [segment.strip() for segment in text.split("\n") if segment.strip()]
    return [SentenceUnit(text=segment, order=index, sentence_id=None) for index, segment in enumerate(rough_sentences)]


def chunk_sentence_units(units: List[SentenceUnit], chunk_size_words: int) -> List[Dict[str, Any]]:
    if not units:
        return []

    chunks: List[Dict[str, Any]] = []
    current: List[SentenceUnit] = []
    current_words = 0

    for unit in units:
        word_count = len(unit.text.split())
        projected = current_words + word_count
        if current and projected > chunk_size_words:
            chunks.append(_build_chunk(current, len(chunks) + 1))
            current = []
            current_words = 0
        current.append(unit)
        current_words += max(word_count, 1)

    if current:
        chunks.append(_build_chunk(current, len(chunks) + 1))

    total = len(chunks)
    for chunk in chunks:
        chunk["total"] = total
    return chunks


def _build_chunk(units: List[SentenceUnit], index: int) -> Dict[str, Any]:
    return {
        "index": index,
        "total": 0,
        "text": "\n".join(unit.text for unit in units),
        "sentence_start_order": units[0].order,
        "sentence_end_order": units[-1].order,
        "sentence_start_id": units[0].sentence_id,
        "sentence_end_id": units[-1].sentence_id,
        "sentence_count": len(units),
    }


def build_user_payload(chunk: Dict[str, Any], flags: Dict[str, bool]) -> str:
    payload = {
        "mode": "ultra_precision_grammar_audit",
        "chunk": {
            "index": chunk["index"],
            "total": chunk["total"],
            "text": chunk["text"],
            "sentence_start_order": chunk["sentence_start_order"],
            "sentence_end_order": chunk["sentence_end_order"],
            "sentence_start_id": chunk["sentence_start_id"],
            "sentence_end_id": chunk["sentence_end_id"],
            "sentence_count": chunk["sentence_count"],
        },
        "flags": flags,
        "instruction": "Return JSON only in the required schema from the system prompt.",
    }
    return json.dumps(payload, ensure_ascii=False)



def summarize_issues(issues: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"total_issues": len(issues), "critical": 0, "major": 0, "minor": 0, "house_style": 0}
    for issue in issues:
        severity = str(issue.get("severity", "")).lower()
        if severity == "critical":
            summary["critical"] += 1
        elif severity == "major":
            summary["major"] += 1
        elif severity == "minor":
            summary["minor"] += 1
        elif severity == "house style":
            summary["house_style"] += 1
    return summary


def build_stub_report(input_source: str, prompt_file: Path, model: str, flags: Dict[str, bool]) -> Dict[str, Any]:
    return {
        "tool": "ultra_precision_grammar_auditor",
        "input_file": input_source,
        "prompt_file": str(prompt_file),
        "model": model,
        "flags": flags,
        "status": "sound",
        "issues": [],
        "summary": {"total_issues": 0, "critical": 0, "major": 0, "minor": 0, "house_style": 0},
        "note": "Preview mode enabled; no model calls executed.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    args = parse_args()

    if not args.prompt.exists():
        raise SystemExit(f"Prompt file not found: {args.prompt}")
    if args.chunk_size < 100:
        raise SystemExit("--chunk-size must be >= 100 words.")

    sentence_units = load_sentence_units(args)
    input_source = str(args.preprocessed / "sentences.jsonl") if args.preprocessed else str(args.file)

    flags = {
        "include_optional_preferences": args.include_optional_preferences,
        "include_style_choices": args.include_style_choices,
        "include_register_consistency": args.include_register_consistency,
        "include_ambiguities": args.include_ambiguities,
        "strict_cmos": args.strict_cmos,
        "clause_level_parse": args.clause_level_parse,
    }

    if args.preview:
        report = build_stub_report(input_source, args.prompt, args.model, flags)
    else:
        system_prompt = args.prompt.read_text(encoding="utf-8")
        chunks = chunk_sentence_units(sentence_units, args.chunk_size)
        if not chunks:
            chunks = [
                {
                    "index": 1,
                    "total": 1,
                    "text": "",
                    "sentence_start_order": 0,
                    "sentence_end_order": 0,
                    "sentence_start_id": None,
                    "sentence_end_id": None,
                    "sentence_count": 0,
                }
            ]

        merged_issues: List[Dict[str, Any]] = []
        for chunk in chunks:
            user_payload = build_user_payload(chunk, flags)
            content = request_chat_completion_content(
                args.base_url,
                args.model,
                system_prompt,
                user_payload,
                args.timeout,
                temperature=0.0,
            )
            try:
                model_output = json.loads(content)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"Model returned non-JSON content: {content}") from exc
            issues = model_output.get("issues", [])
            if not isinstance(issues, list):
                raise SystemExit(f"Invalid model response for chunk {chunk['index']}: 'issues' must be a list.")
            merged_issues.extend(issues)

        summary = summarize_issues(merged_issues)
        report = {
            "tool": "ultra_precision_grammar_auditor",
            "input_file": input_source,
            "prompt_file": str(args.prompt),
            "model": args.model,
            "flags": flags,
            "status": "sound" if not merged_issues else "ok",
            "issues": merged_issues,
            "summary": summary,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    validate_payload(report, DEFAULT_SCHEMA_NAME, "grammar_audit_report")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = args.output_dir / f"grammar_audit_issues_{stamp}.json"
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Saved grammar issues report: {out_file}")


if __name__ == "__main__":
    main()
