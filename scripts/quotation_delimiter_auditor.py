#!/usr/bin/env python3
"""Quotation and delimiter precision auditing CLI with strict JSON output aggregation."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List

from libs.local_llm import (
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content,
)
from schema_validator import validate_payload

DEFAULT_BASE_URL = DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL
DEFAULT_PROMPT_PATH = Path("prompts/Quotation_Delimiter_Precision_Auditor.txt")
DEFAULT_SCHEMA_NAME = "quotation_delimiter_audit_report.schema.json"
DEFAULT_OUTPUT_DIR = Path("quotation_audit_outputs")


@dataclass
class SentenceUnit:
    text: str
    order: int
    sentence_id: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run quotation/delimiter precision analysis and collect all issues into one JSON file."
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
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of chunk requests to run in parallel. Defaults to 1 (sequential).",
    )
    parser.add_argument("--preview", action="store_true", help="Do not call model; emit a stub report.")
    parser.add_argument(
        "--flag-em-en-dash-misuse",
        action="store_true",
        help="Also flag em/en dash misuse inside quoted text.",
    )

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


def align_issue_sentence_indices(
    issues: List[Dict[str, Any]], chunk: Dict[str, Any], total_sentences: int
) -> List[Dict[str, Any]]:
    """Align issue sentence_index values to pre-processing global sentence order."""
    aligned: List[Dict[str, Any]] = []
    sentence_start_order = int(chunk.get("sentence_start_order", 0))
    sentence_count = int(chunk.get("sentence_count", 0))

    for issue in issues:
        issue_copy = dict(issue)
        raw_index = issue_copy.get("sentence_index")
        try:
            local_index = int(raw_index)
        except (TypeError, ValueError):
            aligned.append(issue_copy)
            continue

        if 1 <= local_index <= sentence_count:
            global_index = sentence_start_order + local_index
        elif 0 <= local_index < sentence_count:
            global_index = sentence_start_order + local_index + 1
        else:
            aligned.append(issue_copy)
            continue

        issue_copy["sentence_index"] = max(1, min(global_index, total_sentences))
        aligned.append(issue_copy)

    return aligned


def build_user_payload(chunk: Dict[str, Any], flags: Dict[str, bool], total_sentences: int) -> str:
    payload = {
        "mode": "quotation_delimiter_precision_audit",
        "document": {"sentence_count": total_sentences},
        "chunk": {
            "index": chunk["index"],
            "total": chunk["total"],
            "text": chunk["text"],
            "sentence_start_order": chunk["sentence_start_order"],
            "sentence_end_order": chunk["sentence_end_order"],
            "sentence_start_index": chunk["sentence_start_order"] + 1,
            "sentence_end_index": chunk["sentence_end_order"] + 1,
            "sentence_start_id": chunk["sentence_start_id"],
            "sentence_end_id": chunk["sentence_end_id"],
            "sentence_count": chunk["sentence_count"],
        },
        "flags": flags,
        "instruction": "Return JSON only in the required schema from the system prompt.",
    }
    return json.dumps(payload, ensure_ascii=False)


def summarize_issues(issues: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"total_issues": len(issues), "error": 0, "inconsistency": 0, "style_choice": 0, "encoding_artifact": 0}
    for issue in issues:
        classification = str(issue.get("classification", "")).lower()
        if classification == "error":
            summary["error"] += 1
        elif classification == "inconsistency":
            summary["inconsistency"] += 1
        elif classification == "style choice":
            summary["style_choice"] += 1
        elif classification == "encoding artifact":
            summary["encoding_artifact"] += 1
    return summary


def build_stub_report(input_source: str, prompt_file: Path, model: str, flags: Dict[str, bool]) -> Dict[str, Any]:
    return {
        "tool": "quotation_delimiter_precision_auditor",
        "input_file": input_source,
        "prompt_file": str(prompt_file),
        "model": model,
        "flags": flags,
        "status": "sound",
        "detected_punctuation_convention": "Undetermined",
        "issues": [],
        "summary": {"total_issues": 0, "error": 0, "inconsistency": 0, "style_choice": 0, "encoding_artifact": 0},
        "note": "Preview mode enabled; no model calls executed.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    args = parse_args()

    if not args.prompt.exists():
        raise SystemExit(f"Prompt file not found: {args.prompt}")
    if args.chunk_size < 100:
        raise SystemExit("--chunk-size must be >= 100 words.")
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1.")

    sentence_units = load_sentence_units(args)
    input_source = str(args.preprocessed / "sentences.jsonl") if args.preprocessed else str(args.file)

    flags = {
        "flag_em_en_dash_misuse": args.flag_em_en_dash_misuse,
    }

    if args.preview:
        report = build_stub_report(input_source, args.prompt, args.model, flags)
    else:
        system_prompt = args.prompt.read_text(encoding="utf-8")
        chunks = chunk_sentence_units(sentence_units, args.chunk_size)
        total_sentences = len(sentence_units)
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
            total_sentences = 0

        merged_issues: List[Dict[str, Any]] = []
        detected_punctuation_convention = "Undetermined"

        def run_chunk(chunk: Dict[str, Any]) -> Dict[str, Any]:
            user_payload = build_user_payload(chunk, flags, total_sentences)
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
            aligned = align_issue_sentence_indices(issues, chunk, total_sentences)
            return {
                "issues": aligned,
                "detected_punctuation_convention": model_output.get("detected_punctuation_convention", "Undetermined"),
            }

        total_chunks = len(chunks)
        if args.concurrency == 1:
            for idx, chunk in enumerate(chunks, start=1):
                result = run_chunk(chunk)
                merged_issues.extend(result["issues"])
                if result["detected_punctuation_convention"] != "Undetermined":
                    detected_punctuation_convention = result["detected_punctuation_convention"]
                print(f"Progress: {idx}/{total_chunks} chunks processed")
        else:
            completed = 0
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = {pool.submit(run_chunk, chunk): chunk for chunk in chunks}
                for future in as_completed(futures):
                    chunk = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise SystemExit(f"Chunk {chunk['index']} failed: {exc}") from exc
                    merged_issues.extend(result["issues"])
                    if result["detected_punctuation_convention"] != "Undetermined":
                        detected_punctuation_convention = result["detected_punctuation_convention"]
                    completed += 1
                    print(f"Progress: {completed}/{total_chunks} chunks processed")

        summary = summarize_issues(merged_issues)
        report = {
            "tool": "quotation_delimiter_precision_auditor",
            "input_file": input_source,
            "prompt_file": str(args.prompt),
            "model": args.model,
            "flags": flags,
            "status": "sound" if not merged_issues else "ok",
            "detected_punctuation_convention": detected_punctuation_convention,
            "issues": merged_issues,
            "summary": summary,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = args.output_dir / f"quotation_delimiter_audit_issues_{stamp}.json"
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    try:
        validate_payload(report, DEFAULT_SCHEMA_NAME, "quotation_delimiter_audit_report")
    except ValueError as exc:
        raise SystemExit(
            f"Saved quotation/delimiter issues report (schema-invalid): {out_file}\n{exc}"
        ) from exc

    print(f"Saved quotation/delimiter issues report: {out_file}")


if __name__ == "__main__":
    main()
