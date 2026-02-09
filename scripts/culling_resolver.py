#!/usr/bin/env python3
"""Resolve culling directives one item at a time into deletion actions."""
from __future__ import annotations

import argparse
import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

DEFAULT_BASE_URL = "http://localhost:1234/v1/chat/completions"
DEFAULT_SENTENCES = Path("mosaic_outputs/preprocessing/sentences.jsonl")
DEFAULT_DIRECTIVES = Path("mosaic_outputs/culling_directives.md")
DEFAULT_OUTPUT_DIR = Path("mosaic_outputs/culling_items")

ALLOWED_DELETION_TYPES = {"phrase", "clause", "sentence", "full_sentence"}


@dataclass
class CullingItem:
    item_id: str
    raw_text: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve culling directives into deletion-only JSON records, "
            "processing one culling item per model call with retry support."
        )
    )
    parser.add_argument("--model", required=True, help="Model identifier served by LM Studio.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="LM Studio chat completions URL.")
    parser.add_argument("--sentences", type=Path, default=DEFAULT_SENTENCES, help="Sentence/line JSONL artifact.")
    parser.add_argument("--culling-directives", type=Path, default=DEFAULT_DIRECTIVES, help="Culling directives markdown.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for per-item resolution artifacts.")
    parser.add_argument("--context-window", type=int, default=1, help="Nearby context lines before/after target line(s).")
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout in seconds.")
    parser.add_argument("--confidence-threshold", type=float, default=0.7, help="Minimum confidence required to accept the resolution.")
    parser.add_argument("--max-items", type=int, help="Optional cap on number of culling nodes to process.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.sentences.exists():
        raise SystemExit(f"Sentences JSONL not found: {args.sentences}")
    if not args.culling_directives.exists():
        raise SystemExit(f"Culling directives file not found: {args.culling_directives}")
    if args.context_window < 0:
        raise SystemExit("--context-window must be >= 0")
    if not (0.0 <= args.confidence_threshold <= 1.0):
        raise SystemExit("--confidence-threshold must be in [0, 1]")


def load_sentences(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        payload = json.loads(line)
        text = str(payload.get("text", "")).strip()
        if not text:
            continue
        payload["_source_row"] = i
        payload.setdefault("order", len(rows) + 1)
        rows.append(payload)
    if not rows:
        raise SystemExit(f"No sentence records found in: {path}")
    return rows


def parse_culling_nodes(path: Path) -> list[CullingItem]:
    items: list[CullingItem] = []
    lines = path.read_text(encoding="utf-8").splitlines()
    for idx, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if re.match(r"^([-*]|\d+[.)])\s+", line):
            cleaned = re.sub(r"^([-*]|\d+[.)])\s+", "", line).strip()
            if cleaned:
                items.append(CullingItem(item_id=f"item_{idx:04d}", raw_text=cleaned))
            continue
        if line.lower().startswith("cull") or line.lower().startswith("delete"):
            items.append(CullingItem(item_id=f"item_{idx:04d}", raw_text=line))
    if not items:
        for idx, raw in enumerate(lines, start=1):
            line = raw.strip()
            if line and not line.startswith("#"):
                items.append(CullingItem(item_id=f"item_{idx:04d}", raw_text=line))
    return items


def tokenize(text: str) -> set[str]:
    return {tok for tok in re.findall(r"[a-z0-9']+", text.lower()) if len(tok) > 2}


def serialize_full_index(sentences: list[dict[str, Any]]) -> str:
    rendered: list[str] = []
    for row in sentences:
        line_id = row.get("sentence_id") or row.get("line_id") or row.get("order")
        order = row.get("order")
        rendered.append(f"[{order}] ({line_id}) {row.get('text', '').strip()}")
    return "\n".join(rendered)


def serialize_narrow_context(item_text: str, sentences: list[dict[str, Any]], limit: int = 8) -> str:
    item_tokens = tokenize(item_text)
    scored: list[tuple[int, int, dict[str, Any]]] = []
    for idx, row in enumerate(sentences):
        score = len(item_tokens & tokenize(str(row.get("text", ""))))
        scored.append((score, -idx, row))
    top = [row for score, _, row in sorted(scored, reverse=True)[:limit] if score > 0]
    if not top:
        top = sentences[: min(limit, len(sentences))]
    rendered: list[str] = []
    for row in sorted(top, key=lambda r: int(r.get("order", 0))):
        line_id = row.get("sentence_id") or row.get("line_id") or row.get("order")
        rendered.append(f"[{row.get('order')}] ({line_id}) {row.get('text', '').strip()}")
    return "\n".join(rendered)


def build_nearby_context(sentences: list[dict[str, Any]], start: int | None, end: int | None, window: int) -> str:
    if start is None and end is None:
        return ""
    all_orders = [int(row.get("order", 0)) for row in sentences if row.get("order") is not None]
    if not all_orders:
        return ""
    min_order, max_order = min(all_orders), max(all_orders)
    target_start = start if start is not None else end
    target_end = end if end is not None else start
    if target_start is None or target_end is None:
        return ""
    slice_start = max(min_order, target_start - window)
    slice_end = min(max_order, target_end + window)
    rendered: list[str] = []
    for row in sentences:
        order = int(row.get("order", 0))
        if slice_start <= order <= slice_end:
            line_id = row.get("sentence_id") or row.get("line_id") or row.get("order")
            rendered.append(f"[{order}] ({line_id}) {row.get('text', '').strip()}")
    return "\n".join(rendered)


def build_prompt(item: CullingItem, manuscript_index: str, nearby_context: str, attempt: int) -> tuple[str, str]:
    system = (
        "You are a deletion resolver. Only culling/deletion is permitted. "
        "Never suggest edits, rewrites, substitutions, or additions. "
        "If the directive asks for modification instead of deletion, refuse it and return JSON that marks refusal via ambiguity_flags. "
        "Return strict JSON only, no markdown."
    )
    retry_note = ""
    if attempt > 1:
        retry_note = (
            "This is a retry because prior output was invalid or low-confidence. "
            "Use only the provided narrowed context and return a precise, high-confidence deletion match when possible."
        )
    user = (
        "Resolve exactly one culling node against the indexed manuscript.\n\n"
        f"CULLING_NODE_RAW_TEXT:\n{item.raw_text}\n\n"
        f"INDEXED_MANUSCRIPT:\n{manuscript_index}\n\n"
        f"NEARBY_CONTEXT_WINDOW:\n{nearby_context or '[none provided]'}\n\n"
        "Output JSON schema (all keys required):\n"
        "{\n"
        '  "line_id": string|null,\n'
        '  "line_start": integer|null,\n'
        '  "line_end": integer|null,\n'
        '  "matched_source_span": string,\n'
        '  "deletion_type": "phrase"|"clause"|"sentence"|"full_sentence",\n'
        '  "delete_text": string,\n'
        '  "reason_summary": string,\n'
        '  "metric_tags": string[],\n'
        '  "confidence": number,\n'
        '  "ambiguity_flags": string[]\n'
        "}\n\n"
        "Refusal rule for non-deletion directives: set line_id/line_start/line_end to null, set delete_text empty, "
        "set reason_summary to 'REFUSED_NON_DELETION_REQUEST', and include ambiguity_flags ['refused_non_deletion'].\n"
        f"{retry_note}"
    )
    return system, user


def call_lm(base_url: str, model: str, system_prompt: str, user_prompt: str, timeout: int) -> dict[str, Any]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.1,
    }
    req = request.Request(
        base_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except error.URLError as exc:
        raise SystemExit(f"Failed to contact model endpoint at {base_url}: {exc}") from exc
    return json.loads(body)


def extract_content(response_payload: dict[str, Any]) -> str:
    choices = response_payload.get("choices") or []
    if not choices:
        return ""
    msg = choices[0].get("message") if isinstance(choices[0], dict) else {}
    if not isinstance(msg, dict):
        return ""
    return str(msg.get("content") or "").strip()


def parse_json_strict(content: str) -> dict[str, Any] | None:
    if not content:
        return None
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def validate_resolution(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = [
        "line_id",
        "line_start",
        "line_end",
        "matched_source_span",
        "deletion_type",
        "delete_text",
        "reason_summary",
        "metric_tags",
        "confidence",
        "ambiguity_flags",
    ]
    for key in required:
        if key not in payload:
            errors.append(f"missing:{key}")

    deletion_type = payload.get("deletion_type")
    if deletion_type not in ALLOWED_DELETION_TYPES:
        errors.append("invalid:deletion_type")

    for int_key in ("line_start", "line_end"):
        value = payload.get(int_key)
        if value is not None and not isinstance(value, int):
            errors.append(f"invalid:{int_key}")

    confidence = payload.get("confidence")
    if not isinstance(confidence, (int, float)) or not (0 <= float(confidence) <= 1):
        errors.append("invalid:confidence")

    if not isinstance(payload.get("metric_tags"), list):
        errors.append("invalid:metric_tags")
    if not isinstance(payload.get("ambiguity_flags"), list):
        errors.append("invalid:ambiguity_flags")

    for str_key in ("matched_source_span", "delete_text", "reason_summary"):
        if not isinstance(payload.get(str_key), str):
            errors.append(f"invalid:{str_key}")

    return errors


def resolve_item(
    item: CullingItem,
    sentences: list[dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    full_index = serialize_full_index(sentences)
    final_resolution: dict[str, Any] | None = None
    manual_review_required = False

    for attempt in (1, 2):
        manuscript_context = full_index if attempt == 1 else serialize_narrow_context(item.raw_text, sentences)
        nearby_context = ""
        system_prompt, user_prompt = build_prompt(item, manuscript_context, nearby_context, attempt)

        request_id = str(uuid.uuid4())
        lm_response = call_lm(args.base_url, args.model, system_prompt, user_prompt, args.timeout)
        response_id = lm_response.get("id")
        content = extract_content(lm_response)

        parsed = parse_json_strict(content)
        errors = ["invalid_json"] if parsed is None else validate_resolution(parsed)
        low_confidence = False

        if parsed is not None and not errors:
            conf = float(parsed.get("confidence", 0.0))
            low_confidence = conf < args.confidence_threshold
            if not low_confidence:
                final_resolution = parsed

        attempts.append(
            {
                "attempt": attempt,
                "request_id": request_id,
                "response_id": response_id,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "raw_response_content": content,
                "raw_response_payload": lm_response,
                "parsed_resolution": parsed,
                "validation_errors": errors,
                "low_confidence": low_confidence,
            }
        )

        if final_resolution is not None:
            break

    if final_resolution is None:
        manual_review_required = True
        final_resolution = {
            "line_id": None,
            "line_start": None,
            "line_end": None,
            "matched_source_span": "",
            "deletion_type": "sentence",
            "delete_text": "",
            "reason_summary": "manual_review_required",
            "metric_tags": [],
            "confidence": 0.0,
            "ambiguity_flags": ["manual_review_required"],
        }
    else:
        nearby = build_nearby_context(
            sentences,
            final_resolution.get("line_start"),
            final_resolution.get("line_end"),
            args.context_window,
        )
        if nearby:
            final_resolution["nearby_context_window"] = nearby

    return {
        "item_id": item.item_id,
        "culling_node_raw_text": item.raw_text,
        "manual_review_required": manual_review_required,
        "resolution": final_resolution,
        "attempts": attempts,
    }


def main() -> None:
    args = parse_args()
    validate_args(args)

    sentences = load_sentences(args.sentences)
    items = parse_culling_nodes(args.culling_directives)
    if args.max_items is not None:
        items = items[: args.max_items]

    if not items:
        raise SystemExit("No culling items found in directives input.")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary: list[dict[str, Any]] = []

    for index, item in enumerate(items, start=1):
        result = resolve_item(item, sentences, args)
        out_path = args.output_dir / f"{index:04d}_{item.item_id}.json"
        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        summary.append(
            {
                "item_id": item.item_id,
                "output_path": str(out_path),
                "manual_review_required": result["manual_review_required"],
                "confidence": result["resolution"].get("confidence"),
                "reason_summary": result["resolution"].get("reason_summary"),
            }
        )
        print(f"[{index}/{len(items)}] {item.item_id} -> {out_path}")

    summary_path = args.output_dir / "resolution_summary.json"
    summary_path.write_text(json.dumps({"items": summary}, indent=2), encoding="utf-8")
    print(f"Wrote summary: {summary_path}")


if __name__ == "__main__":
    main()
