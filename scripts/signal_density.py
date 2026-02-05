#!/usr/bin/env python3
"""Mosaic Signal Density (MSD) estimator.

Calculates lexical density, unique token ratios, top signal terms, and optional
paragraph-level density metrics from pre-processed manuscript tokens.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "he",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "there",
    "to",
    "was",
    "were",
    "will",
    "with",
    "you",
    "your",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate lexical signal density for a manuscript.",
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        type=Path,
        help="Path to a .txt or .md file.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top signal terms to include.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write JSON output.",
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        help="Optional path to manuscript_tokens.json for per-paragraph metrics.",
    )
    parser.add_argument(
        "--paragraph-threshold",
        type=float,
        help="Emit edits for paragraphs with lexical or content density below this value.",
    )
    parser.add_argument(
        "--edits-output",
        type=Path,
        help="Optional path to write edits.schema.json output.",
    )
    return parser.parse_args()


def tokenize(text: str) -> list[str]:
    return re.findall(r"[A-Za-z']+", text.lower())


def normalize_tokens(tokens: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for token in tokens:
        normalized.extend(re.findall(r"[A-Za-z']+", token.lower()))
    return normalized


def compute_metrics_from_tokens(tokens: Iterable[str], top_n: int) -> dict[str, object]:
    normalized_tokens = normalize_tokens(tokens)
    total_tokens = len(normalized_tokens)
    unique_tokens = len(set(normalized_tokens))
    stopword_tokens = [token for token in normalized_tokens if token in STOPWORDS]
    content_tokens = [token for token in normalized_tokens if token not in STOPWORDS]
    lexical_density = unique_tokens / total_tokens if total_tokens else 0.0
    content_density = len(content_tokens) / total_tokens if total_tokens else 0.0
    top_terms = [term for term, _ in Counter(content_tokens).most_common(top_n)]
    return {
        "total_tokens": total_tokens,
        "unique_tokens": unique_tokens,
        "lexical_density": round(lexical_density, 4),
        "content_density": round(content_density, 4),
        "top_terms": top_terms,
        "stopword_ratio": round(len(stopword_tokens) / total_tokens, 4) if total_tokens else 0.0,
    }


def compute_metrics(text: str, top_n: int) -> dict[str, object]:
    tokens = tokenize(text)
    return compute_metrics_from_tokens(tokens, top_n)


def load_preprocessed_tokens(path: Path) -> dict[str, object]:
    if not path.exists():
        raise SystemExit(f"Preprocessing file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "paragraphs" not in payload:
        raise SystemExit("Preprocessing file missing 'paragraphs' field.")
    return payload


def build_paragraph_metrics(
    paragraphs: Iterable[dict[str, object]],
    top_n: int,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for paragraph in paragraphs:
        paragraph_id = paragraph.get("paragraph_id")
        order = paragraph.get("order")
        token_texts = []
        token_ids: list[str] = []
        char_starts: list[int] = []
        char_ends: list[int] = []
        for token in paragraph.get("tokens", []):
            token_id = token.get("token_id")
            if token_id:
                token_ids.append(token_id)
            if token.get("start_char") is not None:
                char_starts.append(int(token["start_char"]))
            if token.get("end_char") is not None:
                char_ends.append(int(token["end_char"]))
            token_texts.append(
                token.get("normalized")
                or token.get("text")
                or ""
            )
        metrics = compute_metrics_from_tokens(token_texts, top_n)
        char_range = None
        if char_starts and char_ends:
            char_range = {"start": min(char_starts), "end": max(char_ends)}
        results.append(
            {
                "paragraph_id": paragraph_id,
                "order": order,
                "token_ids": token_ids,
                "char_range": char_range,
                "metrics": metrics,
            }
        )
    return results


def build_edits_payload(
    manuscript_id: str,
    paragraph_metrics: Iterable[dict[str, object]],
    threshold: float,
) -> dict[str, object]:
    items = []
    for entry in paragraph_metrics:
        metrics = entry["metrics"]
        lexical_density = metrics["lexical_density"]
        content_density = metrics["content_density"]
        if lexical_density >= threshold and content_density >= threshold:
            continue
        paragraph_id = entry["paragraph_id"]
        issue_id = f"{paragraph_id}-density"
        location: dict[str, object] = {"paragraph_id": paragraph_id}
        token_ids = entry.get("token_ids") or []
        if token_ids:
            location["token_ids"] = token_ids
        if entry.get("char_range"):
            location["char_range"] = entry["char_range"]
        items.append(
            {
                "issue_id": issue_id,
                "type": "signal_density",
                "status": "open",
                "location": location,
                "evidence": {
                    "summary": (
                        "Paragraph density below threshold "
                        f"({threshold:.3f})."
                    ),
                    "signals": [
                        {"name": "lexical_density", "value": lexical_density},
                        {"name": "content_density", "value": content_density},
                        {"name": "stopword_ratio", "value": metrics["stopword_ratio"]},
                        {"name": "total_tokens", "value": metrics["total_tokens"]},
                        {"name": "unique_tokens", "value": metrics["unique_tokens"]},
                        {"name": "top_terms", "value": metrics["top_terms"]},
                    ],
                    "detector": "signal_density.py",
                },
                "impact": {"severity": "low"},
            }
        )
    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


def main() -> None:
    args = parse_args()
    if not args.input_file and not args.preprocessing:
        raise SystemExit("Provide a manuscript file or --preprocessing tokens.")

    text = None
    if args.input_file:
        if not args.input_file.exists():
            raise SystemExit(f"Input file not found: {args.input_file}")
        if args.input_file.suffix.lower() not in {".txt", ".md"}:
            raise SystemExit("Input file must be .txt or .md")
        text = args.input_file.read_text(encoding="utf-8")

    preprocessing_payload = None
    paragraph_metrics: Optional[list[dict[str, object]]] = None
    if args.preprocessing:
        preprocessing_payload = load_preprocessed_tokens(args.preprocessing)
        paragraph_metrics = build_paragraph_metrics(
            preprocessing_payload["paragraphs"], args.top_n
        )

    if text is not None:
        metrics = compute_metrics(text, args.top_n)
    elif preprocessing_payload:
        all_tokens = []
        for paragraph in preprocessing_payload["paragraphs"]:
            for token in paragraph.get("tokens", []):
                all_tokens.append(
                    token.get("normalized")
                    or token.get("text")
                    or ""
                )
        metrics = compute_metrics_from_tokens(all_tokens, args.top_n)
    else:
        metrics = {}
    payload = {
        "tool": "MSD",
        "metrics": metrics,
    }
    if preprocessing_payload:
        payload["manuscript_id"] = preprocessing_payload.get("manuscript_id")
    if paragraph_metrics is not None:
        payload["paragraphs"] = paragraph_metrics

    print(json.dumps(payload, indent=2))
    if args.output_json:
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if args.edits_output:
        if not preprocessing_payload:
            raise SystemExit("--edits-output requires --preprocessing.")
        if args.paragraph_threshold is None:
            raise SystemExit("--edits-output requires --paragraph-threshold.")
        edits_payload = build_edits_payload(
            preprocessing_payload.get("manuscript_id", "unknown"),
            paragraph_metrics or [],
            args.paragraph_threshold,
        )
        if not edits_payload["items"]:
            raise SystemExit("No paragraphs below threshold to emit edits.")
        args.edits_output.write_text(
            json.dumps(edits_payload, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
