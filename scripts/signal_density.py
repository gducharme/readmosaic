#!/usr/bin/env python3
"""Mosaic Signal Density (MSD) estimator.

Calculates lexical density, unique token ratios, and top signal terms.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

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
    parser.add_argument("input_file", type=Path, help="Path to a .txt or .md file.")
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
    return parser.parse_args()


def tokenize(text: str) -> list[str]:
    return re.findall(r"[A-Za-z']+", text.lower())


def compute_metrics(text: str, top_n: int) -> dict[str, object]:
    tokens = tokenize(text)
    total_tokens = len(tokens)
    unique_tokens = len(set(tokens))
    stopword_tokens = [token for token in tokens if token in STOPWORDS]
    content_tokens = [token for token in tokens if token not in STOPWORDS]
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


def main() -> None:
    args = parse_args()
    if not args.input_file.exists():
        raise SystemExit(f"Input file not found: {args.input_file}")
    if args.input_file.suffix.lower() not in {".txt", ".md"}:
        raise SystemExit("Input file must be .txt or .md")

    text = args.input_file.read_text(encoding="utf-8")
    metrics = compute_metrics(text, args.top_n)
    payload = {
        "tool": "MSD",
        "metrics": metrics,
    }

    print(json.dumps(payload, indent=2))
    if args.output_json:
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
