#!/usr/bin/env python3
"""Word frequency benchmark against a broad English corpus baseline.

This script reports the top manuscript words and compares their usage rates
against NLTK's Brown corpus to provide a rough "humanity average" reference.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import nltk
from nltk.corpus import brown

TOKEN_PATTERN = re.compile(r"[A-Za-z']+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List the top manuscript words and compare each frequency against "
            "a Brown corpus baseline."
        )
    )
    parser.add_argument("input_file", type=Path, help="Path to a .txt or .md manuscript file.")
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top manuscript words to report (default: 10).",
    )
    parser.add_argument(
        "--include-stopwords",
        action="store_true",
        help="Include stopwords in ranking (default filters NLTK English stopwords).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write the report payload as JSON.",
    )
    return parser.parse_args()


def ensure_nltk_resources() -> None:
    for resource in ("corpora/stopwords", "corpora/brown"):
        try:
            nltk.data.find(resource)
        except LookupError as exc:
            raise SystemExit(
                "Missing NLTK data resource: "
                f"{resource}. Run `python scripts/setup_nltk_data.py` to install prerequisites."
            ) from exc


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def build_report(
    manuscript_tokens: list[str],
    top_n: int,
    include_stopwords: bool,
) -> dict[str, object]:
    stopwords: set[str] = set(nltk.corpus.stopwords.words("english")) if not include_stopwords else set()

    filtered_tokens = [token for token in manuscript_tokens if token not in stopwords]
    manuscript_total = len(filtered_tokens)
    manuscript_counts = Counter(filtered_tokens)

    brown_tokens = [token.lower() for token in brown.words() if token.isalpha()]
    brown_filtered = [token for token in brown_tokens if token not in stopwords]
    brown_total = len(brown_filtered)
    brown_counts = Counter(brown_filtered)

    top_words: list[dict[str, object]] = []
    for rank, (word, count) in enumerate(manuscript_counts.most_common(top_n), start=1):
        manuscript_per_million = (count / manuscript_total * 1_000_000) if manuscript_total else 0.0
        brown_count = brown_counts.get(word, 0)
        brown_per_million = (brown_count / brown_total * 1_000_000) if brown_total else 0.0
        relative_to_humanity_avg = (
            manuscript_per_million / brown_per_million if brown_per_million else None
        )
        top_words.append(
            {
                "rank": rank,
                "word": word,
                "manuscript_count": count,
                "manuscript_per_million": round(manuscript_per_million, 2),
                "brown_count": brown_count,
                "brown_per_million": round(brown_per_million, 2),
                "relative_to_humanity_avg": (
                    round(relative_to_humanity_avg, 2)
                    if relative_to_humanity_avg is not None
                    else None
                ),
            }
        )

    return {
        "manuscript_total_tokens": manuscript_total,
        "brown_total_tokens": brown_total,
        "stopwords_filtered": not include_stopwords,
        "top_words": top_words,
    }


def print_report(report: dict[str, object]) -> None:
    print(
        "Word frequency benchmark "
        f"(manuscript tokens={report['manuscript_total_tokens']}, "
        f"brown tokens={report['brown_total_tokens']})"
    )
    print("=" * 88)
    print(
        f"{'#':<3} {'word':<18} {'manuscript':>10} {'ms/million':>12} "
        f"{'brown':>10} {'br/million':>12} {'ratio':>10}"
    )
    print("-" * 88)
    for row in report["top_words"]:
        ratio = row["relative_to_humanity_avg"]
        ratio_display = f"{ratio:.2f}x" if ratio is not None else "n/a"
        print(
            f"{row['rank']:<3} {row['word']:<18} {row['manuscript_count']:>10} "
            f"{row['manuscript_per_million']:>12.2f} {row['brown_count']:>10} "
            f"{row['brown_per_million']:>12.2f} {ratio_display:>10}"
        )


def main() -> None:
    args = parse_args()
    if args.top_n <= 0:
        raise SystemExit("--top-n must be greater than zero.")
    if not args.input_file.exists():
        raise SystemExit(f"Input file not found: {args.input_file}")

    ensure_nltk_resources()

    manuscript_text = args.input_file.read_text(encoding="utf-8")
    manuscript_tokens = tokenize(manuscript_text)
    report = build_report(
        manuscript_tokens=manuscript_tokens,
        top_n=args.top_n,
        include_stopwords=args.include_stopwords,
    )

    print_report(report)

    if args.output_json:
        args.output_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nSaved JSON report: {args.output_json}")


if __name__ == "__main__":
    main()
