#!/usr/bin/env python3
"""Semantic Entropy Evaluator (SEE).

Calculates Shannon entropy across unigrams and bigrams, tracks entropy drift
over a sliding window, computes vocabulary saturation (TTR), and generates
an entropy heatmap.

Usage examples:
  python scripts/entropy_evaluator.py path/to/manuscript.txt
  python scripts/entropy_evaluator.py path/to/manuscript.md --window-size 500
  python scripts/entropy_evaluator.py path/to/manuscript.txt --output results/entropy
  python scripts/entropy_evaluator.py path/to/a.txt --compare-files path/to/b.txt path/to/c.txt

Run with --help to see all options.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

import matplotlib.pyplot as plt
import nltk
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from scipy.stats import entropy as shannon_entropy


@dataclass
class EntropyWindow:
    index: int
    start_token: int
    end_token: int
    unigram_entropy: float
    bigram_entropy: float


def ensure_nltk() -> None:
    resources = [
        "tokenizers/punkt",
        "taggers/averaged_perceptron_tagger",
        "corpora/wordnet",
        "corpora/omw-1.4",
    ]
    for resource in resources:
        try:
            nltk.data.find(resource)
        except LookupError:
            nltk.download(resource.split("/")[-1], quiet=True)


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def normalize_tokens(tokens: Iterable[str]) -> List[str]:
    normalized = []
    for token in tokens:
        token = token.lower()
        token = re.sub(r"[^a-z']", "", token)
        if token and any(char.isalpha() for char in token):
            normalized.append(token)
    return normalized


def get_wordnet_pos(tag: str) -> str:
    if tag.startswith("J"):
        return wordnet.ADJ
    if tag.startswith("V"):
        return wordnet.VERB
    if tag.startswith("R"):
        return wordnet.ADV
    return wordnet.NOUN


def lemmatize_tokens(tokens: Sequence[str]) -> List[str]:
    lemmatizer = WordNetLemmatizer()
    tagged = nltk.pos_tag(tokens)
    return [lemmatizer.lemmatize(word, get_wordnet_pos(tag)) for word, tag in tagged]


def tokenize(text: str, lemmatize: bool) -> List[str]:
    ensure_nltk()
    raw_tokens = nltk.word_tokenize(text)
    normalized = normalize_tokens(raw_tokens)
    if lemmatize:
        return lemmatize_tokens(normalized)
    return normalized


def entropy_from_counts(counts: Counter[str]) -> float:
    total = sum(counts.values())
    if total == 0:
        return 0.0
    probabilities = [count / total for count in counts.values()]
    return float(shannon_entropy(probabilities, base=2))


def build_bigrams(tokens: Sequence[str]) -> List[str]:
    return [f"{left} {right}" for left, right in zip(tokens, tokens[1:])]


def sliding_windows(tokens: Sequence[str], window_size: int, step_size: int) -> List[EntropyWindow]:
    windows: List[EntropyWindow] = []
    for index, start in enumerate(range(0, len(tokens), step_size)):
        end = min(start + window_size, len(tokens))
        if end - start < 2:
            break
        slice_tokens = tokens[start:end]
        unigram_entropy = entropy_from_counts(Counter(slice_tokens))
        bigram_entropy = entropy_from_counts(Counter(build_bigrams(slice_tokens)))
        windows.append(
            EntropyWindow(
                index=index,
                start_token=start,
                end_token=end,
                unigram_entropy=unigram_entropy,
                bigram_entropy=bigram_entropy,
            )
        )
        if end == len(tokens):
            break
    return windows


def vocabulary_saturation(tokens: Sequence[str]) -> float:
    if not tokens:
        return 0.0
    return len(set(tokens)) / len(tokens)


def detect_entropy_depressions(windows: Sequence[EntropyWindow]) -> List[EntropyWindow]:
    if not windows:
        return []
    values = [window.unigram_entropy for window in windows]
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std_dev = math.sqrt(variance)
    threshold = mean - std_dev
    return [window for window in windows if window.unigram_entropy < threshold]


def render_heatmap(windows: Sequence[EntropyWindow], output_path: Path) -> None:
    if not windows:
        return
    unigram_values = [window.unigram_entropy for window in windows]
    bigram_values = [window.bigram_entropy for window in windows]
    data = [unigram_values, bigram_values]

    fig, ax = plt.subplots(figsize=(12, 3))
    image = ax.imshow(data, aspect="auto", cmap="magma")
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["Unigram", "Bigram"])
    ax.set_xlabel("Window Index")
    ax.set_title("Semantic Entropy Heatmap")
    fig.colorbar(image, ax=ax, label="Entropy (bits)")
    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def summarize_file(path: Path, window_size: int, step_size: int, lemmatize: bool) -> dict:
    text = load_text(path)
    tokens = tokenize(text, lemmatize=lemmatize)
    windows = sliding_windows(tokens, window_size, step_size)
    baseline_entropy = entropy_from_counts(Counter(tokens))
    depressions = detect_entropy_depressions(windows)
    return {
        "file": str(path),
        "total_tokens": len(tokens),
        "unigram_entropy": baseline_entropy,
        "bigram_entropy": entropy_from_counts(Counter(build_bigrams(tokens))),
        "type_token_ratio": vocabulary_saturation(tokens),
        "window_size": window_size,
        "step_size": step_size,
        "windows": [
            {
                "index": window.index,
                "start_token": window.start_token,
                "end_token": window.end_token,
                "unigram_entropy": window.unigram_entropy,
                "bigram_entropy": window.bigram_entropy,
            }
            for window in windows
        ],
        "low_entropy_depressions": [
            {
                "index": window.index,
                "start_token": window.start_token,
                "end_token": window.end_token,
                "unigram_entropy": window.unigram_entropy,
            }
            for window in depressions
        ],
    }


def resolve_output_paths(output: str | None, input_path: Path) -> tuple[Path | None, Path | None]:
    if not output:
        return None, None
    output_path = Path(output)
    if output_path.suffix:
        stem = output_path.with_suffix("")
    else:
        stem = output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    heatmap_path = stem.with_name(f"{stem.name}_heatmap.png")
    json_path = stem.with_name(f"{stem.name}_stats.json")
    if output_path.is_dir():
        heatmap_path = output_path / f"{input_path.stem}_heatmap.png"
        json_path = output_path / f"{input_path.stem}_stats.json"
    return heatmap_path, json_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate Shannon entropy across a manuscript, track entropy drift, "
            "and generate a semantic entropy heatmap."
        )
    )
    parser.add_argument("input_file", type=Path, help="Path to a .txt or .md file.")
    parser.add_argument(
        "--window-size",
        type=int,
        default=500,
        help="Number of words per entropy window (default: 500).",
    )
    parser.add_argument(
        "--step-size",
        type=int,
        default=None,
        help="Stride between windows; defaults to half the window size.",
    )
    parser.add_argument(
        "--lemmatize",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable lemmatization for accurate probability grouping (default: True).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output prefix or directory to save heatmap PNG + stats JSON.",
    )
    parser.add_argument(
        "--compare-files",
        nargs="*",
        type=Path,
        default=None,
        help="Additional files to compare overall entropy and TTR.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input_file
    step_size = args.step_size or max(1, args.window_size // 2)
    summary = summarize_file(
        input_path,
        window_size=args.window_size,
        step_size=step_size,
        lemmatize=args.lemmatize,
    )

    compare_results = []
    if args.compare_files:
        for compare_path in [input_path, *args.compare_files]:
            compare_results.append(
                summarize_file(
                    compare_path,
                    window_size=args.window_size,
                    step_size=step_size,
                    lemmatize=args.lemmatize,
                )
            )
    summary["comparative_analysis"] = compare_results

    heatmap_path, json_path = resolve_output_paths(args.output, input_path)
    if heatmap_path:
        render_heatmap(
            [
                EntropyWindow(
                    index=window["index"],
                    start_token=window["start_token"],
                    end_token=window["end_token"],
                    unigram_entropy=window["unigram_entropy"],
                    bigram_entropy=window["bigram_entropy"],
                )
                for window in summary["windows"]
            ],
            heatmap_path,
        )
    if json_path:
        json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    if heatmap_path and json_path:
        print(f"\nSaved heatmap to {heatmap_path}")
        print(f"Saved stats to {json_path}")


if __name__ == "__main__":
    main()
