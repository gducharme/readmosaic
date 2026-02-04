#!/usr/bin/env python3
"""Narrative Burst Monitor (NBM).

Detects statistically significant bursts of terms in a manuscript using
sliding window Z-score analysis across uni/bi/tri-grams.
"""

from __future__ import annotations

import argparse
import pathlib
import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from nltk.util import ngrams
from scipy import stats


CONTENT_POS_PREFIXES = ("NN", "VB", "JJ")
WORD_RE = re.compile(r"^[a-zA-Z]+$")


@dataclass(frozen=True)
class TokenInfo:
    token: str
    is_content: bool


def ensure_nltk_resources() -> None:
    resources = {
        "tokenizers/punkt": "punkt",
        "taggers/averaged_perceptron_tagger": "averaged_perceptron_tagger",
        "corpora/stopwords": "stopwords",
    }
    for path, name in resources.items():
        try:
            nltk.data.find(path)
        except LookupError:
            nltk.download(name, quiet=True)


def load_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def tokenize_content_words(text: str) -> list[TokenInfo]:
    tokens = [token.lower() for token in nltk.word_tokenize(text)]
    word_tokens = [token for token in tokens if WORD_RE.match(token)]
    tags = nltk.pos_tag(word_tokens)
    stop_words = set(stopwords.words("english"))

    token_infos: list[TokenInfo] = []
    for token, tag in tags:
        is_content = tag.startswith(CONTENT_POS_PREFIXES) and token not in stop_words
        token_infos.append(TokenInfo(token=token, is_content=is_content))
    return token_infos


def iter_windows(
    tokens: list[TokenInfo], window_size: int, step_size: int
) -> Iterable[tuple[int, list[TokenInfo]]]:
    total = len(tokens)
    if total == 0:
        return
    if total <= window_size:
        yield 0, tokens
        return
    for start in range(0, total - window_size + 1, step_size):
        yield start, tokens[start : start + window_size]


def build_global_counts(
    content_tokens: list[str], n_sizes: Iterable[int]
) -> dict[int, Counter]:
    global_counts: dict[int, Counter] = {}
    for n in n_sizes:
        if len(content_tokens) < n:
            global_counts[n] = Counter()
            continue
        global_counts[n] = Counter(ngrams(content_tokens, n))
    return global_counts


def window_counts(content_tokens: list[str], n_sizes: Iterable[int]) -> dict[int, Counter]:
    counts: dict[int, Counter] = {}
    for n in n_sizes:
        if len(content_tokens) < n:
            counts[n] = Counter()
            continue
        counts[n] = Counter(ngrams(content_tokens, n))
    return counts


def format_term(term: tuple[str, ...] | str) -> str:
    if isinstance(term, tuple):
        return " ".join(term)
    return term


def compute_term_stats(
    windows: list[dict],
    global_counts: dict[int, Counter],
    total_ngrams: dict[int, int],
    threshold: float,
) -> tuple[pd.DataFrame, dict[str, dict[str, list[float]]]]:
    records = []
    term_series: dict[str, dict[str, list[float]]] = {}

    for n_size, counts in global_counts.items():
        total_ngram_count = total_ngrams[n_size]
        if total_ngram_count == 0:
            continue
        for term, global_count in counts.items():
            local_counts = [
                window["counts"][n_size].get(term, 0) for window in windows
            ]
            stddev = stats.tstd(local_counts) if len(local_counts) > 1 else 0.0
            if stddev == 0:
                zscores = [0.0 for _ in local_counts]
            else:
                rate = global_count / total_ngram_count
                zscores = []
                for window, local in zip(windows, local_counts, strict=True):
                    window_total = window["total_ngrams"][n_size]
                    expected = rate * window_total
                    zscores.append((local - expected) / stddev)

            max_z = max(zscores) if zscores else 0.0
            mean_z = float(np.mean(zscores)) if zscores else 0.0
            burst_windows = sum(1 for z in zscores if z >= threshold)

            formatted_term = format_term(term)
            term_series.setdefault(formatted_term, {})[str(n_size)] = zscores
            records.append(
                {
                    "term": formatted_term,
                    "ngram_size": n_size,
                    "global_count": global_count,
                    "max_z": max_z,
                    "mean_z": mean_z,
                    "burst_windows": burst_windows,
                }
            )

    df = pd.DataFrame(records)
    if not df.empty:
        df.sort_values(["max_z", "global_count"], ascending=False, inplace=True)
    return df, term_series


def plot_terms(
    output_path: pathlib.Path,
    progress: list[float],
    term_series: dict[str, dict[str, list[float]]],
    terms: list[str],
    threshold: float,
) -> None:
    import matplotlib.pyplot as plt

    plt.figure(figsize=(12, 6))
    for term in terms:
        series = term_series.get(term, {})
        combined = None
        for n_size, values in series.items():
            label = f"{term} ({n_size}-gram)"
            if combined is None:
                combined = values
            plt.plot(progress, values, label=label)
        if combined is None:
            continue
    plt.axhline(threshold, color="red", linestyle="--", label=f"threshold {threshold}")
    plt.xlabel("Book progress (%)")
    plt.ylabel("Z-score")
    plt.title("Narrative Burst Monitor")
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def build_report(
    df: pd.DataFrame,
    windows: list[dict],
    threshold: float,
    top_n: int,
) -> str:
    lines = []
    if df.empty:
        return "No bursty terms found."

    top_df = df.head(top_n)
    lines.append("Top bursty terms:\n")
    lines.append(top_df.to_string(index=False))

    lines.append("\n\nHot zones (Z >= threshold):")
    for _, row in top_df.iterrows():
        term = row["term"]
        n_size = row["ngram_size"]
        if row["burst_windows"] == 0:
            continue
        lines.append(f"\n- {term} ({n_size}-gram):")
        for window in windows:
            zscores = window["zscores"].get((term, n_size))
            if zscores is None:
                continue
            if zscores >= threshold:
                lines.append(
                    f"  * {window['start']}-{window['end']} words "
                    f"({window['progress']:.1f}%): z={zscores:.2f}"
                )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Detect statistical bursts of terms using sliding-window Z-score analysis."
        )
    )
    parser.add_argument("input_file", type=pathlib.Path)
    parser.add_argument("--window-size", type=int, default=1000)
    parser.add_argument("--step-size", type=int, default=100)
    parser.add_argument("--threshold", type=float, default=3.0)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument(
        "--plot-terms",
        type=str,
        default="",
        help="Comma-separated list of terms to visualize.",
    )
    parser.add_argument(
        "--plot-file",
        type=pathlib.Path,
        default=None,
        help="Optional output path for the plot image.",
    )

    args = parser.parse_args()
    if args.window_size <= 0 or args.step_size <= 0:
        raise SystemExit("window-size and step-size must be positive")

    ensure_nltk_resources()

    text = load_text(args.input_file)
    token_infos = tokenize_content_words(text)
    all_tokens = [token_info.token for token_info in token_infos]
    content_tokens = [token_info.token for token_info in token_infos if token_info.is_content]

    windows = []
    n_sizes = (1, 2, 3)
    for start, window_tokens in iter_windows(
        token_infos, args.window_size, args.step_size
    ):
        window_content = [token.token for token in window_tokens if token.is_content]
        counts = window_counts(window_content, n_sizes)
        totals = {n: max(len(window_content) - n + 1, 0) for n in n_sizes}
        mid_point = start + len(window_tokens) / 2
        progress = (mid_point / max(len(all_tokens), 1)) * 100
        windows.append(
            {
                "start": start,
                "end": start + len(window_tokens),
                "progress": progress,
                "counts": counts,
                "total_ngrams": totals,
            }
        )

    total_ngrams = {n: max(len(content_tokens) - n + 1, 0) for n in n_sizes}
    global_counts = build_global_counts(content_tokens, n_sizes)

    df, term_series = compute_term_stats(
        windows, global_counts, total_ngrams, args.threshold
    )

    for idx, window in enumerate(windows):
        window["zscores"] = {}
        for term, series_by_ngram in term_series.items():
            for n_size, zscores in series_by_ngram.items():
                window["zscores"][(term, int(n_size))] = zscores[idx]

    print(
        f"Tokens: {len(all_tokens)} | Content tokens: {len(content_tokens)} | "
        f"Windows: {len(windows)}"
    )

    report = build_report(df, windows, args.threshold, args.top_n)
    print(report)

    plot_terms_input = [term.strip() for term in args.plot_terms.split(",") if term.strip()]
    if args.plot_file or plot_terms_input:
        plot_terms_list = plot_terms_input
        if not plot_terms_list:
            plot_terms_list = df.head(min(args.top_n, 5))["term"].tolist()
        output_path = args.plot_file or pathlib.Path("burst_plot.png")
        plot_terms(
            output_path,
            [window["progress"] for window in windows],
            term_series,
            plot_terms_list,
            args.threshold,
        )
        print(f"Plot saved to {output_path}")


if __name__ == "__main__":
    main()
