#!/usr/bin/env python3
"""Narrative Burst Monitor (NBM).

Detects statistically significant bursts of terms in a manuscript using
sliding window Z-score analysis across uni/bi/tri-grams.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from nltk.util import ngrams
from scipy import stats

from schema_validator import validate_payload


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


def load_manuscript_tokens(preprocessing_dir: pathlib.Path) -> dict:
    tokens_path = preprocessing_dir / "manuscript_tokens.json"
    if not tokens_path.exists():
        raise SystemExit(f"Missing manuscript_tokens.json in {preprocessing_dir}")
    payload = json.loads(tokens_path.read_text(encoding="utf-8"))
    validate_payload(payload, "manuscript_tokens.schema.json", "manuscript_tokens.json")
    return payload


def build_filtered_token_index(manuscript_tokens: dict) -> list[dict]:
    filtered_tokens: list[dict] = []
    for paragraph in manuscript_tokens.get("paragraphs", []):
        paragraph_id = paragraph["paragraph_id"]
        for token in paragraph.get("tokens", []):
            text = token.get("text", "")
            if not WORD_RE.match(text):
                continue
            filtered_tokens.append(
                {
                    "paragraph_id": paragraph_id,
                    "token_id": token["token_id"],
                    "start_char": token["start_char"],
                    "end_char": token["end_char"],
                    "global_index": token["global_index"],
                }
            )
    filtered_tokens.sort(key=lambda item: item["global_index"])
    return filtered_tokens


def build_window_location(
    filtered_tokens: list[dict],
    start: int,
    end: int,
) -> dict:
    if not filtered_tokens:
        return {}
    safe_start = max(min(start, len(filtered_tokens) - 1), 0)
    safe_end = max(min(end, len(filtered_tokens)), safe_start + 1)
    anchor_index = min(safe_start + (safe_end - safe_start) // 2, len(filtered_tokens) - 1)
    anchor = filtered_tokens[anchor_index]
    paragraph_id = anchor["paragraph_id"]
    window_tokens = [
        token
        for token in filtered_tokens[safe_start:safe_end]
        if token["paragraph_id"] == paragraph_id
    ]
    if not window_tokens:
        window_tokens = [anchor]
    token_ids = [token["token_id"] for token in window_tokens]
    char_start = min(token["start_char"] for token in window_tokens)
    char_end = max(token["end_char"] for token in window_tokens)
    return {
        "paragraph_id": paragraph_id,
        "token_ids": token_ids,
        "char_range": {"start": char_start, "end": char_end},
    }


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
    parser.add_argument(
        "--preprocessing",
        type=pathlib.Path,
        default=None,
        help="Directory containing manuscript_tokens.json for window mapping.",
    )
    parser.add_argument(
        "--output-json",
        type=pathlib.Path,
        default=None,
        help="Optional output path for edits.schema.json payload.",
    )

    args = parser.parse_args()
    if args.window_size <= 0 or args.step_size <= 0:
        raise SystemExit("window-size and step-size must be positive")

    ensure_nltk_resources()

    if args.output_json and not args.preprocessing:
        raise SystemExit("--output-json requires --preprocessing for token mapping.")

    text = load_text(args.input_file)
    token_infos = tokenize_content_words(text)
    all_tokens = [token_info.token for token_info in token_infos]
    content_tokens = [token_info.token for token_info in token_infos if token_info.is_content]
    manuscript_tokens = None
    filtered_token_index: list[dict] = []
    if args.preprocessing:
        manuscript_tokens = load_manuscript_tokens(args.preprocessing)
        filtered_token_index = build_filtered_token_index(manuscript_tokens)
        if filtered_token_index and len(filtered_token_index) != len(all_tokens):
            print(
                "Warning: manuscript token count does not match burst tokens. "
                "Window mappings may be approximate."
            )

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

    if args.output_json:
        grouped_bursts: dict[tuple[str, tuple[str, ...]], dict[str, object]] = {}
        for window in windows:
            location = build_window_location(
                filtered_token_index,
                window["start"],
                window["end"],
            )
            if not location:
                continue
            paragraph_id = str(location.get("paragraph_id", ""))
            token_ids = tuple(location.get("token_ids", []))
            if not paragraph_id or not token_ids:
                continue
            group_key = (paragraph_id, token_ids)
            group = grouped_bursts.setdefault(
                group_key,
                {
                    "location": location,
                    "progress_values": [],
                    "bursts": [],
                    "max_z": 0.0,
                },
            )

            for (term, n_size), z_score in window["zscores"].items():
                if z_score < args.threshold:
                    continue
                group["progress_values"].append(window["progress"])
                group["bursts"].append(
                    {
                        "term": term,
                        "ngram_size": n_size,
                        "z_score": round(z_score, 4),
                        "window_start": window["start"],
                        "window_end": window["end"],
                    }
                )
                group["max_z"] = max(float(group["max_z"]), float(z_score))

        items = []
        for group in grouped_bursts.values():
            bursts = sorted(
                group["bursts"],
                key=lambda burst: float(burst["z_score"]),
                reverse=True,
            )
            if not bursts:
                continue
            progress_values = group["progress_values"]
            avg_progress = (
                round(sum(progress_values) / len(progress_values), 2) if progress_values else 0.0
            )
            top_terms = [
                f"{burst['term']} ({burst['ngram_size']}-gram, z={burst['z_score']:.2f})"
                for burst in bursts[:5]
            ]
            issue_id = str(uuid.uuid4())
            items.append(
                {
                    "issue_id": issue_id,
                    "type": "burst",
                    "status": "open",
                    "location": group["location"],
                    "evidence": {
                        "summary": (
                            f"{len(bursts)} burst signal(s) detected in this region. "
                            f"Top signals: {', '.join(top_terms)}."
                        ),
                        "detector": "burst_monitor",
                        "signals": [
                            {
                                "name": "burst_count",
                                "value": len(bursts),
                            },
                            {
                                "name": "max_z_score",
                                "value": round(float(group["max_z"]), 4),
                            },
                            {
                                "name": "bursts",
                                "value": bursts,
                            },
                        ],
                    },
                    "extensions": {
                        "progress_percent": avg_progress,
                    },
                }
            )

        if items:
            items.sort(
                key=lambda item: float(
                    next(
                        (
                            signal.get("value", 0.0)
                            for signal in item.get("evidence", {}).get("signals", [])
                            if signal.get("name") == "max_z_score"
                        ),
                        0.0,
                    )
                ),
                reverse=True,
            )

        if not items:
            print("No bursty windows above threshold for JSON output.")
        else:
            edits_payload = {
                "schema_version": "1.0",
                "manuscript_id": manuscript_tokens["manuscript_id"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "items": items,
            }
            validate_payload(edits_payload, "edits.schema.json", "burst monitor edits payload")
            args.output_json.write_text(
                json.dumps(edits_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            total_bursts = sum(
                int(
                    next(
                        (
                            signal.get("value", 0)
                            for signal in item.get("evidence", {}).get("signals", [])
                            if signal.get("name") == "burst_count"
                        ),
                        0,
                    )
                )
                for item in items
            )
            print(
                "Aggregated burst JSON items: "
                f"{len(items)} region(s) from {total_bursts} burst signal(s)."
            )
            print(f"JSON output saved to {args.output_json}")

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
