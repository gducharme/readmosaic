#!/usr/bin/env python3
"""Neutrino Surprisal Scout (NSS).

Compute per-sentence surprisal (average token log-probability) using a local
causal language model from transformers.

Usage examples:
  python scripts/surprisal_scout.py path/to/manuscript.txt
  python scripts/surprisal_scout.py manuscript.md --model gpt2 --percentile 90
  python scripts/surprisal_scout.py manuscript.txt --plot surprisal_map.png \
    --output-json surprisal.json --output-csv surprisal.csv

Run with --help for full options.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


DEFAULT_TRANSITIONS = [
    "it is important to note",
    "in conclusion",
    "a testament to",
    "in summary",
    "as a result",
    "in other words",
    "on the other hand",
]


@dataclass
class SentenceScore:
    index: int
    sentence: str
    token_count: int
    avg_logprob: float
    perplexity: float
    is_slop: bool
    transitions: List[str]


def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def split_sentences(text: str) -> List[str]:
    try:
        import nltk

        try:
            nltk.data.find("tokenizers/punkt")
        except LookupError:
            nltk.download("punkt", quiet=True)
        sentences = nltk.sent_tokenize(text)
        return [s.strip() for s in sentences if s.strip()]
    except (ImportError, LookupError):
        # Fallback regex-based sentence splitting.
        parts = re.split(r"(?<=[.!?])\s+", text)
        return [s.strip() for s in parts if s.strip()]


def detect_transitions(sentence: str, phrases: Sequence[str]) -> List[str]:
    lowered = sentence.lower()
    return [phrase for phrase in phrases if phrase in lowered]


def compute_avg_logprob(
    sentence: str,
    model: AutoModelForCausalLM,
    tokenizer: AutoTokenizer,
    device: torch.device,
    max_length: int,
) -> tuple[float, int]:
    encoded = tokenizer(
        sentence,
        return_tensors="pt",
        truncation=True,
        max_length=max_length,
    )
    input_ids = encoded["input_ids"].to(device)
    if input_ids.size(1) < 2:
        return float("nan"), int(input_ids.size(1))
    with torch.no_grad():
        outputs = model(input_ids)
    logits = outputs.logits
    log_probs = torch.log_softmax(logits[:, :-1, :], dim=-1)
    target_ids = input_ids[:, 1:]
    token_log_probs = log_probs.gather(-1, target_ids.unsqueeze(-1)).squeeze(-1)
    avg_logprob = token_log_probs.mean().item()
    return avg_logprob, int(token_log_probs.size(1))


def build_scores(
    sentences: Sequence[str],
    model: AutoModelForCausalLM,
    tokenizer: AutoTokenizer,
    device: torch.device,
    max_length: int,
    transition_phrases: Sequence[str],
) -> List[SentenceScore]:
    scores: List[SentenceScore] = []
    for idx, sentence in enumerate(sentences, start=1):
        avg_logprob, token_count = compute_avg_logprob(
            sentence,
            model,
            tokenizer,
            device,
            max_length,
        )
        perplexity = float("nan")
        if not math.isnan(avg_logprob):
            perplexity = math.exp(-avg_logprob)
        transitions = detect_transitions(sentence, transition_phrases)
        scores.append(
            SentenceScore(
                index=idx,
                sentence=sentence,
                token_count=token_count,
                avg_logprob=avg_logprob,
                perplexity=perplexity,
                is_slop=False,
                transitions=transitions,
            )
        )
    return scores


def apply_slop_threshold(
    scores: Sequence[SentenceScore], percentile: float
) -> float:
    values = [s.avg_logprob for s in scores if not math.isnan(s.avg_logprob)]
    if not values:
        return float("nan")
    threshold = float(np.percentile(values, percentile))
    for score in scores:
        if not math.isnan(score.avg_logprob) and score.avg_logprob >= threshold:
            score.is_slop = True
    return threshold


def write_csv(path: Path, scores: Sequence[SentenceScore]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "index",
                "sentence",
                "token_count",
                "avg_logprob",
                "perplexity",
                "is_slop",
                "transitions",
            ]
        )
        for score in scores:
            writer.writerow(
                [
                    score.index,
                    score.sentence,
                    score.token_count,
                    f"{score.avg_logprob:.6f}"
                    if not math.isnan(score.avg_logprob)
                    else "nan",
                    f"{score.perplexity:.6f}"
                    if not math.isnan(score.perplexity)
                    else "nan",
                    score.is_slop,
                    "; ".join(score.transitions),
                ]
            )


def write_json(path: Path, scores: Sequence[SentenceScore]) -> None:
    payload = [
        {
            "index": score.index,
            "sentence": score.sentence,
            "token_count": score.token_count,
            "avg_logprob": score.avg_logprob,
            "perplexity": score.perplexity,
            "is_slop": score.is_slop,
            "transitions": score.transitions,
        }
        for score in scores
    ]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def plot_surprisal_map(
    path: Path, scores: Sequence[SentenceScore], threshold: float
) -> None:
    indices = [s.index for s in scores]
    values = [s.avg_logprob for s in scores]
    colors = ["#d1495b" if s.is_slop else "#00798c" for s in scores]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(indices, values, color=colors, alpha=0.85)
    if not math.isnan(threshold):
        ax.axhline(
            threshold,
            color="#f4a259",
            linestyle="--",
            linewidth=1.5,
            label=f"Slop threshold ({threshold:.3f})",
        )
    ax.set_title("Neutrino Surprisal Scout: Sentence Log-Probability Map")
    ax.set_xlabel("Sentence")
    ax.set_ylabel("Average log-probability")
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute per-sentence surprisal (average token log-probability) using a local "
            "transformers causal language model."
        )
    )
    parser.add_argument("input", type=Path, help="Path to a .txt or .md manuscript")
    parser.add_argument(
        "--model",
        default="gpt2",
        help="Hugging Face model name or local path (default: gpt2)",
    )
    parser.add_argument(
        "--device",
        default="auto",
        choices=["auto", "cpu", "cuda", "mps"],
        help="Device selection (default: auto)",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=512,
        help="Max tokens per sentence to evaluate (default: 512)",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default=90.0,
        help="Percentile threshold for slop detection (default: 90)",
    )
    parser.add_argument(
        "--plot",
        type=Path,
        default=Path("surprisal_map.png"),
        help="Path to save the surprisal map plot (default: surprisal_map.png)",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write full sentence metrics as JSON",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        help="Optional path to write full sentence metrics as CSV",
    )
    parser.add_argument(
        "--transition-phrases",
        type=str,
        nargs="*",
        help=(
            "Override the built-in AI transition phrases (space-separated). "
            "Leave unset to use defaults."
        ),
    )
    parser.add_argument(
        "--transition-file",
        type=Path,
        help="Path to a newline-delimited list of transition phrases.",
    )
    return parser.parse_args()


def load_transition_phrases(
    phrases: Optional[Sequence[str]], transition_file: Optional[Path]
) -> List[str]:
    if transition_file:
        if not transition_file.exists():
            raise FileNotFoundError(f"Transition file not found: {transition_file}")
        file_phrases = [
            line.strip()
            for line in transition_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return [p.lower() for p in file_phrases]
    if phrases:
        return [p.lower() for p in phrases]
    return DEFAULT_TRANSITIONS


def resolve_device(choice: str) -> torch.device:
    if choice == "auto":
        if torch.cuda.is_available():
            return torch.device("cuda")
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            return torch.device("mps")
        return torch.device("cpu")
    return torch.device(choice)


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input not found: {args.input}")

    transition_phrases = load_transition_phrases(
        args.transition_phrases, args.transition_file
    )
    device = resolve_device(args.device)

    text = load_text(args.input)
    sentences = split_sentences(text)
    if not sentences:
        raise ValueError("No sentences found in the input.")

    tokenizer = AutoTokenizer.from_pretrained(args.model)
    model = AutoModelForCausalLM.from_pretrained(args.model)
    model.to(device)
    model.eval()

    scores = build_scores(
        sentences,
        model,
        tokenizer,
        device,
        args.max_length,
        transition_phrases,
    )
    threshold = apply_slop_threshold(scores, args.percentile)

    plot_surprisal_map(args.plot, scores, threshold)

    slop_count = sum(1 for s in scores if s.is_slop)
    transition_hits = [s for s in scores if s.transitions]

    print("Surprisal Map Summary")
    print("=====================")
    print(f"Sentences analyzed: {len(scores)}")
    if not math.isnan(threshold):
        print(f"Slop threshold (p{args.percentile:.0f}): {threshold:.4f}")
    print(f"Slop-zone sentences: {slop_count}")
    print(f"Transition hits: {len(transition_hits)}")
    print(f"Plot saved to: {args.plot}")

    if args.output_csv:
        write_csv(args.output_csv, scores)
        print(f"CSV saved to: {args.output_csv}")

    if args.output_json:
        write_json(args.output_json, scores)
        print(f"JSON saved to: {args.output_json}")


if __name__ == "__main__":
    main()
