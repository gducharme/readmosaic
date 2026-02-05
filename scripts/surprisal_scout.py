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
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from schema_validator import validate_payload


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


@dataclass
class SentenceLocation:
    paragraph_id: str
    token_ids: List[str]
    char_range: dict


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


def load_manuscript_tokens(preprocessing_dir: Path) -> dict:
    tokens_path = preprocessing_dir / "manuscript_tokens.json"
    if not tokens_path.exists():
        raise FileNotFoundError(
            f"Missing manuscript_tokens.json in preprocessing dir: {preprocessing_dir}"
        )
    return json.loads(tokens_path.read_text(encoding="utf-8"))


def locate_sentence_spans(paragraph_text: str, sentences: Sequence[str]) -> List[tuple]:
    spans: List[tuple] = []
    search_start = 0
    for sentence in sentences:
        position = paragraph_text.find(sentence, search_start)
        if position == -1:
            position = search_start
        start = position
        end = start + len(sentence)
        spans.append((sentence, start, end))
        search_start = end
    return spans


def build_sentence_locations(
    manuscript_tokens: dict,
) -> tuple[List[str], List[SentenceLocation]]:
    sentences: List[str] = []
    locations: List[SentenceLocation] = []
    for paragraph in manuscript_tokens.get("paragraphs", []):
        paragraph_id = paragraph["paragraph_id"]
        paragraph_text = paragraph.get("text", "")
        paragraph_tokens = paragraph.get("tokens", [])
        paragraph_sentences = split_sentences(paragraph_text)
        if not paragraph_sentences:
            continue
        spans = locate_sentence_spans(paragraph_text, paragraph_sentences)
        for sentence, start, end in spans:
            token_ids = [
                token["token_id"]
                for token in paragraph_tokens
                if token["start_char"] < end and token["end_char"] > start
            ]
            sentences.append(sentence)
            locations.append(
                SentenceLocation(
                    paragraph_id=paragraph_id,
                    token_ids=token_ids,
                    char_range={"start": start, "end": end},
                )
            )
    return sentences, locations


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
        "--preprocessing",
        type=Path,
        help="Directory containing manuscript_tokens.json for sentence mapping.",
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
        "--output-edits",
        type=Path,
        help="Optional path to write edits.schema.json payload for slop sentences.",
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


def build_edits_payload(
    scores: Sequence[SentenceScore],
    locations: Sequence[SentenceLocation],
    manuscript_id: str,
) -> dict:
    items = []
    for score, location in zip(scores, locations, strict=True):
        if not score.is_slop:
            continue
        issue_id = str(uuid.uuid4())
        location_payload = {
            "paragraph_id": location.paragraph_id,
            "char_range": location.char_range,
            "anchor_text": score.sentence,
        }
        if location.token_ids:
            location_payload["token_ids"] = location.token_ids
        items.append(
            {
                "issue_id": issue_id,
                "type": "style",
                "status": "open",
                "location": location_payload,
                "evidence": {
                    "summary": (
                        "High average log-probability sentence flagged as potential slop."
                    ),
                    "detector": "surprisal_scout",
                    "signals": [
                        {
                            "name": "avg_logprob",
                            "value": round(score.avg_logprob, 6)
                            if not math.isnan(score.avg_logprob)
                            else None,
                            "units": "logprob",
                        },
                        {
                            "name": "perplexity",
                            "value": round(score.perplexity, 6)
                            if not math.isnan(score.perplexity)
                            else None,
                        },
                        {"name": "transition_hits", "value": score.transitions},
                    ],
                },
            }
        )
    if not items:
        return {}
    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input not found: {args.input}")
    if args.preprocessing and not args.preprocessing.exists():
        raise FileNotFoundError(f"Preprocessing dir not found: {args.preprocessing}")
    if args.output_edits and not args.preprocessing:
        raise SystemExit("--output-edits requires --preprocessing for token mapping.")

    transition_phrases = load_transition_phrases(
        args.transition_phrases, args.transition_file
    )
    device = resolve_device(args.device)

    manuscript_tokens = None
    sentence_locations: List[SentenceLocation] = []
    if args.preprocessing:
        manuscript_tokens = load_manuscript_tokens(args.preprocessing)
        sentences, sentence_locations = build_sentence_locations(manuscript_tokens)
        if not sentences:
            raise ValueError("No sentences found in preprocessing artifact.")
    else:
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

    if args.output_edits:
        edits_payload = build_edits_payload(
            scores,
            sentence_locations,
            manuscript_tokens["manuscript_id"],
        )
        if not edits_payload:
            print("No slop sentences above threshold for edits output.")
        else:
            validate_payload(edits_payload, "edits.schema.json", "surprisal edits payload")
            args.output_edits.write_text(
                json.dumps(edits_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"Edits saved to: {args.output_edits}")


if __name__ == "__main__":
    main()
