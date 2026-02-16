#!/usr/bin/env python3
"""Interactive lexical enhancer (v2) for target-word paragraph rewrites.

This CLI loads manuscript preprocessing artifacts once, then enters an interactive
loop. For each user-provided target word it generates lexical suggestions,
produes LLM rewrites for matching paragraphs, and prompts for manual review
before moving to the next target word.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sentence_transformers import SentenceTransformer

from lexical_entropy_amplifier import (
    DEFAULT_EMBEDDING_MODEL,
    build_text_frequency,
    ensure_nltk_resources,
    find_paragraphs_for_word,
    load_exception_words,
    method_a_frequency_band_jump,
    method_b_embedding_drift,
    method_c_wordnet_lateral,
    method_d_datamuse,
    read_jsonl,
    rewrite_paragraph,
    write_markdown_report,
)


EXIT_TOKENS = {"quit", "exit", "q", ":q"}
SKIP_TOKENS = {"skip", "s"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Interactive lexical enhancer that loads preprocessing once, then lets "
            "you enter target words for iterative LLM-assisted paragraph rewrites "
            "with manual review."
        )
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        required=True,
        help="Path to preprocessing directory (expects paragraphs.jsonl).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("lexical_enhancer_review.json"),
        help="Where to save reviewed bundles at the end of the session.",
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        help="Optional markdown report path with accepted before/after pairs.",
    )
    parser.add_argument(
        "--exceptions-json",
        type=Path,
        help=(
            "Optional JSON file listing words to exclude from processing. "
            "Supported formats: array of strings, or object with `words` array."
        ),
    )
    parser.add_argument(
        "--max-paragraphs-per-word",
        type=int,
        default=3,
        help="Maximum paragraphs to rewrite per target word (default: 3).",
    )
    parser.add_argument(
        "--method-a-top-n",
        type=int,
        default=8,
        help="Max suggestions for method A (frequency-band jump).",
    )
    parser.add_argument(
        "--method-b-top-n",
        type=int,
        default=8,
        help="Max suggestions for method B (embedding drift).",
    )
    parser.add_argument(
        "--method-c-top-n",
        type=int,
        default=8,
        help="Max suggestions for method C (WordNet lateral expansion).",
    )
    parser.add_argument(
        "--method-d-top-n",
        type=int,
        default=8,
        help="Max suggestions for method D (Datamuse thesaurus).",
    )
    parser.add_argument(
        "--lm-base-url",
        default="http://localhost:1234",
        help="Base URL for local OpenAI-compatible endpoint (default: http://localhost:1234).",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Model identifier exposed by local LLM endpoint.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout seconds for model and Datamuse requests (default: 120).",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"SentenceTransformer model for method B (default: {DEFAULT_EMBEDDING_MODEL}).",
    )
    return parser.parse_args()


def _prompt_target_word() -> str:
    raw = input("\nTarget word (or 'quit'): ").strip().lower()
    return raw


def _prompt_review_action() -> str:
    while True:
        action = input("Review action [a=accept, e=edit, s=skip, q=quit session]: ").strip().lower()
        if action in {"a", "e", "s", "q"}:
            return action
        print("Invalid action. Use one of: a, e, s, q")


def _print_suggestions(suggestions: dict[str, list[str]]) -> None:
    print("\nSuggestion bundle:")
    for method_name, words in suggestions.items():
        joined = ", ".join(words) if words else "(none)"
        print(f"- {method_name}: {joined}")


def _review_paragraph(
    paragraph: dict[str, Any],
    target_word: str,
    suggestions: dict[str, list[str]],
    args: argparse.Namespace,
) -> dict[str, Any] | None:
    paragraph_id = str(paragraph.get("id", "unknown"))
    before = str(paragraph.get("text", "")).strip()
    if not before:
        return None

    print("\n" + "=" * 80)
    print(f"Paragraph: {paragraph_id} | target: {target_word}")
    print("-" * 80)
    print("BEFORE:\n")
    print(before)
    print("\nGenerating rewrite...\n")

    proposed_after = rewrite_paragraph(
        paragraph_text=before,
        target_word=target_word,
        suggestions=suggestions,
        base_url=args.lm_base_url,
        model=args.model,
        timeout=args.timeout,
    )

    print("PROPOSED AFTER:\n")
    print(proposed_after)

    action = _prompt_review_action()
    if action == "q":
        raise KeyboardInterrupt
    if action == "s":
        print("Skipped paragraph.")
        return {
            "bundle_id": f"{paragraph_id}:{target_word}",
            "target_word": target_word,
            "paragraph_id": paragraph_id,
            "before": before,
            "after": before,
            "suggestions": suggestions,
            "review_status": "skipped",
            "proposed_after": proposed_after,
        }
    if action == "e":
        print("Enter your final reviewed paragraph text. Press Enter when done.")
        final_after = input("Final paragraph: ").strip()
        if not final_after:
            final_after = proposed_after
            print("No manual edit entered; keeping proposed rewrite.")
        status = "edited"
    else:
        final_after = proposed_after
        status = "accepted"

    return {
        "bundle_id": f"{paragraph_id}:{target_word}",
        "target_word": target_word,
        "paragraph_id": paragraph_id,
        "before": before,
        "after": final_after,
        "suggestions": suggestions,
        "review_status": status,
    }


def _build_suggestions(
    target_word: str,
    args: argparse.Namespace,
    text_frequency: dict[str, int],
    embedding_model: SentenceTransformer,
) -> dict[str, list[str]]:
    return {
        "method_a_frequency_band_jump": method_a_frequency_band_jump(
            target_word=target_word,
            text_freq=text_frequency,
            top_n=args.method_a_top_n,
        ),
        "method_b_embedding_drift": method_b_embedding_drift(
            target_word=target_word,
            text_freq=text_frequency,
            model=embedding_model,
            top_n=args.method_b_top_n,
        ),
        "method_c_wordnet_lateral_expansion": method_c_wordnet_lateral(
            target_word=target_word,
            top_n=args.method_c_top_n,
        ),
        "method_d_datamuse_thesaurus": method_d_datamuse(
            target_word=target_word,
            timeout=args.timeout,
            top_n=args.method_d_top_n,
        ),
    }


def main() -> None:
    args = parse_args()
    ensure_nltk_resources()

    paragraphs_path = args.preprocessing / "paragraphs.jsonl"
    if not paragraphs_path.exists():
        raise SystemExit(f"Expected paragraphs artifact not found: {paragraphs_path}")
    if args.exceptions_json and not args.exceptions_json.exists():
        raise SystemExit(f"Exceptions file not found: {args.exceptions_json}")

    paragraphs = read_jsonl(paragraphs_path)
    if not paragraphs:
        raise SystemExit("No paragraph records found in paragraphs.jsonl")

    exceptions = load_exception_words(args.exceptions_json) if args.exceptions_json else set()
    manuscript_id = paragraphs[0].get("manuscript_id")
    text_frequency = build_text_frequency(paragraphs)
    embedding_model = SentenceTransformer(args.embedding_model)

    print("Loaded preprocessing artifacts.")
    print(f"Paragraph count: {len(paragraphs)}")
    print("Enter words one-by-one; each word triggers generation + manual review.")

    bundles: list[dict[str, Any]] = []
    reviewed_words: list[str] = []

    while True:
        target_word = _prompt_target_word()
        if not target_word:
            print("Please enter a word or 'quit'.")
            continue
        if target_word in EXIT_TOKENS:
            break
        if target_word in SKIP_TOKENS:
            continue
        if target_word in exceptions:
            print(f"'{target_word}' is in exceptions and will be skipped.")
            continue

        candidates = find_paragraphs_for_word(
            paragraphs=paragraphs,
            word=target_word,
            max_items=args.max_paragraphs_per_word,
        )
        if not candidates:
            print(f"No matching paragraphs found for '{target_word}'.")
            continue

        suggestions = _build_suggestions(
            target_word=target_word,
            args=args,
            text_frequency=text_frequency,
            embedding_model=embedding_model,
        )
        _print_suggestions(suggestions)

        try:
            for paragraph in candidates:
                reviewed = _review_paragraph(
                    paragraph=paragraph,
                    target_word=target_word,
                    suggestions=suggestions,
                    args=args,
                )
                if reviewed is not None:
                    bundles.append(reviewed)
        except KeyboardInterrupt:
            print("\nSession stop requested during review.")
            break

        reviewed_words.append(target_word)
        print(f"Completed review set for '{target_word}'. You may choose the next word.")

    payload = {
        "schema_version": "2.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manuscript_id": manuscript_id,
        "instruction": "interactive lexical enhancement with manual review",
        "reviewed_words": reviewed_words,
        "bundle_count": len(bundles),
        "bundles": bundles,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved reviewed bundles JSON: {args.output_json}")

    if args.output_markdown:
        accepted = [bundle for bundle in bundles if bundle.get("review_status") in {"accepted", "edited"}]
        write_markdown_report(args.output_markdown, accepted)
        print(f"Saved markdown report (accepted/edited only): {args.output_markdown}")


if __name__ == "__main__":
    main()
