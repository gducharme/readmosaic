#!/usr/bin/env python3
"""Generate paragraph rewrite bundles to escape lexical overuse attractors.

Given an overuse report and pre-processing artifacts, this tool:
1) finds paragraphs containing each overused word,
2) generates lexical alternatives via multiple methods,
3) asks a local LLM to rewrite each paragraph while preserving semantics,
4) emits a bundle artifact with before/after paragraphs.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import nltk
import numpy as np
import requests
from nltk.corpus import wordnet as wn
from sentence_transformers import SentenceTransformer

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from libs.local_llm import request_chat_completion_content

TOKEN_PATTERN = re.compile(r"[A-Za-z']+")
DEFAULT_EMBEDDING_MODEL = "all-MiniLM-L6-v2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build lexical-expansion bundles for overused words and produce full "
            "paragraph rewrites using a local LLM."
        )
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        required=True,
        help="Path to preprocessing directory (expects paragraphs.jsonl).",
    )
    parser.add_argument(
        "--overuse-report",
        type=Path,
        required=True,
        help=(
            "Path to JSON overuse report. Supports either:\n"
            "- word_frequency_benchmark --output-json payload (uses top_words), or\n"
            "- compact frequency dict (word -> count)."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        required=True,
        help="Output JSON path for complete bundles and rewrites.",
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        help="Optional markdown report path with before/after paragraph pairs.",
    )
    parser.add_argument(
        "--max-words",
        type=int,
        default=10,
        help="Maximum number of overused words to process (default: 10).",
    )
    parser.add_argument(
        "--max-paragraphs-per-word",
        type=int,
        default=3,
        help="Maximum paragraphs to rewrite per overused word (default: 3).",
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


def ensure_nltk_resources() -> None:
    for resource in ("corpora/wordnet", "corpora/omw-1.4", "corpora/stopwords"):
        try:
            nltk.data.find(resource)
        except LookupError as exc:
            raise SystemExit(
                "Missing NLTK data resource: "
                f"{resource}. Run `python scripts/setup_nltk_data.py` to install prerequisites."
            ) from exc


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def load_overused_words(path: Path, max_words: int) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(payload, dict) and "top_words" in payload:
        top_words = payload.get("top_words", [])
        if not isinstance(top_words, list):
            raise SystemExit("Invalid overuse report: top_words must be a list.")

        ranked: list[tuple[str, float, int]] = []
        for row in top_words:
            if not isinstance(row, dict):
                continue
            word = row.get("word")
            if not isinstance(word, str):
                continue
            ratio = row.get("relative_to_humanity_avg")
            if isinstance(ratio, (int, float)):
                score = float(ratio)
            else:
                score = 0.0
            count = int(row.get("manuscript_count", 0) or 0)
            ranked.append((word.lower(), score, count))

        ranked.sort(key=lambda item: (item[1], item[2]), reverse=True)
        return [word for word, _, _ in ranked[:max_words]]

    if isinstance(payload, dict):
        ranked_by_count = sorted(
            ((word.lower(), count) for word, count in payload.items() if isinstance(word, str)),
            key=lambda pair: int(pair[1]) if isinstance(pair[1], (int, float)) else 0,
            reverse=True,
        )
        return [word for word, _ in ranked_by_count[:max_words]]

    raise SystemExit("Unsupported overuse report format. Expected object JSON payload.")


def build_text_frequency(paragraphs: list[dict[str, Any]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for paragraph in paragraphs:
        text = paragraph.get("text", "")
        if isinstance(text, str):
            counter.update(tokenize(text))
    return counter


def find_paragraphs_for_word(paragraphs: list[dict[str, Any]], word: str, max_items: int) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    pattern = re.compile(rf"\b{re.escape(word)}\b", flags=re.IGNORECASE)
    for paragraph in paragraphs:
        text = paragraph.get("text")
        if not isinstance(text, str):
            continue
        if pattern.search(text):
            matches.append(paragraph)
            if len(matches) >= max_items:
                break
    return matches


def method_a_frequency_band_jump(target_word: str, text_freq: Counter[str], top_n: int) -> list[str]:
    try:
        from wordfreq import top_n_list, zipf_frequency
    except ImportError:
        return []

    high_frequency_vocab = top_n_list("en", 5000)
    candidates: list[tuple[str, float]] = []
    for token in high_frequency_vocab:
        if token == target_word:
            continue
        if token in text_freq and text_freq[token] > 1:
            continue
        score = zipf_frequency(token, "en")
        if score < 4.5:
            candidates.append((token, score))

    candidates.sort(key=lambda item: item[1], reverse=True)
    return [token for token, _ in candidates[:top_n]]


def method_b_embedding_drift(
    target_word: str,
    text_freq: Counter[str],
    model: SentenceTransformer,
    top_n: int,
) -> list[str]:
    try:
        from wordfreq import top_n_list
    except ImportError:
        return []

    vocabulary = [token for token in top_n_list("en", 10000) if token != target_word]
    filtered = [token for token in vocabulary if text_freq.get(token, 0) <= 1]
    if not filtered:
        return []

    target_embedding = model.encode([target_word], normalize_embeddings=True)
    candidate_embeddings = model.encode(filtered, normalize_embeddings=True, batch_size=256)
    sims = np.dot(candidate_embeddings, target_embedding[0])

    top_indices = np.argsort(sims)[::-1][: top_n * 4]
    out: list[str] = []
    for idx in top_indices:
        token = filtered[int(idx)]
        if token not in out:
            out.append(token)
        if len(out) >= top_n:
            break
    return out


def method_c_wordnet_lateral(target_word: str, top_n: int) -> list[str]:
    candidates: list[str] = []

    for synset in wn.synsets(target_word):
        hypernyms = synset.hypernyms()
        for hyper in hypernyms:
            for coordinate in hyper.hyponyms():
                for lemma in coordinate.lemmas():
                    token = lemma.name().replace("_", " ").lower()
                    if token != target_word and token not in candidates:
                        candidates.append(token)

        for hypo in synset.hyponyms():
            for lemma in hypo.lemmas():
                token = lemma.name().replace("_", " ").lower()
                if token != target_word and token not in candidates:
                    candidates.append(token)

        if synset.pos() == "v":
            for trope in synset.hyponyms():
                for lemma in trope.lemmas():
                    token = lemma.name().replace("_", " ").lower()
                    if token != target_word and token not in candidates:
                        candidates.append(token)

    return candidates[:top_n]


def method_d_datamuse(target_word: str, timeout: int, top_n: int) -> list[str]:
    url = "https://api.datamuse.com/words"
    params = {"ml": target_word, "max": max(top_n * 2, 10)}
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException:
        return []

    payload = response.json()
    out: list[str] = []
    if not isinstance(payload, list):
        return out

    for row in payload:
        if not isinstance(row, dict):
            continue
        word = row.get("word")
        if isinstance(word, str) and word.lower() != target_word and word.lower() not in out:
            out.append(word.lower())
        if len(out) >= top_n:
            break
    return out


def rewrite_paragraph(
    paragraph_text: str,
    target_word: str,
    suggestions: dict[str, list[str]],
    base_url: str,
    model: str,
    timeout: int,
) -> str:
    system_prompt = (
        "You are a precision prose editor. Escape lexical attractors while preserving semantic identity, "
        "narrator voice, and factual content. Return only the rewritten paragraph."
    )
    user_payload = {
        "instruction": "escape the lexical attractor while preserving semantic identity",
        "target_word": target_word,
        "paragraph": paragraph_text,
        "suggestions": suggestions,
        "rules": [
            "Do not add or remove facts.",
            "Keep paragraph length close to original.",
            "Reduce direct repetition of the target word.",
            "Return exactly one full paragraph, no bullets or commentary.",
        ],
    }
    return request_chat_completion_content(
        base_url=base_url,
        model=model,
        system_prompt=system_prompt,
        user_content=json.dumps(user_payload, ensure_ascii=False),
        timeout=timeout,
        temperature=0.2,
    )


def write_markdown_report(path: Path, bundles: list[dict[str, Any]]) -> None:
    lines: list[str] = ["# Lexical Entropy Amplifier Report", ""]
    for item in bundles:
        lines.extend(
            [
                f"## Target word: `{item['target_word']}`",
                f"- Paragraph ID: `{item['paragraph_id']}`",
                "",
                "### Before",
                item["before"],
                "",
                "### After",
                item["after"],
                "",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    ensure_nltk_resources()

    paragraphs_path = args.preprocessing / "paragraphs.jsonl"
    if not paragraphs_path.exists():
        raise SystemExit(f"Expected paragraphs artifact not found: {paragraphs_path}")
    if not args.overuse_report.exists():
        raise SystemExit(f"Overuse report not found: {args.overuse_report}")

    paragraphs = read_jsonl(paragraphs_path)
    if not paragraphs:
        raise SystemExit("No paragraph records found in paragraphs.jsonl")

    target_words = load_overused_words(args.overuse_report, args.max_words)
    text_frequency = build_text_frequency(paragraphs)

    embedding_model = SentenceTransformer(args.embedding_model)

    bundles: list[dict[str, Any]] = []
    manuscript_id = paragraphs[0].get("manuscript_id")

    for target_word in target_words:
        candidate_paragraphs = find_paragraphs_for_word(
            paragraphs=paragraphs,
            word=target_word,
            max_items=args.max_paragraphs_per_word,
        )
        if not candidate_paragraphs:
            continue

        suggestions = {
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

        for paragraph in candidate_paragraphs:
            paragraph_id = paragraph.get("id", "unknown")
            paragraph_text = str(paragraph.get("text", "")).strip()
            if not paragraph_text:
                continue

            rewritten = rewrite_paragraph(
                paragraph_text=paragraph_text,
                target_word=target_word,
                suggestions=suggestions,
                base_url=args.lm_base_url,
                model=args.model,
                timeout=args.timeout,
            )

            bundles.append(
                {
                    "bundle_id": f"{paragraph_id}:{target_word}",
                    "target_word": target_word,
                    "paragraph_id": paragraph_id,
                    "before": paragraph_text,
                    "after": rewritten,
                    "suggestions": suggestions,
                }
            )

    output_payload = {
        "schema_version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manuscript_id": manuscript_id,
        "instruction": "escape the lexical attractor while preserving semantic identity",
        "bundle_count": len(bundles),
        "bundles": bundles,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(output_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.output_markdown:
        write_markdown_report(args.output_markdown, bundles)

    print(f"Saved lexical amplification bundles: {args.output_json}")
    if args.output_markdown:
        print(f"Saved markdown report: {args.output_markdown}")


if __name__ == "__main__":
    main()
