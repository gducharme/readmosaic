#!/usr/bin/env python3
"""Vivid Verb Upgrader (VVU).

Scans manuscript sentences for generic/light verbs, proposes specific troponym
upgrades, and ranks candidates using semantic fit, concreteness, frequency, and
WordNet specificity depth.

Usage examples:
  python scripts/vivid_verb_upgrader.py path/to/manuscript.md
  python scripts/vivid_verb_upgrader.py --preprocessed /preprocessed/sentences.jsonl
  python scripts/vivid_verb_upgrader.py manuscript.md --concreteness-csv concreteness.csv --output-json vvu.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import spacy
from nltk.corpus import verbnet as vn
from nltk.corpus import wordnet as wn
from sentence_transformers import SentenceTransformer
from wordfreq import zipf_frequency

GENERIC_VERBS = {
    "make",
    "do",
    "get",
    "take",
    "put",
    "go",
    "come",
    "move",
    "open",
    "close",
    "look",
    "turn",
    "set",
    "give",
    "run",
    "keep",
}

DEFAULT_EMBEDDING_MODEL = "all-MiniLM-L6-v2"
DEFAULT_CONCRETENESS_FALLBACK = 2.5
CONCRETENESS_HINT_MAP = {
    "high": 4.2,
    "medium": 3.0,
    "low": 1.8,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Suggest vivid verb upgrades by detecting generic verbs and ranking "
            "WordNet troponyms with semantic/contextual scoring."
        )
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        type=Path,
        help="Path to manuscript (.md or .txt).",
    )
    parser.add_argument(
        "--preprocessed",
        type=Path,
        help=(
            "Path to preprocessed sentences artifact. Accepts either a sentences.jsonl "
            "file or a preprocessing directory containing sentences.jsonl."
        ),
    )
    parser.add_argument(
        "--concreteness-csv",
        type=Path,
        default=Path("concreteness.csv"),
        help=(
            "Path to concreteness CSV with columns verb, category, and "
            "concreteness_hint (default: ./concreteness.csv)."
        ),
    )
    parser.add_argument(
        "--spacy-model",
        default="en_core_web_sm",
        help="spaCy model used for POS/dependency parsing (default: en_core_web_sm).",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"SentenceTransformer model name (default: {DEFAULT_EMBEDDING_MODEL}).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Max candidate replacements per generic verb hit (default: 5).",
    )
    parser.add_argument(
        "--frequency-delta-threshold",
        type=float,
        default=0.0,
        help=(
            "Require candidate Zipf frequency <= original frequency - threshold. "
            "Use 0.0 for simple <= check (default: 0.0)."
        ),
    )
    parser.add_argument(
        "--generic-verbs",
        help="Optional comma-separated generic verb lemma override list.",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help=(
            "Include generic verb hits even when no ranked suggestions survive filters. "
            "Useful for auditing every occurrence."
        ),
    )
    parser.add_argument(
        "--print-text",
        action="store_true",
        help=(
            "Print a human-readable report with one block per generic-verb occurrence "
            "(Original / Generic / Suggestions)."
        ),
    )
    parser.add_argument(
        "--strict-verbnet",
        action="store_true",
        help=(
            "Require every candidate to have a VerbNet class. Disabled by default to "
            "avoid dropping valid WordNet alternatives."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to save full JSON report.",
    )
    return parser.parse_args()


def load_concreteness(path: Path) -> dict[str, float]:
    if not path.exists():
        raise SystemExit(f"Concreteness CSV not found: {path}")
    df = pd.read_csv(path)
    required_columns = {"verb", "category", "concreteness_hint"}
    if not required_columns.issubset(df.columns):
        raise SystemExit(
            f"Concreteness CSV missing required columns {sorted(required_columns)}: {path}"
        )

    words = df["verb"].astype(str).str.strip().str.lower()
    hints = df["concreteness_hint"].astype(str).str.strip().str.lower()
    scores = hints.map(CONCRETENESS_HINT_MAP).fillna(DEFAULT_CONCRETENESS_FALLBACK)
    return dict(zip(words, scores))


def resolve_sentences_path(preprocessed: Path) -> Path:
    if preprocessed.is_file():
        return preprocessed
    candidate = preprocessed / "sentences.jsonl"
    if candidate.exists():
        return candidate
    raise SystemExit(
        "--preprocessed must be a sentences.jsonl file or a directory containing sentences.jsonl."
    )


def read_preprocessed_sentences(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Preprocessed file not found: {path}")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON in {path}:{line_number}: {exc}") from exc
        if not isinstance(payload, dict):
            continue
        text = str(payload.get("text", "")).strip()
        if not text:
            continue
        rows.append(payload)
    return rows


def concreteness(word: str, table: dict[str, float]) -> float:
    return float(table.get(word.lower(), DEFAULT_CONCRETENESS_FALLBACK))


def verb_depth(synset: wn.synset) -> int:
    depth = 0
    current = synset
    while current.hypernyms():
        current = current.hypernyms()[0]
        depth += 1
    return depth


def get_troponyms(verb: str) -> list[tuple[str, int]]:
    candidates: dict[str, int] = {}
    for synset in wn.synsets(verb, pos=wn.VERB):
        # NLTK WordNet exposes verb "troponyms" through hyponyms().
        # Some wrappers may provide troponyms(); prefer it when present.
        if hasattr(synset, "troponyms"):
            related_synsets = synset.troponyms()
        else:
            related_synsets = synset.hyponyms()

        for troponym in related_synsets:
            depth = verb_depth(troponym)
            for lemma in troponym.lemma_names():
                normalized = lemma.replace("_", "-").lower()
                if normalized == verb.lower():
                    continue
                prev_depth = candidates.get(normalized)
                if prev_depth is None or depth > prev_depth:
                    candidates[normalized] = depth
    return sorted(candidates.items(), key=lambda item: item[0])


def verbnet_compatible(verb: str) -> bool:
    try:
        return len(vn.classids(verb)) > 0
    except Exception:
        return False


def semantic_similarity(embedder: SentenceTransformer, context: str, candidate_phrase: str) -> float:
    vec1 = embedder.encode(context)
    vec2 = embedder.encode(candidate_phrase)
    denom = np.linalg.norm(vec1) * np.linalg.norm(vec2)
    if denom == 0:
        return 0.0
    return float(np.dot(vec1, vec2) / denom)


def find_generic_verbs(sentence: spacy.tokens.Span, generic_verbs: set[str]) -> list[tuple[spacy.tokens.Token, spacy.tokens.Token]]:
    hits: list[tuple[spacy.tokens.Token, spacy.tokens.Token]] = []
    for token in sentence:
        if token.pos_ != "VERB":
            continue
        lemma = token.lemma_.lower()
        if lemma not in generic_verbs:
            continue
        direct_objects = [child for child in token.children if child.dep_ in {"dobj", "obj"}]
        if direct_objects:
            hits.append((token, direct_objects[0]))
    return hits


def rank_candidates(
    embedder: SentenceTransformer,
    concreteness_table: dict[str, float],
    context: str,
    original_verb: str,
    obj_text: str,
    top_k: int,
    frequency_delta_threshold: float,
    strict_verbnet: bool,
) -> list[dict[str, float | str | int]]:
    original_frequency = zipf_frequency(original_verb, "en")
    original_concrete = concreteness(original_verb, concreteness_table)

    scored: list[dict[str, float | str | int]] = []
    for candidate, depth in get_troponyms(original_verb):
        candidate_frequency = zipf_frequency(candidate, "en")
        if candidate_frequency > (original_frequency - frequency_delta_threshold):
            continue

        candidate_concrete = concreteness(candidate, concreteness_table)
        if candidate_concrete < original_concrete:
            continue

        if strict_verbnet and not verbnet_compatible(candidate):
            continue

        sim = semantic_similarity(embedder, context, f"{candidate} {obj_text}")
        score = sim + 0.3 * candidate_concrete - 0.2 * candidate_frequency + 0.05 * depth
        scored.append(
            {
                "candidate": candidate,
                "score": round(score, 4),
                "semantic_similarity": round(sim, 4),
                "concreteness": round(candidate_concrete, 4),
                "zipf_frequency": round(candidate_frequency, 4),
                "depth": depth,
                "verbnet_compatible": int(verbnet_compatible(candidate)),
            }
        )

    scored.sort(key=lambda item: float(item["score"]), reverse=True)
    return scored[:top_k]


def process_sentence_records(
    nlp: spacy.language.Language,
    embedder: SentenceTransformer,
    concreteness_table: dict[str, float],
    sentence_records: list[dict[str, Any]],
    generic_verbs: set[str],
    top_k: int,
    frequency_delta_threshold: float,
    include_empty: bool,
    strict_verbnet: bool,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for record in sentence_records:
        sentence_text = str(record.get("text", "")).strip()
        if not sentence_text:
            continue
        doc = nlp(sentence_text)
        for sent in doc.sents:
            generic_hits = find_generic_verbs(sent, generic_verbs)
            for verb, obj in generic_hits:
                ranked = rank_candidates(
                    embedder=embedder,
                    concreteness_table=concreteness_table,
                    context=sent.text,
                    original_verb=verb.lemma_.lower(),
                    obj_text=obj.text,
                    top_k=top_k,
                    frequency_delta_threshold=frequency_delta_threshold,
                    strict_verbnet=strict_verbnet,
                )
                if not ranked and not include_empty:
                    continue
                output.append(
                    {
                        "sentence": sent.text,
                        "verb": verb.text,
                        "verb_lemma": verb.lemma_.lower(),
                        "object": obj.text,
                        "sentence_id": record.get("id"),
                        "paragraph_id": record.get("paragraph_id"),
                        "suggestions": ranked,
                    }
                )
    return output


def print_human_readable_report(results: list[dict[str, Any]]) -> None:
    for hit in results:
        suggestions = [str(item.get("candidate", "")).strip() for item in hit.get("suggestions", [])]
        suggestion_text = ", ".join(s for s in suggestions if s)
        if not suggestion_text:
            suggestion_text = "(no suggestions after filters)"

        print(f"Original: {hit.get('sentence', '').strip()}")
        print(f"Generic: {hit.get('verb', '').strip()} → {hit.get('object', '').strip()}")
        print(f"Suggestions: [{suggestion_text}]")
        print()


def read_raw_manuscript(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"Input file not found: {path}")
    if path.suffix.lower() not in {".md", ".txt"}:
        raise SystemExit("Input file must be .md or .txt")
    text = path.read_text(encoding="utf-8")
    return [{"id": None, "paragraph_id": None, "text": line} for line in text.splitlines() if line.strip()]


def parse_generic_verbs(raw: str | None) -> set[str]:
    if not raw:
        return set(GENERIC_VERBS)
    verbs = {item.strip().lower() for item in raw.split(",") if item.strip()}
    if not verbs:
        raise SystemExit("--generic-verbs produced an empty set; provide comma-separated lemmas.")
    return verbs


def main() -> None:
    args = parse_args()
    if not args.input_file and not args.preprocessed:
        raise SystemExit("Provide a manuscript file or --preprocessed sentences artifact.")
    if args.top_k <= 0:
        raise SystemExit("--top-k must be greater than zero.")

    generic_verbs = parse_generic_verbs(args.generic_verbs)
    concreteness_table = load_concreteness(args.concreteness_csv)

    try:
        nlp = spacy.load(args.spacy_model)
    except OSError as exc:
        raise SystemExit(
            f"Unable to load spaCy model '{args.spacy_model}'. Install it first, e.g. "
            "`python -m spacy download en_core_web_sm`."
        ) from exc

    embedder = SentenceTransformer(args.embedding_model)

    sentence_records: list[dict[str, Any]] = []
    if args.input_file:
        sentence_records.extend(read_raw_manuscript(args.input_file))
    if args.preprocessed:
        sentences_path = resolve_sentences_path(args.preprocessed)
        sentence_records.extend(read_preprocessed_sentences(sentences_path))

    upgrades = process_sentence_records(
        nlp=nlp,
        embedder=embedder,
        concreteness_table=concreteness_table,
        sentence_records=sentence_records,
        generic_verbs=generic_verbs,
        top_k=args.top_k,
        frequency_delta_threshold=args.frequency_delta_threshold,
        include_empty=args.include_empty,
        strict_verbnet=args.strict_verbnet,
    )

    payload = {
        "tool": "VVU",
        "generic_verbs": sorted(generic_verbs),
        "top_k": args.top_k,
        "results": upgrades,
        "result_count": len(upgrades),
    }

    if args.print_text:
        print_human_readable_report(upgrades)

    print(json.dumps(payload, indent=2))

    if args.output_json:
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nSaved JSON report: {args.output_json}")


if __name__ == "__main__":
    main()
