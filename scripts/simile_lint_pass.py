#!/usr/bin/env python3
"""Simile lint pass for markdown/plain-text manuscripts.

Detects `like` similes, scores semantic gain, classifies quality, and writes
rewrite recommendations to a markdown report.

Usage examples:
  python scripts/simile_lint_pass.py manuscript.md
  python scripts/simile_lint_pass.py --preprocessed /preprocessed
  python scripts/simile_lint_pass.py notes/ --output-file simile_recommendation.md
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import spacy
from sentence_transformers import SentenceTransformer
DEFAULT_EMBEDDING_MODEL = "all-MiniLM-L6-v2"
DEFAULT_CONCRETENESS_FALLBACK = 2.5
DEFAULT_GAIN_EMPTY_THRESHOLD = 0.15
DEFAULT_GAIN_POTENTIAL_THRESHOLD = 0.38
DEFAULT_OUTPUT_FILE = Path("simile_recommendation.md")

def resolve_zipf_frequency() -> Any:
    spec = importlib.util.find_spec("wordfreq")
    if spec is None:
        return lambda _token, _lang: 0.0
    module = importlib.import_module("wordfreq")
    return module.zipf_frequency


ZIPF_FREQUENCY = resolve_zipf_frequency()


@dataclass
class SentenceItem:
    source: str
    sentence_id: str
    text: str


@dataclass
class SimileFinding:
    source: str
    sentence_id: str
    sentence: str
    left: str
    right: str
    semantic_gain: float
    left_concreteness: float
    right_concreteness: float
    concreteness_delta: float
    right_phrase_zipf: float
    label: str
    suggestions: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lint similes built with 'like', classify weak/cliche comparisons, "
            "and emit structural rewrite recommendations."
        )
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        type=Path,
        help="Path to a markdown/text file or directory containing .md/.txt files.",
    )
    parser.add_argument(
        "--preprocessed",
        type=Path,
        help=(
            "Path to preprocessed sentences artifact. Accepts a sentences.jsonl file "
            "or a preprocessing directory containing sentences.jsonl."
        ),
    )
    parser.add_argument(
        "--concreteness-csv",
        type=Path,
        default=Path("concreteness.csv"),
        help="Path to concreteness CSV containing Word and Conc.M columns.",
    )
    parser.add_argument(
        "--cliche-file",
        type=Path,
        default=Path("cliche_similes.txt"),
        help="Path to newline-separated cliche simile phrases (default: ./cliche_similes.txt).",
    )
    parser.add_argument(
        "--spacy-model",
        default="en_core_web_sm",
        help="spaCy model used for sentence parsing (default: en_core_web_sm).",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"SentenceTransformer model name (default: {DEFAULT_EMBEDDING_MODEL}).",
    )
    parser.add_argument(
        "--empty-gain-threshold",
        type=float,
        default=DEFAULT_GAIN_EMPTY_THRESHOLD,
        help="Semantic gain threshold below which a simile is labeled EMPTY.",
    )
    parser.add_argument(
        "--potential-gain-threshold",
        type=float,
        default=DEFAULT_GAIN_POTENTIAL_THRESHOLD,
        help="Semantic gain threshold at/above which a non-cliche simile is potentially good.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="Markdown report path (default: simile_recommendation.md).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path for machine-readable JSON findings.",
    )
    return parser.parse_args()


def resolve_sentences_path(preprocessed: Path) -> Path:
    if preprocessed.is_file():
        return preprocessed
    candidate = preprocessed / "sentences.jsonl"
    if candidate.exists():
        return candidate
    raise SystemExit(
        "--preprocessed must be a sentences.jsonl file or directory containing sentences.jsonl."
    )


def read_preprocessed_sentences(path: Path) -> list[SentenceItem]:
    if not path.exists():
        raise SystemExit(f"Preprocessed file not found: {path}")

    items: list[SentenceItem] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = line.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON in {path}:{line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            continue
        sentence = str(payload.get("text", "")).strip()
        if not sentence:
            continue
        items.append(
            SentenceItem(
                source=str(payload.get("source", path.name)),
                sentence_id=str(payload.get("id", f"line-{line_no}")),
                text=sentence,
            )
        )
    return items


def strip_markdown_noise(text: str) -> str:
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    text = re.sub(r"`[^`]+`", "", text)
    text = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^\)]*\)", r"\1", text)
    return text


def load_text_sentences(input_path: Path, nlp: spacy.Language) -> list[SentenceItem]:
    files: list[Path] = []
    if input_path.is_file():
        files = [input_path]
    else:
        files = [
            path
            for path in sorted(input_path.glob("**/*"))
            if path.is_file() and path.suffix.lower() in {".md", ".txt"}
        ]

    if not files:
        raise SystemExit("No .md or .txt files found in input path.")

    results: list[SentenceItem] = []
    sentence_counter = 0
    for file_path in files:
        cleaned = strip_markdown_noise(file_path.read_text(encoding="utf-8"))
        doc = nlp(cleaned)
        for sent in doc.sents:
            sentence = sent.text.strip()
            if not sentence:
                continue
            sentence_counter += 1
            results.append(
                SentenceItem(
                    source=str(file_path),
                    sentence_id=f"sent-{sentence_counter}",
                    text=sentence,
                )
            )
    return results


def load_concreteness(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    required = {"Word", "Conc.M"}
    if not required.issubset(df.columns):
        raise SystemExit(f"Concreteness CSV missing columns {sorted(required)}: {path}")
    words = df["Word"].astype(str).str.lower()
    scores = pd.to_numeric(df["Conc.M"], errors="coerce").fillna(DEFAULT_CONCRETENESS_FALLBACK)
    return dict(zip(words, scores))


def load_cliches(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def phrase_concreteness(phrase: str, table: dict[str, float]) -> float:
    tokens = re.findall(r"[A-Za-z][A-Za-z'-]*", phrase.lower())
    if not tokens:
        return DEFAULT_CONCRETENESS_FALLBACK
    values = [float(table.get(token, DEFAULT_CONCRETENESS_FALLBACK)) for token in tokens]
    return float(sum(values) / len(values))


def phrase_zipf(phrase: str) -> float:
    tokens = re.findall(r"[A-Za-z][A-Za-z'-]*", phrase.lower())
    if not tokens:
        return 0.0
    vals = [ZIPF_FREQUENCY(token, "en") for token in tokens]
    return float(sum(vals) / len(vals))


def extract_similes(sentence_doc: spacy.tokens.Doc) -> list[tuple[str, str]]:
    similes: list[tuple[str, str]] = []
    for token in sentence_doc:
        if token.text.lower() != "like" or token.dep_ != "prep":
            continue
        left = " ".join(t.text for t in token.head.subtree).strip()
        right = " ".join(t.text for t in token.subtree).strip()
        if left and right:
            similes.append((left, right))
    return similes


def semantic_gain(embedder: SentenceTransformer, left: str, right: str) -> float:
    vec_left = embedder.encode(left)
    vec_right = embedder.encode(right)
    return 1.0 - cosine(vec_left, vec_right)


def classify_simile(
    right: str,
    gain: float,
    cliches: set[str],
    empty_gain_threshold: float,
    potential_gain_threshold: float,
) -> str:
    right_l = right.lower()
    if any(phrase in right_l for phrase in cliches):
        return "CLICHE"
    if gain < empty_gain_threshold:
        return "EMPTY"
    if gain >= potential_gain_threshold:
        return "POTENTIALLY_GOOD"
    return "WEAK_OR_FUNCTIONAL"


def suggest_actions(label: str) -> list[str]:
    if label in {"EMPTY", "CLICHE", "WEAK_OR_FUNCTIONAL"}:
        return [
            "DELETE simile → keep the base statement.",
            "COMPRESS → keep one concrete image-bearing noun only.",
            "VERB UPGRADE → replace generic verb with specific physical action.",
            "METAPHORIZE → remove 'like' and fuse the images directly.",
            "SCENE-DERIVED REWRITE HOOK → replace comparison with local sensory detail from setting/props/body.",
        ]
    return ["KEEP if context supports it, or SCENE-DERIVE an even more specific comparison."]


def analyze_sentences(
    sentences: Iterable[SentenceItem],
    nlp: spacy.Language,
    embedder: SentenceTransformer,
    concreteness_table: dict[str, float],
    cliches: set[str],
    empty_gain_threshold: float,
    potential_gain_threshold: float,
) -> list[SimileFinding]:
    findings: list[SimileFinding] = []
    for item in sentences:
        doc = nlp(item.text)
        for left, right in extract_similes(doc):
            gain = semantic_gain(embedder, left, right)
            left_conc = phrase_concreteness(left, concreteness_table)
            right_conc = phrase_concreteness(right, concreteness_table)
            label = classify_simile(
                right=right,
                gain=gain,
                cliches=cliches,
                empty_gain_threshold=empty_gain_threshold,
                potential_gain_threshold=potential_gain_threshold,
            )
            findings.append(
                SimileFinding(
                    source=item.source,
                    sentence_id=item.sentence_id,
                    sentence=item.text,
                    left=left,
                    right=right,
                    semantic_gain=float(gain),
                    left_concreteness=float(left_conc),
                    right_concreteness=float(right_conc),
                    concreteness_delta=float(right_conc - left_conc),
                    right_phrase_zipf=phrase_zipf(right),
                    label=label,
                    suggestions=suggest_actions(label),
                )
            )
    return findings


def write_markdown_report(path: Path, findings: list[SimileFinding]) -> None:
    lines: list[str] = ["# Simile Recommendation Report", ""]
    lines.append(f"Total similes found: **{len(findings)}**")
    lines.append("")

    if not findings:
        lines.append("No `like` similes were detected.")
    else:
        for idx, hit in enumerate(findings, start=1):
            lines.extend(
                [
                    f"## {idx}. {hit.label}",
                    f"- Source: `{hit.source}`",
                    f"- Sentence ID: `{hit.sentence_id}`",
                    f"- Sentence: {hit.sentence}",
                    f"- Simile: **{hit.left}** ↔ **{hit.right}**",
                    f"- Semantic gain: `{hit.semantic_gain:.3f}`",
                    (
                        "- Concreteness (left/right/Δ): "
                        f"`{hit.left_concreteness:.2f} / {hit.right_concreteness:.2f} / {hit.concreteness_delta:+.2f}`"
                    ),
                    f"- Right phrase Zipf frequency: `{hit.right_phrase_zipf:.2f}`",
                    "- Upgrade path:",
                ]
            )
            lines.extend([f"  - {action}" for action in hit.suggestions])
            lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def findings_to_json(findings: list[SimileFinding]) -> list[dict[str, Any]]:
    return [
        {
            "source": hit.source,
            "sentence_id": hit.sentence_id,
            "sentence": hit.sentence,
            "left": hit.left,
            "right": hit.right,
            "semantic_gain": round(hit.semantic_gain, 6),
            "left_concreteness": round(hit.left_concreteness, 6),
            "right_concreteness": round(hit.right_concreteness, 6),
            "concreteness_delta": round(hit.concreteness_delta, 6),
            "right_phrase_zipf": round(hit.right_phrase_zipf, 6),
            "label": hit.label,
            "suggestions": hit.suggestions,
        }
        for hit in findings
    ]


def auto_preprocessed_path() -> Path | None:
    candidate = Path("/preprocessed/sentences.jsonl")
    return candidate if candidate.exists() else None


def main() -> None:
    args = parse_args()

    try:
        nlp = spacy.load(args.spacy_model)
    except OSError as exc:
        raise SystemExit(
            f"spaCy model not available: {args.spacy_model}. "
            "Install it with: python -m spacy download en_core_web_sm"
        ) from exc

    try:
        embedder = SentenceTransformer(args.embedding_model)
    except Exception as exc:
        raise SystemExit(
            f"Embedding model unavailable: {args.embedding_model}. "
            "If running offline, pre-download the model cache before execution."
        ) from exc

    if args.preprocessed:
        sentences_path = resolve_sentences_path(args.preprocessed)
        sentence_items = read_preprocessed_sentences(sentences_path)
    else:
        auto_path = auto_preprocessed_path()
        if auto_path is not None:
            sentence_items = read_preprocessed_sentences(auto_path)
        elif args.input_path:
            sentence_items = load_text_sentences(args.input_path, nlp)
        else:
            raise SystemExit("Provide input_path or --preprocessed (or ensure /preprocessed/sentences.jsonl exists).")

    concreteness_table = load_concreteness(args.concreteness_csv)
    cliches = load_cliches(args.cliche_file)

    findings = analyze_sentences(
        sentences=sentence_items,
        nlp=nlp,
        embedder=embedder,
        concreteness_table=concreteness_table,
        cliches=cliches,
        empty_gain_threshold=args.empty_gain_threshold,
        potential_gain_threshold=args.potential_gain_threshold,
    )

    write_markdown_report(args.output_file, findings)
    print(f"Wrote report: {args.output_file}")

    if args.output_json:
        args.output_json.write_text(
            json.dumps(findings_to_json(findings), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote JSON: {args.output_json}")


if __name__ == "__main__":
    main()
