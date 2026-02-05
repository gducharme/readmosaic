#!/usr/bin/env python3
"""Conceptual Theme Mapper (CTM).

Usage examples:
  python scripts/theme_mapper.py path/to/manuscript.txt --num-topics 10
  python scripts/theme_mapper.py path/to/manuscript.txt --chunk-mode chapter --chapter-pattern "^CHAPTER\\s+\\d+"
  python scripts/theme_mapper.py path/to/manuscript.txt --chunk-size 5000 --output-dir outputs

Run with --help to see all options.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pyLDAvis
import pyLDAvis.gensim_models
import spacy
from gensim import corpora
from gensim.models import CoherenceModel, LdaModel
from sklearn.feature_extraction.text import CountVectorizer

from schema_validator import validate_payload

CHUNK_MODE_WORDS = "words"
CHUNK_MODE_CHAPTER = "chapter"
WORD_TOKEN_RE = re.compile(r"\b\w+\b")


@dataclass
class Chunk:
    index: int
    label: str
    text: str
    word_count: int
    paragraph_range: Optional[Tuple[str, str]] = None
    start_token_index: Optional[int] = None
    end_token_index: Optional[int] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover thematic clusters across manuscript chunks using LDA.",
    )
    parser.add_argument("input_file", type=Path, help="Path to a .txt or .md manuscript.")
    parser.add_argument(
        "--num-topics",
        type=int,
        default=10,
        help="Number of dominant themes to discover.",
    )
    parser.add_argument(
        "--chunk-mode",
        choices=[CHUNK_MODE_WORDS, CHUNK_MODE_CHAPTER],
        default=CHUNK_MODE_WORDS,
        help="Chunk by fixed word count or chapter headings.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Word count per chunk when chunk-mode is 'words'.",
    )
    parser.add_argument(
        "--chapter-pattern",
        type=str,
        default=r"^chapter\s+\d+",
        help="Regex (case-insensitive) to identify chapter headings.",
    )
    parser.add_argument(
        "--spacy-model",
        type=str,
        default="en_core_web_sm",
        help="spaCy model to use for lemmatization and POS tagging.",
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        default=None,
        help="Directory containing manuscript_tokens.json for paragraph ranges.",
    )
    parser.add_argument(
        "--passes",
        type=int,
        default=10,
        help="Number of LDA training passes.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of LDA iterations.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducible topic discovery.",
    )
    parser.add_argument(
        "--top-words",
        type=int,
        default=10,
        help="Number of defining words to display per topic.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory to write reports and visualizations.",
    )
    parser.add_argument(
        "--heatmap-file",
        type=str,
        default="topic_heatmap.png",
        help="Filename for the topic distribution heatmap.",
    )
    parser.add_argument(
        "--distribution-csv",
        type=str,
        default="topic_distribution.csv",
        help="Filename for the topic distribution CSV table.",
    )
    parser.add_argument(
        "--pyldavis-html",
        type=str,
        default="topic_map.html",
        help="Filename for the interactive pyLDAvis HTML output.",
    )
    parser.add_argument(
        "--topic-shift-json",
        type=Path,
        default=None,
        help="Optional JSON output (edits.schema.json) for abrupt topic shifts.",
    )
    parser.add_argument(
        "--topic-shift-threshold",
        type=float,
        default=0.45,
        help="L1 delta threshold between adjacent chunks for shift detection.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.input_file.exists():
        raise SystemExit(f"Input file not found: {args.input_file}")
    if args.num_topics < 2:
        raise SystemExit("num-topics must be at least 2")
    if args.chunk_mode == CHUNK_MODE_WORDS and args.chunk_size < 100:
        raise SystemExit("chunk-size must be at least 100 words")
    if args.topic_shift_json and not args.preprocessing:
        raise SystemExit("--topic-shift-json requires --preprocessing for paragraph ranges")


def count_words(text: str) -> int:
    return len(WORD_TOKEN_RE.findall(text))


def load_spacy(model_name: str) -> spacy.language.Language:
    try:
        return spacy.load(model_name, disable=["ner"])
    except OSError as exc:
        raise SystemExit(
            f"spaCy model '{model_name}' not found. "
            "Install it with: python -m spacy download en_core_web_sm"
        ) from exc


def split_into_word_chunks(text: str, chunk_size: int) -> List[Chunk]:
    words = WORD_TOKEN_RE.findall(text)
    chunks: List[Chunk] = []
    for idx in range(0, len(words), chunk_size):
        chunk_words = words[idx : idx + chunk_size]
        if not chunk_words:
            continue
        label = f"Chunk {len(chunks) + 1}"
        chunk_text = " ".join(chunk_words)
        chunks.append(
            Chunk(
                index=len(chunks),
                label=label,
                text=chunk_text,
                word_count=len(chunk_words),
            )
        )
    return chunks


def split_into_chapters(text: str, pattern: str) -> List[Chunk]:
    lines = text.splitlines()
    regex = re.compile(pattern, flags=re.IGNORECASE)
    chunks: List[Chunk] = []
    current_lines: List[str] = []
    current_label = "Chapter 1"
    for line in lines:
        if regex.match(line.strip()):
            if current_lines:
                chunk_text = "\n".join(current_lines).strip()
                chunks.append(
                    Chunk(
                        index=len(chunks),
                        label=current_label,
                        text=chunk_text,
                        word_count=count_words(chunk_text),
                    )
                )
                current_lines = []
            current_label = line.strip()
        current_lines.append(line)
    if current_lines:
        chunk_text = "\n".join(current_lines).strip()
        chunks.append(
            Chunk(
                index=len(chunks),
                label=current_label,
                text=chunk_text,
                word_count=count_words(chunk_text),
            )
        )
    return chunks


def build_chunks(text: str, args: argparse.Namespace) -> List[Chunk]:
    if args.chunk_mode == CHUNK_MODE_CHAPTER:
        chunks = split_into_chapters(text, args.chapter_pattern)
        if len(chunks) > 1:
            return chunks
    return split_into_word_chunks(text, args.chunk_size)


def load_manuscript_tokens(preprocessing_dir: Path) -> dict:
    tokens_path = preprocessing_dir / "manuscript_tokens.json"
    if not tokens_path.exists():
        raise SystemExit(f"Missing manuscript_tokens.json in {preprocessing_dir}")
    return json.loads(tokens_path.read_text(encoding="utf-8"))


def build_word_token_index(manuscript_tokens: dict) -> List[dict]:
    tokens: List[dict] = []
    for paragraph in manuscript_tokens.get("paragraphs", []):
        paragraph_id = paragraph["paragraph_id"]
        for token in paragraph.get("tokens", []):
            text = token.get("text", "")
            if not WORD_TOKEN_RE.fullmatch(text):
                continue
            tokens.append(
                {
                    "paragraph_id": paragraph_id,
                    "token_id": token.get("token_id"),
                    "start_char": token.get("start_char"),
                    "end_char": token.get("end_char"),
                    "global_index": token["global_index"],
                }
            )
    tokens.sort(key=lambda item: item["global_index"])
    return tokens


def map_chunks_to_paragraph_ranges(chunks: Sequence[Chunk], token_index: Sequence[dict]) -> None:
    if not token_index:
        return
    cursor = 0
    token_count = len(token_index)
    for chunk in chunks:
        if chunk.word_count <= 0:
            continue
        if cursor >= token_count:
            break
        start = cursor
        end = min(cursor + chunk.word_count - 1, token_count - 1)
        start_paragraph = token_index[start]["paragraph_id"]
        end_paragraph = token_index[end]["paragraph_id"]
        chunk.paragraph_range = (start_paragraph, end_paragraph)
        chunk.start_token_index = start
        chunk.end_token_index = end
        cursor += chunk.word_count


def format_paragraph_range(paragraph_range: Optional[Tuple[str, str]]) -> Optional[str]:
    if not paragraph_range:
        return None
    start, end = paragraph_range
    if start == end:
        return start
    return f"{start}..{end}"


def build_chunk_location(
    token_index: Sequence[dict],
    start: Optional[int],
    end: Optional[int],
) -> Optional[dict]:
    if not token_index or start is None or end is None:
        return None
    safe_start = max(min(start, len(token_index) - 1), 0)
    safe_end = max(min(end, len(token_index) - 1), safe_start)
    anchor_index = min(safe_start + (safe_end - safe_start) // 2, len(token_index) - 1)
    anchor = token_index[anchor_index]
    paragraph_id = anchor["paragraph_id"]
    window_tokens = [
        token
        for token in token_index[safe_start : safe_end + 1]
        if token["paragraph_id"] == paragraph_id
    ]
    if not window_tokens:
        window_tokens = [anchor]
    token_ids = [token["token_id"] for token in window_tokens if token.get("token_id")]
    char_starts = [token["start_char"] for token in window_tokens if token.get("start_char") is not None]
    char_ends = [token["end_char"] for token in window_tokens if token.get("end_char") is not None]
    char_range = None
    if char_starts and char_ends:
        char_range = {"start": min(char_starts), "end": max(char_ends)}
    location = {"paragraph_id": paragraph_id}
    if token_ids:
        location["token_ids"] = token_ids
    if char_range:
        location["char_range"] = char_range
    return location


def preprocess_chunks(
    chunks: Sequence[Chunk], nlp: spacy.language.Language
) -> List[List[str]]:
    processed: List[List[str]] = []
    for chunk in chunks:
        doc = nlp(chunk.text)
        tokens = [
            token.lemma_.lower()
            for token in doc
            if token.pos_ in {"NOUN", "ADJ"}
            and token.is_alpha
            and not token.is_stop
        ]
        processed.append(tokens)
    return processed


def vectorize_chunks(tokens_per_chunk: Sequence[Sequence[str]]) -> CountVectorizer:
    vectorizer = CountVectorizer(
        tokenizer=lambda x: x,
        preprocessor=lambda x: x,
        token_pattern=None,
    )
    vectorizer.fit(tokens_per_chunk)
    return vectorizer


def build_topic_distribution(
    model: LdaModel, corpus: Sequence[Iterable[tuple[int, int]]], num_topics: int
) -> np.ndarray:
    distribution = np.zeros((len(corpus), num_topics))
    for idx, doc in enumerate(corpus):
        for topic_id, weight in model.get_document_topics(doc):
            distribution[idx, topic_id] = weight
    return distribution


def write_distribution_csv(
    output_path: Path, chunks: Sequence[Chunk], distribution: np.ndarray
) -> None:
    headers = ["chunk_index", "chunk_label"] + [
        f"topic_{idx}" for idx in range(distribution.shape[1])
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for idx, chunk in enumerate(chunks):
            values = [chunk.index, chunk.label] + [
                f"{weight:.6f}" for weight in distribution[idx]
            ]
            writer.writerow(values)


def plot_heatmap(output_path: Path, distribution: np.ndarray, chunks: Sequence[Chunk]) -> None:
    fig, ax = plt.subplots(figsize=(10, max(4, len(chunks) * 0.4)))
    heatmap = ax.imshow(distribution, aspect="auto", cmap="magma")
    ax.set_xlabel("Topic")
    ax.set_ylabel("Chunk")
    ax.set_xticks(range(distribution.shape[1]))
    ax.set_xticklabels([str(idx) for idx in range(distribution.shape[1])])
    ax.set_yticks(range(len(chunks)))
    ax.set_yticklabels([chunk.label for chunk in chunks])
    fig.colorbar(heatmap, ax=ax, label="Topic Weight")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def resolve_output_path(output_dir: Path, path: Optional[Path]) -> Optional[Path]:
    if path is None:
        return None
    if path.is_absolute():
        return path
    return output_dir / path


def write_topic_shift_json(
    output_path: Path,
    chunks: Sequence[Chunk],
    distribution: np.ndarray,
    threshold: float,
    manuscript_id: str,
    token_index: Optional[Sequence[dict]] = None,
) -> None:
    items: List[dict] = []
    for idx in range(1, len(chunks)):
        previous = distribution[idx - 1]
        current = distribution[idx]
        deltas = np.abs(current - previous)
        delta_value = float(deltas.sum())
        if delta_value < threshold:
            continue
        paragraph_range = format_paragraph_range(chunks[idx].paragraph_range)
        location_data = build_chunk_location(
            token_index or [],
            chunks[idx].start_token_index,
            chunks[idx].end_token_index,
        )
        paragraph_id_value = paragraph_range or (location_data["paragraph_id"] if location_data else None)
        if not paragraph_id_value:
            continue
        location: dict[str, object] = {"paragraph_id": paragraph_id_value}
        if location_data:
            if location_data.get("token_ids"):
                location["token_ids"] = location_data["token_ids"]
            if location_data.get("char_range"):
                location["char_range"] = location_data["char_range"]
        items.append(
            {
                "issue_id": str(uuid.uuid4()),
                "type": "topic_shift",
                "location": location,
                "evidence": {
                    "summary": (
                        f"Abrupt topic shift detected between {chunks[idx - 1].label} "
                        f"and {chunks[idx].label} (delta={delta_value:.3f})."
                    ),
                    "detector": "theme_mapper",
                    "signals": [
                        {
                            "name": "topic_weights_previous",
                            "value": [round(float(weight), 6) for weight in previous],
                            "units": "weight",
                        },
                        {
                            "name": "topic_weights_current",
                            "value": [round(float(weight), 6) for weight in current],
                            "units": "weight",
                        },
                        {
                            "name": "topic_weight_deltas",
                            "value": [round(float(delta), 6) for delta in deltas],
                            "units": "weight_delta",
                        },
                        {
                            "name": "delta_l1",
                            "value": round(delta_value, 6),
                        },
                        {
                            "name": "delta_threshold",
                            "value": threshold,
                        },
                    ],
                },
            }
        )

    if not items:
        print("No topic shifts exceeded threshold for JSON output.")
        return

    payload = {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }
    validate_payload(payload, "edits.schema.json", "theme mapper edits payload")
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Topic shift JSON saved to: {output_path}")


def main() -> None:
    args = parse_args()
    validate_args(args)

    raw_text = args.input_file.read_text(encoding="utf-8")
    chunks = build_chunks(raw_text, args)
    if not chunks:
        raise SystemExit("No chunks were created. Check your input or chunk settings.")

    manuscript_tokens = None
    token_index: Optional[List[dict]] = None
    if args.preprocessing:
        manuscript_tokens = load_manuscript_tokens(args.preprocessing)
        token_index = build_word_token_index(manuscript_tokens)
        map_chunks_to_paragraph_ranges(chunks, token_index)

    nlp = load_spacy(args.spacy_model)
    tokens_per_chunk = preprocess_chunks(chunks, nlp)

    if not any(tokens_per_chunk):
        raise SystemExit("No tokens left after preprocessing. Adjust filters or input.")

    dictionary = corpora.Dictionary(tokens_per_chunk)
    corpus = [dictionary.doc2bow(tokens) for tokens in tokens_per_chunk]

    vectorizer = vectorize_chunks(tokens_per_chunk)
    count_matrix = vectorizer.transform(tokens_per_chunk)

    lda = LdaModel(
        corpus=corpus,
        id2word=dictionary,
        num_topics=args.num_topics,
        random_state=args.random_state,
        passes=args.passes,
        iterations=args.iterations,
    )

    coherence_model = CoherenceModel(
        model=lda, texts=tokens_per_chunk, dictionary=dictionary, coherence="c_v"
    )
    coherence_score = coherence_model.get_coherence()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("\nCorpus Summary:")
    print(f"Chunks analyzed: {len(chunks)}")
    print(f"Vocabulary size: {count_matrix.shape[1]}")

    print("\nDominant Topics:")
    for topic_id, topic_terms in lda.show_topics(num_topics=args.num_topics, num_words=args.top_words):
        print(f"Topic {topic_id}: {topic_terms}")

    print(f"\nCoherence Score (c_v): {coherence_score:.3f}")

    distribution = build_topic_distribution(lda, corpus, args.num_topics)
    csv_path = args.output_dir / args.distribution_csv
    write_distribution_csv(csv_path, chunks, distribution)
    print(f"Topic distribution table written to: {csv_path}")

    heatmap_path = args.output_dir / args.heatmap_file
    plot_heatmap(heatmap_path, distribution, chunks)
    print(f"Heatmap saved to: {heatmap_path}")

    html_path = args.output_dir / args.pyldavis_html
    vis = pyLDAvis.gensim_models.prepare(lda, corpus, dictionary)
    pyLDAvis.save_html(vis, str(html_path))
    print(f"pyLDAvis visualization saved to: {html_path}")

    topic_shift_path = resolve_output_path(args.output_dir, args.topic_shift_json)
    if topic_shift_path:
        manuscript_id = (
            manuscript_tokens["manuscript_id"]
            if manuscript_tokens and "manuscript_id" in manuscript_tokens
            else args.input_file.stem
        )
        write_topic_shift_json(
            topic_shift_path,
            chunks,
            distribution,
            args.topic_shift_threshold,
            manuscript_id,
            token_index,
        )


if __name__ == "__main__":
    main()
