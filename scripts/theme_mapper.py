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
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pyLDAvis
import pyLDAvis.gensim_models
import spacy
from gensim import corpora
from gensim.models import CoherenceModel, LdaModel
from sklearn.feature_extraction.text import CountVectorizer


CHUNK_MODE_WORDS = "words"
CHUNK_MODE_CHAPTER = "chapter"


@dataclass
class Chunk:
    index: int
    label: str
    text: str


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
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.input_file.exists():
        raise SystemExit(f"Input file not found: {args.input_file}")
    if args.num_topics < 2:
        raise SystemExit("num-topics must be at least 2")
    if args.chunk_mode == CHUNK_MODE_WORDS and args.chunk_size < 100:
        raise SystemExit("chunk-size must be at least 100 words")


def load_spacy(model_name: str) -> spacy.language.Language:
    try:
        return spacy.load(model_name, disable=["ner"])
    except OSError as exc:
        raise SystemExit(
            f"spaCy model '{model_name}' not found. "
            "Install it with: python -m spacy download en_core_web_sm"
        ) from exc


def split_into_word_chunks(text: str, chunk_size: int) -> List[Chunk]:
    words = re.findall(r"\b\w+\b", text)
    chunks: List[Chunk] = []
    for idx in range(0, len(words), chunk_size):
        chunk_words = words[idx : idx + chunk_size]
        if not chunk_words:
            continue
        label = f"Chunk {len(chunks) + 1}"
        chunk_text = " ".join(chunk_words)
        chunks.append(Chunk(index=len(chunks), label=label, text=chunk_text))
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
                chunks.append(
                    Chunk(
                        index=len(chunks),
                        label=current_label,
                        text="\n".join(current_lines).strip(),
                    )
                )
                current_lines = []
            current_label = line.strip()
        current_lines.append(line)
    if current_lines:
        chunks.append(
            Chunk(
                index=len(chunks),
                label=current_label,
                text="\n".join(current_lines).strip(),
            )
        )
    return chunks


def build_chunks(text: str, args: argparse.Namespace) -> List[Chunk]:
    if args.chunk_mode == CHUNK_MODE_CHAPTER:
        chunks = split_into_chapters(text, args.chapter_pattern)
        if len(chunks) > 1:
            return chunks
    return split_into_word_chunks(text, args.chunk_size)


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
    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(headers) + "\n")
        for idx, chunk in enumerate(chunks):
            values = [
                str(chunk.index),
                f'"{chunk.label.replace(\'"\', \'""\')}"',
            ] + [f"{weight:.6f}" for weight in distribution[idx]]
            handle.write(",".join(values) + "\n")


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


def main() -> None:
    args = parse_args()
    validate_args(args)

    raw_text = args.input_file.read_text(encoding="utf-8")
    chunks = build_chunks(raw_text, args)
    if not chunks:
        raise SystemExit("No chunks were created. Check your input or chunk settings.")

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


if __name__ == "__main__":
    main()
