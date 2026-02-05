#!/usr/bin/env python3
"""Semantic repetition analyzer.

Usage examples:
  python scripts/analyzer.py path/to/file.md
  python scripts/analyzer.py path/to/file.txt --threshold 0.9 --min-length 30
  python scripts/analyzer.py path/to/file.md --context-window section --top-n 15
  python scripts/analyzer.py path/to/file.md --output-format csv --output-file echoes.csv

Run with --help to see all options.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import numpy as np
from rich.console import Console
from rich.table import Table
from sentence_transformers import SentenceTransformer, util
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
import torch

import nltk


@dataclass
class SentenceRecord:
    text: str
    line_number: int
    paragraph_index: int
    section_index: int
    start_char: int
    end_char: int


@dataclass
class SentenceLocation:
    paragraph_id: str
    token_ids: List[str]
    start_char: int
    end_char: int


@dataclass
class ParagraphRecord:
    text: str
    line_numbers: List[int]
    paragraph_index: int
    section_index: int
    line_offsets: List[int]


def ensure_nltk() -> None:
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)


def clean_markdown_line(line: str) -> str:
    line = re.sub(r"^\s{0,3}#{1,6}\s+", "", line)
    line = re.sub(r"^\s*>\s?", "", line)
    line = re.sub(r"^\s*[-*+]\s+", "", line)
    line = re.sub(r"^\s*\d+\.\s+", "", line)
    line = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", line)
    line = re.sub(r"\[([^\]]+)\]\([^\)]*\)", r"\1", line)
    line = re.sub(r"`[^`]+`", "", line)
    line = re.sub(r"[*_]{1,3}", "", line)
    line = re.sub(r"\s+", " ", line)
    return line.strip()


def strip_code_blocks(text: str) -> str:
    return re.sub(r"```.*?```", "", text, flags=re.DOTALL)


def parse_paragraphs(raw_text: str) -> List[ParagraphRecord]:
    raw_text = strip_code_blocks(raw_text)
    lines = raw_text.splitlines()

    section_index = 0
    cleaned_lines = []
    for idx, raw_line in enumerate(lines, start=1):
        if re.match(r"^\s{0,3}#{1,6}\s+", raw_line):
            section_index += 1
        cleaned_line = clean_markdown_line(raw_line)
        cleaned_lines.append((idx, section_index, cleaned_line))

    paragraphs: List[ParagraphRecord] = []
    buffer: List[tuple[int, int, str]] = []
    paragraph_index = 0

    def flush_buffer() -> None:
        nonlocal paragraph_index
        if not buffer:
            return
        paragraph_index += 1
        section = buffer[0][1]
        line_numbers = [item[0] for item in buffer]
        line_offsets: List[int] = []
        running = 0
        for _, _, line_text in buffer:
            line_offsets.append(running)
            running += len(line_text) + 1
        paragraph_text = "\n".join(item[2] for item in buffer)
        paragraphs.append(
            ParagraphRecord(
                text=paragraph_text,
                line_numbers=line_numbers,
                paragraph_index=paragraph_index,
                section_index=section,
                line_offsets=line_offsets,
            )
        )

    for item in cleaned_lines:
        if item[2].strip() == "":
            flush_buffer()
            buffer = []
            continue
        buffer.append(item)
    flush_buffer()

    return paragraphs


def extract_sentences(paragraphs: Sequence[ParagraphRecord], min_length: int) -> List[SentenceRecord]:
    ensure_nltk()
    records: List[SentenceRecord] = []
    for paragraph in paragraphs:
        sentences = nltk.sent_tokenize(paragraph.text)
        search_start = 0
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) < min_length:
                continue
            position = paragraph.text.find(sentence, search_start)
            if position == -1:
                position = search_start
            search_start = position + len(sentence)
            sentence_start = position
            sentence_end = position + len(sentence)
            line_number = paragraph.line_numbers[0]
            for offset, ln in zip(paragraph.line_offsets, paragraph.line_numbers):
                if offset <= position:
                    line_number = ln
                else:
                    break
            records.append(
                SentenceRecord(
                    text=sentence,
                    line_number=line_number,
                    paragraph_index=paragraph.paragraph_index,
                    section_index=paragraph.section_index,
                    start_char=sentence_start,
                    end_char=sentence_end,
                )
            )
    return records


def build_groups(
    sentences: Sequence[SentenceRecord], context_window: str
) -> List[List[int]]:
    if context_window == "document":
        return [list(range(len(sentences)))]

    groups_map = {}
    for idx, sentence in enumerate(sentences):
        key = (
            sentence.paragraph_index
            if context_window == "paragraph"
            else sentence.section_index
        )
        groups_map.setdefault(key, []).append(idx)
    return list(groups_map.values())


def mine_pairs(
    embeddings: np.ndarray, groups: Iterable[List[int]]
) -> List[tuple[float, int, int]]:
    pairs: List[tuple[float, int, int]] = []
    for group in groups:
        if len(group) < 2:
            continue
        group_embeddings = embeddings[group]
        mined = util.paraphrase_mining_embeddings(group_embeddings)
        for score, i, j in mined:
            pairs.append((score, group[i], group[j]))
    return pairs


def redundancy_score(pairs: Iterable[tuple[float, int, int]], total: int, threshold: float) -> float:
    if total == 0:
        return 0.0
    echoed = set()
    for score, i, j in pairs:
        if score >= threshold:
            echoed.add(i)
            echoed.add(j)
    return (len(echoed) / total) * 100


def summarize_entropy(redundancy_pct: float) -> float:
    return max(0.0, 100.0 - redundancy_pct)


def cluster_sentences(
    embeddings: np.ndarray,
    sentences: Sequence[SentenceRecord],
    threshold: float,
) -> dict[int, List[int]]:
    if len(sentences) < 2:
        return {}
    similarity = cosine_similarity(embeddings)
    distance = 1 - similarity
    clustering = AgglomerativeClustering(
        metric="precomputed",
        linkage="average",
        distance_threshold=1 - threshold,
        n_clusters=None,
    )
    labels = clustering.fit_predict(distance)
    clusters: dict[int, List[int]] = {}
    for idx, label in enumerate(labels):
        clusters.setdefault(label, []).append(idx)
    return {label: members for label, members in clusters.items() if len(members) > 1}


def render_text_report(
    sentences: Sequence[SentenceRecord],
    pairs: Sequence[tuple[float, int, int]],
    threshold: float,
    top_n: int,
    redundancy_pct: float,
    entropy_pct: float,
    clusters: dict[int, List[int]],
) -> None:
    console = Console()
    console.print("\n[bold]Semantic Echo Analysis[/bold]")
    console.print(f"Sentences analyzed: {len(sentences)}")
    console.print(f"Redundancy score (>= {threshold:.2f}): {redundancy_pct:.2f}%")
    console.print(f"Semantic entropy: {entropy_pct:.2f}%")

    table = Table(title=f"Top {top_n} Echoes")
    table.add_column("Score", justify="right")
    table.add_column("Line A")
    table.add_column("Sentence A", overflow="fold")
    table.add_column("Line B")
    table.add_column("Sentence B", overflow="fold")

    top_pairs = [pair for pair in pairs if pair[0] >= threshold]
    top_pairs.sort(key=lambda item: item[0], reverse=True)
    for score, i, j in top_pairs[:top_n]:
        table.add_row(
            f"{score:.3f}",
            str(sentences[i].line_number),
            sentences[i].text,
            str(sentences[j].line_number),
            sentences[j].text,
        )
    console.print(table)

    if clusters:
        cluster_table = Table(title="Conceptual Clusters")
        cluster_table.add_column("Cluster")
        cluster_table.add_column("Sentence Count", justify="right")
        cluster_table.add_column("Example Sentences", overflow="fold")
        for label, members in sorted(clusters.items(), key=lambda item: len(item[1]), reverse=True):
            example = " | ".join(sentences[idx].text for idx in members[:3])
            cluster_table.add_row(str(label), str(len(members)), example)
        console.print(cluster_table)


def write_csv_report(
    output_path: Path,
    sentences: Sequence[SentenceRecord],
    pairs: Sequence[tuple[float, int, int]],
    threshold: float,
    top_n: int,
) -> None:
    top_pairs = [pair for pair in pairs if pair[0] >= threshold]
    top_pairs.sort(key=lambda item: item[0], reverse=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["score", "line_a", "sentence_a", "line_b", "sentence_b"])
        for score, i, j in top_pairs[:top_n]:
            writer.writerow(
                [
                    f"{score:.6f}",
                    sentences[i].line_number,
                    sentences[i].text,
                    sentences[j].line_number,
                    sentences[j].text,
                ]
            )


def load_tokens_artifact(preprocessing_dir: Path) -> dict:
    artifact_path = preprocessing_dir / "manuscript_tokens.json"
    if not artifact_path.exists():
        raise SystemExit(f"Tokens artifact not found: {artifact_path}")
    return json.loads(artifact_path.read_text(encoding="utf-8"))


def build_paragraph_map(tokens_artifact: dict) -> dict[int, dict]:
    paragraph_map: dict[int, dict] = {}
    for paragraph in tokens_artifact.get("paragraphs", []):
        order = paragraph.get("order")
        if order is None:
            continue
        paragraph_map[int(order)] = paragraph
    return paragraph_map


def map_sentence_location(
    sentence: SentenceRecord, paragraph_map: dict[int, dict]
) -> Optional[SentenceLocation]:
    paragraph = paragraph_map.get(sentence.paragraph_index - 1)
    if not paragraph:
        return None
    start = sentence.start_char
    end = sentence.end_char
    token_ids: List[str] = []
    for token in paragraph.get("tokens", []):
        token_start = token.get("start_char", 0)
        token_end = token.get("end_char", 0)
        if token_end <= start or token_start >= end:
            continue
        token_id = token.get("token_id")
        if token_id:
            token_ids.append(token_id)
    return SentenceLocation(
        paragraph_id=paragraph["paragraph_id"],
        token_ids=token_ids,
        start_char=start,
        end_char=end,
    )


def write_json_report(
    output_path: Path,
    sentences: Sequence[SentenceRecord],
    pairs: Sequence[tuple[float, int, int]],
    threshold: float,
    context_window: str,
    preprocessing_dir: Path,
) -> None:
    tokens_artifact = load_tokens_artifact(preprocessing_dir)
    paragraph_map = build_paragraph_map(tokens_artifact)
    manuscript_id = tokens_artifact.get("manuscript_id", "unknown")

    echo_pairs = [pair for pair in pairs if pair[0] >= threshold]
    echo_pairs.sort(key=lambda item: item[0], reverse=True)

    items: List[dict] = []
    for idx, (score, i, j) in enumerate(echo_pairs, start=1):
        sentence_a = sentences[i]
        sentence_b = sentences[j]
        location_a = map_sentence_location(sentence_a, paragraph_map)
        location_b = map_sentence_location(sentence_b, paragraph_map)
        if not location_a or not location_b:
            continue
        items.append(
            {
                "issue_id": f"semantic-echo-{idx:04d}",
                "type": "semantic_repetition",
                "location": {
                    "paragraph_id": location_a.paragraph_id,
                    "token_ids": location_a.token_ids,
                    "char_range": {"start": location_a.start_char, "end": location_a.end_char},
                    "anchor_text": sentence_a.text,
                },
                "evidence": {
                    "summary": "Semantic repetition detected between sentences.",
                    "signals": [
                        {"name": "similarity_score", "value": round(float(score), 6)},
                        {"name": "context_window", "value": context_window},
                    ],
                    "detector": "semantic_echo_analyzer",
                    "primary_sentence": sentence_a.text,
                    "echo_sentence": sentence_b.text,
                },
                "extensions": {
                    "echo_location": {
                        "paragraph_id": location_b.paragraph_id,
                        "token_ids": location_b.token_ids,
                        "char_range": {"start": location_b.start_char, "end": location_b.end_char},
                        "anchor_text": sentence_b.text,
                    }
                },
            }
        )

    if not items:
        raise SystemExit("No semantic echoes met the threshold for JSON output.")

    payload = {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze a text or Markdown file for semantic repetition.",
    )
    parser.add_argument("input_file", type=Path, help="Path to a .txt or .md file.")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="Similarity threshold for echoes (0.0 to 1.0).",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=20,
        help="Minimum sentence length to include in analysis.",
    )
    parser.add_argument(
        "--output-format",
        choices=["text", "csv", "json"],
        default="text",
        help="Output format for the report.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Output path for CSV/JSON reports.",
    )
    parser.add_argument(
        "--output-edits",
        type=Path,
        default=None,
        help="Optional output path for edits.schema.json payload.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top echo pairs to report.",
    )
    parser.add_argument(
        "--context-window",
        choices=["document", "paragraph", "section"],
        default="document",
        help="Scope for echo detection.",
    )
    parser.add_argument(
        "--model",
        choices=["all-MiniLM-L6-v2", "all-mpnet-base-v2"],
        default="all-MiniLM-L6-v2",
        help="SentenceTransformer model to use.",
    )
    parser.add_argument(
        "--clusters",
        action="store_true",
        help="Enable conceptual clustering summary.",
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        help="Directory containing manuscript_tokens.json for schema-aligned spans.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.input_file.exists():
        raise SystemExit(f"Input file not found: {args.input_file}")
    if args.input_file.suffix.lower() not in {".txt", ".md"}:
        raise SystemExit("Input file must be .txt or .md")
    if not 0.0 <= args.threshold <= 1.0:
        raise SystemExit("Threshold must be between 0.0 and 1.0")
    if args.output_format == "json" and not args.preprocessing:
        raise SystemExit("--preprocessing is required for JSON output.")
    if args.output_edits and not args.preprocessing:
        raise SystemExit("--output-edits requires --preprocessing for token mapping.")
    if args.preprocessing and not args.preprocessing.exists():
        raise SystemExit(f"Preprocessing directory not found: {args.preprocessing}")


def main() -> None:
    args = parse_args()
    validate_args(args)

    raw_text = args.input_file.read_text(encoding="utf-8")
    paragraphs = parse_paragraphs(raw_text)
    sentences = extract_sentences(paragraphs, args.min_length)

    if not sentences:
        raise SystemExit("No sentences found to analyze.")

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    model = SentenceTransformer(args.model, device=device)
    embeddings = model.encode(
        [sentence.text for sentence in sentences],
        convert_to_tensor=True,
        show_progress_bar=True,
    )

    groups = build_groups(sentences, args.context_window)
    pairs = mine_pairs(embeddings, groups)

    redundancy_pct = redundancy_score(pairs, len(sentences), args.threshold)
    entropy_pct = summarize_entropy(redundancy_pct)

    clusters: dict[int, List[int]] = {}
    if args.clusters:
        clusters = cluster_sentences(embeddings.cpu().numpy(), sentences, args.threshold)

    if args.output_format == "csv":
        output_path = args.output_file or Path("echo_report.csv")
        write_csv_report(output_path, sentences, pairs, args.threshold, args.top_n)
        Console().print(f"CSV report written to {output_path}")
    elif args.output_format == "json":
        output_path = args.output_file or Path("echo_report.json")
        write_json_report(
            output_path,
            sentences,
            pairs,
            args.threshold,
            args.context_window,
            args.preprocessing,
        )
        Console().print(f"JSON report written to {output_path}")
    else:
        render_text_report(
            sentences,
            pairs,
            args.threshold,
            args.top_n,
            redundancy_pct,
            entropy_pct,
            clusters,
        )

    if args.output_edits:
        write_json_report(
            args.output_edits,
            sentences,
            pairs,
            args.threshold,
            args.context_window,
            args.preprocessing,
        )
        Console().print(f"Edits payload written to {args.output_edits}")


if __name__ == "__main__":
    main()
