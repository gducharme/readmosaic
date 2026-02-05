#!/usr/bin/env python3
"""
Linguistic Pattern Extractor (LPE)

Extracts phrasal verbs, action chains, descriptive pairs, and adverbial intent
patterns from markdown or text manuscripts using spaCy's DependencyMatcher.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import DefaultDict, Iterable, List, Tuple

import pandas as pd
import spacy
from rich.console import Console
from rich.table import Table
from spacy.matcher import DependencyMatcher

PATTERN_TYPES = {
    "phrasal_verbs": "PHRASAL_VERB",
    "action_chains": "ACTION_CHAIN",
    "descriptive_pairs": "DESCRIPTIVE_PAIR",
    "adverbial_intent": "ADVERBIAL_INTENT",
}


@dataclass
class PatternStats:
    counts: DefaultDict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    contexts: DefaultDict[str, DefaultDict[str, List[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(list))
    )
    token_count: int = 0

    def add(self, pattern_type: str, pattern: str, sentence: str, context_limit: int) -> None:
        self.counts[pattern_type][pattern] += 1
        if len(self.contexts[pattern_type][pattern]) < context_limit:
            if sentence not in self.contexts[pattern_type][pattern]:
                self.contexts[pattern_type][pattern].append(sentence)


@dataclass
class PatternOccurrence:
    pattern_type: str
    pattern: str
    sentence: str
    paragraph_id: str
    token_ids: List[str]
    char_start: int
    char_end: int
    anchor_text: str


@dataclass
class AnalysisResults:
    stats: PatternStats
    occurrences: List[PatternOccurrence] = field(default_factory=list)
    manuscript_id: str = "unknown"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract structural linguistic patterns to surface stylistic entropy and overused phrasing."
        ),
        epilog=(
            "Model download: run scripts/download_spacy_model.sh to install en_core_web_trf. "
            "Example: scripts/pattern_extractor.py manuscript.md --min-freq 3 --top-n 15"
        ),
    )
    parser.add_argument(
        "input_path",
        help="Path to a .txt/.md file or a directory containing multiple files.",
    )
    parser.add_argument(
        "--compare",
        help="Optional second file to compare against the primary input for variance analysis.",
    )
    parser.add_argument(
        "--min-freq",
        type=int,
        default=2,
        help="Only show patterns that appear at least this many times.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Maximum number of patterns to display per pattern type.",
    )
    parser.add_argument(
        "--pattern-type",
        choices=list(PATTERN_TYPES.keys()),
        action="append",
        help="Filter to a specific pattern type (repeatable).",
    )
    parser.add_argument(
        "--exclude-stopwords",
        action="store_true",
        help="Exclude patterns that include stop words to reduce noise.",
    )
    parser.add_argument(
        "--context-count",
        type=int,
        default=3,
        help="Number of sentence examples to show for each pattern.",
    )
    parser.add_argument(
        "--show-pattern",
        help="Show sentence contexts for a specific pattern lemma string (e.g., 'bend down').",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
        help="Number of processes to use with spaCy Language.pipe for speed.",
    )
    parser.add_argument(
        "--collision-threshold",
        type=float,
        default=0.15,
        help=(
            "Relative density difference threshold for flagging voice collision (0.15 = 15%%)."
        ),
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        help="Directory containing manuscript_tokens.json for schema-aligned spans.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional output path for edits.schema.json payload.",
    )
    parser.add_argument(
        "--output-edits",
        type=Path,
        help="Optional output path for edits.schema.json payload.",
    )
    parser.add_argument(
        "--aggregate-by-type",
        action="store_true",
        help="Emit one edits payload item per pattern type instead of per occurrence.",
    )
    return parser.parse_args()


def load_texts(path: Path) -> Tuple[List[str], List[str]]:
    if path.is_file():
        return [path.read_text(encoding="utf-8")], [path.name]
    texts: List[str] = []
    labels: List[str] = []
    for file_path in sorted(path.glob("**/*")):
        if file_path.suffix.lower() in {".txt", ".md"} and file_path.is_file():
            texts.append(file_path.read_text(encoding="utf-8"))
            labels.append(file_path.name)
    if not texts:
        raise FileNotFoundError("No .txt or .md files found in the provided directory.")
    return texts, labels


def build_matcher(nlp: spacy.Language) -> DependencyMatcher:
    matcher = DependencyMatcher(nlp.vocab)
    matcher.add(
        PATTERN_TYPES["phrasal_verbs"],
        [
            [
                {"RIGHT_ID": "verb", "RIGHT_ATTRS": {"POS": "VERB"}},
                {
                    "LEFT_ID": "verb",
                    "REL_OP": ">",
                    "RIGHT_ID": "particle",
                    "RIGHT_ATTRS": {"DEP": {"IN": ["prt", "compound:prt"]}},
                },
            ]
        ],
    )
    matcher.add(
        PATTERN_TYPES["action_chains"],
        [
            [
                {"RIGHT_ID": "verb", "RIGHT_ATTRS": {"POS": "VERB"}},
                {
                    "LEFT_ID": "verb",
                    "REL_OP": ">",
                    "RIGHT_ID": "object",
                    "RIGHT_ATTRS": {"DEP": {"IN": ["dobj", "obj"]}},
                },
            ]
        ],
    )
    matcher.add(
        PATTERN_TYPES["descriptive_pairs"],
        [
            [
                {"RIGHT_ID": "noun", "RIGHT_ATTRS": {"POS": {"IN": ["NOUN", "PROPN"]}}},
                {
                    "LEFT_ID": "noun",
                    "REL_OP": ">",
                    "RIGHT_ID": "adj",
                    "RIGHT_ATTRS": {"DEP": "amod"},
                },
            ]
        ],
    )
    matcher.add(
        PATTERN_TYPES["adverbial_intent"],
        [
            [
                {"RIGHT_ID": "verb", "RIGHT_ATTRS": {"POS": "VERB"}},
                {
                    "LEFT_ID": "verb",
                    "REL_OP": ">",
                    "RIGHT_ID": "adv",
                    "RIGHT_ATTRS": {"DEP": "advmod"},
                },
            ]
        ],
    )
    return matcher


def load_tokens_artifact(preprocessing_dir: Path) -> dict:
    artifact_path = preprocessing_dir / "manuscript_tokens.json"
    if not artifact_path.exists():
        raise SystemExit(f"Tokens artifact not found: {artifact_path}")
    return json.loads(artifact_path.read_text(encoding="utf-8"))


def map_span_to_tokens(paragraph: dict, start: int, end: int) -> List[str]:
    token_ids: List[str] = []
    for token in paragraph.get("tokens", []):
        token_start = token.get("start_char", 0)
        token_end = token.get("end_char", 0)
        if token_end <= start or token_start >= end:
            continue
        token_id = token.get("token_id")
        if token_id:
            token_ids.append(token_id)
    return token_ids


def normalize_tokens(tokens: Iterable[spacy.tokens.Token]) -> List[str]:
    return [token.lemma_.lower() for token in tokens]


def extract_patterns(
    doc: spacy.tokens.Doc,
    matcher: DependencyMatcher,
    stats: PatternStats,
    exclude_stopwords: bool,
    context_limit: int,
    pattern_filter: List[str] | None,
    paragraph: dict | None = None,
    occurrences: List[PatternOccurrence] | None = None,
) -> None:
    stats.token_count += sum(1 for token in doc if not token.is_space)
    matches = matcher(doc)
    for match_id, token_ids in matches:
        label = doc.vocab.strings[match_id]
        if pattern_filter and label not in pattern_filter:
            continue
        tokens = [doc[token_id] for token_id in token_ids]
        if exclude_stopwords and any(token.is_stop for token in tokens):
            continue
        if label == PATTERN_TYPES["phrasal_verbs"]:
            verb = tokens[0]
            particle = tokens[1]
            pattern_tokens = normalize_tokens([verb, particle])
        elif label == PATTERN_TYPES["action_chains"]:
            verb = tokens[0]
            obj = tokens[1]
            pattern_tokens = normalize_tokens([verb, obj])
        elif label == PATTERN_TYPES["descriptive_pairs"]:
            noun = tokens[0]
            adj = tokens[1]
            pattern_tokens = normalize_tokens([adj, noun])
        else:
            verb = tokens[0]
            adv = tokens[1]
            pattern_tokens = normalize_tokens([adv, verb])
        pattern = " ".join(pattern_tokens)
        stats.add(label, pattern, doc[token_ids[0]].sent.text.strip(), context_limit)
        if paragraph and occurrences is not None:
            char_start = min(token.idx for token in tokens)
            char_end = max(token.idx + len(token) for token in tokens)
            token_ids_mapped = map_span_to_tokens(paragraph, char_start, char_end)
            anchor_text = doc.text[char_start:char_end].strip()
            occurrences.append(
                PatternOccurrence(
                    pattern_type=label,
                    pattern=pattern,
                    sentence=doc[token_ids[0]].sent.text.strip(),
                    paragraph_id=paragraph["paragraph_id"],
                    token_ids=token_ids_mapped,
                    char_start=char_start,
                    char_end=char_end,
                    anchor_text=anchor_text,
                )
            )


def entropy_score(counts: Counter) -> float:
    total = sum(counts.values())
    if total == 0:
        return 0.0
    entropy = 0.0
    for count in counts.values():
        probability = count / total
        entropy -= probability * math.log2(probability)
    return entropy


def density_per_k(counts: Counter, token_count: int) -> float:
    if token_count == 0:
        return 0.0
    return (sum(counts.values()) / token_count) * 1000


def build_table(
    title: str,
    counts: Counter,
    contexts: DefaultDict[str, List[str]],
    min_freq: int,
    top_n: int,
) -> Table:
    table = Table(title=title)
    table.add_column("Pattern", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Sample contexts")
    rows = [(pattern, count) for pattern, count in counts.items() if count >= min_freq]
    rows.sort(key=lambda item: item[1], reverse=True)
    for pattern, count in rows[:top_n]:
        examples = " | ".join(contexts[pattern])
        table.add_row(pattern, str(count), examples)
    return table


def summarize(stats: PatternStats, min_freq: int, top_n: int) -> pd.DataFrame:
    records = []
    for pattern_type, counts in stats.counts.items():
        for pattern, count in counts.items():
            if count >= min_freq:
                records.append({"pattern_type": pattern_type, "pattern": pattern, "count": count})
    return pd.DataFrame(records)


def compare_densities(
    primary: PatternStats,
    secondary: PatternStats,
    threshold: float,
) -> List[Tuple[str, float, float, float, bool]]:
    results = []
    for label in PATTERN_TYPES.values():
        primary_density = density_per_k(primary.counts[label], primary.token_count)
        secondary_density = density_per_k(secondary.counts[label], secondary.token_count)
        max_density = max(primary_density, secondary_density)
        diff_ratio = 0.0 if max_density == 0 else abs(primary_density - secondary_density) / max_density
        collision = max_density > 0 and diff_ratio <= threshold
        results.append((label, primary_density, secondary_density, diff_ratio, collision))
    return results


def run_analysis(
    input_path: Path,
    args: argparse.Namespace,
    tokens_artifact: dict | None = None,
) -> AnalysisResults:
    if tokens_artifact:
        paragraphs = tokens_artifact.get("paragraphs", [])
        texts = [paragraph.get("text", "") for paragraph in paragraphs]
    else:
        texts, _ = load_texts(input_path)
    console = Console()
    console.print("Loading spaCy model: en_core_web_trf ...")
    nlp = spacy.load("en_core_web_trf")
    matcher = build_matcher(nlp)
    stats = PatternStats()
    occurrences: List[PatternOccurrence] = []
    pattern_filter = None
    if args.pattern_type:
        pattern_filter = [PATTERN_TYPES[key] for key in args.pattern_type]
    for idx, doc in enumerate(nlp.pipe(texts, n_process=args.processes, batch_size=4)):
        paragraph = None
        if tokens_artifact:
            paragraph = tokens_artifact.get("paragraphs", [])[idx]
        extract_patterns(
            doc,
            matcher,
            stats,
            args.exclude_stopwords,
            args.context_count,
            pattern_filter,
            paragraph=paragraph,
            occurrences=occurrences if tokens_artifact else None,
        )
    return AnalysisResults(
        stats=stats,
        occurrences=occurrences,
        manuscript_id=tokens_artifact.get("manuscript_id", "unknown") if tokens_artifact else "unknown",
    )


def build_edits_payload(
    results: AnalysisResults,
    min_freq: int,
    aggregate_by_type: bool,
) -> dict:
    stats = results.stats
    occurrences = results.occurrences
    items: List[dict] = []
    if aggregate_by_type:
        for pattern_type, counts in stats.counts.items():
            total_count = sum(counts.values())
            if total_count < min_freq:
                continue
            type_occurrences = [occ for occ in occurrences if occ.pattern_type == pattern_type]
            if not type_occurrences:
                continue
            top_pattern, top_count = counts.most_common(1)[0]
            anchor_occurrence = type_occurrences[0]
            items.append(
                {
                    "issue_id": f"pattern-{pattern_type.lower()}",
                    "type": "linguistic_pattern",
                    "location": {
                        "paragraph_id": anchor_occurrence.paragraph_id,
                        "token_ids": anchor_occurrence.token_ids,
                        "char_range": {
                            "start": anchor_occurrence.char_start,
                            "end": anchor_occurrence.char_end,
                        },
                        "anchor_text": anchor_occurrence.anchor_text,
                    },
                    "evidence": {
                        "summary": (
                            f"Detected {pattern_type} patterns {total_count} times; "
                            f"top pattern '{top_pattern}' appears {top_count} times."
                        ),
                        "signals": [
                            {"name": "pattern_type", "value": pattern_type},
                            {"name": "pattern_lemma", "value": top_pattern},
                            {"name": "pattern_count", "value": total_count},
                            {"name": "distinct_patterns", "value": len(counts)},
                        ],
                        "detector": "pattern_extractor",
                    },
                }
            )
    else:
        for idx, occurrence in enumerate(occurrences, start=1):
            if stats.counts[occurrence.pattern_type][occurrence.pattern] < min_freq:
                continue
            items.append(
                {
                    "issue_id": f"pattern-{idx:04d}",
                    "type": "linguistic_pattern",
                    "location": {
                        "paragraph_id": occurrence.paragraph_id,
                        "token_ids": occurrence.token_ids,
                        "char_range": {
                            "start": occurrence.char_start,
                            "end": occurrence.char_end,
                        },
                        "anchor_text": occurrence.anchor_text,
                    },
                    "evidence": {
                        "summary": (
                            f"Detected {occurrence.pattern_type} pattern "
                            f"'{occurrence.pattern}'."
                        ),
                        "signals": [
                            {"name": "pattern_type", "value": occurrence.pattern_type},
                            {"name": "pattern_lemma", "value": occurrence.pattern},
                            {
                                "name": "pattern_count",
                                "value": stats.counts[occurrence.pattern_type][occurrence.pattern],
                            },
                        ],
                        "detector": "pattern_extractor",
                        "sentence": occurrence.sentence,
                    },
                }
            )
    return {
        "schema_version": "1.0",
        "manuscript_id": results.manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")
    edits_output = args.output_edits or args.output_json
    if edits_output and not args.preprocessing:
        raise SystemExit("--output-edits requires --preprocessing for token mapping.")
    if args.preprocessing and not args.preprocessing.exists():
        raise SystemExit(f"Preprocessing directory not found: {args.preprocessing}")
    if args.preprocessing and input_path.is_dir():
        raise SystemExit("--preprocessing expects a single manuscript input path.")

    console = Console()
    tokens_artifact = load_tokens_artifact(args.preprocessing) if args.preprocessing else None
    primary_results = run_analysis(input_path, args, tokens_artifact=tokens_artifact)
    primary_stats = primary_results.stats

    for label, label_title in PATTERN_TYPES.items():
        pattern_label = PATTERN_TYPES[label]
        table = build_table(
            title=label.replace("_", " ").title(),
            counts=primary_stats.counts[pattern_label],
            contexts=primary_stats.contexts[pattern_label],
            min_freq=args.min_freq,
            top_n=args.top_n,
        )
        console.print(table)

    all_counts = Counter()
    for counts in primary_stats.counts.values():
        all_counts.update(counts)

    entropy = entropy_score(all_counts)
    console.print(f"\nStructural Entropy Score: [bold]{entropy:.2f}[/bold]")
    console.print(f"Total tokens analyzed: {primary_stats.token_count}")

    summary_df = summarize(primary_stats, args.min_freq, args.top_n)
    if not summary_df.empty:
        console.print("\nPattern density per 1k tokens:")
        for label, counts in primary_stats.counts.items():
            density = density_per_k(counts, primary_stats.token_count)
            console.print(f"- {label}: {density:.2f}")

    if args.show_pattern:
        console.print(f"\nContext examples for pattern: [bold]{args.show_pattern}[/bold]")
        context_table = Table(title="Pattern Contexts")
        context_table.add_column("Pattern Type")
        context_table.add_column("Contexts")
        for label, contexts in primary_stats.contexts.items():
            examples = contexts.get(args.show_pattern.lower(), [])
            if examples:
                context_table.add_row(label, " | ".join(examples))
        console.print(context_table)

    if args.compare:
        compare_path = Path(args.compare)
        if not compare_path.is_file():
            raise FileNotFoundError("--compare expects a file path.")
        console.print("\nRunning comparison analysis...")
        secondary_stats = run_analysis(compare_path, args).stats
        results = compare_densities(primary_stats, secondary_stats, args.collision_threshold)
        comparison_table = Table(title="Voice Collision Check")
        comparison_table.add_column("Pattern Type")
        comparison_table.add_column("Primary Density")
        comparison_table.add_column("Secondary Density")
        comparison_table.add_column("Diff Ratio")
        comparison_table.add_column("Collision")
        for label, primary_density, secondary_density, diff_ratio, collision in results:
            comparison_table.add_row(
                label,
                f"{primary_density:.2f}",
                f"{secondary_density:.2f}",
                f"{diff_ratio:.2f}",
                "⚠️" if collision else "",
            )
        console.print(comparison_table)

    if edits_output and tokens_artifact:
        payload = build_edits_payload(primary_results, args.min_freq, args.aggregate_by_type)
        if not payload["items"]:
            console.print("No patterns met the criteria for JSON output.")
            return
        edits_output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        console.print(f"JSON edits payload written to {edits_output}")


if __name__ == "__main__":
    main()
