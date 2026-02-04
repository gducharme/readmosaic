#!/usr/bin/env python3
"""
Linguistic Pattern Extractor (LPE)

Extracts phrasal verbs, action chains, descriptive pairs, and adverbial intent
patterns from markdown or text manuscripts using spaCy's DependencyMatcher.
"""
from __future__ import annotations

import argparse
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, field
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


def normalize_tokens(tokens: Iterable[spacy.tokens.Token]) -> List[str]:
    return [token.lemma_.lower() for token in tokens]


def extract_patterns(
    doc: spacy.tokens.Doc,
    matcher: DependencyMatcher,
    stats: PatternStats,
    exclude_stopwords: bool,
    context_limit: int,
    pattern_filter: List[str] | None,
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
) -> PatternStats:
    texts, _ = load_texts(input_path)
    console = Console()
    console.print("Loading spaCy model: en_core_web_trf ...")
    nlp = spacy.load("en_core_web_trf")
    matcher = build_matcher(nlp)
    stats = PatternStats()
    pattern_filter = None
    if args.pattern_type:
        pattern_filter = [PATTERN_TYPES[key] for key in args.pattern_type]
    for doc in nlp.pipe(texts, n_process=args.processes, batch_size=4):
        extract_patterns(doc, matcher, stats, args.exclude_stopwords, args.context_count, pattern_filter)
    return stats


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")

    console = Console()
    primary_stats = run_analysis(input_path, args)

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
        secondary_stats = run_analysis(compare_path, args)
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


if __name__ == "__main__":
    main()
