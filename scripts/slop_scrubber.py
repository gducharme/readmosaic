#!/usr/bin/env python3
"""Cliché wrap-up scrubber for AI-ish ending drift.

Usage examples:
  python scripts/slop_scrubber.py path/to/scene.txt
  python scripts/slop_scrubber.py path/to/scene.txt --report
  python scripts/slop_scrubber.py path/to/scene.txt --aggressive --report
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import spacy

LEXICON_PATH = Path(__file__).with_name("ai_ending_lexicon.json")

POSITIVE_WORDS = {
    "hope",
    "hopeful",
    "promise",
    "bright",
    "light",
    "warm",
    "renew",
    "renewal",
    "future",
    "healing",
    "growth",
    "resolve",
    "peace",
    "optimism",
}

NEGATIVE_WORDS = {
    "cold",
    "grim",
    "dark",
    "bleak",
    "uncertain",
    "loss",
    "fear",
    "risk",
    "fracture",
    "ruin",
    "regret",
    "dread",
}

PATTERNS = [
    re.compile(r"as they .*?couldn't help but feel that", re.IGNORECASE),
    re.compile(r"in the end,? it wasn't about .*? but about", re.IGNORECASE),
]


@dataclass
class Paragraph:
    text: str
    index: int


@dataclass
class SlopReport:
    score: int
    abstract_hits: List[str]
    adjective_hits: List[str]
    phrase_hits: List[str]
    pattern_hits: List[str]
    lexicon_spans: List[Tuple[int, int]]
    pattern_spans: List[Tuple[int, int]]


@dataclass
class SentimentReport:
    score: float
    positive_hits: List[str]
    negative_hits: List[str]


def load_lexicon(path: Path) -> dict[str, list[str]]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_paragraphs(text: str) -> List[Paragraph]:
    raw_paragraphs = [p for p in re.split(r"\n\s*\n", text) if p.strip()]
    return [Paragraph(text=p.strip(), index=idx) for idx, p in enumerate(raw_paragraphs)]


def score_sentiment(tokens: Iterable[spacy.tokens.Token]) -> SentimentReport:
    positives: List[str] = []
    negatives: List[str] = []
    total = 0
    for token in tokens:
        if token.is_space or token.is_punct:
            continue
        total += 1
        lemma = token.lemma_.lower()
        if lemma in POSITIVE_WORDS:
            positives.append(token.text)
        if lemma in NEGATIVE_WORDS:
            negatives.append(token.text)
    if total == 0:
        score = 0.0
    else:
        score = (len(positives) - len(negatives)) / total
    return SentimentReport(score=score, positive_hits=positives, negative_hits=negatives)


def score_paragraph(doc: spacy.tokens.Doc, lexicon: dict[str, list[str]]) -> SlopReport:
    abstract_hits: List[str] = []
    adjective_hits: List[str] = []
    phrase_hits: List[str] = []
    pattern_hits: List[str] = []
    lexicon_spans: List[Tuple[int, int]] = []
    pattern_spans: List[Tuple[int, int]] = []

    abstract_nouns = {word.lower() for word in lexicon.get("abstract_nouns", [])}
    vague_adjectives = {word.lower() for word in lexicon.get("vague_adjectives", [])}
    togetherness_phrases = [phrase.lower() for phrase in lexicon.get("togetherness_phrases", [])]
    reflective_phrases = [phrase.lower() for phrase in lexicon.get("reflective_phrases", [])]

    for token in doc:
        if token.is_space or token.is_punct:
            continue
        lemma = token.lemma_.lower()
        if token.pos_ == "NOUN" and lemma in abstract_nouns:
            abstract_hits.append(token.text)
            lexicon_spans.append((token.idx, token.idx + len(token.text)))
        if token.pos_ == "ADJ" and lemma in vague_adjectives:
            adjective_hits.append(token.text)
            lexicon_spans.append((token.idx, token.idx + len(token.text)))

    phrase_hits_set = set()
    for phrase in togetherness_phrases + reflective_phrases:
        for match in re.finditer(re.escape(phrase), doc.text, re.IGNORECASE):
            phrase_hits_set.add(phrase)
            lexicon_spans.append((match.start(), match.end()))
    phrase_hits.extend(sorted(phrase_hits_set))

    for pattern in PATTERNS:
        for match in pattern.finditer(doc.text):
            pattern_hits.append(pattern.pattern)
            pattern_spans.append((match.start(), match.end()))

    score = (
        len(abstract_hits)
        + len(adjective_hits)
        + (2 * len(phrase_hits))
        + (3 * len(pattern_hits))
    )

    return SlopReport(
        score=score,
        abstract_hits=abstract_hits,
        adjective_hits=adjective_hits,
        phrase_hits=phrase_hits,
        pattern_hits=pattern_hits,
        lexicon_spans=lexicon_spans,
        pattern_spans=pattern_spans,
    )


def summarize_hits(label: str, hits: List[str]) -> str:
    if not hits:
        return f"{label}: none"
    return f"{label}: {', '.join(sorted(set(hits)))}"


def describe_paragraph_result(index: int, slop: SlopReport, sentiment: SentimentReport) -> str:
    parts = [
        f"Paragraph {index + 1} slop score: {slop.score}",
        summarize_hits("Abstract nouns", slop.abstract_hits),
        summarize_hits("Vague adjectives", slop.adjective_hits),
        summarize_hits("Phrase hits", slop.phrase_hits),
        summarize_hits("Pattern hits", slop.pattern_hits),
        f"Sentiment score: {sentiment.score:.2f}",
    ]
    return "\n".join(parts)


def build_report(
    paragraphs: List[Paragraph],
    tail_count: int,
    lexicon: dict[str, list[str]],
    nlp: spacy.language.Language,
    threshold: int,
) -> tuple[str, bool, bool, SlopReport | None, List[tuple[Paragraph, SlopReport, SentimentReport]]]:
    if not paragraphs:
        return "No paragraphs detected.", False, False, None, []

    tail = paragraphs[-tail_count:]
    report_lines: List[str] = []
    slop_reports: List[SlopReport] = []
    sentiment_reports: List[SentimentReport] = []
    tail_results: List[tuple[Paragraph, SlopReport, SentimentReport]] = []

    for paragraph in tail:
        doc = nlp(paragraph.text)
        slop = score_paragraph(doc, lexicon)
        sentiment = score_sentiment(doc)
        slop_reports.append(slop)
        sentiment_reports.append(sentiment)
        tail_results.append((paragraph, slop, sentiment))
        report_lines.append(describe_paragraph_result(paragraph.index, slop, sentiment))
        report_lines.append("-" * 60)

    final_slop = slop_reports[-1]
    final_sentiment = sentiment_reports[-1]

    body_sentiment = None
    if len(paragraphs) > 1:
        body_text = "\n\n".join(p.text for p in paragraphs[:-1])
        body_doc = nlp(body_text)
        body_sentiment = score_sentiment(body_doc)

    moralizing_drift = False
    if body_sentiment:
        moralizing_drift = body_sentiment.score <= -0.5 and final_sentiment.score >= 0.3
        report_lines.append(
            f"Body sentiment: {body_sentiment.score:.2f} | Final sentiment: {final_sentiment.score:.2f}"
        )
        if moralizing_drift:
            report_lines.append("Moralizing Drift detected (sentiment pivot).")

    hard_cut = False
    if len(slop_reports) > 1:
        previous_slop = slop_reports[-2]
        hard_cut = final_slop.score >= threshold and previous_slop.score < threshold
        if hard_cut:
            report_lines.append(
                "Hard Cut suggestion: previous paragraph is more abrupt than the slop-heavy finale."
            )

    if final_slop.score >= threshold:
        report_lines.append("Final paragraph flagged as Hope-Slop.")

    return "\n".join(report_lines), moralizing_drift, hard_cut, final_slop, tail_results


def load_manuscript_tokens(preprocessing_dir: Path) -> dict:
    tokens_path = preprocessing_dir / "manuscript_tokens.json"
    if not tokens_path.exists():
        raise SystemExit(f"Missing manuscript_tokens.json in {preprocessing_dir}")
    return json.loads(tokens_path.read_text(encoding="utf-8"))


def build_paragraph_lookup(manuscript_tokens: dict) -> dict[int, dict]:
    paragraphs = manuscript_tokens.get("paragraphs", [])
    return {paragraph["order"]: paragraph for paragraph in paragraphs}


def map_spans_to_token_ids(
    spans: Sequence[Tuple[int, int]],
    paragraph_tokens: Sequence[dict],
) -> List[str]:
    if not spans:
        return []
    token_ids: List[str] = []
    seen = set()
    for token in paragraph_tokens:
        token_start = token.get("start_char", 0)
        token_end = token.get("end_char", 0)
        for span_start, span_end in spans:
            if token_start < span_end and token_end > span_start:
                token_id = token.get("token_id")
                if token_id and token_id not in seen:
                    token_ids.append(token_id)
                    seen.add(token_id)
                break
    return token_ids


def render_report(report: str) -> None:
    print(report)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "The Cliché Wrap-Up Scrubber (CWS) scans the last 1-2 paragraphs of a scene for "
            "LLM-style hopeful wrap-ups. It scores lexicon hits, cliché clauses, sentiment pivots, "
            "and suggests hard cuts when the finale turns reflective."
        )
    )
    parser.add_argument("input_file", type=Path, help="Path to the scene or chapter text file.")
    parser.add_argument(
        "--tail-paragraphs",
        type=int,
        default=2,
        help="How many paragraphs from the end to analyze (default: 2).",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=4,
        help="Slop score threshold to flag a paragraph (default: 4).",
    )
    parser.add_argument(
        "--aggressive",
        action="store_true",
        help=(
            "If set, automatically deletes the final paragraph when it exceeds the slop threshold "
            "and prints the scrubbed text to stdout."
        ),
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Show a before/after snapshot of the ending paragraphs.",
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        default=None,
        help="Directory containing manuscript_tokens.json for paragraph mapping.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional output path for edits.schema.json payload.",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"File not found: {args.input_file}", file=sys.stderr)
        return 1
    if args.output_json and not args.preprocessing:
        raise SystemExit("--output-json requires --preprocessing for token mapping.")
    if args.preprocessing and not args.preprocessing.exists():
        raise SystemExit(f"Preprocessing directory not found: {args.preprocessing}")

    lexicon = load_lexicon(LEXICON_PATH)

    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError as exc:
        print(
            "spaCy model 'en_core_web_sm' is required. Install via: python -m spacy download en_core_web_sm",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    text = args.input_file.read_text(encoding="utf-8")
    paragraphs = parse_paragraphs(text)

    report, moralizing_drift, hard_cut, final_slop, tail_results = build_report(
        paragraphs, args.tail_paragraphs, lexicon, nlp, args.threshold
    )

    render_report(report)

    if args.report and paragraphs:
        tail = paragraphs[-args.tail_paragraphs :]
        print("\n=== Ending Snapshot (Before) ===")
        print("\n\n".join(p.text for p in tail))

    manuscript_tokens = None
    paragraph_lookup: dict[int, dict] = {}
    if args.preprocessing:
        manuscript_tokens = load_manuscript_tokens(args.preprocessing)
        paragraph_lookup = build_paragraph_lookup(manuscript_tokens)
        if len(paragraph_lookup) != len(paragraphs):
            print(
                "Warning: paragraph count does not match preprocessing tokens. "
                "Paragraph mapping may be approximate."
            )

    if args.output_json and manuscript_tokens:
        items = []
        final_index = paragraphs[-1].index if paragraphs else -1
        for paragraph, slop, sentiment in tail_results:
            if slop.score < args.threshold:
                continue
            paragraph_data = paragraph_lookup.get(paragraph.index)
            if not paragraph_data:
                continue
            paragraph_id = paragraph_data["paragraph_id"]
            token_ids = map_spans_to_token_ids(
                slop.lexicon_spans + slop.pattern_spans,
                paragraph_data.get("tokens", []),
            )
            location = {"paragraph_id": paragraph_id}
            if token_ids:
                location["token_ids"] = token_ids
            is_final = paragraph.index == final_index
            summary_parts = [
                f"Paragraph {paragraph.index + 1} flagged as hope-slop (score {slop.score}).",
                f"Abstract nouns: {len(slop.abstract_hits)}.",
                f"Vague adjectives: {len(slop.adjective_hits)}.",
                f"Phrase hits: {len(slop.phrase_hits)}.",
                f"Pattern hits: {len(slop.pattern_hits)}.",
            ]
            if is_final and moralizing_drift:
                summary_parts.append("Sentiment pivot detected.")
            if is_final and hard_cut:
                summary_parts.append("Hard cut suggested.")
            evidence = {
                "summary": " ".join(summary_parts),
                "detector": "slop_scrubber",
                "signals": [
                    {"name": "slop_score", "value": slop.score},
                    {"name": "abstract_noun_hits", "value": len(slop.abstract_hits), "units": "count"},
                    {
                        "name": "vague_adjective_hits",
                        "value": len(slop.adjective_hits),
                        "units": "count",
                    },
                    {"name": "phrase_hits", "value": len(slop.phrase_hits), "units": "count"},
                    {"name": "pattern_hits", "value": len(slop.pattern_hits), "units": "count"},
                    {"name": "sentiment_score", "value": round(sentiment.score, 4)},
                    {
                        "name": "sentiment_pivot",
                        "value": bool(is_final and moralizing_drift),
                    },
                    {"name": "hard_cut_suggestion", "value": bool(is_final and hard_cut)},
                ],
            }
            items.append(
                {
                    "issue_id": str(uuid.uuid4()),
                    "type": "style",
                    "status": "open",
                    "location": location,
                    "evidence": evidence,
                }
            )

        if not items:
            print("No flagged paragraphs above threshold for JSON output.")
        else:
            edits_payload = {
                "schema_version": "1.0",
                "manuscript_id": manuscript_tokens["manuscript_id"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "items": items,
            }
            args.output_json.write_text(
                json.dumps(edits_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"JSON output saved to {args.output_json}")

    if paragraphs and final_slop and final_slop.score >= args.threshold:
        scrubbed = "\n\n".join(p.text for p in paragraphs[:-1])
        if hard_cut:
            scrubbed = scrubbed.rstrip()
        if args.aggressive:
            if args.report:
                print("\n=== Ending Snapshot (After) ===")
                print("\n\n".join(scrubbed.split("\n\n")[-args.tail_paragraphs :]))
            else:
                print(scrubbed)
        elif args.report:
            print("\n=== Ending Snapshot (Suggested Hard Cut) ===")
            print("\n\n".join(scrubbed.split("\n\n")[-args.tail_paragraphs :]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
