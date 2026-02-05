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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

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


@dataclass
class SentimentReport:
    score: float
    positive_hits: List[str]
    negative_hits: List[str]


@dataclass
class ParagraphAnalysis:
    paragraph: Paragraph
    paragraph_id: Optional[str]
    slop: SlopReport
    sentiment: SentimentReport
    token_ids: List[str]


def load_lexicon(path: Path) -> dict[str, list[str]]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_paragraphs(text: str) -> List[Paragraph]:
    raw_paragraphs = [p for p in re.split(r"\n\s*\n", text) if p.strip()]
    return [Paragraph(text=p.strip(), index=idx) for idx, p in enumerate(raw_paragraphs)]


def load_tokens_artifact(preprocessing_dir: Path) -> dict:
    artifact_path = preprocessing_dir / "manuscript_tokens.json"
    if not artifact_path.exists():
        raise SystemExit(f"Missing manuscript_tokens.json in {preprocessing_dir}")
    return json.loads(artifact_path.read_text(encoding="utf-8"))


def build_paragraph_lookup(manuscript_tokens: dict) -> dict[int, dict]:
    lookup: dict[int, dict] = {}
    for idx, paragraph in enumerate(manuscript_tokens.get("paragraphs", [])):
        order = paragraph.get("order", idx)
        lookup[order] = paragraph
    return lookup


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
        if token.pos_ == "ADJ" and lemma in vague_adjectives:
            adjective_hits.append(token.text)

    lower_text = doc.text.lower()
    for phrase in togetherness_phrases + reflective_phrases:
        if phrase in lower_text:
            phrase_hits.append(phrase)

    for pattern in PATTERNS:
        if pattern.search(doc.text):
            pattern_hits.append(pattern.pattern)

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
    )


def collect_hit_token_ids(
    doc: spacy.tokens.Doc,
    paragraph: Optional[dict],
    lexicon: dict[str, list[str]],
) -> List[str]:
    if not paragraph:
        return []
    abstract_nouns = {word.lower() for word in lexicon.get("abstract_nouns", [])}
    vague_adjectives = {word.lower() for word in lexicon.get("vague_adjectives", [])}
    togetherness_phrases = [phrase.lower() for phrase in lexicon.get("togetherness_phrases", [])]
    reflective_phrases = [phrase.lower() for phrase in lexicon.get("reflective_phrases", [])]

    token_ids: List[str] = []
    seen: set[str] = set()

    def add_span(start: int, end: int) -> None:
        for token_id in map_span_to_tokens(paragraph, start, end):
            if token_id in seen:
                continue
            token_ids.append(token_id)
            seen.add(token_id)

    for token in doc:
        if token.is_space or token.is_punct:
            continue
        lemma = token.lemma_.lower()
        if token.pos_ == "NOUN" and lemma in abstract_nouns:
            add_span(token.idx, token.idx + len(token))
        if token.pos_ == "ADJ" and lemma in vague_adjectives:
            add_span(token.idx, token.idx + len(token))

    lower_text = doc.text.lower()
    for phrase in togetherness_phrases + reflective_phrases:
        start = 0
        while True:
            idx = lower_text.find(phrase, start)
            if idx == -1:
                break
            add_span(idx, idx + len(phrase))
            start = idx + len(phrase)

    for pattern in PATTERNS:
        for match in pattern.finditer(doc.text):
            add_span(match.start(), match.end())

    return token_ids


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
    paragraph_lookup: dict[int, dict] | None = None,
) -> tuple[str, bool, bool, SlopReport | None, List[ParagraphAnalysis]]:
    if not paragraphs:
        return "No paragraphs detected.", False, False, None, []

    tail = paragraphs[-tail_count:]
    report_lines: List[str] = []
    slop_reports: List[SlopReport] = []
    sentiment_reports: List[SentimentReport] = []
    analyses: List[ParagraphAnalysis] = []

    for paragraph in tail:
        doc = nlp(paragraph.text)
        slop = score_paragraph(doc, lexicon)
        sentiment = score_sentiment(doc)
        paragraph_data = paragraph_lookup.get(paragraph.index) if paragraph_lookup else None
        token_ids = collect_hit_token_ids(doc, paragraph_data, lexicon)
        slop_reports.append(slop)
        sentiment_reports.append(sentiment)
        analyses.append(
            ParagraphAnalysis(
                paragraph=paragraph,
                paragraph_id=paragraph_data.get("paragraph_id") if paragraph_data else None,
                slop=slop,
                sentiment=sentiment,
                token_ids=token_ids,
            )
        )
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

    return "\n".join(report_lines), moralizing_drift, hard_cut, final_slop, analyses


def build_edits_payload(
    manuscript_tokens: dict,
    analyses: List[ParagraphAnalysis],
    threshold: int,
    moralizing_drift: bool,
    hard_cut: bool,
) -> dict[str, object]:
    items: List[dict[str, object]] = []
    if not analyses:
        return {
            "schema_version": "1.0",
            "manuscript_id": manuscript_tokens.get("manuscript_id", "unknown"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "items": items,
        }
    final_index = analyses[-1].paragraph.index
    for analysis in analyses:
        if analysis.slop.score < threshold or not analysis.paragraph_id:
            continue
        is_final = analysis.paragraph.index == final_index
        pivot_flag = moralizing_drift if is_final else False
        hard_cut_flag = hard_cut if is_final else False
        location: dict[str, object] = {"paragraph_id": analysis.paragraph_id}
        if analysis.token_ids:
            location["token_ids"] = analysis.token_ids
        issue_id = f"{analysis.paragraph_id}-slop"
        items.append(
            {
                "issue_id": issue_id,
                "type": "hope_slop",
                "status": "open",
                "location": location,
                "evidence": {
                    "summary": (
                        "Paragraph flagged as Hope-Slop with score "
                        f"{analysis.slop.score}."
                    ),
                    "signals": [
                        {"name": "slop_score", "value": analysis.slop.score},
                        {
                            "name": "abstract_hit_count",
                            "value": len(analysis.slop.abstract_hits),
                        },
                        {
                            "name": "adjective_hit_count",
                            "value": len(analysis.slop.adjective_hits),
                        },
                        {
                            "name": "phrase_hit_count",
                            "value": len(analysis.slop.phrase_hits),
                        },
                        {
                            "name": "pattern_hit_count",
                            "value": len(analysis.slop.pattern_hits),
                        },
                        {"name": "sentiment_score", "value": analysis.sentiment.score},
                        {"name": "sentiment_pivot", "value": pivot_flag},
                        {"name": "hard_cut_suggestion", "value": hard_cut_flag},
                    ],
                    "detector": "slop_scrubber.py",
                },
                "impact": {"severity": "medium"},
            }
        )
    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_tokens.get("manuscript_id", "unknown"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


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
        help="Directory containing manuscript_tokens.json for paragraph/token mapping.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write edits.schema.json payload for flagged paragraphs.",
    )
    parser.add_argument(
        "--output-edits",
        type=Path,
        help="Optional path to write edits.schema.json payload for flagged paragraphs.",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"File not found: {args.input_file}", file=sys.stderr)
        return 1
    edits_output = args.output_edits or args.output_json
    if edits_output and not args.preprocessing:
        raise SystemExit("--output-edits requires --preprocessing for token mapping.")
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
    manuscript_tokens = None
    paragraph_lookup = None
    if args.preprocessing:
        manuscript_tokens = load_tokens_artifact(args.preprocessing)
        paragraph_lookup = build_paragraph_lookup(manuscript_tokens)

    report, moralizing_drift, hard_cut, final_slop, analyses = build_report(
        paragraphs, args.tail_paragraphs, lexicon, nlp, args.threshold, paragraph_lookup
    )

    render_report(report)

    if args.report and paragraphs:
        tail = paragraphs[-args.tail_paragraphs :]
        print("\n=== Ending Snapshot (Before) ===")
        print("\n\n".join(p.text for p in tail))

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

    if edits_output and manuscript_tokens is not None:
        payload = build_edits_payload(
            manuscript_tokens,
            analyses,
            args.threshold,
            moralizing_drift,
            hard_cut,
        )
        if not payload["items"]:
            print("No slop paragraphs met the threshold for edits output.")
        else:
            edits_output.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
