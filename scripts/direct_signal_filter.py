#!/usr/bin/env python3
"""Direct Signal Filter (DSF).

Detects negative pacing (negation + contrast action), vague intensity clichés,
and high ambivalence ratios in paragraphs using spaCy dependency parses.

Usage examples:
  python scripts/direct_signal_filter.py path/to/manuscript.md --output-json dsf.json
  python scripts/direct_signal_filter.py path/to/manuscript.md --preprocessing /preprocessed --output-edits dsf_edits.json
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

from schema_validator import validate_payload

CONTRAST_TERMS = {"just", "only", "but"}
HEDGE_WORDS = {"seemed", "almost", "nearly", "just", "like", "felt"}
QUIET_ADJECTIVES = {"quiet", "silent", "lurking", "languid"}
ABSTRACT_NOUNS = {"intensity", "strength", "moment", "connection"}


@dataclass
class Paragraph:
    text: str
    index: int


@dataclass
class Issue:
    issue_type: str
    paragraph_index: int
    paragraph_id: Optional[str]
    start: int
    end: int
    summary: str
    suggestion: Optional[str]
    signals: List[dict]
    token_ids: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Direct Signal Filter (DSF) detects negative pacing, vague intensity clichés, "
            "and hedged ambivalence paragraphs."
        )
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the manuscript .txt or .md file.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional path to write DSF summary JSON.",
    )
    parser.add_argument(
        "--preprocessing",
        type=Path,
        help="Directory containing manuscript_tokens.json for token mapping.",
    )
    parser.add_argument(
        "--output-edits",
        type=Path,
        help="Optional path to write edits.schema.json output.",
    )
    parser.add_argument(
        "--ambivalence-threshold",
        type=float,
        default=0.08,
        help="Hedge-word ratio required to flag a paragraph (default: 0.08).",
    )
    return parser.parse_args()


def parse_paragraphs(text: str) -> List[Paragraph]:
    raw_paragraphs = [p for p in re.split(r"\n\s*\n", text) if p.strip()]
    return [Paragraph(text=p.strip(), index=idx) for idx, p in enumerate(raw_paragraphs)]


def load_tokens_artifact(preprocessing_dir: Path) -> dict:
    artifact_path = preprocessing_dir / "manuscript_tokens.json"
    if not artifact_path.exists():
        raise SystemExit(f"Missing manuscript_tokens.json in {preprocessing_dir}")
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    validate_payload(payload, "manuscript_tokens.schema.json", "manuscript_tokens.json")
    return payload


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


def find_subject(verb: spacy.tokens.Token) -> Optional[spacy.tokens.Token]:
    for child in verb.children:
        if child.dep_ in {"nsubj", "nsubjpass"}:
            return child
    for ancestor in verb.ancestors:
        for child in ancestor.children:
            if child.dep_ in {"nsubj", "nsubjpass"}:
                return child
    return None


def find_object_span(verb: spacy.tokens.Token) -> Optional[spacy.tokens.Span]:
    for child in verb.children:
        if child.dep_ in {"dobj", "obj", "attr", "oprd"}:
            return child.doc[child.left_edge.i : child.right_edge.i + 1]
    for child in verb.children:
        if child.dep_ == "prep":
            for pobj in child.children:
                if pobj.dep_ == "pobj":
                    return child.doc[child.left_edge.i : pobj.right_edge.i + 1]
    return None


def build_direct_strike(
    main_verb: spacy.tokens.Token,
    fallback_subject: Optional[spacy.tokens.Token],
) -> Optional[str]:
    subject = find_subject(main_verb) or fallback_subject
    object_span = find_object_span(main_verb)
    parts: List[str] = []
    if subject is not None:
        parts.append(subject.text)
    parts.append(main_verb.text)
    if object_span is not None:
        parts.append(object_span.text)
    if not parts:
        return None
    suggestion = " ".join(parts).strip()
    if suggestion and suggestion[-1] not in ".!?":
        suggestion += "."
    return suggestion


def detect_negation_contrast(
    sentence: spacy.tokens.Span,
) -> List[tuple[spacy.tokens.Token, spacy.tokens.Token, spacy.tokens.Token]]:
    hits: List[tuple[spacy.tokens.Token, spacy.tokens.Token, spacy.tokens.Token]] = []
    for token in sentence:
        if token.dep_ != "neg":
            continue
        if token.lower_ not in {"not", "n't"}:
            continue
        negated = token.head
        if negated.pos_ not in {"VERB", "AUX"}:
            continue
        contrast_token = None
        for candidate in sentence:
            if candidate.i <= negated.i:
                continue
            if candidate.lower_ in CONTRAST_TERMS:
                contrast_token = candidate
                break
        if contrast_token is None:
            continue
        main_verb = None
        for candidate in sentence:
            if candidate.i <= contrast_token.i:
                continue
            if candidate.pos_ == "VERB" and candidate.dep_ not in {"aux", "auxpass"}:
                main_verb = candidate
                break
        if main_verb is None:
            continue
        hits.append((negated, contrast_token, main_verb))
    return hits


def detect_quiet_intensity(sentence: spacy.tokens.Span) -> List[tuple[spacy.tokens.Token, spacy.tokens.Token]]:
    hits: List[tuple[spacy.tokens.Token, spacy.tokens.Token]] = []
    for token in sentence:
        if token.dep_ != "amod":
            continue
        if token.lemma_.lower() not in QUIET_ADJECTIVES:
            continue
        head = token.head
        if head.lemma_.lower() not in ABSTRACT_NOUNS:
            continue
        hits.append((token, head))
    return hits


def ambivalence_ratio(tokens: Iterable[spacy.tokens.Token]) -> tuple[float, int, int]:
    total = 0
    hedges = 0
    for token in tokens:
        if token.is_space or token.is_punct:
            continue
        total += 1
        if token.lemma_.lower() in HEDGE_WORDS:
            hedges += 1
    ratio = hedges / total if total else 0.0
    return ratio, hedges, total


def summarize_issues(issues: List[Issue]) -> dict[str, int]:
    summary = {"negative_pacing": 0, "vague_intensity": 0, "ambivalence": 0}
    for issue in issues:
        if issue.issue_type in summary:
            summary[issue.issue_type] += 1
    return summary


def build_edits_payload(manuscript_tokens: dict, issues: List[Issue]) -> dict[str, object]:
    items: List[dict[str, object]] = []
    manuscript_id = manuscript_tokens.get("manuscript_id", "unknown")
    for idx, issue in enumerate(issues):
        if not issue.paragraph_id:
            continue
        location: dict[str, object] = {"paragraph_id": issue.paragraph_id}
        if issue.token_ids:
            location["token_ids"] = issue.token_ids
        item = {
            "issue_id": f"{issue.paragraph_id}-dsf-{idx}",
            "type": issue.issue_type,
            "status": "open",
            "location": location,
            "evidence": {
                "summary": issue.summary,
                "signals": issue.signals,
                "detector": "direct_signal_filter.py",
            },
            "impact": {
                "severity": "high" if issue.issue_type == "ambivalence" else "medium"
            },
        }
        if issue.suggestion:
            item["suggested_actions"] = [issue.suggestion]
        items.append(item)
    if not items:
        return {
            "schema_version": "1.0",
            "manuscript_id": manuscript_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "items": [],
        }
    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


def main() -> int:
    args = parse_args()

    if not args.input_file.exists():
        print(f"File not found: {args.input_file}", file=sys.stderr)
        return 1
    if args.output_edits and not args.preprocessing:
        raise SystemExit("--output-edits requires --preprocessing for token mapping.")
    if args.preprocessing and not args.preprocessing.exists():
        raise SystemExit(f"Preprocessing directory not found: {args.preprocessing}")

    try:
        nlp = spacy.load("en_core_web_trf")
    except OSError as exc:
        print(
            "spaCy model 'en_core_web_trf' is required. Install via: "
            "python -m spacy download en_core_web_trf",
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

    issues: List[Issue] = []
    paragraph_summaries: List[dict[str, object]] = []

    for paragraph in paragraphs:
        doc = nlp(paragraph.text)
        paragraph_data = paragraph_lookup.get(paragraph.index) if paragraph_lookup else None
        paragraph_id = paragraph_data.get("paragraph_id") if paragraph_data else None
        neg_hits = 0
        quiet_hits = 0
        for sentence in doc.sents:
            for negated, contrast, main_verb in detect_negation_contrast(sentence):
                neg_hits += 1
                suggestion = build_direct_strike(main_verb, find_subject(negated))
                token_ids = (
                    map_span_to_tokens(paragraph_data, sentence.start_char, sentence.end_char)
                    if paragraph_data
                    else []
                )
                issues.append(
                    Issue(
                        issue_type="negative_pacing",
                        paragraph_index=paragraph.index,
                        paragraph_id=paragraph_id,
                        start=sentence.start_char,
                        end=sentence.end_char,
                        summary=(
                            "Negated lead-in followed by contrast action "
                            f"('{negated.text}' ... '{contrast.text} {main_verb.text}')."
                        ),
                        suggestion=suggestion,
                        signals=[
                            {"name": "negated_verb", "value": negated.text},
                            {"name": "contrast_token", "value": contrast.text},
                            {"name": "main_verb", "value": main_verb.text},
                        ],
                        token_ids=token_ids,
                    )
                )
            for adjective, noun in detect_quiet_intensity(sentence):
                quiet_hits += 1
                span_start = min(adjective.idx, noun.idx)
                span_end = max(adjective.idx + len(adjective), noun.idx + len(noun))
                token_ids = (
                    map_span_to_tokens(paragraph_data, span_start, span_end)
                    if paragraph_data
                    else []
                )
                issues.append(
                    Issue(
                        issue_type="vague_intensity",
                        paragraph_index=paragraph.index,
                        paragraph_id=paragraph_id,
                        start=span_start,
                        end=span_end,
                        summary=(
                            "Quiet/silent intensity cliché detected "
                            f"('{adjective.text} {noun.text}')."
                        ),
                        suggestion=None,
                        signals=[
                            {"name": "adjective", "value": adjective.text},
                            {"name": "abstract_noun", "value": noun.text},
                        ],
                        token_ids=token_ids,
                    )
                )

        ratio, hedge_count, total_count = ambivalence_ratio(doc)
        red_pencil = ratio >= args.ambivalence_threshold
        hedge_token_ids: List[str] = []
        if paragraph_data:
            for token in doc:
                if token.lemma_.lower() in HEDGE_WORDS:
                    hedge_token_ids.extend(
                        map_span_to_tokens(
                            paragraph_data, token.idx, token.idx + len(token)
                        )
                    )
        if red_pencil:
            issues.append(
                Issue(
                    issue_type="ambivalence",
                    paragraph_index=paragraph.index,
                    paragraph_id=paragraph_id,
                    start=0,
                    end=len(paragraph.text),
                    summary=(
                        f"Ambivalence ratio {ratio:.2f} (hedges {hedge_count}/{total_count}) "
                        "exceeds threshold."
                    ),
                    suggestion="[RED-PENCIL] Compress or delete hedged paragraph.",
                    signals=[
                        {"name": "ambivalence_ratio", "value": round(ratio, 4)},
                        {"name": "hedge_count", "value": hedge_count},
                        {"name": "token_count", "value": total_count},
                        {"name": "threshold", "value": args.ambivalence_threshold},
                    ],
                    token_ids=hedge_token_ids,
                )
            )

        paragraph_summaries.append(
            {
                "paragraph_index": paragraph.index,
                "paragraph_id": paragraph_id,
                "negative_pacing_hits": neg_hits,
                "vague_intensity_hits": quiet_hits,
                "ambivalence_ratio": round(ratio, 4),
                "ambivalence_flag": red_pencil,
            }
        )

    summary_counts = summarize_issues(issues)
    summary_payload = {
        "schema_version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manuscript_id": manuscript_tokens.get("manuscript_id", args.input_file.stem)
        if manuscript_tokens
        else args.input_file.stem,
        "negative_pacing_hits": summary_counts["negative_pacing"],
        "vague_intensity_hits": summary_counts["vague_intensity"],
        "ambivalence_flags": summary_counts["ambivalence"],
        "paragraphs": paragraph_summaries,
    }

    if args.output_json:
        args.output_json.write_text(
            json.dumps(summary_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if args.output_edits and manuscript_tokens is not None:
        payload = build_edits_payload(manuscript_tokens, issues)
        if payload["items"]:
            validate_payload(payload, "edits.schema.json", "dsf edits payload")
            args.output_edits.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        else:
            print("No DSF issues met criteria for edits output.")

    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
