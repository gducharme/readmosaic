from __future__ import annotations

from collections import Counter

from ._artifacts import read_json, write_json_artifact, write_jsonl_artifact

LOW_SIGNAL_PHRASES = (
    "in the end",
    "at the end of the day",
    "quietly",
    "silence",
    "together",
    "hope",
)
HEDGE_WORDS = (
    "maybe",
    "perhaps",
    "somewhat",
    "kind of",
    "sort of",
    "almost",
)


def _paragraph_flags(text: str) -> dict[str, object]:
    lowered = text.lower()
    phrase_hits = [phrase for phrase in LOW_SIGNAL_PHRASES if phrase in lowered]
    hedge_hits = [hedge for hedge in HEDGE_WORDS if hedge in lowered]
    return {
        "low_signal_phrase_hits": phrase_hits,
        "hedge_hits": hedge_hits,
        "needs_rewrite": bool(phrase_hits or len(hedge_hits) >= 2),
    }


def run_whole(ctx) -> None:
    preprocessed = read_json(ctx, "preprocessed/preprocessed.json", family="preprocessed")
    lexical = read_json(ctx, "lexical/word_frequency_report.json", family="lexical_frequency")

    paragraphs = [p for p in preprocessed.get("paragraphs", []) if isinstance(p, str)]
    overused_words = {
        str(entry.get("word", "")).lower()
        for entry in lexical.get("top_words", [])
        if isinstance(entry, dict) and entry.get("word")
    }

    findings: list[dict[str, object]] = []
    rewrite_candidates: list[dict[str, object]] = []
    reason_counter: Counter[str] = Counter()

    for idx, paragraph in enumerate(paragraphs, start=1):
        paragraph_id = f"p-{idx:04d}"
        flags = _paragraph_flags(paragraph)
        paragraph_words = {word.strip(".,;:!?\"'()").lower() for word in paragraph.split()}
        overuse_hits = sorted(word for word in paragraph_words if word in overused_words)

        reasons: list[str] = []
        if flags["low_signal_phrase_hits"]:
            reasons.append("low_signal_phrase")
        if flags["hedge_hits"]:
            reasons.append("hedge_density")
        if len(overuse_hits) >= 2:
            reasons.append("lexical_overuse_cluster")

        if not reasons:
            continue

        for reason in reasons:
            reason_counter[reason] += 1

        findings.append(
            {
                "paragraph_id": paragraph_id,
                "reasons": reasons,
                "low_signal_phrase_hits": flags["low_signal_phrase_hits"],
                "hedge_hits": flags["hedge_hits"],
                "overused_word_hits": overuse_hits,
            }
        )
        rewrite_candidates.append(
            {
                "item_id": paragraph_id,
                "paragraph_id": paragraph_id,
                "text": paragraph,
                "reasons": reasons,
                "instruction": (
                    "Tighten phrasing, remove hedging or low-signal language, and preserve meaning."
                ),
                "overused_word_hits": overuse_hits,
            }
        )

    findings_payload = {
        "paragraph_count": len(paragraphs),
        "flagged_paragraph_count": len(findings),
        "reason_counts": dict(reason_counter),
        "findings": findings,
    }
    write_json_artifact(
        ctx,
        "style_slop_findings.json",
        findings_payload,
        family="style_slop_findings",
    )
    write_jsonl_artifact(
        ctx,
        "rewrite_candidates.jsonl",
        rewrite_candidates,
        family="rewrite_candidates",
    )
