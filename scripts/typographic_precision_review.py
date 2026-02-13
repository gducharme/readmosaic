#!/usr/bin/env python3
"""Interactive reviewer for typographic/grammar auditor issue outputs.

Loads auditor JSON output and pre-processing sentence artifacts, then walks issues in
sentence order. For each issue, displays before/after sentence previews, captures
accept/reject/edit decisions, and writes a final merged sentence output.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Review auditor issues against preprocessed sentence lines, in ascending "
            "sentence order, with interactive accept/reject decisions."
        )
    )
    parser.add_argument(
        "--audit",
        type=Path,
        required=True,
        help="Path to auditor JSON output (object with an 'issues' array).",
    )
    parser.add_argument(
        "--preprocessed",
        type=Path,
        required=True,
        help="Pre-processing directory containing sentences.jsonl.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("typographic_review_decisions.json"),
        help="Where to write review decisions JSON (default: typographic_review_decisions.json).",
    )
    parser.add_argument(
        "--final-output",
        type=Path,
        default=Path("typographic_review_final_sentences.jsonl"),
        help=(
            "Where to write updated sentences JSONL with accepted edits applied "
            "(default: typographic_review_final_sentences.jsonl)."
        ),
    )
    return parser.parse_args()


def load_sentences(preprocessed_dir: Path) -> list[dict[str, Any]]:
    sentences_path = preprocessed_dir / "sentences.jsonl"
    if not sentences_path.exists():
        raise RuntimeError(
            f"Missing file: {sentences_path}. Run scripts/pre_processing.py first."
        )

    sentences: list[dict[str, Any]] = []
    with sentences_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSON in {sentences_path} at line {line_number}."
                ) from exc

            if "text" not in record:
                raise RuntimeError(
                    f"Missing 'text' field in {sentences_path} at line {line_number}."
                )
            sentences.append(record)

    sentences.sort(key=lambda record: int(record.get("order", 0)))
    return sentences


def load_issues(audit_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not audit_path.exists():
        raise RuntimeError(f"Audit JSON not found: {audit_path}")

    try:
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in {audit_path}.") from exc

    if isinstance(payload, dict) and isinstance(payload.get("issues"), list):
        issues = [issue for issue in payload["issues"] if isinstance(issue, dict)]
        metadata = {key: value for key, value in payload.items() if key != "issues"}
        return issues, metadata

    raise RuntimeError(
        "Unsupported audit JSON format. Expected object with top-level 'issues' array."
    )


def prompt_decision(default_correction: str) -> tuple[str, str]:
    while True:
        answer = input("Decision [Y/n/e/q] (default: Y accept): ").strip().lower()

        if answer in {"", "y", "yes"}:
            return "accepted", default_correction
        if answer in {"n", "no", "r", "reject", "refuse"}:
            return "rejected", ""
        if answer in {"e", "edit"}:
            edited = input("Enter revised correction text: ").strip()
            if not edited:
                print("Edited correction cannot be empty.")
                continue
            return "accepted_with_edit", edited
        if answer in {"q", "quit"}:
            return "quit", ""

        print("Please respond with Y, n, e, or q.")


def apply_correction(sentence_text: str, issue: dict[str, Any], correction: str) -> tuple[str, str]:
    excerpt = str(issue.get("excerpt", "") or "").strip()
    if excerpt and excerpt in sentence_text:
        return sentence_text.replace(excerpt, correction, 1), "replaced_excerpt"

    issue_sentence = str(issue.get("sentence_text", "") or "").strip()
    if issue_sentence and sentence_text.strip() == issue_sentence:
        if correction and correction != issue_sentence:
            return correction, "replaced_whole_sentence"
        return sentence_text, "no_change"

    if correction and correction != sentence_text:
        return correction, "fallback_replaced_whole_sentence"

    return sentence_text, "no_change"


def write_sentences_jsonl(path: Path, sentences: list[dict[str, Any]], final_texts: list[str]) -> None:
    if len(sentences) != len(final_texts):
        raise RuntimeError("Internal error: sentence/text count mismatch.")

    with path.open("w", encoding="utf-8") as handle:
        for record, text in zip(sentences, final_texts):
            updated = dict(record)
            updated["text"] = text
            handle.write(json.dumps(updated, ensure_ascii=False) + "\n")


def main() -> int:
    args = parse_args()

    try:
        sentences = load_sentences(args.preprocessed)
        issues, metadata = load_issues(args.audit)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1

    if not sentences:
        print("No sentences found in sentences.jsonl.", file=sys.stderr)
        return 1

    indexed_issues: list[tuple[int, int, dict[str, Any]]] = []
    for original_position, issue in enumerate(issues, start=1):
        idx = int(issue.get("sentence_index", 1))
        idx = max(1, min(idx, len(sentences)))
        indexed_issues.append((idx, original_position, issue))

    indexed_issues.sort(key=lambda row: (row[0], row[1]))

    if not indexed_issues:
        print("No issues found in the audit payload.")
        final_texts = [str(record.get("text", "")) for record in sentences]
        write_sentences_jsonl(args.final_output, sentences, final_texts)
        decisions_payload = {
            "audit_file": str(args.audit),
            "preprocessed": str(args.preprocessed),
            "metadata": metadata,
            "decisions": [],
            "summary": {
                "reviewed": 0,
                "accepted": 0,
                "accepted_with_edit": 0,
                "rejected": 0,
                "remaining_unreviewed": 0,
            },
            "final_output": str(args.final_output),
        }
        args.output.write_text(json.dumps(decisions_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote decisions to {args.output}")
        print(f"Wrote final sentences to {args.final_output}")
        return 0

    final_texts = [str(record.get("text", "")) for record in sentences]

    decisions: list[dict[str, Any]] = []
    accepted = 0
    accepted_with_edit = 0
    rejected = 0

    total_issues = len(indexed_issues)
    for issue_number, (sentence_index, original_position, issue) in enumerate(indexed_issues, start=1):
        line_before = final_texts[sentence_index - 1]

        proposed = str(issue.get("minimal_correction", "")).strip()
        if not proposed:
            proposed = str(issue.get("sentence_text", "")).strip()

        preview_after, preview_mode = apply_correction(line_before, issue, proposed)

        print("\n" + "=" * 80)
        print(f"Issue {issue_number}/{total_issues} (original position {original_position})")
        print(f"Sentence index: {sentence_index}")
        print(f"Issue type: {issue.get('issue_type', 'N/A')}")
        print(f"Classification: {issue.get('classification', 'N/A')}")
        print(f"Confidence: {issue.get('confidence', 'N/A')}")
        print(f"Location: {issue.get('location', 'N/A')}")
        print(f"Excerpt: {issue.get('excerpt', issue.get('phrase', 'N/A'))}")
        print(f"Explanation: {issue.get('explanation', 'N/A')}")

        print("\nBefore line:")
        print(line_before)
        print("\nProposed correction token/text:")
        print(proposed or "N/A")
        print(f"\nAfter line preview ({preview_mode}):")
        print(preview_after)

        decision, final_correction = prompt_decision(proposed)
        if decision == "quit":
            print("Stopped by user.")
            break

        applied = False
        apply_mode = "not_applied"
        line_after = line_before

        if decision in {"accepted", "accepted_with_edit"}:
            correction = final_correction if decision == "accepted_with_edit" else proposed
            line_after, apply_mode = apply_correction(line_before, issue, correction)
            final_texts[sentence_index - 1] = line_after
            applied = apply_mode != "no_change"

        if decision == "accepted":
            accepted += 1
        elif decision == "accepted_with_edit":
            accepted_with_edit += 1
        else:
            rejected += 1

        decisions.append(
            {
                "issue_number": issue_number,
                "original_issue_position": original_position,
                "sentence_index": sentence_index,
                "decision": decision,
                "proposed_correction": proposed,
                "final_correction": final_correction,
                "line_before": line_before,
                "line_after": line_after,
                "applied": applied,
                "apply_mode": apply_mode,
                "issue": issue,
            }
        )

    reviewed = len(decisions)
    summary = {
        "reviewed": reviewed,
        "accepted": accepted,
        "accepted_with_edit": accepted_with_edit,
        "rejected": rejected,
        "remaining_unreviewed": total_issues - reviewed,
    }

    write_sentences_jsonl(args.final_output, sentences, final_texts)

    decisions_payload = {
        "audit_file": str(args.audit),
        "preprocessed": str(args.preprocessed),
        "metadata": metadata,
        "decisions": decisions,
        "summary": summary,
        "final_output": str(args.final_output),
    }
    args.output.write_text(json.dumps(decisions_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("\n" + "=" * 80)
    print("Review complete.")
    print(json.dumps(summary, indent=2))
    print(f"Wrote decisions to {args.output}")
    print(f"Wrote final sentences to {args.final_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
