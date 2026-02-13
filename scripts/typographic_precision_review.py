#!/usr/bin/env python3
"""Interactive reviewer for typographic/grammar auditor issue outputs.

Loads auditor JSON output plus pre-processing sentence artifacts, then walks issue-by-issue
with surrounding sentence context and prompts for accept/reject decisions.
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
            "Review auditor issue outputs line-by-line with ±2 sentence context and "
            "interactive accept/reject decisions. Default action accepts correction."
        )
    )
    parser.add_argument(
        "--audit",
        type=Path,
        required=True,
        help="Path to auditor JSON output (object with issues, array of issues, or single issue object).",
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
        "--context-window",
        type=int,
        default=2,
        help="Number of preceding/following sentences to display (default: 2).",
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

    metadata: dict[str, Any] = {}
    if isinstance(payload, dict) and isinstance(payload.get("issues"), list):
        issues = [issue for issue in payload["issues"] if isinstance(issue, dict)]
        metadata = {
            key: value
            for key, value in payload.items()
            if key != "issues"
        }
        return issues, metadata

    if isinstance(payload, list):
        issues = [issue for issue in payload if isinstance(issue, dict)]
        return issues, metadata

    if isinstance(payload, dict) and "sentence_index" in payload:
        return [payload], metadata

    raise RuntimeError(
        "Unsupported audit JSON format. Expected object with 'issues', array of issues, or single issue object."
    )


def display_context(sentences: list[dict[str, Any]], target_index_1_based: int, window: int) -> None:
    total = len(sentences)
    target_index_1_based = max(1, min(target_index_1_based, total if total else 1))
    start = max(1, target_index_1_based - window)
    end = min(total, target_index_1_based + window)

    print(f"\nContext ({window} preceding and {window} following, when available):")
    for idx in range(start, end + 1):
        prefix = "=>" if idx == target_index_1_based else "  "
        text = str(sentences[idx - 1].get("text", "")).strip()
        print(f"{prefix} [{idx:>4}] {text}")


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

    if not issues:
        print("No issues found in the audit payload.")
        decisions_payload = {
            "audit_file": str(args.audit),
            "preprocessed": str(args.preprocessed),
            "metadata": metadata,
            "decisions": [],
            "summary": {"reviewed": 0, "accepted": 0, "accepted_with_edit": 0, "rejected": 0},
        }
        args.output.write_text(json.dumps(decisions_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote decisions to {args.output}")
        return 0

    decisions: list[dict[str, Any]] = []
    accepted = 0
    accepted_with_edit = 0
    rejected = 0

    total_issues = len(issues)
    for issue_number, issue in enumerate(issues, start=1):
        sentence_index = int(issue.get("sentence_index", 1))
        sentence_index = max(1, min(sentence_index, len(sentences)))

        print("\n" + "=" * 80)
        print(f"Issue {issue_number}/{total_issues}")
        print(f"Sentence index: {sentence_index}")
        print(f"Issue type: {issue.get('issue_type', 'N/A')}")
        print(f"Classification: {issue.get('classification', 'N/A')}")
        print(f"Confidence: {issue.get('confidence', 'N/A')}")
        print(f"Location: {issue.get('location', 'N/A')}")
        print(f"Excerpt: {issue.get('excerpt', issue.get('phrase', 'N/A'))}")
        print(f"Explanation: {issue.get('explanation', 'N/A')}")

        proposed = str(issue.get("minimal_correction", "")).strip()
        if not proposed:
            proposed = str(issue.get("sentence_text", "")).strip()

        print("\nCurrent sentence text:")
        print(str(issue.get("sentence_text", sentences[sentence_index - 1].get("text", ""))))
        print("\nProposed correction (default accept):")
        print(proposed)

        display_context(sentences, sentence_index, args.context_window)

        decision, final_correction = prompt_decision(proposed)
        if decision == "quit":
            print("Stopped by user.")
            break

        if decision == "accepted":
            accepted += 1
        elif decision == "accepted_with_edit":
            accepted_with_edit += 1
        else:
            rejected += 1

        decisions.append(
            {
                "issue_number": issue_number,
                "sentence_index": sentence_index,
                "decision": decision,
                "proposed_correction": proposed,
                "final_correction": final_correction,
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

    decisions_payload = {
        "audit_file": str(args.audit),
        "preprocessed": str(args.preprocessed),
        "metadata": metadata,
        "decisions": decisions,
        "summary": summary,
    }
    args.output.write_text(json.dumps(decisions_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("\n" + "=" * 80)
    print("Review complete.")
    print(json.dumps(summary, indent=2))
    print(f"Wrote decisions to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
