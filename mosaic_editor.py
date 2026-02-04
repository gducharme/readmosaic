#!/usr/bin/env python3
"""Mosaic Surgical Editor (MSE).

Applies a structured edits.json map to a markdown manuscript with fuzzy matching,
producing a draft output and an HTML diff for review.
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from rapidfuzz import fuzz, process


@dataclass
class EditInstruction:
    action: str
    search_anchor: str
    location_hint: str | None = None
    reason: str | None = None
    replace_with: str | None = None
    global_replace: bool = False
    entropy: str | None = None


GLOBAL_SCRUB_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bjust\b", flags=re.IGNORECASE), ""),
]


def load_edits(path: Path) -> List[EditInstruction]:
    data = json.loads(path.read_text(encoding="utf-8"))
    edits = []
    for entry in data:
        edits.append(
            EditInstruction(
                action=entry["action"],
                search_anchor=entry.get("search_anchor", ""),
                location_hint=entry.get("location_hint"),
                reason=entry.get("reason"),
                replace_with=entry.get("replace_with"),
                global_replace=bool(entry.get("global", False)),
                entropy=entry.get("entropy"),
            )
        )
    return edits


def split_sections(text: str) -> List[str]:
    sections = re.split(r"(?m)^(?=#+\s)", text)
    return [section for section in sections if section.strip()]


def find_section(text: str, location_hint: Optional[str]) -> str:
    if not location_hint:
        return text
    sections = split_sections(text)
    for section in sections:
        header_match = re.match(r"(?m)^#+\s+(.*)$", section)
        if header_match and location_hint.lower() in header_match.group(1).lower():
            return section
    return text


def remove_paragraph(text: str, paragraph: str) -> str:
    escaped = re.escape(paragraph)
    pattern = re.compile(rf"\n*{escaped}\n*", flags=re.MULTILINE)
    return pattern.sub("\n\n", text)


def apply_delete(text: str, edit: EditInstruction, threshold: int) -> Tuple[str, Optional[str]]:
    target_section = find_section(text, edit.location_hint)
    paragraphs = [p for p in target_section.split("\n\n") if p.strip()]
    if not paragraphs:
        return text, None
    match = process.extractOne(
        edit.search_anchor,
        paragraphs,
        scorer=fuzz.partial_ratio,
    )
    if not match:
        return text, None
    matched_paragraph, score, _ = match
    if score < threshold:
        return text, None
    updated_section = remove_paragraph(target_section, matched_paragraph)
    return text.replace(target_section, updated_section, 1), matched_paragraph


def apply_replace(text: str, edit: EditInstruction) -> Tuple[str, int]:
    target_section = find_section(text, edit.location_hint)
    if edit.replace_with is None:
        return text, 0
    if edit.global_replace:
        updated_section, count = re.subn(re.escape(edit.search_anchor), edit.replace_with, target_section)
    else:
        updated_section, count = re.subn(
            re.escape(edit.search_anchor),
            edit.replace_with,
            target_section,
            count=1,
        )
    return text.replace(target_section, updated_section, 1), count


def apply_global_scrub(text: str) -> Tuple[str, List[str]]:
    applied = []
    for pattern, replacement in GLOBAL_SCRUB_PATTERNS:
        if pattern.search(text):
            text = pattern.sub(replacement, text)
            applied.append(pattern.pattern)
    return text, applied


def collect_entropy_warning(text: str, edit: EditInstruction) -> Optional[str]:
    target_section = find_section(text, edit.location_hint)
    paragraphs = [p for p in target_section.split("\n\n") if p.strip()]
    if not paragraphs:
        return None
    match = process.extractOne(
        edit.search_anchor,
        paragraphs,
        scorer=fuzz.partial_ratio,
    )
    if not match:
        return None
    matched_paragraph, score, _ = match
    if score < 90:
        return None
    entropy_note = f"Entropy: {edit.entropy}" if edit.entropy else "Entropy: unknown"
    prompt = (
        "The following section is mathematically redundant ("
        f"{entropy_note}). Rewrite this to increase information density while maintaining the Archivist tone."  # noqa: E501
    )
    return f"{prompt}\n\n{matched_paragraph}\n"


def render_diff(original: str, updated: str, output_path: Path) -> None:
    diff = difflib.HtmlDiff(tabsize=2, wrapcolumn=80)
    html = diff.make_file(
        original.splitlines(),
        updated.splitlines(),
        fromdesc="original",
        todesc="draft",
    )
    output_path.write_text(html, encoding="utf-8")


def apply_edits(
    manuscript: str,
    edits: Iterable[EditInstruction],
    threshold: int,
    scratchpad_path: Path,
) -> Tuple[str, List[str]]:
    updated = manuscript
    entropy_notes: List[str] = []
    for edit in edits:
        action = edit.action.lower()
        if action == "delete":
            updated, _ = apply_delete(updated, edit, threshold)
        elif action == "replace":
            updated, _ = apply_replace(updated, edit)
        elif action in {"entropy_warning", "rewrite"}:
            note = collect_entropy_warning(updated, edit)
            if note:
                entropy_notes.append(note)
        else:
            raise ValueError(f"Unknown action: {edit.action}")
    updated, scrubbed = apply_global_scrub(updated)
    if scrubbed:
        entropy_notes.append(
            "Global scrubbing applied with patterns: " + ", ".join(scrubbed)
        )
    if entropy_notes:
        scratchpad_path.write_text("\n\n".join(entropy_notes).strip() + "\n", encoding="utf-8")
    return updated, entropy_notes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Apply Mosaic Orchestrator edits.json directives to a manuscript "
            "using fuzzy matching, and generate a draft plus HTML diff."
        )
    )
    parser.add_argument("manuscript", type=Path, help="Path to the markdown manuscript")
    parser.add_argument("edits", type=Path, help="Path to edits.json")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("manuscript_v2_DRAFT.md"),
        help="Draft manuscript output path (default: manuscript_v2_DRAFT.md)",
    )
    parser.add_argument(
        "--diff",
        type=Path,
        default=Path("changes.diff"),
        help="HTML diff output path (default: changes.diff)",
    )
    parser.add_argument(
        "--scratchpad",
        type=Path,
        default=Path("rewrite_scratchpad.md"),
        help="Entropy rewrite scratchpad output path",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=90,
        help="Fuzzy match threshold for deletions (default: 90)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    manuscript = args.manuscript.read_text(encoding="utf-8")
    edits = load_edits(args.edits)

    updated, _ = apply_edits(manuscript, edits, args.threshold, args.scratchpad)
    args.output.write_text(updated, encoding="utf-8")
    render_diff(manuscript, updated, args.diff)


if __name__ == "__main__":
    main()
