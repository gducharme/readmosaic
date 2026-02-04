#!/usr/bin/env python3
"""Mosaic Interactive Refiner (MIR).

This CLI ingests a Markdown manuscript and a JSON edit report, then guides you
through block-level refinements using a local LM Studio model.
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import difflib
import requests
from rapidfuzz import fuzz, process
from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

DEFAULT_WORKDIR = Path("mosaic_work")
DEFAULT_API_URL = "http://localhost:1234/v1/chat/completions"


@dataclass
class EditRecord:
    edit_id: int
    action: str
    search_anchor: str
    reason: str
    location_hint: str
    replace_with: Optional[str]
    status: str
    last_refined: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Mosaic Interactive Refiner (MIR): refine manuscript paragraphs with "
            "LM Studio and approve changes via a rich CLI UI."
        )
    )
    parser.add_argument("input_file", type=Path, help="Markdown manuscript to refine.")
    parser.add_argument("edits_json", type=Path, help="JSON report of suggested edits.")
    parser.add_argument(
        "--workdir",
        type=Path,
        default=DEFAULT_WORKDIR,
        help="State directory for session_state.json (default: ./mosaic_work).",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Model ID to use in LM Studio (e.g., llama3:8b-instruct-q8_0).",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help="LM Studio chat completions URL (default: http://localhost:1234/v1/chat/completions).",
    )
    return parser.parse_args()


def load_edits(edits_path: Path) -> List[Dict[str, Any]]:
    with edits_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def split_paragraphs(text: str) -> List[str]:
    paragraphs: List[str] = []
    buffer: List[str] = []
    for line in text.splitlines():
        if line.strip() == "":
            if buffer:
                paragraphs.append("\n".join(buffer).strip())
                buffer = []
            continue
        buffer.append(line.rstrip())
    if buffer:
        paragraphs.append("\n".join(buffer).strip())
    return paragraphs


def join_paragraphs(paragraphs: List[str]) -> str:
    return "\n\n".join(paragraphs).strip() + "\n"


def locate_paragraph(paragraphs: List[str], anchor: str) -> tuple[int, int]:
    if not paragraphs:
        raise ValueError("No paragraphs found in manuscript.")
    best = process.extractOne(anchor, paragraphs, scorer=fuzz.partial_ratio)
    if best is None:
        raise ValueError("No matching paragraph found for anchor.")
    match_text, score, index = best
    return index, int(score)


def build_instruction(edit: EditRecord) -> str:
    if edit.action == "delete":
        return (
            "Remove the targeted content around the anchor while keeping the paragraph "
            "coherent and information-dense."
        )
    if edit.action == "replace" and edit.replace_with:
        return (
            "Replace the targeted phrasing around the anchor with the provided "
            f"replacement: '{edit.replace_with}'."
        )
    return "Refine the paragraph to increase information density and remove AI-slop."


def call_lm_studio(
    api_url: str, model: str, paragraph: str, instruction: str, reason: str
) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are the Mosaic Archivist. Increase information density (entropy) "
                    "and remove AI-slop. Output strictly the refined paragraph only. "
                    "No preamble, no commentary."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Instruction: "
                    + instruction
                    + "\nReason: "
                    + reason
                    + "\n\nOriginal paragraph:\n"
                    + paragraph
                ),
            },
        ],
        "temperature": 0.4,
    }
    response = requests.post(api_url, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def highlight_diff(original: str, refined: str) -> tuple[Text, Text]:
    original_words = original.split()
    refined_words = refined.split()
    matcher = difflib.SequenceMatcher(a=original_words, b=refined_words)

    before_text = Text()
    after_text = Text()
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            before_text.append(" ".join(original_words[i1:i2]) + " ")
            after_text.append(" ".join(refined_words[j1:j2]) + " ")
        elif tag == "delete":
            before_text.append(" ".join(original_words[i1:i2]) + " ", style="red")
        elif tag == "insert":
            after_text.append(" ".join(refined_words[j1:j2]) + " ", style="green")
        elif tag == "replace":
            before_text.append(" ".join(original_words[i1:i2]) + " ", style="red")
            after_text.append(" ".join(refined_words[j1:j2]) + " ", style="green")
    return before_text, after_text


def render_edit_view(
    console: Console,
    edit: EditRecord,
    score: int,
    original: str,
    refined: str,
    context_before: Optional[str],
    context_after: Optional[str],
) -> None:
    instruction = build_instruction(edit)
    header = (
        f"Edit #{edit.edit_id} | Status: {edit.status}\n"
        f"Instruction: {instruction}\n"
        f"Reason: {edit.reason}\n"
        f"Anchor match score: {score}"
    )
    console.print(Panel(header, title="Mosaic Interactive Refiner", style="bold cyan"))

    context_text = Text()
    if context_before:
        context_text.append("Context (before):\n", style="dim")
        context_text.append(context_before + "\n\n", style="dim")
    context_text.append("Target paragraph:\n", style="bold")
    context_text.append(original + "\n\n")
    if context_after:
        context_text.append("Context (after):\n", style="dim")
        context_text.append(context_after + "\n", style="dim")
    console.print(Panel(context_text, title="Context", style="white"))

    before_text, after_text = highlight_diff(original, refined)
    diff_columns = Columns(
        [
            Panel(before_text, title="Before", style="red"),
            Panel(after_text, title="After", style="green"),
        ],
        equal=True,
        expand=True,
    )
    console.print(Panel(diff_columns, title="Diff", style="white"))
    console.print("[A]pprove  [N]egate  [R]egenerate  [Q]uit", style="bold yellow")


def load_state(state_path: Path) -> Optional[Dict[str, Any]]:
    if not state_path.exists():
        return None
    with state_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_state(state_path: Path, state: Dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, ensure_ascii=False)


def hydrate_edits(edits_raw: List[Dict[str, Any]]) -> List[EditRecord]:
    edits: List[EditRecord] = []
    for idx, item in enumerate(edits_raw, start=1):
        edits.append(
            EditRecord(
                edit_id=idx,
                action=item.get("action", "replace"),
                search_anchor=item.get("search_anchor", ""),
                reason=item.get("reason", "Unspecified"),
                location_hint=item.get("location_hint", ""),
                replace_with=item.get("replace_with"),
                status=item.get("status", "PENDING"),
                last_refined=item.get("last_refined"),
            )
        )
    return edits


def serialize_edits(edits: List[EditRecord]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for edit in edits:
        payload.append(
            {
                "edit_id": edit.edit_id,
                "action": edit.action,
                "search_anchor": edit.search_anchor,
                "reason": edit.reason,
                "location_hint": edit.location_hint,
                "replace_with": edit.replace_with,
                "status": edit.status,
                "last_refined": edit.last_refined,
            }
        )
    return payload


def initialize_state(
    input_file: Path, edits_json: Path, workdir: Path, model: str
) -> Dict[str, Any]:
    manuscript = input_file.read_text(encoding="utf-8")
    edits_raw = load_edits(edits_json)
    edits: List[Dict[str, Any]] = []
    for idx, item in enumerate(edits_raw, start=1):
        entry = dict(item)
        entry["edit_id"] = idx
        entry["status"] = "PENDING"
        entry["last_refined"] = None
        edits.append(entry)
    return {
        "input_file": str(input_file),
        "edits_json": str(edits_json),
        "model": model,
        "manuscript": manuscript,
        "edits": edits,
    }


def prompt_choice() -> str:
    choice = input("Select action: ").strip().lower()
    return choice


def main() -> int:
    args = parse_args()
    console = Console()

    state_path = args.workdir / "session_state.json"
    state = load_state(state_path)
    if state is None:
        state = initialize_state(args.input_file, args.edits_json, args.workdir, args.model)
        save_state(state_path, state)

    manuscript = state.get("manuscript", "")
    edits = hydrate_edits(state.get("edits", []))

    def submit_refinement(
        target_edit: EditRecord, paragraph: str
    ) -> Future[str]:
        instruction = build_instruction(target_edit)
        return executor.submit(
            call_lm_studio,
            args.api_url,
            args.model,
            paragraph,
            instruction,
            target_edit.reason,
        )

    executor = ThreadPoolExecutor(max_workers=1)
    futures: Dict[int, Future[str]] = {}

    for edit in edits:
        if edit.status not in {"PENDING", "REGENERATING"}:
            continue

        paragraphs = split_paragraphs(manuscript)
        try:
            paragraph_index, score = locate_paragraph(paragraphs, edit.search_anchor)
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            edit.status = "NEGATED"
            save_state(state_path, {**state, "edits": serialize_edits(edits)})
            continue

        context_before = paragraphs[paragraph_index - 1] if paragraph_index > 0 else None
        context_after = (
            paragraphs[paragraph_index + 1]
            if paragraph_index + 1 < len(paragraphs)
            else None
        )
        original_paragraph = paragraphs[paragraph_index]

        if not edit.last_refined and edit.edit_id not in futures:
            edit.status = "REGENERATING"
            save_state(state_path, {**state, "edits": serialize_edits(edits)})
            futures[edit.edit_id] = submit_refinement(edit, original_paragraph)

        if edit.last_refined:
            refined = edit.last_refined
        else:
            with console.status("[bold cyan]Refining paragraph with LM Studio..."):
                refined = futures[edit.edit_id].result()
            edit.last_refined = refined
            edit.status = "PENDING"
            save_state(state_path, {**state, "edits": serialize_edits(edits)})

        while True:
            render_edit_view(
                console,
                edit,
                score,
                original_paragraph,
                refined,
                context_before,
                context_after,
            )
            choice = prompt_choice()
            if choice == "a":
                paragraphs[paragraph_index] = refined
                manuscript = join_paragraphs(paragraphs)
                edit.status = "APPROVED"
                edit.last_refined = None
                break
            if choice == "n":
                edit.status = "NEGATED"
                edit.last_refined = None
                break
            if choice == "r":
                edit.last_refined = None
                edit.status = "REGENERATING"
                save_state(state_path, {**state, "edits": serialize_edits(edits)})
                futures[edit.edit_id] = submit_refinement(edit, original_paragraph)
                with console.status("[bold cyan]Regenerating with LM Studio..."):
                    refined = futures[edit.edit_id].result()
                edit.last_refined = refined
                edit.status = "PENDING"
                save_state(state_path, {**state, "edits": serialize_edits(edits)})
            if choice == "q":
                state["manuscript"] = manuscript
                state["edits"] = serialize_edits(edits)
                save_state(state_path, state)
                console.print("Progress saved. Exiting.")
                return 0
            if choice not in {"a", "n", "r", "q"}:
                console.print("Invalid choice. Please select A, N, R, or Q.")

        state["manuscript"] = manuscript
        state["edits"] = serialize_edits(edits)
        save_state(state_path, state)

        for upcoming in edits:
            if upcoming.status == "PENDING" and upcoming.edit_id not in futures:
                upcoming_paragraphs = split_paragraphs(manuscript)
                try:
                    upcoming_index, _ = locate_paragraph(
                        upcoming_paragraphs, upcoming.search_anchor
                    )
                except ValueError:
                    continue
                futures[upcoming.edit_id] = submit_refinement(
                    upcoming, upcoming_paragraphs[upcoming_index]
                )
                break

    approved = sum(1 for edit in edits if edit.status == "APPROVED")
    negated = sum(1 for edit in edits if edit.status == "NEGATED")
    output_path = args.input_file.with_name(
        f"{args.input_file.stem}_mosaic_final.md"
    )
    output_path.write_text(manuscript, encoding="utf-8")
    executor.shutdown(wait=False)
    console.print(
        Panel(
            (
                "All edits processed.\n"
                f"Approved: {approved}\n"
                f"Negated: {negated}\n"
                f"Final manuscript: {output_path}"
            ),
            title="Summary",
            style="bold green",
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
