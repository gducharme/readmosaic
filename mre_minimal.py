#!/usr/bin/env python3
"""Minimal Mosaic Recursive Engine (MRE) prototype."""
from __future__ import annotations

import argparse
import difflib
import importlib.util
import inspect
import json
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from rapidfuzz import fuzz, process


DEFAULT_BASE_URL = "http://localhost:1234/v1/chat/completions"
DEFAULT_PROMPT_PATH = Path("prompts/MRE_Archivist_Minimal.txt")
DEFAULT_OUTPUT_PATH = Path("mre_outputs/approved_manuscript.md")
DEFAULT_TOOLS_DIR = Path("tools")
CAPABILITIES_LOG = Path("mosaic_capabilities.log")


@dataclass
class DiagnosticItem:
    failure: str
    anchor: str
    paragraph_index: Optional[int]


@dataclass
class ToolEntry:
    name: str
    description: str
    signature: str


@dataclass
class StagedEdit:
    failure: str
    tool_name: str
    forged: bool
    paragraph_index: int
    anchor: str
    before: str
    after: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a minimal Mosaic Recursive Engine pass over a manuscript.",
    )
    parser.add_argument("--file", type=Path, required=True, help="Path to the manuscript (.md).")
    parser.add_argument("--diagnostics", type=Path, required=True, help="Path to diagnostics JSON.")
    parser.add_argument("--model", required=True, help="LM Studio model identifier.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="LM Studio chat completions URL.")
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT_PATH, help="System prompt path.")
    parser.add_argument("--tools-dir", type=Path, default=DEFAULT_TOOLS_DIR, help="Directory for forged tools.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output manuscript path.")
    parser.add_argument("--max-forge", type=int, default=2, help="Max forge attempts per diagnostic.")
    parser.add_argument("--max-fix", type=int, default=2, help="Max fix attempts per tool failure.")
    parser.add_argument("--threshold", type=int, default=70, help="Anchor match threshold (0-100).")
    return parser.parse_args()


def load_diagnostics(path: Path) -> List[DiagnosticItem]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        items = payload.get("items", [])
    elif isinstance(payload, list):
        items = payload
    else:
        raise ValueError("Diagnostics JSON must be a list or an object with 'items'.")
    diagnostics: List[DiagnosticItem] = []
    for item in items:
        failure = str(item.get("failure") or item.get("reason") or item.get("description") or "")
        anchor = str(item.get("anchor") or item.get("search_anchor") or "")
        index = item.get("paragraph_index")
        diagnostics.append(DiagnosticItem(failure=failure, anchor=anchor, paragraph_index=index))
    return diagnostics


def split_paragraphs(text: str) -> List[str]:
    return [block.strip() for block in text.split("\n\n") if block.strip()]


def find_paragraph_index(anchor: str, paragraphs: List[str], threshold: int) -> int:
    if not anchor:
        return 0
    match = process.extractOne(anchor, paragraphs, scorer=fuzz.token_set_ratio)
    if not match:
        return 0
    _, score, index = match
    if score < threshold:
        return 0
    return int(index)


def load_module_from_path(path: Path) -> Any:
    module_name = f"mre_tool_{path.stem}_{int(path.stat().st_mtime)}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_manifest(tools_dir: Path) -> List[ToolEntry]:
    manifest: List[ToolEntry] = []
    for tool_path in sorted(tools_dir.glob("*.py")):
        if tool_path.name == "__init__.py":
            continue
        try:
            module = load_module_from_path(tool_path)
        except Exception:
            continue
        description = (module.__doc__ or "").strip()
        run_tool = getattr(module, "run_tool", None)
        signature = "run_tool(text_block, anchor, params=None)"
        if callable(run_tool):
            signature = str(inspect.signature(run_tool))
        manifest.append(
            ToolEntry(
                name=tool_path.stem,
                description=description,
                signature=signature,
            )
        )
    return manifest


def manifest_payload(manifest: Iterable[ToolEntry]) -> List[Dict[str, str]]:
    return [
        {
            "name": entry.name,
            "description": entry.description,
            "signature": entry.signature,
        }
        for entry in manifest
    ]


def call_lm_studio(
    base_url: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    response = requests.post(base_url, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


def extract_json(response_text: str) -> Dict[str, Any]:
    response_text = response_text.strip()
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for idx, char in enumerate(response_text):
            if char != "{":
                continue
            try:
                obj, _ = decoder.raw_decode(response_text[idx:])
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                continue
    raise ValueError("LM response did not contain valid JSON.")


def save_tool_code(tools_dir: Path, tool_name: str, tool_code: str) -> Path:
    tools_dir.mkdir(parents=True, exist_ok=True)
    tool_path = tools_dir / f"{tool_name}.py"
    tool_path.write_text(tool_code.strip() + "\n", encoding="utf-8")
    timestamp = datetime.utcnow().isoformat() + "Z"
    with CAPABILITIES_LOG.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} {tool_name}\n")
    return tool_path


def run_tool(
    tools_dir: Path,
    tool_name: str,
    text_block: str,
    anchor: str,
    params: Optional[Dict[str, Any]],
    max_fix: int,
    base_url: str,
    model: str,
    system_prompt: str,
) -> str:
    tool_path = tools_dir / f"{tool_name}.py"
    if not tool_path.exists():
        raise FileNotFoundError(f"Tool not found: {tool_path}")
    attempt = 0
    last_error = None
    while attempt <= max_fix:
        try:
            module = load_module_from_path(tool_path)
            run_fn = getattr(module, "run_tool", None)
            if not callable(run_fn):
                raise AttributeError(f"run_tool not found in {tool_name}")
            return run_fn(text_block, anchor, params or {})
        except Exception as exc:
            last_error = exc
            traceback_text = traceback.format_exc()
            tool_code = tool_path.read_text(encoding="utf-8")
            fix_prompt = (
                "The forged tool failed to run. Fix the code.\n\n"
                f"Error:\n{traceback_text}\n\n"
                f"Tool code:\n{tool_code}\n\n"
                "Return JSON: {\"action\": \"forge\", \"tool_name\": \"...\", \"tool_code\": \"...\"}"
            )
            response_text = call_lm_studio(base_url, model, system_prompt, fix_prompt)
            response = extract_json(response_text)
            if response.get("action") != "forge":
                raise RuntimeError("LM did not return corrected tool code.") from exc
            save_tool_code(tools_dir, response["tool_name"], response["tool_code"])
            tool_path = tools_dir / f"{response['tool_name']}.py"
            attempt += 1
    raise RuntimeError("Tool failed after fix attempts.") from last_error


def build_user_prompt(
    diagnostic: DiagnosticItem,
    paragraph_index: int,
    paragraph: str,
    manifest: List[ToolEntry],
) -> str:
    return (
        "Failure:\n"
        f"{diagnostic.failure}\n\n"
        "Paragraph:\n"
        f"{paragraph}\n\n"
        f"Paragraph Index: {paragraph_index}\n"
        f"Anchor: {diagnostic.anchor}\n\n"
        "Tool Manifest:\n"
        f"{json.dumps(manifest_payload(manifest), indent=2)}\n"
    )


def process_diagnostic(
    diagnostic: DiagnosticItem,
    paragraphs: List[str],
    manifest: List[ToolEntry],
    args: argparse.Namespace,
    system_prompt: str,
) -> StagedEdit:
    paragraph_index = diagnostic.paragraph_index
    if paragraph_index is None or paragraph_index >= len(paragraphs):
        paragraph_index = find_paragraph_index(diagnostic.anchor, paragraphs, args.threshold)
    paragraph_index = max(0, min(paragraph_index, len(paragraphs) - 1))
    before = paragraphs[paragraph_index]

    forge_attempts = 0
    forged = False
    while forge_attempts <= args.max_forge:
        prompt = build_user_prompt(diagnostic, paragraph_index, before, manifest)
        response_text = call_lm_studio(args.base_url, args.model, system_prompt, prompt)
        response = extract_json(response_text)
        action = response.get("action")
        if action == "forge":
            tool_path = save_tool_code(args.tools_dir, response["tool_name"], response["tool_code"])
            forged = True
            manifest = build_manifest(args.tools_dir)
            forge_attempts += 1
            continue
        if action == "call":
            tool_name = response["tool_name"]
            anchor = response.get("search_anchor") or diagnostic.anchor
            params = response.get("params") or {}
            after = run_tool(
                args.tools_dir,
                tool_name,
                before,
                anchor,
                params,
                args.max_fix,
                args.base_url,
                args.model,
                system_prompt,
            )
            paragraphs[paragraph_index] = after
            return StagedEdit(
                failure=diagnostic.failure,
                tool_name=tool_name,
                forged=forged,
                paragraph_index=paragraph_index,
                anchor=anchor,
                before=before,
                after=after,
            )
        raise RuntimeError("LM response missing action.")
    raise RuntimeError("Exceeded forge attempts.")


def review_edits(
    staged: List[StagedEdit],
    diagnostics: List[DiagnosticItem],
    paragraphs: List[str],
    manifest: List[ToolEntry],
    args: argparse.Namespace,
    system_prompt: str,
) -> List[StagedEdit]:
    approved: List[StagedEdit] = []
    for index, edit in enumerate(staged):
        while True:
            print("\nFailure:", edit.failure)
            print("Tool:", edit.tool_name, "(forged)" if edit.forged else "(existing)")
            diff = difflib.unified_diff(
                edit.before.splitlines(),
                edit.after.splitlines(),
                fromfile="before",
                tofile="after",
                lineterm="",
            )
            print("\n".join(diff))
            choice = input("[A]pprove, [N]egate, [R]edo: ").strip().lower()
            if choice in {"a", "approve"}:
                approved.append(edit)
                break
            if choice in {"n", "negate"}:
                paragraphs[edit.paragraph_index] = edit.before
                break
            if choice in {"r", "redo"}:
                new_edit = process_diagnostic(
                    diagnostics[index],
                    paragraphs,
                    manifest,
                    args,
                    system_prompt,
                )
                staged[index] = new_edit
                edit = new_edit
                continue
            print("Invalid choice. Enter A, N, or R.")
    return approved


def main() -> None:
    args = parse_args()
    manuscript_path = args.file
    diagnostics_path = args.diagnostics
    if not manuscript_path.exists():
        raise SystemExit(f"Manuscript not found: {manuscript_path}")
    if not diagnostics_path.exists():
        raise SystemExit(f"Diagnostics not found: {diagnostics_path}")
    if not args.prompt.exists():
        raise SystemExit(f"Prompt not found: {args.prompt}")

    args.tools_dir.mkdir(parents=True, exist_ok=True)
    system_prompt = args.prompt.read_text(encoding="utf-8")
    diagnostics = load_diagnostics(diagnostics_path)

    manuscript_text = manuscript_path.read_text(encoding="utf-8")
    paragraphs = split_paragraphs(manuscript_text)
    manifest = build_manifest(args.tools_dir)
    staged: List[StagedEdit] = []

    for diagnostic in diagnostics:
        staged.append(
            process_diagnostic(diagnostic, paragraphs, manifest, args, system_prompt)
        )
        manifest = build_manifest(args.tools_dir)

    approved = review_edits(staged, diagnostics, paragraphs, manifest, args, system_prompt)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n\n".join(paragraphs).strip() + "\n", encoding="utf-8")
    print(f"\nApproved edits: {len(approved)}")
    print(f"Output written to: {args.output}")


if __name__ == "__main__":
    main()
