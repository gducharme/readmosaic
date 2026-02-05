#!/usr/bin/env python3
"""Mosaic Orchestrator (MO).

Runs the Mosaic Engineering Stack tools against a manuscript and produces:
1) A Fidelity Context JSON artifact.
2) A Culling Directives markdown report from a local LLM.
"""
from __future__ import annotations

import argparse
import json
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from urllib import request

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from tool_wrapper import TOOL_DEFINITIONS, run_tool, tool_definitions_payload


DEFAULT_BASE_URL = "http://localhost:1234/v1/chat/completions"
DEFAULT_PROMPT_PATH = Path("prompts/Archivist_Core_V1.txt")
NLTK_BOOTSTRAP_PATH = Path("scripts/setup_nltk_data.py")
PREPROCESSING_PATH = Path("scripts/pre_processing.py")
DEFAULT_LM_TIMEOUT_S = 300


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Mosaic Orchestrator to generate a fidelity report and culling directives.",
    )
    parser.add_argument(
        "--file",
        required=True,
        type=Path,
        help="Path to the manuscript .md file to analyze.",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Model identifier for the local LM Studio server.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Base URL for LM Studio chat completions endpoint.",
    )
    parser.add_argument(
        "--prompt",
        type=Path,
        default=DEFAULT_PROMPT_PATH,
        help="Path to the Archivist system prompt file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("mosaic_outputs"),
        help="Directory to write output artifacts.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of parallel tool workers.",
    )
    parser.add_argument(
        "--lm-timeout",
        type=int,
        default=DEFAULT_LM_TIMEOUT_S,
        help="Timeout in seconds for the LM Studio completion request.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.file.exists():
        raise SystemExit(f"Input file not found: {args.file}")
    if args.file.suffix.lower() != ".md":
        raise SystemExit("Input file must be a .md manuscript")
    if not args.prompt.exists():
        raise SystemExit(f"Prompt file not found: {args.prompt}")


def run_nltk_preflight() -> None:
    if not NLTK_BOOTSTRAP_PATH.exists():
        raise SystemExit(
            f"NLTK bootstrap script not found: {NLTK_BOOTSTRAP_PATH}"
        )
    subprocess.run(
        [sys.executable, str(NLTK_BOOTSTRAP_PATH), "--quiet"],
        check=True,
    )


def run_tools_with_progress(
    input_path: Path,
    output_root: Path,
    max_workers: int,
    preprocessing_dir: Path,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    console = Console()
    console.print("\n[bold cyan]Initiating Neutrino Scan...[/bold cyan]")

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Mosaic tool sweep", total=len(TOOL_DEFINITIONS))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {}
            for tool in TOOL_DEFINITIONS:
                tool_output_dir = output_root / tool.code.lower()
                future = executor.submit(
                    run_tool, tool, input_path, tool_output_dir, preprocessing_dir
                )
                future_map[future] = tool
            for future in as_completed(future_map):
                tool = future_map[future]
                try:
                    result = future.result()
                    results.append(
                        {
                            "code": result.code,
                            "name": result.name,
                            "description": result.description,
                            "status": result.status,
                            "summary": result.summary,
                            "duration_s": result.duration_s,
                            "output_path": str(result.output_path),
                            "edits_path": str(result.edits_path)
                            if result.edits_path
                            else None,
                            "edits_item_count": result.edits_item_count,
                            "stderr": result.stderr if result.status == "error" else None,
                        }
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    results.append(
                        {
                            "code": tool.code,
                            "name": tool.name,
                            "description": tool.description,
                            "status": "error",
                            "summary": {},
                            "duration_s": 0.0,
                            "output_path": None,
                            "edits_path": None,
                            "edits_item_count": None,
                            "stderr": str(exc),
                        }
                    )
                progress.advance(task)
    return results


def build_fidelity_context(
    args: argparse.Namespace, tool_results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "source_file": str(args.file),
        "model": args.model,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "tool_definitions": tool_definitions_payload(),
        "tool_results": tool_results,
    }


def run_preprocessing(input_path: Path, output_root: Path) -> Path:
    if not PREPROCESSING_PATH.exists():
        raise SystemExit(
            f"Pre-processing script not found: {PREPROCESSING_PATH}"
        )
    preprocessing_dir = output_root / "preprocessing"
    preprocessing_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable,
            str(PREPROCESSING_PATH),
            str(input_path),
            "--output-dir",
            str(preprocessing_dir),
            "--manuscript-id",
            input_path.stem,
        ],
        check=True,
    )
    return preprocessing_dir


def call_lm_studio(
    base_url: str,
    model: str,
    system_prompt: str,
    input_text: str,
    fidelity_context: Dict[str, Any],
    timeout_s: int,
) -> str:
    user_instruction = (
        "Analyze the following metrics. Identify where the 'Semantic Muzzle' is strongest. "
        "Recommend specific deletions to maximize Signal and remove Slop. "
        "Use the provided Surprisal and Entropy scores to justify your culls.\n\n"
        "Original Manuscript:\n"
        f"{input_text}\n\n"
        "Fidelity Context:\n"
        f"{json.dumps(fidelity_context, indent=2)}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_instruction},
        ],
        "temperature": 0.2,
    }
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        base_url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=timeout_s) as response:
        response_data = response.read().decode("utf-8")
    completion = json.loads(response_data)
    return completion["choices"][0]["message"]["content"]


def main() -> None:
    args = parse_args()
    validate_args(args)

    output_root = args.output_dir
    output_root.mkdir(parents=True, exist_ok=True)

    run_nltk_preflight()
    preprocessing_dir = run_preprocessing(args.file, output_root)
    tool_results = run_tools_with_progress(
        args.file, output_root, args.max_workers, preprocessing_dir
    )
    fidelity_context = build_fidelity_context(args, tool_results)

    fidelity_path = output_root / "fidelity_context.json"
    fidelity_path.write_text(json.dumps(fidelity_context, indent=2), encoding="utf-8")

    system_prompt = args.prompt.read_text(encoding="utf-8")
    input_text = args.file.read_text(encoding="utf-8")

    console = Console()
    console.print("\n[bold]Feeding Archivist model...[/bold]")
    try:
        directives = call_lm_studio(
            args.base_url,
            args.model,
            system_prompt,
            input_text,
            fidelity_context,
            args.lm_timeout,
        )
    except Exception as exc:  # pragma: no cover - network or runtime error
        console.print(f"[red]Failed to contact LM Studio: {exc}[/red]")
        sys.exit(1)

    directives_path = output_root / "culling_directives.md"
    directives_path.write_text(directives.strip() + "\n", encoding="utf-8")

    console.print("\n[bold green]Artifacts written:[/bold green]")
    console.print(f"- Fidelity Context: {fidelity_path}")
    console.print(f"- Culling Directives: {directives_path}")


if __name__ == "__main__":
    main()
