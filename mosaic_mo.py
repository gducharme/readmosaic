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
import uuid
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
PARAGRAPH_BUNDLE_PATH = Path("scripts/paragraph_issue_bundle.py")
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
    args: argparse.Namespace,
    tool_results: List[Dict[str, Any]],
    objective_path: Path | None = None,
    proposal_path: Path | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "source_file": str(args.file),
        "model": args.model,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "tool_definitions": tool_definitions_payload(),
        "tool_results": tool_results,
    }
    if objective_path:
        payload["objective_artifact"] = str(objective_path)
    if proposal_path:
        payload["proposal_artifact"] = str(proposal_path)
    return payload


def load_issues_from_tool_results(tool_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    for result in tool_results:
        edits_path = result.get("edits_path")
        if not edits_path:
            continue
        path = Path(edits_path)
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        for issue in payload.get("items", []):
            if not isinstance(issue, dict):
                continue
            normalized = dict(issue)
            normalized.setdefault("issue_id", str(uuid.uuid4()))
            normalized["source_tool"] = result.get("code")
            issues.append(normalized)
    return issues


def build_objective_for_issue(issue: Dict[str, Any]) -> Dict[str, Any]:
    issue_id = str(issue.get("issue_id") or str(uuid.uuid4()))
    issue_type = str(issue.get("type") or "style")
    objective_id = f"objective-{issue_id}"
    return {
        "objective_id": objective_id,
        "issue_id": issue_id,
        "hard_constraints": [
            {
                "type": "entity",
                "value": str(issue.get("location", {}).get("anchor_text") or "Preserve anchored entities and claims."),
                "rationale": "Anchor-linked terms should survive edits.",
            }
        ],
        "soft_constraints": [
            "Maintain original authorial voice.",
            "Prefer concise, direct phrasing.",
            f"Prioritize improvements for issue type '{issue_type}'.",
        ],
        "metric_targets": {
            "surprisal_range": {"min": 0.2, "max": 0.8},
            "entropy_delta_bounds": {"min": -0.15, "max": 0.25},
            "repetition_reduction_pct": {"target_min": 5, "target_max": 35},
        },
        "acceptance_thresholds": {
            "min_required_metrics": 3,
            "must_pass": ["hard_constraints_preserved", "surprisal_range"],
            "notes": "Accept only if hard constraints are preserved and metric targets trend positive.",
        },
        "rollback_criteria": [
            "Any hard constraint is violated.",
            "Entropy delta drops below lower bound.",
            "Repetition increases relative to baseline.",
        ],
        "extensions": {
            "source_tool": issue.get("source_tool"),
            "issue_type": issue_type,
        },
    }


def build_objectives_payload(manuscript_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "items": [build_objective_for_issue(issue) for issue in issues],
    }


def build_proposals_payload(manuscript_id: str, objectives: Dict[str, Any]) -> Dict[str, Any]:
    items = []
    for objective in objectives.get("items", []):
        issue_id = objective.get("issue_id")
        objective_id = objective.get("objective_id")
        items.append(
            {
                "issue_id": issue_id,
                "proposal_id": f"proposal-{issue_id}",
                "objective_id": objective_id,
                "status": "pending",
            }
        )
    return {
        "schema_version": "1.0",
        "manuscript_id": manuscript_id,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "items": items,
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


def build_paragraph_issue_bundle(
    preprocessing_dir: Path,
    tool_results: List[Dict[str, Any]],
    output_root: Path,
    manuscript_id: str,
) -> Path:
    if not PARAGRAPH_BUNDLE_PATH.exists():
        raise SystemExit(
            f"Paragraph issue bundle script not found: {PARAGRAPH_BUNDLE_PATH}"
        )

    tool_results_path = output_root / "tool_results.json"
    tool_results_path.write_text(json.dumps(tool_results, indent=2), encoding="utf-8")

    bundle_path = output_root / "paragraph_issue_bundle.json"
    subprocess.run(
        [
            sys.executable,
            str(PARAGRAPH_BUNDLE_PATH),
            "--preprocessing",
            str(preprocessing_dir),
            "--tool-results",
            str(tool_results_path),
            "--output",
            str(bundle_path),
            "--manuscript-id",
            manuscript_id,
        ],
        check=True,
    )
    return bundle_path


def load_issues_from_bundle(bundle_path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    items = payload.get("items", []) if isinstance(payload, dict) else []
    issues: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for issue in item.get("issues", []):
            if isinstance(issue, dict):
                issues.append(issue)
    return issues


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
    paragraph_bundle_path = build_paragraph_issue_bundle(
        preprocessing_dir,
        tool_results,
        output_root,
        args.file.stem,
    )
    issues = load_issues_from_bundle(paragraph_bundle_path)
    objectives_payload = build_objectives_payload(args.file.stem, issues)
    objectives_path = output_root / "edit_objectives.json"
    objectives_path.write_text(json.dumps(objectives_payload, indent=2), encoding="utf-8")

    proposals_payload = build_proposals_payload(args.file.stem, objectives_payload)
    proposals_path = output_root / "proposal_payload.json"
    proposals_path.write_text(json.dumps(proposals_payload, indent=2), encoding="utf-8")

    fidelity_context = build_fidelity_context(
        args,
        tool_results,
        objective_path=objectives_path,
        proposal_path=proposals_path,
    )

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
    console.print(f"- Edit Objectives: {objectives_path}")
    console.print(f"- Proposal Payload: {proposals_path}")
    console.print(f"- Paragraph Issue Bundle: {paragraph_bundle_path}")


if __name__ == "__main__":
    main()
