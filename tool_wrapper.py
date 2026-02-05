#!/usr/bin/env python3
"""Helpers for running Mosaic tools and standardizing their outputs."""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


@dataclass(frozen=True)
class ToolDefinition:
    code: str
    name: str
    description: str
    script_path: Path
    build_command: Callable[[Path, Path, Optional[Path]], List[str]]
    parser: Callable[[str, Path], Dict[str, Any]]
    edits_output_name: Optional[str] = None


@dataclass
class ToolResult:
    code: str
    name: str
    description: str
    status: str
    summary: Dict[str, Any]
    stdout: str
    stderr: str
    duration_s: float
    output_path: Path
    edits_path: Optional[Path]
    edits_item_count: Optional[int]


def _parse_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def _parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def parse_sra(stdout: str, _: Path) -> Dict[str, Any]:
    redundancy = None
    entropy = None
    sentences = None
    for line in stdout.splitlines():
        if "Redundancy score" in line:
            match = re.search(r":\s*([0-9.]+)%", line)
            if match:
                redundancy = _parse_float(match.group(1))
        if "Semantic entropy" in line:
            match = re.search(r":\s*([0-9.]+)%", line)
            if match:
                entropy = _parse_float(match.group(1))
        if "Sentences analyzed" in line:
            match = re.search(r":\s*(\d+)", line)
            if match:
                sentences = _parse_int(match.group(1))
    return {
        "sentences_analyzed": sentences,
        "redundancy_pct": redundancy,
        "semantic_entropy_pct": entropy,
    }


def parse_lpe(stdout: str, _: Path) -> Dict[str, Any]:
    entropy = None
    tokens = None
    densities: Dict[str, float] = {}
    in_density = False
    for line in stdout.splitlines():
        if "Structural Entropy Score" in line:
            match = re.search(r"Score:.*?(\d+\.\d+)", line)
            if match:
                entropy = _parse_float(match.group(1))
        if "Total tokens analyzed" in line:
            match = re.search(r":\s*(\d+)", line)
            if match:
                tokens = _parse_int(match.group(1))
        if "Pattern density per 1k tokens" in line:
            in_density = True
            continue
        if in_density:
            if not line.strip():
                in_density = False
                continue
            match = re.match(r"-\s+(.*?):\s+([0-9.]+)", line.strip())
            if match:
                densities[match.group(1)] = float(match.group(2))
    return {
        "structural_entropy": entropy,
        "tokens_analyzed": tokens,
        "pattern_density_per_1k": densities,
    }


def parse_ctm(stdout: str, _: Path) -> Dict[str, Any]:
    chunks = None
    vocab = None
    coherence = None
    topics: List[str] = []
    for line in stdout.splitlines():
        if "Chunks analyzed" in line:
            match = re.search(r":\s*(\d+)", line)
            if match:
                chunks = _parse_int(match.group(1))
        if "Vocabulary size" in line:
            match = re.search(r":\s*(\d+)", line)
            if match:
                vocab = _parse_int(match.group(1))
        if "Coherence Score" in line:
            match = re.search(r":\s*([0-9.]+)", line)
            if match:
                coherence = _parse_float(match.group(1))
        if line.strip().startswith("Topic "):
            topics.append(line.strip())
    return {
        "chunks_analyzed": chunks,
        "vocabulary_size": vocab,
        "coherence_score": coherence,
        "dominant_topics": topics[:5],
    }


def parse_nbm(stdout: str, _: Path) -> Dict[str, Any]:
    tokens = None
    content_tokens = None
    windows = None
    top_terms: List[str] = []
    lines = stdout.splitlines()
    for line in lines:
        if line.startswith("Tokens:"):
            match = re.search(r"Tokens:\s*(\d+)\s*\|\s*Content tokens:\s*(\d+)\s*\|\s*Windows:\s*(\d+)", line)
            if match:
                tokens = _parse_int(match.group(1))
                content_tokens = _parse_int(match.group(2))
                windows = _parse_int(match.group(3))
    if "Top bursty terms:" in stdout:
        table_started = False
        for line in lines:
            if line.strip().startswith("Top bursty terms"):
                table_started = True
                continue
            if table_started:
                if line.strip() == "":
                    break
                if line.strip().startswith("term"):
                    continue
                if line.strip().startswith("-"):
                    continue
                parts = line.split()
                if parts:
                    top_terms.append(parts[0])
        top_terms = top_terms[:5]
    return {
        "tokens": tokens,
        "content_tokens": content_tokens,
        "windows": windows,
        "top_bursty_terms": top_terms,
    }


def parse_see(stdout: str, _: Path) -> Dict[str, Any]:
    stripped = stdout.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(stdout[start : end + 1])
        except json.JSONDecodeError:
            return {}
    return {}


def parse_nss(_: str, output_dir: Path) -> Dict[str, Any]:
    json_path = output_dir / "nss_scores.json"
    if not json_path.exists():
        return {}
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        scores = payload
        model = None
        percentile = None
        threshold = None
    else:
        scores = payload.get("scores", [])
        model = payload.get("model")
        percentile = payload.get("percentile")
        threshold = payload.get("threshold")

    def _score_value(item: Dict[str, Any]) -> float:
        if "surprisal" in item:
            return float(item.get("surprisal", 0.0))
        return float(item.get("avg_logprob", 0.0))

    def _is_slop(item: Dict[str, Any]) -> bool:
        if "is_slop_zone" in item:
            return bool(item.get("is_slop_zone"))
        return bool(item.get("is_slop"))

    if scores:
        average = sum(_score_value(item) for item in scores) / len(scores)
        slop_count = sum(1 for item in scores if _is_slop(item))
    else:
        average = None
        slop_count = 0
    return {
        "model": model,
        "percentile": percentile,
        "threshold": threshold,
        "sentence_count": len(scores),
        "average_surprisal": average,
        "slop_zone_count": slop_count,
    }


def parse_cws(stdout: str, _: Path) -> Dict[str, Any]:
    slop_scores: List[int] = []
    moralizing = False
    for line in stdout.splitlines():
        if "slop score" in line:
            match = re.search(r"slop score:\s*(\d+)", line)
            if match:
                slop_scores.append(int(match.group(1)))
        if "Moralizing Drift detected" in line:
            moralizing = True
    return {
        "paragraph_slop_scores": slop_scores,
        "max_slop_score": max(slop_scores) if slop_scores else None,
        "moralizing_drift": moralizing,
    }


def parse_msd(_: str, output_dir: Path) -> Dict[str, Any]:
    json_path = output_dir / "msd.json"
    if not json_path.exists():
        return {}
    return json.loads(json_path.read_text(encoding="utf-8"))


def parse_dsf(_: str, output_dir: Path) -> Dict[str, Any]:
    json_path = output_dir / "dsf.json"
    if not json_path.exists():
        return {}
    return json.loads(json_path.read_text(encoding="utf-8"))


def _build_command_simple(
    script: str,
    extra: List[str],
    *,
    preprocessing_flag: Optional[str] = None,
    edits_flag: Optional[str] = None,
    edits_output_name: Optional[str] = None,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        command = ["python", script, str(input_path), *extra]
        if preprocessing_flag and preprocessing_dir:
            command.extend([preprocessing_flag, str(preprocessing_dir)])
        if edits_flag and edits_output_name:
            edits_path = output_dir / edits_output_name
            command.extend([edits_flag, str(edits_path)])
        return command

    return _builder


def _build_command_with_output(
    script: str,
    output_name: str,
    extra: List[str],
    *,
    preprocessing_flag: Optional[str] = None,
    edits_flag: Optional[str] = None,
    edits_output_name: Optional[str] = None,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        output_path = output_dir / output_name
        command = ["python", script, str(input_path), *extra, str(output_path)]
        if preprocessing_flag and preprocessing_dir:
            command.extend([preprocessing_flag, str(preprocessing_dir)])
        if edits_flag and edits_output_name:
            edits_path = output_dir / edits_output_name
            command.extend([edits_flag, str(edits_path)])
        return command

    return _builder


def _build_command_see(
    script: str,
    *,
    preprocessing_flag: Optional[str] = None,
    edits_flag: Optional[str] = None,
    edits_output_name: Optional[str] = None,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        command = ["python", script, str(input_path), "--output", str(output_dir)]
        if preprocessing_flag and preprocessing_dir:
            command.extend([preprocessing_flag, str(preprocessing_dir)])
        if edits_flag and edits_output_name:
            edits_path = output_dir / edits_output_name
            command.extend([edits_flag, str(edits_path)])
        return command

    return _builder


def _build_command_nss(
    script: str,
    *,
    preprocessing_flag: Optional[str] = None,
    edits_flag: Optional[str] = None,
    edits_output_name: Optional[str] = None,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        json_path = output_dir / "nss_scores.json"
        command = [
            "python",
            script,
            str(input_path),
            "--model",
            "gpt2",
            "--percentile",
            "90",
            "--output-json",
            str(json_path),
        ]
        if preprocessing_flag and preprocessing_dir:
            command.extend([preprocessing_flag, str(preprocessing_dir)])
        if edits_flag and edits_output_name:
            edits_path = output_dir / edits_output_name
            command.extend([edits_flag, str(edits_path)])
        return command

    return _builder


def _build_command_ctm(
    script: str,
    *,
    preprocessing_flag: Optional[str] = None,
    edits_flag: Optional[str] = None,
    edits_output_name: Optional[str] = None,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        command = [
            "python",
            script,
            str(input_path),
            "--output-dir",
            str(output_dir),
        ]
        if preprocessing_flag and preprocessing_dir:
            command.extend([preprocessing_flag, str(preprocessing_dir)])
        if edits_flag and edits_output_name:
            edits_path = output_dir / edits_output_name
            command.extend([edits_flag, str(edits_path)])
        return command

    return _builder


def _build_command_msd(
    script: str,
    output_name: str,
    edits_output_name: str,
    paragraph_threshold: float,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        output_path = output_dir / output_name
        command = ["python", script, str(input_path), "--output-json", str(output_path)]
        if preprocessing_dir:
            command.extend(
                [
                    "--preprocessing",
                    str(preprocessing_dir),
                    "--paragraph-threshold",
                    str(paragraph_threshold),
                    "--edits-output",
                    str(output_dir / edits_output_name),
                ]
            )
        return command

    return _builder


def _build_command_dsf(
    script: str,
    output_name: str,
    edits_output_name: str,
    ambivalence_threshold: float,
) -> Callable[[Path, Path, Optional[Path]], List[str]]:
    def _builder(
        input_path: Path, output_dir: Path, preprocessing_dir: Optional[Path]
    ) -> List[str]:
        output_path = output_dir / output_name
        command = [
            "python",
            script,
            str(input_path),
            "--output-json",
            str(output_path),
            "--ambivalence-threshold",
            str(ambivalence_threshold),
        ]
        if preprocessing_dir:
            command.extend(
                [
                    "--preprocessing",
                    str(preprocessing_dir),
                    "--output-edits",
                    str(output_dir / edits_output_name),
                ]
            )
        return command

    return _builder


TOOL_DEFINITIONS: List[ToolDefinition] = [
    ToolDefinition(
        code="SRA",
        name="Semantic Repetition Analyzer",
        description="Detects semantic echoes and redundancy in prose.",
        script_path=Path("scripts/analyzer.py"),
        build_command=_build_command_simple(
            "scripts/analyzer.py",
            ["--threshold", "0.85", "--min-length", "20", "--top-n", "5"],
            preprocessing_flag="--preprocessing",
            edits_flag="--output-edits",
            edits_output_name="sra_edits.json",
        ),
        parser=parse_sra,
        edits_output_name="sra_edits.json",
    ),
    ToolDefinition(
        code="LPE",
        name="Linguistic Pattern Extractor",
        description="Extracts phrasal and stylistic patterns to estimate structural entropy.",
        script_path=Path("scripts/pattern_extractor.py"),
        build_command=_build_command_simple(
            "scripts/pattern_extractor.py",
            ["--min-freq", "2", "--top-n", "10"],
            preprocessing_flag="--preprocessing",
            edits_flag="--output-json",
            edits_output_name="lpe_edits.json",
        ),
        parser=parse_lpe,
        edits_output_name="lpe_edits.json",
    ),
    ToolDefinition(
        code="CTM",
        name="Conceptual Theme Mapper",
        description="Maps thematic clusters and topic coherence across the manuscript.",
        script_path=Path("scripts/theme_mapper.py"),
        build_command=_build_command_ctm(
            "scripts/theme_mapper.py",
            preprocessing_flag="--preprocessing",
            edits_flag="--topic-shift-json",
            edits_output_name="ctm_edits.json",
        ),
        parser=parse_ctm,
        edits_output_name="ctm_edits.json",
    ),
    ToolDefinition(
        code="NBM",
        name="Narrative Burst Monitor",
        description="Detects bursty term clusters and hot zones across the text.",
        script_path=Path("scripts/burst_monitor.py"),
        build_command=_build_command_simple(
            "scripts/burst_monitor.py",
            ["--window-size", "500", "--step-size", "100", "--threshold", "3.0", "--top-n", "10"],
            preprocessing_flag="--preprocessing",
            edits_flag="--output-json",
            edits_output_name="nbm_edits.json",
        ),
        parser=parse_nbm,
        edits_output_name="nbm_edits.json",
    ),
    ToolDefinition(
        code="SEE",
        name="Semantic Entropy Evaluator",
        description="Computes Shannon entropy metrics and drift across the manuscript.",
        script_path=Path("scripts/entropy_evaluator.py"),
        build_command=_build_command_see(
            "scripts/entropy_evaluator.py",
            preprocessing_flag="--preprocessing",
            edits_flag="--output-edits",
            edits_output_name="see_edits.json",
        ),
        parser=parse_see,
        edits_output_name="see_edits.json",
    ),
    ToolDefinition(
        code="NSS",
        name="Neutrino Surprisal Scout",
        description="Computes sentence-level surprisal and flags low-signal zones.",
        script_path=Path("scripts/surprisal_scout.py"),
        build_command=_build_command_nss(
            "scripts/surprisal_scout.py",
            preprocessing_flag="--preprocessing",
            edits_flag="--output-edits",
            edits_output_name="nss_edits.json",
        ),
        parser=parse_nss,
        edits_output_name="nss_edits.json",
    ),
    ToolDefinition(
        code="MSD",
        name="Mosaic Signal Density",
        description="Estimates lexical signal density and repetition pressure.",
        script_path=Path("scripts/signal_density.py"),
        build_command=_build_command_msd(
            "scripts/signal_density.py",
            "msd.json",
            "msd_edits.json",
            0.35,
        ),
        parser=parse_msd,
        edits_output_name="msd_edits.json",
    ),
    ToolDefinition(
        code="DSF",
        name="Direct Signal Filter",
        description=(
            "Finds negative pacing, vague intensity clichés, and ambivalent hedging."
        ),
        script_path=Path("scripts/direct_signal_filter.py"),
        build_command=_build_command_dsf(
            "scripts/direct_signal_filter.py",
            "dsf.json",
            "dsf_edits.json",
            0.08,
        ),
        parser=parse_dsf,
        edits_output_name="dsf_edits.json",
    ),
    ToolDefinition(
        code="CWS",
        name="Cliche Wrap-Up Scrubber",
        description="Flags AI-style wrap-up drift and sentiment pivots.",
        script_path=Path("scripts/slop_scrubber.py"),
        build_command=_build_command_simple(
            "scripts/slop_scrubber.py",
            ["--report"],
            preprocessing_flag="--preprocessing",
            edits_flag="--output-json",
            edits_output_name="cws_edits.json",
        ),
        parser=parse_cws,
        edits_output_name="cws_edits.json",
    ),
]


def tool_definitions_payload() -> List[Dict[str, Any]]:
    payload = []
    for tool in TOOL_DEFINITIONS:
        payload.append(
            {
                "name": tool.code,
                "description": tool.description,
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "file": {"type": "string", "description": "Path to the manuscript."}
                    },
                    "required": ["file"],
                },
            }
        )
    return payload


def run_tool(
    tool: ToolDefinition,
    input_path: Path,
    output_dir: Path,
    preprocessing_dir: Optional[Path],
) -> ToolResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    start = datetime.utcnow()
    result = subprocess.run(
        tool.build_command(input_path, output_dir, preprocessing_dir),
        capture_output=True,
        text=True,
    )
    duration = (datetime.utcnow() - start).total_seconds()
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    status = "ok" if result.returncode == 0 else "error"
    summary: Dict[str, Any] = {}
    edits_path = None
    edits_item_count = None
    if status == "ok":
        summary = tool.parser(stdout, output_dir)
        if tool.edits_output_name:
            candidate_path = output_dir / tool.edits_output_name
            if candidate_path.exists():
                edits_path = candidate_path
                try:
                    edits_payload = json.loads(
                        candidate_path.read_text(encoding="utf-8")
                    )
                except json.JSONDecodeError:
                    edits_payload = {}
                if isinstance(edits_payload, dict):
                    items = edits_payload.get("items")
                    if isinstance(items, list):
                        edits_item_count = len(items)
            else:
                edits_path = None
    summary_path = output_dir / f"{tool.code.lower()}_summary.json"
    summary_payload = {
        "tool": tool.code,
        "name": tool.name,
        "status": status,
        "summary": summary,
        "edits_path": str(edits_path) if edits_path else None,
        "edits_item_count": edits_item_count,
        "stderr": stderr if status == "error" else None,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return ToolResult(
        code=tool.code,
        name=tool.name,
        description=tool.description,
        status=status,
        summary=summary,
        stdout=stdout,
        stderr=stderr,
        duration_s=duration,
        output_path=summary_path,
        edits_path=edits_path,
        edits_item_count=edits_item_count,
    )
