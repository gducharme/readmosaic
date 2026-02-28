from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ._artifacts import read_json, write_json_artifact, write_text_artifact
from .contract import validate_bundle


def _flatten_highlights(metric_name: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    highlights = payload.get("highlights", [])
    if not isinstance(highlights, list):
        return []
    rows: list[dict[str, Any]] = []
    for row in highlights:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "kind": str(row.get("kind", metric_name)),
                "severity": str(row.get("severity", "info")),
                "message": str(row.get("message", "")),
                "anchors": row.get("anchors"),
                "source_metric": metric_name,
            }
        )
    return rows


def _build_report_markdown(bundle: dict[str, Any]) -> str:
    metrics = bundle.get("metrics", {})
    highlights = bundle.get("highlights", [])
    lines = [
        "# Narrative Diagnostics Report",
        "",
        f"- Run ID: `{bundle['run']['run_id']}`",
        f"- Contract: `{bundle['contract_version']}`",
        f"- Generated: `{bundle['run']['created_at']}`",
        "",
        "## Executive Summary",
        f"- Diagnostics metrics computed: `{len(metrics)}`",
        f"- Highlight findings: `{len(highlights)}`",
        "",
        "## Top Findings",
    ]
    if highlights:
        for row in highlights[:15]:
            lines.append(
                f"- [{row.get('severity', 'info')}] {row.get('kind', 'unknown')}: {row.get('message', '')}"
            )
    else:
        lines.append("- No highlights generated.")

    lines.append("")
    lines.append("## Metrics")
    for metric_name, metric_payload in metrics.items():
        summary = {}
        if isinstance(metric_payload, dict):
            summary = metric_payload.get("summary", {})
        lines.append(f"### {metric_name}")
        if isinstance(summary, dict) and summary:
            for key, value in summary.items():
                lines.append(f"- {key}: `{value}`")
        else:
            lines.append("- No summary available.")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def run_whole(ctx) -> None:
    document_themes = read_json(
        ctx,
        "diagnostics/document_themes.json",
        family="document_themes",
    )
    document_patterns = read_json(
        ctx,
        "diagnostics/document_patterns.json",
        family="document_patterns",
    )
    document_entropy = read_json(
        ctx,
        "diagnostics/document_entropy.json",
        family="document_entropy",
    )
    paragraph_signal_density = read_json(
        ctx,
        "diagnostics/paragraph_signal_density.json",
        family="paragraph_signal_density",
    )
    paragraph_surprisal = read_json(
        ctx,
        "diagnostics/paragraph_surprisal.json",
        family="paragraph_surprisal",
    )
    paragraph_entropy = read_json(
        ctx,
        "diagnostics/paragraph_entropy.json",
        family="paragraph_entropy",
    )
    hybrid_semantic_repetition = read_json(
        ctx,
        "diagnostics/hybrid_semantic_repetition.json",
        family="hybrid_semantic_repetition",
    )
    hybrid_burstiness = read_json(
        ctx,
        "diagnostics/hybrid_burstiness.json",
        family="hybrid_burstiness",
    )

    entropy_metric = dict(paragraph_entropy)
    entropy_summary = dict(paragraph_entropy.get("summary", {}))
    entropy_summary["document_entropy"] = document_entropy.get("summary", {}).get("document_entropy", 0.0)
    entropy_metric["summary"] = entropy_summary

    metrics = {
        "semantic_repetition": hybrid_semantic_repetition,
        "signal_density": paragraph_signal_density,
        "surprisal": paragraph_surprisal,
        "entropy": entropy_metric,
        "burstiness": hybrid_burstiness,
        "themes": document_themes,
        "patterns": document_patterns,
    }
    index_payload = read_json(
        ctx,
        "preprocessed/paragraph_index.json",
        family="paragraph_index",
    )

    all_highlights: list[dict[str, Any]] = []
    for metric_name, payload in metrics.items():
        all_highlights.extend(_flatten_highlights(metric_name, payload))

    bundle = {
        "contract_version": "narrative_diagnostics.v1",
        "run": {
            "run_id": str(getattr(ctx, "run_id", "local-run")),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "input": {"source_path": index_payload.get("source_path", "unknown")},
            "tool_versions": {
                "run_document_diagnostics": "seedpipe-stage-v2",
                "run_paragraph_diagnostics": "seedpipe-stage-v2",
                "run_hybrid_diagnostics": "seedpipe-stage-v2",
            },
        },
        "artifacts": {
            "document_themes": {"path": "diagnostics/document_themes.json"},
            "document_patterns": {"path": "diagnostics/document_patterns.json"},
            "document_entropy": {"path": "diagnostics/document_entropy.json"},
            "paragraph_signal_density": {"path": "diagnostics/paragraph_signal_density.json"},
            "paragraph_surprisal": {"path": "diagnostics/paragraph_surprisal.json"},
            "paragraph_entropy": {"path": "diagnostics/paragraph_entropy.json"},
            "hybrid_semantic_repetition": {"path": "diagnostics/hybrid_semantic_repetition.json"},
            "hybrid_burstiness": {"path": "diagnostics/hybrid_burstiness.json"},
        },
        "metrics": metrics,
        "highlights": all_highlights,
        "anchors": {
            "strategy": "paragraph_id",
            "paragraph_count": index_payload.get("paragraph_count", 0),
        },
        "diagnostics_scopes": {
            "document": ["themes", "patterns", "entropy.summary.document_entropy"],
            "paragraph": ["signal_density", "surprisal", "entropy.paragraphs"],
            "hybrid": ["semantic_repetition", "burstiness"],
        },
    }
    validate_bundle(bundle)
    write_json_artifact(
        ctx,
        "diagnostics/diagnostics_bundle.json",
        bundle,
        family="diagnostics_bundle",
    )
    write_text_artifact(
        ctx,
        "diagnostics/diagnostics_report.md",
        _build_report_markdown(bundle),
        family="diagnostics_report",
    )
