from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._artifacts import read_json, stage_config, write_json_artifact, write_text_artifact
from .contract import validate_delta


def _flatten_numeric_metrics(payload: dict[str, Any], prefix: str = "") -> dict[str, float]:
    rows: dict[str, float] = {}
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, (int, float)):
            rows[path] = float(value)
        elif isinstance(value, dict):
            rows.update(_flatten_numeric_metrics(value, path))
    return rows


def _load_baseline_bundle(cfg: dict[str, Any]) -> dict[str, Any] | None:
    baseline_path = cfg.get("baseline_bundle")
    if not isinstance(baseline_path, str) or not baseline_path.strip():
        return None
    path = Path(baseline_path.strip())
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        return None
    return read_json_from_path(path)


def read_json_from_path(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _build_report(delta: dict[str, Any]) -> str:
    lines = [
        "# Narrative Diagnostics Delta Report",
        "",
        f"- Compared at: `{delta['comparison']['compared_at']}`",
        f"- Baseline run: `{delta['comparison'].get('baseline_run_id', 'none')}`",
        f"- Current run: `{delta['comparison']['current_run_id']}`",
        "",
        "## Metric Deltas",
    ]
    if delta["metric_deltas"]:
        for row in delta["metric_deltas"][:20]:
            lines.append(
                f"- `{row['metric']}`: baseline `{row['baseline']}` -> current `{row['current']}` (delta `{row['delta']}`)"
            )
    else:
        lines.append("- No comparable baseline metrics.")

    lines.extend(
        [
            "",
            "## Highlight Changes",
            f"- New highlights: `{len(delta.get('new_highlights', []))}`",
            f"- Resolved highlights: `{len(delta.get('resolved_highlights', []))}`",
            "",
        ]
    )
    return "\n".join(lines)


def run_whole(ctx) -> None:
    cfg = stage_config(ctx, "baseline_compare")
    current_bundle = read_json(
        ctx,
        "diagnostics/diagnostics_bundle.json",
        family="diagnostics_bundle",
    )
    baseline_bundle = _load_baseline_bundle(cfg)
    current_run_id = str(getattr(ctx, "run_id", "local-run"))
    compared_at = datetime.now(timezone.utc).isoformat()

    current_metrics = _flatten_numeric_metrics(current_bundle.get("metrics", {}))
    baseline_metrics = (
        _flatten_numeric_metrics(baseline_bundle.get("metrics", {}))
        if baseline_bundle
        else {}
    )

    shared_keys = sorted(set(current_metrics).intersection(baseline_metrics))
    metric_deltas = [
        {
            "metric": key,
            "baseline": round(baseline_metrics[key], 6),
            "current": round(current_metrics[key], 6),
            "delta": round(current_metrics[key] - baseline_metrics[key], 6),
        }
        for key in shared_keys
    ]
    metric_deltas.sort(key=lambda row: abs(row["delta"]), reverse=True)

    current_highlights = {
        f"{row.get('kind','')}|{row.get('message','')}|{row.get('anchors')}"
        for row in current_bundle.get("highlights", [])
        if isinstance(row, dict)
    }
    baseline_highlights = {
        f"{row.get('kind','')}|{row.get('message','')}|{row.get('anchors')}"
        for row in (baseline_bundle.get("highlights", []) if baseline_bundle else [])
        if isinstance(row, dict)
    }

    delta = {
        "contract_version": "narrative_diagnostics_delta.v1",
        "comparison": {
            "baseline_run_id": (
                baseline_bundle.get("run", {}).get("run_id")
                if baseline_bundle
                else None
            ),
            "current_run_id": current_run_id,
            "compared_at": compared_at,
            "baseline_bundle_provided": baseline_bundle is not None,
        },
        "metric_deltas": metric_deltas,
        "new_highlights": sorted(current_highlights - baseline_highlights),
        "resolved_highlights": sorted(baseline_highlights - current_highlights),
    }
    validate_delta(delta)
    write_json_artifact(
        ctx,
        "diagnostics/diagnostics_delta.json",
        delta,
        family="diagnostics_delta",
    )
    write_text_artifact(
        ctx,
        "diagnostics/diagnostics_delta_report.md",
        _build_report(delta),
        family="diagnostics_delta_report",
    )

