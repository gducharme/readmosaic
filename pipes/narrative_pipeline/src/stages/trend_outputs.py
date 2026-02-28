from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ._artifacts import read_json, stage_config, write_json_artifact, write_text_artifact


def _safe_get(payload: dict[str, Any], *path: str) -> float | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if isinstance(current, (int, float)):
        return float(current)
    return None


def _load_history(history_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not history_root.exists():
        return rows
    for candidate in history_root.iterdir():
        if not candidate.is_dir():
            continue
        bundle_path = candidate / "diagnostics" / "diagnostics_bundle.json"
        if not bundle_path.exists():
            continue
        import json

        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            rows.append(payload)
    rows.sort(
        key=lambda row: (
            str(row.get("run", {}).get("created_at", "")),
            str(row.get("run", {}).get("run_id", "")),
        )
    )
    return rows


def run_whole(ctx) -> None:
    cfg = stage_config(ctx, "trend_outputs")
    current_bundle = read_json(
        ctx,
        "diagnostics/diagnostics_bundle.json",
        family="diagnostics_bundle",
    )
    current_delta = read_json(
        ctx,
        "diagnostics/diagnostics_delta.json",
        family="diagnostics_delta",
    )

    history_dir = cfg.get("history_dir")
    if isinstance(history_dir, str) and history_dir.strip():
        history_root = Path(history_dir.strip())
        if not history_root.is_absolute():
            history_root = Path.cwd() / history_root
    else:
        history_root = Path.cwd().parent
    history_bundles = _load_history(history_root)

    run_rows = []
    for bundle in history_bundles:
        run_rows.append(
            {
                "run_id": str(bundle.get("run", {}).get("run_id", "")),
                "created_at": str(bundle.get("run", {}).get("created_at", "")),
                "document_entropy": _safe_get(
                    bundle, "metrics", "entropy", "summary", "document_entropy"
                ),
                "mean_surprisal": _safe_get(
                    bundle, "metrics", "surprisal", "summary", "mean_surprisal"
                ),
                "average_lexical_density": _safe_get(
                    bundle,
                    "metrics",
                    "signal_density",
                    "summary",
                    "average_lexical_density",
                ),
            }
        )

    current_row = {
        "run_id": str(current_bundle.get("run", {}).get("run_id", getattr(ctx, "run_id", "local-run"))),
        "created_at": str(current_bundle.get("run", {}).get("created_at", datetime.now(timezone.utc).isoformat())),
        "document_entropy": _safe_get(
            current_bundle, "metrics", "entropy", "summary", "document_entropy"
        ),
        "mean_surprisal": _safe_get(
            current_bundle, "metrics", "surprisal", "summary", "mean_surprisal"
        ),
        "average_lexical_density": _safe_get(
            current_bundle,
            "metrics",
            "signal_density",
            "summary",
            "average_lexical_density",
        ),
    }
    if not any(row["run_id"] == current_row["run_id"] for row in run_rows):
        run_rows.append(current_row)
    run_rows.sort(key=lambda row: (row["created_at"], row["run_id"]))

    summary_payload = {
        "contract_version": "narrative_trend.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": run_rows,
        "latest_delta_metric_count": len(current_delta.get("metric_deltas", [])),
    }
    write_json_artifact(ctx, "trends/trend_summary.json", summary_payload, family="trend_summary")

    csv_path = Path("trends/trend_table.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "run_id",
                "created_at",
                "document_entropy",
                "mean_surprisal",
                "average_lexical_density",
            ],
        )
        writer.writeheader()
        for row in run_rows:
            writer.writerow(row)

    report_lines = [
        "# Narrative Diagnostics Trends",
        "",
        f"- Runs in trend window: `{len(run_rows)}`",
        f"- Latest run: `{current_row['run_id']}`",
        f"- Delta metrics tracked: `{len(current_delta.get('metric_deltas', []))}`",
        "",
        "## Recent Runs",
    ]
    for row in run_rows[-10:]:
        report_lines.append(
            f"- {row['run_id']} | entropy={row['document_entropy']} | surprisal={row['mean_surprisal']} | density={row['average_lexical_density']}"
        )
    write_text_artifact(
        ctx,
        "trends/trend_report.md",
        "\n".join(report_lines) + "\n",
        family="trend_report",
    )

