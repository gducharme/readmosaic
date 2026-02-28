from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def run_whole(ctx) -> None:
    bundle_path = Path("diagnostics/diagnostics_bundle.json")
    delta_path = Path("diagnostics/diagnostics_delta.json")
    trend_path = Path("trends/trend_summary.json")
    manifest_path = Path("narrative_manifest.json")

    bundle = json.loads(bundle_path.read_text(encoding="utf-8")) if bundle_path.exists() else {}
    delta = json.loads(delta_path.read_text(encoding="utf-8")) if delta_path.exists() else {}
    trend = json.loads(trend_path.read_text(encoding="utf-8")) if trend_path.exists() else {}

    manifest = {
        "manifest_version": "narrative-pipeline.v1",
        "pipeline_id": "narrative-diagnostics-pipeline",
        "run_id": str(getattr(ctx, "run_id", "local-run")),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "artifacts": {
            "diagnostics_bundle": str(bundle_path),
            "diagnostics_delta": str(delta_path),
            "trend_summary": str(trend_path),
        },
        "bundle_contract_version": bundle.get("contract_version"),
        "delta_contract_version": delta.get("contract_version"),
        "trend_contract_version": trend.get("contract_version"),
        "highlights_count": len(bundle.get("highlights", [])) if isinstance(bundle, dict) else 0,
        "metric_delta_count": len(delta.get("metric_deltas", [])) if isinstance(delta, dict) else 0,
        "trend_run_count": len(trend.get("runs", [])) if isinstance(trend, dict) else 0,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
