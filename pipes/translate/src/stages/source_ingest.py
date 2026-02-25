from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def run_whole(ctx) -> None:
    """Seed stage for the translate pipeline.

    This stage intentionally emits minimal scaffold artifacts so downstream
    placeholder stages have declared inputs available when needed.
    """

    run_id = getattr(ctx, "run_id", "local-run")
    pipeline_id = getattr(ctx, "pipeline_id", "translate-pipeline")
    now = datetime.now(timezone.utc).isoformat()

    paragraphs_path = Path("source_pre/paragraphs.jsonl")
    state_path = Path("state/paragraph_state.jsonl")
    manifest_path = Path("manifest.json")

    paragraphs_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    # Scaffold with a single starter paragraph row.
    paragraph = {
        "paragraph_id": "p-0001",
        "text": "",
    }
    paragraphs_path.write_text(json.dumps(paragraph) + "\n", encoding="utf-8")

    paragraph_state = {
        "paragraph_id": "p-0001",
        "state": "pending",
        "updated_at": now,
    }
    state_path.write_text(json.dumps(paragraph_state) + "\n", encoding="utf-8")

    manifest = {
        "manifest_version": "phase1-v0",
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "created_at": now,
        "stage_id": "source_ingest",
        "outputs": [
            "source_pre/paragraphs.jsonl",
            "state/paragraph_state.jsonl",
            "manifest.json",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
