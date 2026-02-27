from __future__ import annotations

import json
from datetime import datetime, timezone

from ._artifacts import (
    read_json,
    read_jsonl,
    resolve_output_path,
    write_text_artifact,
)


def run_whole(ctx) -> None:
    reviewed_rows = read_jsonl(ctx, "review/reviewed.jsonl", family="reviewed_rewrites")
    summary = read_json(ctx, "review/review_summary.json", family="review_summary")
    manifest_path = resolve_output_path(ctx, default_name="manifest.json", family="manifest")

    manifest = {
        'run_id': str(getattr(ctx, 'run_id', '')),
        'pipeline_id': 'slop-stop-pipeline',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'artifacts_dir': str(manifest_path.parent),
        'reviewed_item_count': len(reviewed_rows),
        'approved_item_count': int(summary.get("approved_item_count", len(reviewed_rows))),
        'rejected_item_count': int(summary.get("rejected_item_count", 0)),
        'summary_artifact': "review/review_summary.json",
    }
    write_text_artifact(
        ctx,
        'manifest.json',
        json.dumps(manifest, ensure_ascii=False, indent=2),
        family="manifest",
    )
