from __future__ import annotations

import json
from datetime import datetime, timezone

from ._artifacts import input_artifact_dir, output_artifact_dir, write_text_artifact


def _count_jsonl_rows(path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding='utf-8').splitlines() if line.strip())


def run_whole(ctx) -> None:
    in_dir = input_artifact_dir(ctx)
    out_dir = output_artifact_dir(ctx)
    reviewed = in_dir / 'reviewed.jsonl'

    manifest = {
        'run_id': str(getattr(ctx, 'run_id', '')),
        'pipeline_id': 'example-pipeline',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'artifacts_dir': str(out_dir),
        'reviewed_item_count': _count_jsonl_rows(reviewed),
    }
    write_text_artifact(ctx, 'manifest.json', json.dumps(manifest, ensure_ascii=False, indent=2))
