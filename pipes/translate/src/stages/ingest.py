from __future__ import annotations

import json
from pathlib import Path


def run_whole(ctx) -> None:
    _ = ctx
    rows = [{"item_id": "item-001"}]
    payload = "".join(json.dumps(row) + "\n" for row in rows)
    Path("artifacts/items.jsonl").write_text(payload)
