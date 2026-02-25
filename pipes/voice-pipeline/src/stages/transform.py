from __future__ import annotations

import json
from pathlib import Path


def run_item(ctx, item: dict[str, object]) -> None:
    _ = ctx
    output = Path("artifacts/transformed.jsonl")
    transformed = {"item_id": item.get("item_id", ""), "transformed": True}
    with output.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(transformed) + "\n")
