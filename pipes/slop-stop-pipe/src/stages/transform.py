from __future__ import annotations

from ._artifacts import append_jsonl_artifact


def run_item(ctx, item: dict[str, object]) -> None:
    transformed = {
        'item_id': str(item.get('item_id', '')),
        'status': 'transformed',
        'payload': item,
    }
    append_jsonl_artifact(ctx, 'transformed.jsonl', transformed)
