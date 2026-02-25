from __future__ import annotations

from ._artifacts import append_jsonl_artifact, read_json


def run_whole(ctx) -> None:
    report = read_json(ctx, 'word_frequency_report.json')
    item = {
        'item_id': 'top_words_summary',
        'manuscript': report.get('manuscript', 'unknown'),
        'top_words': report.get('top_words', []),
    }
    append_jsonl_artifact(ctx, 'items.jsonl', item)
