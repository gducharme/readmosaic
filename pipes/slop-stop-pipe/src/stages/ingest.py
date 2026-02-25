from __future__ import annotations

import sys

from ._artifacts import append_jsonl_artifact, read_json


def run_whole(ctx) -> None:
    print(
        "[debug][ingest] Starting ingest stage; expecting input artifact "
        "'word_frequency_report.json'.",
        file=sys.stderr,
    )
    report = read_json(ctx, 'word_frequency_report.json')
    print(
        "[debug][ingest] Loaded word_frequency_report.json successfully; "
        f"top_words={len(report.get('top_words', []))}",
        file=sys.stderr,
    )
    item = {
        'item_id': 'top_words_summary',
        'manuscript': report.get('manuscript', 'unknown'),
        'top_words': report.get('top_words', []),
    }
    append_jsonl_artifact(ctx, 'items.jsonl', item)
    print(
        "[debug][ingest] Wrote items.jsonl with top_words_summary item.",
        file=sys.stderr,
    )
