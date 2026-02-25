from __future__ import annotations

from ._artifacts import input_artifact_dir, write_text_artifact


def run_whole(ctx) -> None:
    in_dir = input_artifact_dir(ctx)
    transformed = in_dir / 'transformed.jsonl'
    content = transformed.read_text(encoding='utf-8') if transformed.exists() else ''
    write_text_artifact(ctx, 'reviewed.jsonl', content)
