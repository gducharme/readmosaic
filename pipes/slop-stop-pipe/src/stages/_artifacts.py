from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _pipe_root() -> Path:
    return Path(__file__).resolve().parents[2]


def output_artifact_dir(ctx: Any) -> Path:
    run_id = str(getattr(ctx, 'run_id', '') or '').strip()
    if run_id:
        return _pipe_root() / 'artifacts' / 'outputs' / run_id / 'artifacts'
    return _pipe_root() / 'artifacts'


def input_artifact_dir(ctx: Any) -> Path:
    run_id = str(getattr(ctx, 'run_id', '') or '').strip()
    if run_id:
        return _pipe_root() / 'artifacts' / 'outputs' / run_id / 'artifacts'
    return _pipe_root() / 'artifacts'


def read_json(ctx: Any, name: str) -> dict[str, Any]:
    path = input_artifact_dir(ctx) / name
    return json.loads(path.read_text(encoding='utf-8'))


def write_text_artifact(ctx: Any, name: str, content: str) -> Path:
    out_dir = output_artifact_dir(ctx)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / name
    out_path.write_text(content, encoding='utf-8')
    return out_path


def append_jsonl_artifact(ctx: Any, name: str, row: dict[str, Any]) -> Path:
    out_dir = output_artifact_dir(ctx)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / name
    with out_path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(row, ensure_ascii=False) + '\n')
    return out_path
