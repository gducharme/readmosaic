from __future__ import annotations

import json
import sys
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
    if not path.exists():
        run_id = str(getattr(ctx, 'run_id', '') or '').strip() or '<none>'
        stage_id = str(getattr(ctx, 'stage_id', '') or '').strip() or '<unknown>'
        artifact_dir = input_artifact_dir(ctx)
        pipe_root = _pipe_root()

        print(
            f"[debug][artifacts] Missing required JSON artifact: {path}",
            file=sys.stderr,
        )
        print(
            f"[debug][artifacts] stage={stage_id} run_id={run_id}",
            file=sys.stderr,
        )
        print(
            f"[debug][artifacts] input_artifact_dir={artifact_dir}",
            file=sys.stderr,
        )
        print(
            f"[debug][artifacts] pipe_root={pipe_root}",
            file=sys.stderr,
        )

        if artifact_dir.exists():
            artifacts = sorted(p.name for p in artifact_dir.iterdir())
            print(
                f"[debug][artifacts] Files in input_artifact_dir: {artifacts or ['<empty>']}",
                file=sys.stderr,
            )
        else:
            print(
                "[debug][artifacts] input_artifact_dir does not exist.",
                file=sys.stderr,
            )

        shared_artifact_dir = pipe_root / 'artifacts'
        if shared_artifact_dir.exists():
            shared_artifacts = sorted(p.name for p in shared_artifact_dir.iterdir())
            print(
                f"[debug][artifacts] Files in shared artifacts dir ({shared_artifact_dir}): {shared_artifacts or ['<empty>']}",
                file=sys.stderr,
            )

        raise FileNotFoundError(
            f"Required artifact '{name}' was not found in '{artifact_dir}'. "
            "This pipeline currently expects run-scoped artifacts "
            "(artifacts/outputs/<run_id>/artifacts). If upstream stages wrote files to "
            "the shared artifacts/ directory, they will not be visible to this run. "
            "Also confirm that an upstream stage actually produces this artifact in "
            "generated/ir.json (artifact_producers)."
        )

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
