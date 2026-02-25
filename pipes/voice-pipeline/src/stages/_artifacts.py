from __future__ import annotations

from pathlib import Path
from typing import Any


def _pipe_root() -> Path:
    return Path(__file__).resolve().parents[2]


def output_artifact_dir(ctx: Any) -> Path:
    """Return run-scoped output directory: artifacts/outputs/<run_id>/."""
    run_id = str(getattr(ctx, 'run_id', '') or '').strip()
    if not run_id:
        return _pipe_root() / 'artifacts' / 'outputs'
    return _pipe_root() / 'artifacts' / 'outputs' / run_id


def input_artifact_dir(_: Any) -> Path:
    """Return shared pipeline input directory: artifacts/inputs/."""
    return _pipe_root() / 'artifacts' / 'inputs'


def default_input_candidates(ctx: Any, artifact_name: str) -> list[Path]:
    """Return preferred input artifact locations.

    Order:
    1) pipeline input directory (artifacts/inputs)
    2) current working directory relative path (backward compatibility)
    """
    return [
        input_artifact_dir(ctx) / artifact_name,
        Path.cwd() / artifact_name,
    ]


def stage_config(ctx: Any, stage_id: str) -> dict[str, Any]:
    run_config = getattr(ctx, 'run_config', None)
    if not isinstance(run_config, dict):
        return {}

    rc = run_config.get('rc')
    if not isinstance(rc, dict):
        return {}

    candidate = rc.get(stage_id)
    if isinstance(candidate, dict):
        return candidate

    stages = rc.get('stages')
    if isinstance(stages, dict):
        nested = stages.get(stage_id)
        if isinstance(nested, dict):
            return nested

    return {}
