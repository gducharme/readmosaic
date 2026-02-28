from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def _iter_artifact_specs(ctx: Any, kind: str) -> Iterable[dict[str, Any]]:
    attrs = (
        ("input", ("inputs", "input_artifacts", "stage_inputs", "expected_inputs", "artifacts")),
        ("output", ("outputs", "output_artifacts", "stage_outputs", "expected_outputs", "artifacts")),
    )
    names: tuple[str, ...] = ()
    for artifact_kind, candidates in attrs:
        if artifact_kind == kind:
            names = candidates
            break
    for attr in names:
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            for row in value:
                if isinstance(row, dict):
                    yield row
                elif isinstance(row, str) and row.strip():
                    yield {"path": row.strip()}


def _resolve_from_ctx(
    ctx: Any,
    *,
    kind: str,
    family: str | None = None,
    suffix: str | None = None,
) -> Path | None:
    for spec in _iter_artifact_specs(ctx, kind):
        spec_family = spec.get("family")
        concrete_path = spec.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        if family and spec_family and spec_family != family:
            continue
        if suffix and not concrete_path.endswith(suffix):
            continue
        return Path(concrete_path)
    return None


def resolve_input_path(ctx: Any, *, default_name: str, family: str | None = None) -> Path:
    resolved = _resolve_from_ctx(ctx, kind="input", family=family, suffix=default_name)
    if resolved is not None:
        return resolved
    return Path(default_name)


def resolve_output_path(ctx: Any, *, default_name: str, family: str | None = None) -> Path:
    resolved = _resolve_from_ctx(ctx, kind="output", family=family, suffix=default_name)
    if resolved is not None:
        return resolved
    return Path(default_name)


def read_text(ctx: Any, name: str, *, family: str | None = None) -> str:
    path = resolve_input_path(ctx, default_name=name, family=family)
    return path.read_text(encoding="utf-8")


def read_json(ctx: Any, name: str, *, family: str | None = None) -> dict[str, Any]:
    path = resolve_input_path(ctx, default_name=name, family=family)
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(ctx: Any, name: str, *, family: str | None = None) -> list[dict[str, Any]]:
    path = resolve_input_path(ctx, default_name=name, family=family)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def write_text_artifact(ctx: Any, name: str, content: str, *, family: str | None = None) -> Path:
    out_path = resolve_output_path(ctx, default_name=name, family=family)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def write_json_artifact(
    ctx: Any,
    name: str,
    payload: dict[str, Any],
    *,
    family: str | None = None,
) -> Path:
    out_path = resolve_output_path(ctx, default_name=name, family=family)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path


def write_jsonl_artifact(
    ctx: Any,
    name: str,
    rows: list[dict[str, Any]],
    *,
    family: str | None = None,
) -> Path:
    out_path = resolve_output_path(ctx, default_name=name, family=family)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out_path


def stage_config(ctx: Any, stage_id: str) -> dict[str, Any]:
    run_config = getattr(ctx, "run_config", None)
    if not isinstance(run_config, dict):
        return {}
    rc = run_config.get("rc")
    if not isinstance(rc, dict):
        return {}
    direct = rc.get(stage_id)
    if isinstance(direct, dict):
        return direct
    stages = rc.get("stages")
    if isinstance(stages, dict):
        nested = stages.get(stage_id)
        if isinstance(nested, dict):
            return nested
    return {}
