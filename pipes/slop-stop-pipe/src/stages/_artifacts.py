from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Iterable


def _pipe_root() -> Path:
    return Path(__file__).resolve().parents[2]


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


def output_artifact_dir(ctx: Any) -> Path:
    _ = ctx
    # Seedpipe executes stages from the run directory, so relative paths should
    # resolve directly under artifacts/outputs/<run_id>/.
    return Path.cwd()


def input_artifact_dir(ctx: Any) -> Path:
    _ = ctx
    return Path.cwd()


def resolve_input_path(ctx: Any, *, default_name: str, family: str | None = None) -> Path:
    resolved = _resolve_from_ctx(ctx, kind="input", family=family, suffix=default_name)
    if resolved is not None:
        return resolved
    return input_artifact_dir(ctx) / default_name


def resolve_output_path(ctx: Any, *, default_name: str, family: str | None = None) -> Path:
    resolved = _resolve_from_ctx(ctx, kind="output", family=family, suffix=default_name)
    if resolved is not None:
        return resolved
    return output_artifact_dir(ctx) / default_name


def read_json(ctx: Any, name: str, *, family: str | None = None) -> dict[str, Any]:
    path = resolve_input_path(ctx, default_name=name, family=family)
    if not path.exists():
        run_id = str(getattr(ctx, 'run_id', '') or '').strip() or '<none>'
        stage_id = str(getattr(ctx, 'stage_id', '') or '').strip() or '<unknown>'
        artifact_dir = path.parent
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
            "This pipeline expects run-scoped artifacts under "
            "artifacts/outputs/<run_id>/. If upstream stages wrote files outside the run "
            "directory, they will not be visible to this run. "
            "Also confirm that an upstream stage actually produces this artifact in "
            "generated/ir.json (artifact_producers)."
        )

    return json.loads(path.read_text(encoding='utf-8'))


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
    out_path.write_text(content, encoding='utf-8')
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
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def append_jsonl_artifact(
    ctx: Any,
    name: str,
    row: dict[str, Any],
    *,
    family: str | None = None,
) -> Path:
    out_path = resolve_output_path(ctx, default_name=name, family=family)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(row, ensure_ascii=False) + '\n')
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
    with out_path.open("w", encoding="utf-8") as out:
        for row in rows:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out_path


def stage_config(ctx: Any, stage_id: str) -> dict[str, Any]:
    """Return per-stage configuration from run_config.rc.

    Supported run_config shapes:
    - {"rc": {"<stage_id>": {...}}}
    - {"rc": {"stages": {"<stage_id>": {...}}}}
    """
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
