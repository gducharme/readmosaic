from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import jsonschema

from stages.helpers import resolve_input_path


class EditCancelled(Exception):
    pass


def edit_json_artifact_with_schema(
    ctx: Any,
    *,
    artifact_name: str,
    schema: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, int]]:
    path = resolve_input_path(ctx, artifact_name)
    if not path.exists():
        raise FileNotFoundError(f"editable artifact missing: {path}")

    editor = os.getenv("EDITOR", "vi")
    attempts = 0
    validation_failures = 0
    current_text = path.read_text(encoding="utf-8")

    while True:
        attempts += 1
        with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as tmp:
            tmp.write(current_text)
            tmp_path = Path(tmp.name)

        try:
            subprocess.run([editor, str(tmp_path)], check=False)
            edited_text = tmp_path.read_text(encoding="utf-8")
        finally:
            tmp_path.unlink(missing_ok=True)

        if edited_text == current_text and _confirm_cancel():
            raise EditCancelled("User cancelled JSON editing.")

        current_text = edited_text
        try:
            payload = json.loads(edited_text)
        except json.JSONDecodeError as exc:
            validation_failures += 1
            print(f"Invalid JSON: {exc.msg} at line {exc.lineno}, column {exc.colno}", file=sys.stderr)
            if _confirm_cancel():
                raise EditCancelled("User cancelled JSON editing after parse error.")
            continue

        try:
            jsonschema.validate(instance=payload, schema=schema)
        except jsonschema.ValidationError as exc:
            validation_failures += 1
            path_label = ".".join(str(part) for part in exc.absolute_path) or "$"
            print(f"Schema validation failed at {path_label}: {exc.message}", file=sys.stderr)
            if _confirm_cancel():
                raise EditCancelled("User cancelled JSON editing after schema validation error.")
            continue

        _atomic_write(path, json.dumps(payload, indent=2) + "\n")
        return payload, {"edit_attempts": attempts, "validation_failures": validation_failures}


def _confirm_cancel() -> bool:
    answer = input("Cancel edit? [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)
