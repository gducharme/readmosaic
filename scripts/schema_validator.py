#!/usr/bin/env python3
"""Shared JSON schema validation utilities for Mosaic scripts."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping

from jsonschema import Draft202012Validator


def _load_schema(schema_name: str) -> dict:
    schema_path = Path(__file__).resolve().parents[1] / "schemas" / schema_name
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _format_errors(errors: Iterable[str], label: str) -> str:
    joined = "\n".join(f"- {error}" for error in errors)
    return f"Schema validation failed for {label}:\n{joined}"


def validate_payload(payload: Mapping[str, object], schema_name: str, label: str) -> None:
    schema = _load_schema(schema_name)
    validator = Draft202012Validator(schema)
    errors = []
    for error in sorted(validator.iter_errors(payload), key=lambda e: e.path):
        location = " -> ".join(str(part) for part in error.path) or "<root>"
        errors.append(f"{location}: {error.message}")
    if errors:
        raise ValueError(_format_errors(errors, label))


def validate_records(
    records: Iterable[Mapping[str, object]],
    schema_name: str,
    label: str,
) -> None:
    schema = _load_schema(schema_name)
    validator = Draft202012Validator(schema)
    for index, record in enumerate(records, start=1):
        errors = []
        for error in sorted(validator.iter_errors(record), key=lambda e: e.path):
            location = " -> ".join(str(part) for part in error.path) or "<root>"
            errors.append(f"{location}: {error.message}")
        if errors:
            raise ValueError(_format_errors(errors, f"{label} #{index}"))
