from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SOURCE_PATTERN = "pass2_pre/{lang}/paragraphs.jsonl"
CANDIDATE_PATTERN = "final/{lang}/candidate.md"
MAP_PATTERN = "final/{lang}/candidate_map.jsonl"


def _binding_value(ctx, key: str) -> str | None:
    bindings = getattr(ctx, "bindings", None)
    if not isinstance(bindings, dict):
        return None
    value = bindings.get(key)
    if value is None:
        return None
    return str(value)


def _normalize_language(language: str) -> str:
    return language.strip().strip("/")


def _resolve_input_artifact_path(ctx, language: str) -> Path:
    input_candidates: list[dict[str, object]] = []
    for attr in ("inputs", "input_artifacts", "stage_inputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            input_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    for candidate in input_candidates:
        concrete_path = candidate.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        candidate_bindings = candidate.get("bindings")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue
        return Path(concrete_path)

    return Path(SOURCE_PATTERN.format(lang=language))


def _resolve_output_artifact_path(ctx, language: str, default_pattern: str, *, file_suffix: str) -> Path:
    output_candidates: list[dict[str, object]] = []
    for attr in ("outputs", "output_artifacts", "stage_outputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            output_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    for candidate in output_candidates:
        concrete_path = candidate.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        if file_suffix and not concrete_path.endswith(file_suffix):
            continue
        candidate_bindings = candidate.get("bindings")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue
        return Path(concrete_path)

    return Path(default_pattern.format(lang=language))


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"candidate_assembly input artifact missing: {path}")

    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            continue
        rows.append(payload)
    return rows


def _paragraph_text(row: dict[str, Any]) -> str:
    for key in ("text", "translation", "target_text", "content"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _paragraph_id(row: dict[str, Any], index: int) -> str:
    for key in ("paragraph_id", "item_id"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"paragraph_{index:04d}"


def run_whole(ctx) -> None:
    language_binding = _binding_value(ctx, "lang") or _binding_value(ctx, "language")
    language = _normalize_language(language_binding or "")
    if not language:
        raise ValueError("candidate_assembly is missing required language binding ('lang').")

    input_path = _resolve_input_artifact_path(ctx, language)
    rows = _load_rows(input_path)

    candidate_path = _resolve_output_artifact_path(
        ctx,
        language,
        CANDIDATE_PATTERN,
        file_suffix="candidate.md",
    )
    map_path = _resolve_output_artifact_path(
        ctx,
        language,
        MAP_PATTERN,
        file_suffix="candidate_map.jsonl",
    )

    candidate_lines: list[str] = []
    map_rows: list[dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        paragraph_id = _paragraph_id(row, idx)
        paragraph_text = _paragraph_text(row)

        start_line = len(candidate_lines) + 1
        paragraph_lines = paragraph_text.splitlines() if paragraph_text else [""]
        candidate_lines.extend(paragraph_lines)
        end_line = len(candidate_lines)

        map_rows.append(
            {
                "paragraph_id": paragraph_id,
                "paragraph_index": idx,
                "start_line": start_line,
                "end_line": end_line,
            }
        )

        if idx < len(rows):
            candidate_lines.append("")

    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.parent.mkdir(parents=True, exist_ok=True)

    candidate_path.write_text("\n".join(candidate_lines), encoding="utf-8")
    with map_path.open("w", encoding="utf-8") as output_file:
        for map_row in map_rows:
            output_file.write(json.dumps(map_row, ensure_ascii=False) + "\n")


def run_item(ctx, item: dict[str, object]) -> None:
    _ = item
    run_whole(ctx)
