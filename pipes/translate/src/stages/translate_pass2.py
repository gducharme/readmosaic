from __future__ import annotations

from pathlib import Path
import shutil


SOURCE_PATTERN = "pass1_pre/{lang}/paragraphs.jsonl"
TARGET_PATTERN = "pass2_pre/{lang}/paragraphs.jsonl"


def _context_keys(ctx) -> dict[str, str]:
    for attr in ("keys", "bindings"):
        key_map = getattr(ctx, attr, None)
        if isinstance(key_map, dict):
            return {str(key): str(value) for key, value in key_map.items()}
    return {}


def _binding_value(ctx, key: str) -> str | None:
    value = _context_keys(ctx).get(key)
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
        candidate_bindings = candidate.get("keys") or candidate.get("bindings")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue
        return Path(concrete_path)

    return Path(SOURCE_PATTERN.format(lang=language))


def _resolve_output_artifact_path(ctx, language: str) -> Path:
    output_candidates: list[dict[str, object]] = []
    for attr in ("outputs", "output_artifacts", "stage_outputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            output_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    for candidate in output_candidates:
        concrete_path = candidate.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        candidate_bindings = candidate.get("keys") or candidate.get("bindings")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue
        return Path(concrete_path)

    return Path(TARGET_PATTERN.format(lang=language))


def run_whole(ctx) -> None:
    language_binding = _binding_value(ctx, "lang") or _binding_value(ctx, "language")
    language = _normalize_language(language_binding or "")
    if not language:
        raise ValueError("translate_pass2 is missing required language binding ('lang').")

    input_path = _resolve_input_artifact_path(ctx, language)
    if not input_path.exists():
        raise FileNotFoundError(f"translate_pass2 input artifact missing: {input_path}")

    output_path = _resolve_output_artifact_path(ctx, language)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        return
    shutil.copyfile(input_path, output_path)


def run_item(ctx, item: dict[str, object]) -> None:
    _ = item
    run_whole(ctx)
