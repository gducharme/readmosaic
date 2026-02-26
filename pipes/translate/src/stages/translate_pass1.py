from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

sys.path.append(str(Path(__file__).resolve().parents[4]))

from libs.local_llm import (  # noqa: E402
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content_streaming,
)

DEFAULT_PROMPT_ROOTS = [Path("prompt/translate"), Path("prompts/translate")]
STAGE_CONFIG_PATH = Path(__file__).with_name("translate_pass1_config.json")
SOURCE_ARTIFACT_PATH = Path("paragraphs.jsonl")


class TranslationLengthExceededError(RuntimeError):
    """Raised when streamed translation grows beyond configured max ratio."""


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise TypeError(f"Expected object JSON in {path}")
    return parsed


def _stage_config(ctx) -> dict[str, object]:
    config = _load_json(STAGE_CONFIG_PATH)

    run_config = getattr(ctx, "run_config", None)
    if isinstance(run_config, dict):
        rc = run_config.get("rc")
        if isinstance(rc, dict):
            direct = rc.get("translate_pass1")
            if isinstance(direct, dict):
                config.update(direct)
            nested = rc.get("stages")
            if isinstance(nested, dict):
                stage_cfg = nested.get("translate_pass1")
                if isinstance(stage_cfg, dict):
                    config.update(stage_cfg)

    return config


def _binding_value(ctx, key: str) -> str | None:
    bindings = getattr(ctx, "bindings", None)
    if not isinstance(bindings, dict):
        return None

    value = bindings.get(key)
    if value is None:
        return None
    return str(value)




def _binding_map(ctx) -> dict[str, str]:
    bindings = getattr(ctx, "bindings", None)
    if not isinstance(bindings, dict):
        return {}
    return {str(key): str(value) for key, value in bindings.items()}


def _resolve_output_artifact_path(ctx, language: str) -> Path:
    bindings = _binding_map(ctx)
    if language:
        bindings.setdefault("lang", language)
        bindings.setdefault("language", language)

    output_candidates: list[dict[str, object]] = []
    for attr in ("outputs", "output_artifacts", "stage_outputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            output_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    for candidate in output_candidates:
        pattern_entry = candidate.get("pattern")
        concrete_path = candidate.get("path")
        if not isinstance(pattern_entry, str) or not isinstance(concrete_path, str):
            continue
        if not concrete_path.strip():
            continue

        candidate_bindings = candidate.get("bindings")
        if isinstance(candidate_bindings, dict):
            normalized = {str(key): str(value) for key, value in candidate_bindings.items()}
            if bindings and normalized and normalized != bindings:
                continue
            lang_value = normalized.get("lang") or normalized.get("language")
            if language and lang_value and lang_value != language:
                continue

        if "{lang}" in pattern_entry and language and f"/{language}/" not in concrete_path.replace('\\', '/'):
            continue

        return Path(concrete_path)

    pattern = "pass1_pre/{lang}/paragraphs.jsonl"
    try:
        return Path(pattern.format(**bindings))
    except KeyError:
        return Path("pass1_pre") / language / "paragraphs.jsonl"


def _normalize_language(language: str) -> str:
    return language.strip().strip("/")


def _candidate_prompt_paths(prompt_root: Path, language: str) -> list[Path]:
    variants = [language, language.lower(), language.replace(" ", "_"), language.lower().replace(" ", "_")]
    exts = ["", ".txt", ".md"]
    candidates: list[Path] = []
    for variant in variants:
        for ext in exts:
            candidates.append(prompt_root / f"{variant}{ext}")
    return candidates


def _resolve_prompt_path(language: str, prompt_root: str | None) -> Path:
    roots = [Path(prompt_root)] if prompt_root else DEFAULT_PROMPT_ROOTS
    scope_roots = [Path.cwd(), Path(__file__).resolve().parents[3], Path(__file__).resolve().parents[4]]

    checked_roots: list[Path] = []
    for root in roots:
        expanded_roots = [root] if root.is_absolute() else [scope_root / root for scope_root in scope_roots]
        for expanded_root in expanded_roots:
            checked_roots.append(expanded_root)
            if not expanded_root.exists() or not expanded_root.is_dir():
                continue
            for candidate in _candidate_prompt_paths(expanded_root, language):
                if candidate.exists() and candidate.is_file():
                    return candidate

    roots_display = ", ".join(str(root) for root in checked_roots)
    raise FileNotFoundError(
        f"No translation prompt found for language '{language}'. Checked: {roots_display}."
    )


def _call_lm(
    *,
    base_url: str,
    model: str,
    system_prompt: str,
    language: str,
    text: str,
    timeout: int,
    max_length_ratio: float,
    temperature: float,
) -> str:
    user_prompt = (
        f"Translate this single source paragraph into {language}. Return only translated text.\\n\\n"
        f"SOURCE PARAGRAPH:\n{text}"
    )

    source_len = max(1, len(text))
    max_streamed_len = int(source_len * max_length_ratio)
    streamed_len = 0

    def check_chunk(chunk: str) -> None:
        nonlocal streamed_len
        streamed_len += len(chunk)
        if streamed_len > max_streamed_len:
            raise TranslationLengthExceededError(
                "Streamed translation exceeded configured max ratio "
                f"({streamed_len}>{max_streamed_len})."
            )

    return request_chat_completion_content_streaming(
        base_url,
        model,
        system_prompt,
        user_prompt,
        timeout,
        temperature=temperature,
        chunk_callback=check_chunk,
    )


def _load_paragraph_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"translate_pass1 input artifact missing: {path}")

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


def _translate_row(
    row: dict[str, Any],
    *,
    base_url: str,
    model: str,
    system_prompt: str,
    language: str,
    timeout: int,
    retry: int,
    max_length_ratio: float,
    temperature: float,
) -> dict[str, Any]:
    source_text = str(row.get("text", "")).strip()
    if not source_text:
        raise ValueError("translate_pass1 item is missing non-empty 'text'.")

    translation = ""
    error: str | None = None
    for attempt in range(retry + 1):
        try:
            translation = _call_lm(
                base_url=base_url,
                model=model,
                system_prompt=system_prompt,
                language=language,
                text=source_text,
                timeout=timeout,
                max_length_ratio=max_length_ratio,
                temperature=temperature,
            )
            error = None
            break
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            if attempt >= retry:
                break

    return {
        "item_id": str(row.get("item_id") or row.get("paragraph_id") or ""),
        "paragraph_id": str(row.get("paragraph_id") or row.get("item_id") or ""),
        "text": translation,
        "source_text": source_text,
        "translation": translation,
        "language": language,
        "model": model,
        "prompt": str(row.get("prompt") or ""),
        "error": error,
        "content_hash": row.get("content_hash"),
    }


def run_whole(ctx) -> None:
    cfg = _stage_config(ctx)

    language_binding = _binding_value(ctx, "lang") or _binding_value(ctx, "language")
    language = _normalize_language(language_binding or "")
    if not language:
        raise ValueError("translate_pass1 is missing required language binding ('lang').")

    model = str(cfg.get("model", "")).strip()
    if not model:
        raise ValueError("translate_pass1 config is missing required 'model'.")

    prompt_path = _resolve_prompt_path(language, cfg.get("prompt_root") and str(cfg.get("prompt_root")))
    system_prompt = prompt_path.read_text(encoding="utf-8")

    base_url = str(cfg.get("base_url", DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL))
    timeout = int(cfg.get("timeout", 180))
    retry = int(cfg.get("retry", 1))
    max_length_ratio = float(cfg.get("max_length_ratio", 3.0))
    temperature = float(cfg.get("temperature", 0.2))
    concurrency = max(1, int(cfg.get("concurrency", 1)))

    input_rows = _load_paragraph_rows(SOURCE_ARTIFACT_PATH)
    results: list[dict[str, Any]] = []

    def process_row(row: dict[str, Any]) -> dict[str, Any]:
        translated = _translate_row(
            row,
            base_url=base_url,
            model=model,
            system_prompt=system_prompt,
            language=language,
            timeout=timeout,
            retry=retry,
            max_length_ratio=max_length_ratio,
            temperature=temperature,
        )
        translated["prompt"] = str(prompt_path)
        return translated

    if concurrency == 1:
        for row in input_rows:
            results.append(process_row(row))
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(process_row, row) for row in input_rows]
            for future in as_completed(futures):
                results.append(future.result())
        order = {str(row.get("paragraph_id") or row.get("item_id") or ""): idx for idx, row in enumerate(input_rows)}
        results.sort(key=lambda row: order.get(str(row.get("paragraph_id") or row.get("item_id") or ""), 10**9))

    output_artifact_path = _resolve_output_artifact_path(ctx, language)
    output_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    with output_artifact_path.open("w", encoding="utf-8") as output_file:
        for row in results:
            output_file.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_item(ctx, item: dict[str, object]) -> None:
    """Compatibility shim for older per-item mode; delegates to whole-run processing."""
    _ = item
    run_whole(ctx)
