from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[4]))

from libs.local_llm import (  # noqa: E402
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content_streaming,
)

DEFAULT_PROMPT_ROOTS = [Path("prompt/translate"), Path("prompts/translate")]
STAGE_CONFIG_PATH = Path(__file__).with_name("translate_pass1_config.json")


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
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for candidate in _candidate_prompt_paths(root, language):
            if candidate.exists() and candidate.is_file():
                return candidate

    roots_display = ", ".join(str(root) for root in roots)
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


def run_item(ctx, item: dict[str, object]) -> None:
    cfg = _stage_config(ctx)

    language = _normalize_language(str(cfg.get("language", "")))
    if not language:
        raise ValueError("translate_pass1 config is missing required 'language'.")

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

    source_text = str(item.get("text", "")).strip()
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

    row = {
        "item_id": str(item.get("item_id") or item.get("paragraph_id") or ""),
        "paragraph_id": str(item.get("paragraph_id") or item.get("item_id") or ""),
        "text": source_text,
        "translation": translation,
        "language": language,
        "model": model,
        "prompt": str(prompt_path),
        "error": error,
    }

    out_path = Path("pass1_pre/paragraphs.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
