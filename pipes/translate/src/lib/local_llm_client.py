from __future__ import annotations

import sys
from pathlib import Path


def _append_repo_root_for_shared_libs() -> None:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "libs" / "local_llm.py").exists():
            sys.path.append(str(parent))
            return


# Ensure repository-level shared libs (libs/local_llm.py) are importable at runtime.
_append_repo_root_for_shared_libs()

from libs.local_llm import (  # noqa: E402
    DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL,
    request_chat_completion_content,
    request_chat_completion_content_streaming,
)

DEFAULT_CHAT_COMPLETIONS_URL = DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL


def chat_completion(
    *,
    base_url: str,
    model: str,
    system_prompt: str,
    user_content: str,
    timeout: int,
    temperature: float = 0.0,
) -> str:
    return request_chat_completion_content(
        base_url=base_url,
        model=model,
        system_prompt=system_prompt,
        user_content=user_content,
        timeout=timeout,
        temperature=temperature,
    )


def chat_completion_streaming(
    *,
    base_url: str,
    model: str,
    system_prompt: str,
    user_content: str,
    timeout: int,
    temperature: float = 0.0,
    chunk_callback=None,
) -> str:
    return request_chat_completion_content_streaming(
        base_url=base_url,
        model=model,
        system_prompt=system_prompt,
        user_content=user_content,
        timeout=timeout,
        temperature=temperature,
        chunk_callback=chunk_callback,
    )
