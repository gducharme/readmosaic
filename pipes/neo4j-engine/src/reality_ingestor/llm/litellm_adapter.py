from __future__ import annotations

from typing import Any

from .base import LLMAdapter

try:
    import litellm
except ImportError:  # pragma: no cover
    litellm = None


class LiteLLMAdapter(LLMAdapter):
    def structured_extract(
        self,
        *,
        model: str,
        prompt: str,
        json_schema: dict[str, Any],
        temperature: float = 0.0,
        timeout_s: int = 90,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if litellm is None:
            raise RuntimeError("litellm is not installed in this environment")
        llm = litellm.Client(model=model)
        response = llm.generate([{"role": "user", "content": prompt}], temperature=temperature)
        if isinstance(response, str):
            return response
        return response.content
