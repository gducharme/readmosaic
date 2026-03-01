from __future__ import annotations

from typing import Any

from .base import LLMAdapter

try:
    from langchain.llms import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None


class LangChainAdapter(LLMAdapter):
    def structured_extract(
        self,
        *,
        model: str,
        prompt: str,
        json_schema: dict[str, Any],
        temperature: float = 0.0,
        timeout_s: int = 90,
        **kwargs: Any,
    ) -> str:
        if OpenAI is None:
            raise RuntimeError("langchain is not installed in this environment")
        client = OpenAI(model_name=model, temperature=temperature, request_timeout=timeout_s)
        return client(prompt)
