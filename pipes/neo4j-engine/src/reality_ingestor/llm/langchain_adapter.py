from __future__ import annotations

from typing import Any

from .base import LLMAdapter
from ..errors import ExtractionAdapterError

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
            raise ExtractionAdapterError("langchain is not installed in this environment")
        try:
            client = OpenAI(model_name=model, temperature=temperature, request_timeout=timeout_s)
            return client(prompt)
        except Exception as exc:  # pragma: no cover - runtime dependent
            raise ExtractionAdapterError("LangChain adapter request failed") from exc
