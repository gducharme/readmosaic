from __future__ import annotations

from typing import Any, Protocol, Union


class LLMAdapter(Protocol):
    def structured_extract(
        self,
        *,
        model: str,
        prompt: str,
        json_schema: dict[str, Any],
        temperature: float = 0.0,
        timeout_s: int = 90,
    ) -> Union[str, dict[str, Any]]:
        ...
