from __future__ import annotations

import json
from typing import Any
from urllib import error, request
from urllib.parse import urlparse

from .base import LLMAdapter
from ..errors import ExtractionAdapterError


def _normalize_chat_completions_url(base_url: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        return "http://127.0.0.1:1234/v1/chat/completions"
    parsed = urlparse(raw)
    path = parsed.path.rstrip("/")
    if path.endswith("/chat/completions"):
        return raw.rstrip("/")
    if path.endswith("/v1"):
        return f"{raw.rstrip('/')}/chat/completions"
    if path == "":
        return f"{raw.rstrip('/')}/v1/chat/completions"
    return raw.rstrip("/")


class LiteLLMAdapter(LLMAdapter):
    """OpenAI-compatible HTTP adapter (works with LM Studio, no litellm dependency)."""

    def __init__(self, *, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

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
        endpoint = _normalize_chat_completions_url(self.base_url)
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "extraction_response",
                    "strict": True,
                    "schema": json_schema,
                },
            },
            "stream": False,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        req = request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=timeout_s) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:  # pragma: no cover - runtime dependent
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            detail = f"HTTP {exc.code} from LM Studio at {endpoint}"
            if body:
                detail += f": {body[:300]}"
            raise ExtractionAdapterError(detail) from exc
        except Exception as exc:  # pragma: no cover - runtime dependent
            raise ExtractionAdapterError(f"LM Studio unreachable at {endpoint}") from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExtractionAdapterError(f"Model endpoint returned invalid JSON: {raw[:300]}") from exc

        try:
            content = parsed["choices"][0]["message"]["content"]
        except Exception as exc:
            raise ExtractionAdapterError("Model response missing choices[0].message.content") from exc
        if not isinstance(content, str):
            raise ExtractionAdapterError("Model response content is not a string")
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            raise ExtractionAdapterError("Model content is not valid JSON text") from exc
