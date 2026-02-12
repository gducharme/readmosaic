#!/usr/bin/env python3
"""Shared helpers for calling a local LM Studio chat-completions endpoint."""
from __future__ import annotations

import json
from urllib import error, request
from urllib.parse import urlparse

DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL = "http://localhost:1234/v1/chat/completions"


def normalize_chat_completions_url(base_url: str) -> str:
    """Normalize a base URL to LM Studio's OpenAI-compatible chat-completions endpoint."""
    raw = base_url.strip()
    if not raw:
        return DEFAULT_LM_STUDIO_CHAT_COMPLETIONS_URL

    parsed = urlparse(raw)
    path = parsed.path.rstrip("/")

    if path.endswith("/chat/completions"):
        return raw.rstrip("/")
    if path.endswith("/v1"):
        return f"{raw.rstrip('/')}/chat/completions"
    if path == "":
        return f"{raw.rstrip('/')}/v1/chat/completions"
    return raw.rstrip("/")


def post_chat_completion(base_url: str, payload: dict[str, object], timeout: int) -> dict[str, object]:
    """POST a chat completion request and return parsed JSON response."""
    endpoint = normalize_chat_completions_url(base_url)
    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except error.URLError as exc:
        raise SystemExit(f"Failed to contact model endpoint at {endpoint}: {exc}") from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Model endpoint returned invalid JSON from {endpoint}: {body[:400]}") from exc

    if not isinstance(parsed, dict):
        raise SystemExit(f"Model endpoint returned non-object JSON from {endpoint}.")
    return parsed


def extract_message_content(response_payload: dict[str, object]) -> str:
    """Extract assistant message content from an OpenAI-compatible response payload."""
    choices = response_payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise SystemExit("Model response missing non-empty 'choices' array.")
    first = choices[0]
    if not isinstance(first, dict):
        raise SystemExit("Model response 'choices[0]' is not an object.")
    message = first.get("message")
    if not isinstance(message, dict):
        raise SystemExit("Model response missing 'choices[0].message' object.")
    content = message.get("content")
    if not isinstance(content, str):
        raise SystemExit("Model response missing string 'choices[0].message.content'.")
    return content.strip()
