#!/usr/bin/env python3
"""Shared helpers for calling a local LM Studio chat-completions endpoint."""
from __future__ import annotations

import json
from typing import Callable
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


def request_chat_completion_content(
    base_url: str,
    model: str,
    system_prompt: str,
    user_content: str,
    timeout: int,
    temperature: float = 0.0,
) -> str:
    """Send a standard system+user chat request and return assistant text content."""
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
    }
    parsed = post_chat_completion(base_url, payload, timeout)
    return extract_message_content(parsed)


def request_chat_completion_content_streaming(
    base_url: str,
    model: str,
    system_prompt: str,
    user_content: str,
    timeout: int,
    temperature: float = 0.0,
    chunk_callback: Callable[[str], None] | None = None,
) -> str:
    """Send a streaming chat request and return assistant text content.

    If ``chunk_callback`` is provided, each text delta is passed in order. If the callback
    raises, streaming stops and that exception is propagated.
    """
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
        "stream": True,
    }

    endpoint = normalize_chat_completions_url(base_url)
    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )

    collected: list[str] = []
    try:
        with request.urlopen(req, timeout=timeout) as response:
            for raw_line in response:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if not data or data == "[DONE]":
                    continue
                try:
                    event = json.loads(data)
                except json.JSONDecodeError as exc:
                    raise SystemExit(
                        f"Model endpoint returned invalid stream JSON from {endpoint}: {data[:400]}"
                    ) from exc

                choices = event.get("choices")
                if not isinstance(choices, list) or not choices:
                    continue
                first = choices[0]
                if not isinstance(first, dict):
                    continue
                delta = first.get("delta")
                if not isinstance(delta, dict):
                    continue
                chunk = delta.get("content")
                if not isinstance(chunk, str) or not chunk:
                    continue

                collected.append(chunk)
                if chunk_callback is not None:
                    chunk_callback(chunk)
    except error.URLError as exc:
        raise SystemExit(f"Failed to contact model endpoint at {endpoint}: {exc}") from exc

    content = "".join(collected).strip()
    if not content:
        raise SystemExit("Model stream completed without translated content.")
    return content
