#!/usr/bin/env python3
"""Run every critic prompt in prompts/critics against a local LM Studio endpoint."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, request

DEFAULT_BASE_URL = "http://localhost:1234/v1/chat/completions"
DEFAULT_CRITICS_DIR = Path("prompts/critics")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load each markdown critic prompt from prompts/critics as a system prompt, "
            "submit a manuscript markdown as the first user message, and write one "
            "unstructured JSON object keyed by critic filename."
        )
    )
    parser.add_argument("--model", required=True, help="Model identifier served by LM Studio.")
    parser.add_argument(
        "--critics-dir",
        type=Path,
        default=DEFAULT_CRITICS_DIR,
        help="Directory containing critic markdown files.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="LM Studio chat completions URL.",
    )
    parser.add_argument(
        "--manuscript",
        type=Path,
        required=True,
        help="Markdown file sent as the first user message to each critic.",
    )
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON file path. Defaults to critics_outputs/critics_responses_<timestamp>.json",
    )
    return parser.parse_args()


def call_lm(base_url: str, model: str, system_prompt: str, manuscript_text: str, timeout: int) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": manuscript_text,
            }
        ],
        "temperature": 0.2,
    }
    req = request.Request(
        base_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except error.URLError as exc:
        raise SystemExit(f"Failed to contact model endpoint at {base_url}: {exc}") from exc

    parsed = json.loads(body)
    return str(parsed["choices"][0]["message"]["content"]).strip()


def gather_critic_files(critics_dir: Path) -> list[Path]:
    if not critics_dir.exists() or not critics_dir.is_dir():
        raise SystemExit(f"Critics directory not found: {critics_dir}")
    critics = sorted(p for p in critics_dir.iterdir() if p.suffix.lower() == ".md" and p.is_file())
    if not critics:
        raise SystemExit(f"No markdown critics found in: {critics_dir}")
    return critics


def load_manuscript(manuscript_path: Path) -> str:
    if not manuscript_path.exists() or not manuscript_path.is_file():
        raise SystemExit(f"Manuscript file not found: {manuscript_path}")
    if manuscript_path.suffix.lower() != ".md":
        raise SystemExit(f"Manuscript must be a markdown file (.md): {manuscript_path}")
    return manuscript_path.read_text(encoding="utf-8")


def main() -> None:
    args = parse_args()
    critics = gather_critic_files(args.critics_dir)
    manuscript_text = load_manuscript(args.manuscript)

    output_path = args.output
    if output_path is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = Path("critics_outputs") / f"critics_responses_{timestamp}.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    results: dict[str, str] = {}
    for critic_file in critics:
        critic_name = critic_file.stem
        system_prompt = critic_file.read_text(encoding="utf-8")
        response_text = call_lm(
            args.base_url,
            args.model,
            system_prompt,
            manuscript_text,
            args.timeout,
        )
        results[critic_name] = response_text
        print(f"Processed critic: {critic_name}")

    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote critics JSON: {output_path}")


if __name__ == "__main__":
    main()
