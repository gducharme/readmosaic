from __future__ import annotations

import json
import re
from pathlib import Path

PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n+")
WORD_RE = re.compile(r"[A-Za-z']+")
MANUSCRIPT_PATH = Path("artifacts/inputs/manuscript.md")


def _clean_markdown_line(line: str) -> str:
    line = re.sub(r"^\s{0,3}#{1,6}\s+", "", line)
    line = re.sub(r"^\s*>\s?", "", line)
    line = re.sub(r"^\s*[-*+]\s+", "", line)
    line = re.sub(r"^\s*\d+\.\s+", "", line)
    line = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", line)
    line = re.sub(r"\[([^\]]+)\]\([^\)]*\)", r"\1", line)
    line = re.sub(r"`[^`]+`", "", line)
    line = re.sub(r"[*_]{1,3}", "", line)
    return re.sub(r"\s+", " ", line).strip()


def _normalize_text(markdown_text: str) -> str:
    without_code_blocks = re.sub(r"```.*?```", "", markdown_text, flags=re.DOTALL)
    cleaned_lines = [_clean_markdown_line(line) for line in without_code_blocks.splitlines()]
    return "\n".join(cleaned_lines)


def _build_payload(normalized_text: str, manuscript_name: str) -> dict[str, object]:
    paragraphs = [p.strip() for p in PARAGRAPH_SPLIT_RE.split(normalized_text) if p.strip()]
    tokens = [t.lower() for t in WORD_RE.findall(normalized_text)]
    return {
        "manuscript": manuscript_name,
        "paragraph_count": len(paragraphs),
        "token_count": len(tokens),
        "paragraphs": paragraphs,
        "tokens": tokens,
    }


def run_whole(ctx) -> None:
    _ = ctx
    manuscript_path = MANUSCRIPT_PATH
    if not manuscript_path.exists():
        raise FileNotFoundError(
            f"Expected hardcoded manuscript file '{MANUSCRIPT_PATH}' relative to the pipe root."
        )

    output_dir = Path("artifacts")
    output_dir.mkdir(parents=True, exist_ok=True)

    normalized_text = _normalize_text(manuscript_path.read_text(encoding="utf-8"))
    payload = _build_payload(normalized_text, manuscript_name=manuscript_path.name)

    (output_dir / "preprocessed.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
