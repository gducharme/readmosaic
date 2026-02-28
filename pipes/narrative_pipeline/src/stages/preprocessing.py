from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from ._artifacts import stage_config, write_json_artifact, write_jsonl_artifact, write_text_artifact

WORD_RE = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
PARA_SPLIT_RE = re.compile(r"\n\s*\n+")


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _clean_markdown(markdown_text: str) -> str:
    normalized = (
        markdown_text.replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u2014", "-")
        .replace("\u2013", "-")
    )
    without_fenced_code = re.sub(r"```.*?```", "", normalized, flags=re.DOTALL)
    cleaned_lines = []
    for line in without_fenced_code.splitlines():
        line = re.sub(r"^\s{0,3}#{1,6}\s+", "", line)
        line = re.sub(r"^\s*>\s?", "", line)
        line = re.sub(r"^\s*[-*+]\s+", "", line)
        line = re.sub(r"^\s*\d+\.\s+", "", line)
        line = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", line)
        line = re.sub(r"\[([^\]]+)\]\([^\)]*\)", r"\1", line)
        line = re.sub(r"`[^`]+`", "", line)
        line = re.sub(r"[*_]{1,3}", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def _resolve_input_path(ctx, cfg: dict[str, object]) -> Path:
    configured = cfg.get("input_manuscript")
    if isinstance(configured, str) and configured.strip():
        path = Path(configured.strip())
        return path if path.is_absolute() else Path.cwd() / path

    inputs_dir = getattr(ctx, "inputs_dir", None)
    candidates: list[Path] = []
    if inputs_dir:
        input_root = Path(inputs_dir)
        candidates.extend(
            [
                input_root / "manuscript.md",
                input_root / "markdown.md",
                input_root / "manuscript.txt",
            ]
        )
    candidates.extend(
        [
            Path("artifacts/inputs/manuscript.md"),
            Path("artifacts/inputs/markdown.md"),
            Path("artifacts/inputs/manuscript.txt"),
            Path("artifacts/inputs/markdown.txt"),
        ]
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "Unable to locate manuscript input. Expected manuscript.md/markdown.md in artifacts/inputs or inputs_dir."
    )


def _split_paragraphs(text: str) -> list[str]:
    return [part.strip() for part in PARA_SPLIT_RE.split(text) if part.strip()]


def _build_index(normalized_text: str, paragraphs: list[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    cursor = 0
    for idx, paragraph in enumerate(paragraphs, start=1):
        offset = normalized_text.find(paragraph, cursor)
        start = offset if offset >= 0 else cursor
        end = start + len(paragraph)
        cursor = end
        rows.append(
            {
                "paragraph_id": f"p-{idx:04d}",
                "paragraph_index": idx - 1,
                "start_char": start,
                "end_char": end,
                "section_index": 0,
                "text": paragraph,
                "token_count": len(WORD_RE.findall(paragraph)),
                "content_hash": _sha256_text(paragraph),
            }
        )
    return rows


def run_whole(ctx) -> None:
    cfg = stage_config(ctx, "preprocessing")
    source_path = _resolve_input_path(ctx, cfg)
    raw_text = source_path.read_text(encoding="utf-8")
    normalized_text = _clean_markdown(raw_text)
    paragraphs = _split_paragraphs(normalized_text)
    paragraph_index = _build_index(normalized_text, paragraphs)
    tokens = [token.lower().replace("'", "") for token in WORD_RE.findall(normalized_text)]
    tokens = [token for token in tokens if token]
    run_id = str(getattr(ctx, "run_id", "local-run"))

    write_json_artifact(
        ctx,
        "preprocessed/manuscript_raw.json",
        {"text": raw_text},
        family="preprocessed_raw",
    )
    write_json_artifact(
        ctx,
        "preprocessed/manuscript_normalized.json",
        {"text": normalized_text},
        family="preprocessed_normalized",
    )
    write_text_artifact(ctx, "preprocessed/manuscript_raw.txt", raw_text)
    write_text_artifact(ctx, "preprocessed/manuscript_normalized.txt", normalized_text)

    token_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "source_path": str(source_path),
        "token_count": len(tokens),
        "unique_token_count": len(set(tokens)),
        "tokens": tokens,
    }
    write_json_artifact(
        ctx,
        "preprocessed/manuscript_tokens.json",
        token_payload,
        family="manuscript_tokens",
    )

    index_payload = {
        "run_id": run_id,
        "source_path": str(source_path),
        "paragraph_count": len(paragraph_index),
        "paragraphs": paragraph_index,
    }
    write_json_artifact(
        ctx,
        "preprocessed/paragraph_index.json",
        index_payload,
        family="paragraph_index",
    )

    paragraph_rows = [
        {
            "item_id": row["paragraph_id"],
            "paragraph_id": row["paragraph_id"],
            "paragraph_index": row["paragraph_index"],
            "text": row["text"],
            "content_hash": row["content_hash"],
        }
        for row in paragraph_index
    ]
    write_jsonl_artifact(
        ctx,
        "preprocessed/paragraphs.jsonl",
        paragraph_rows,
        family="paragraphs",
    )
