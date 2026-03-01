from __future__ import annotations

import hashlib
from pathlib import Path

from .errors import MarkdownParseError
from .models import Chunk, ParsedChapter


def parse_markdown(markdown_path: Path | str) -> ParsedChapter:
    path = Path(markdown_path)
    if not path.exists():
        raise MarkdownParseError(f"Markdown file not found: {path}")

    raw = path.read_text(encoding="utf-8")
    normalized = raw.replace("\r\n", "\n").strip()
    if not normalized:
        raise MarkdownParseError("Markdown file is empty after normalization")

    chapter_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    chunk_texts = [chunk.strip() for chunk in normalized.split("\n\n") if chunk.strip()]
    chunks: list[Chunk] = []
    for idx, text in enumerate(chunk_texts):
        chunk_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        chunk_id = f"{chapter_hash[:8]}-chunk-{idx + 1}"
        chunks.append(Chunk(chunk_id=chunk_id, text=text, hash=chunk_hash, sequence_id=idx))

    return ParsedChapter(
        chapter_id=chapter_hash[:16],
        chapter_hash=chapter_hash,
        source_path=str(path.resolve()),
        full_text=normalized,
        chunks=chunks,
    )
