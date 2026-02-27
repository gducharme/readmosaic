from __future__ import annotations

import json
import re
from pathlib import Path

from ._artifacts import resolve_output_path, stage_config, write_jsonl_artifact

PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n+")
WORD_RE = re.compile(r"[A-Za-z']+")
DEFAULT_INPUT_MANUSCRIPT = Path("artifacts/inputs/manuscript.md")


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
    cfg = stage_config(ctx, 'preprocessing')

    manuscript_path_raw = cfg.get('input_manuscript') or cfg.get('manuscript_path')
    manuscript_path = Path(manuscript_path_raw) if manuscript_path_raw else DEFAULT_INPUT_MANUSCRIPT
    if not manuscript_path.is_absolute():
        manuscript_path = Path.cwd() / manuscript_path

    if not manuscript_path.exists():
        raise FileNotFoundError(
            f"Expected manuscript file '{manuscript_path}'. Configure run_config.rc.preprocessing.input_manuscript if needed."
        )

    normalized_text = _normalize_text(manuscript_path.read_text(encoding='utf-8'))
    payload = _build_payload(normalized_text, manuscript_name=manuscript_path.name)

    output_name = str(cfg.get('output_name', 'preprocessed.json'))
    preprocessed_path = resolve_output_path(
        ctx,
        default_name=output_name,
        family="preprocessed",
    )
    preprocessed_path.parent.mkdir(parents=True, exist_ok=True)
    preprocessed_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )

    paragraph_rows = []
    for index, paragraph in enumerate(payload["paragraphs"], start=1):
        paragraph_rows.append(
            {
                "item_id": f"p-{index:04d}",
                "paragraph_id": f"p-{index:04d}",
                "text": paragraph,
            }
        )
    write_jsonl_artifact(
        ctx,
        "paragraphs.jsonl",
        paragraph_rows,
        family="paragraphs",
    )
