from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reality_ingestor.reality_ingestor import RealityIngestor


def _resolve_markdown_path() -> Path:
    env_path = os.getenv("REALITY_MARKDOWN_PATH")
    if env_path:
        return Path(env_path)
    candidates = sorted(Path("artifacts/inputs").rglob("*.md"))
    if not candidates:
        raise FileNotFoundError("No markdown files found in artifacts/inputs")
    return candidates[0]


def run_whole(ctx) -> None:  # noqa: ARG001
    ingestor = RealityIngestor.from_env()
    markdown_path = _resolve_markdown_path()
    parsed = ingestor.parse_markdown(str(markdown_path))
    output_path = Path("artifacts/parsed_chapter.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(parsed.to_dict(), indent=2), encoding="utf-8")
