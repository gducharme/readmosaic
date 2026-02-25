from __future__ import annotations

from pathlib import Path


def run_whole(ctx) -> None:
    Path("artifacts/items.jsonl").write_text("")
