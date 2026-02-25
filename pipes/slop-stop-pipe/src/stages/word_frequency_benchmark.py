from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

TOKEN_PATTERN = re.compile(r"[A-Za-z']+")
TOP_N = 10


# Based on scripts/word_frequency_benchmark.py, but adapted for stage execution.
def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def run_whole(ctx) -> None:
    _ = ctx
    preprocessed_path = Path("artifacts/preprocessed.json")
    if not preprocessed_path.exists():
        raise FileNotFoundError(
            "Missing preprocessing output: artifacts/preprocessed.json. "
            "Run preprocessing stage first."
        )

    payload = json.loads(preprocessed_path.read_text(encoding="utf-8"))
    tokens = payload.get("tokens")
    if not isinstance(tokens, list):
        normalized_text = "\n".join(payload.get("paragraphs", []))
        tokens = _tokenize(normalized_text)

    counts = Counter(token for token in tokens if isinstance(token, str))
    top_words = [
        {"rank": idx, "word": word, "count": count}
        for idx, (word, count) in enumerate(counts.most_common(TOP_N), start=1)
    ]

    output = {
        "manuscript": payload.get("manuscript", "manuscript.md"),
        "token_count": sum(counts.values()),
        "unique_word_count": len(counts),
        "top_words": top_words,
        "word_frequency": dict(counts.most_common()),
    }

    output_dir = Path("artifacts")
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "word_frequency_report.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
