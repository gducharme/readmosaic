from __future__ import annotations

import json
import re
from collections import Counter

from ._artifacts import output_artifact_dir, read_json, stage_config

TOKEN_PATTERN = re.compile(r"[A-Za-z']+")
TOP_N = 10


# Based on scripts/word_frequency_benchmark.py, but adapted for stage execution.
def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def run_whole(ctx) -> None:
    cfg = stage_config(ctx, 'word_frequency_benchmark')

    preprocessed_input_name = str(cfg.get('preprocessed_input', 'preprocessed.json'))
    preprocessed_payload = read_json(ctx, preprocessed_input_name)

    tokens = preprocessed_payload.get('tokens')
    if not isinstance(tokens, list):
        normalized_text = "\n".join(preprocessed_payload.get('paragraphs', []))
        tokens = _tokenize(normalized_text)

    counts = Counter(token for token in tokens if isinstance(token, str))
    top_n = int(cfg.get('top_n', TOP_N))
    top_words = [
        {'rank': idx, 'word': word, 'count': count}
        for idx, (word, count) in enumerate(counts.most_common(top_n), start=1)
    ]

    output = {
        'manuscript': preprocessed_payload.get('manuscript', 'manuscript.md'),
        'token_count': sum(counts.values()),
        'unique_word_count': len(counts),
        'top_words': top_words,
        'word_frequency': dict(counts.most_common()),
    }

    output_name = str(cfg.get('output_name', 'word_frequency_report.json'))
    output_dir = output_artifact_dir(ctx)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / output_name).write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
