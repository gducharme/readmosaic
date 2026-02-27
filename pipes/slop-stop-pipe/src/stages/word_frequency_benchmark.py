from __future__ import annotations

import re
from collections import Counter

import nltk
from nltk.corpus import brown

from ._artifacts import read_json, stage_config, write_json_artifact

TOKEN_PATTERN = re.compile(r"[A-Za-z']+")
TOP_N = 10


# Based on scripts/word_frequency_benchmark.py, but adapted for stage execution.
def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def _ensure_nltk_resources() -> None:
    for resource in ('corpora/stopwords', 'corpora/brown'):
        try:
            nltk.data.find(resource)
        except LookupError as exc:
            raise RuntimeError(
                'Missing NLTK data resource: '
                f"{resource}. Run `python scripts/setup_nltk_data.py` to install prerequisites."
            ) from exc


def _stopword_set(include_stopwords: bool) -> set[str]:
    if include_stopwords:
        return set()
    return set(nltk.corpus.stopwords.words('english'))


def _build_word_frequency(tokens: list[str], stopwords: set[str]) -> Counter[str]:
    filtered_tokens = [token for token in tokens if token not in stopwords]
    return Counter(filtered_tokens)


def run_whole(ctx) -> None:
    cfg = stage_config(ctx, 'word_frequency_benchmark')
    _ensure_nltk_resources()

    preprocessed_input_name = str(cfg.get('preprocessed_input', 'preprocessed/preprocessed.json'))
    preprocessed_payload = read_json(ctx, preprocessed_input_name, family="preprocessed")

    tokens = preprocessed_payload.get('tokens')
    if not isinstance(tokens, list):
        normalized_text = "\n".join(preprocessed_payload.get('paragraphs', []))
        tokens = _tokenize(normalized_text)

    manuscript_tokens = [token for token in tokens if isinstance(token, str)]
    include_stopwords = bool(cfg.get('include_stopwords', False))
    stopwords = _stopword_set(include_stopwords)

    counts = _build_word_frequency(manuscript_tokens, stopwords)
    manuscript_total = sum(counts.values())

    brown_tokens = [token.lower() for token in brown.words() if token.isalpha()]
    brown_counts = _build_word_frequency(brown_tokens, stopwords)
    brown_total = sum(brown_counts.values())

    top_n = int(cfg.get('top_n', TOP_N))
    top_words = []
    for idx, (word, count) in enumerate(counts.most_common(top_n), start=1):
        manuscript_per_million = (count / manuscript_total * 1_000_000) if manuscript_total else 0.0
        brown_count = brown_counts.get(word, 0)
        brown_per_million = (brown_count / brown_total * 1_000_000) if brown_total else 0.0
        relative_to_humanity_avg = (
            manuscript_per_million / brown_per_million if brown_per_million else None
        )
        top_words.append(
            {
                'rank': idx,
                'word': word,
                'manuscript_count': count,
                'manuscript_per_million': round(manuscript_per_million, 2),
                'brown_count': brown_count,
                'brown_per_million': round(brown_per_million, 2),
                'relative_to_humanity_avg': (
                    round(relative_to_humanity_avg, 2)
                    if relative_to_humanity_avg is not None
                    else None
                ),
            }
        )

    output = {
        'manuscript': preprocessed_payload.get('manuscript', 'manuscript.md'),
        'token_count': manuscript_total,
        'unique_word_count': len(counts),
        'brown_total_tokens': brown_total,
        'stopwords_filtered': not include_stopwords,
        'top_words': top_words,
        'word_frequency': dict(counts.most_common()),
    }

    output_name = str(cfg.get('output_name', 'word_frequency_report.json'))
    write_json_artifact(
        ctx,
        output_name,
        output,
        family="lexical_frequency",
    )
