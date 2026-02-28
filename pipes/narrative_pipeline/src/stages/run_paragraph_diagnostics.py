from __future__ import annotations

from ._artifacts import read_json, write_json_artifact
from .metrics_core import (
    normalize_token,
    metric_paragraph_entropy,
    metric_paragraph_signal_density,
    metric_paragraph_surprisal,
)


def run_whole(ctx) -> None:
    token_payload = read_json(
        ctx,
        "preprocessed/manuscript_tokens.json",
        family="manuscript_tokens",
    )
    index_payload = read_json(
        ctx,
        "preprocessed/paragraph_index.json",
        family="paragraph_index",
    )
    paragraphs = [row.get("text", "") for row in index_payload.get("paragraphs", []) if isinstance(row, dict)]
    raw_tokens = [token for token in token_payload.get("tokens", []) if isinstance(token, str)]
    tokens = [normalize_token(token) for token in raw_tokens if normalize_token(token)]

    signal_density = metric_paragraph_signal_density(paragraphs)
    surprisal = metric_paragraph_surprisal(paragraphs, tokens)
    entropy = metric_paragraph_entropy(paragraphs)

    write_json_artifact(
        ctx,
        "diagnostics/paragraph_signal_density.json",
        signal_density,
        family="paragraph_signal_density",
    )
    write_json_artifact(
        ctx,
        "diagnostics/paragraph_surprisal.json",
        surprisal,
        family="paragraph_surprisal",
    )
    write_json_artifact(
        ctx,
        "diagnostics/paragraph_entropy.json",
        entropy,
        family="paragraph_entropy",
    )
