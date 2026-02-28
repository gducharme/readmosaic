from __future__ import annotations

from ._artifacts import read_json, write_json_artifact
from .metrics_core import metric_document_entropy, metric_document_patterns, metric_document_themes


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
    tokens = [token for token in token_payload.get("tokens", []) if isinstance(token, str)]

    themes = metric_document_themes(paragraphs, tokens)
    patterns = metric_document_patterns(paragraphs)
    entropy = metric_document_entropy(tokens)

    write_json_artifact(
        ctx,
        "diagnostics/document_themes.json",
        themes,
        family="document_themes",
    )
    write_json_artifact(
        ctx,
        "diagnostics/document_patterns.json",
        patterns,
        family="document_patterns",
    )
    write_json_artifact(
        ctx,
        "diagnostics/document_entropy.json",
        entropy,
        family="document_entropy",
    )
