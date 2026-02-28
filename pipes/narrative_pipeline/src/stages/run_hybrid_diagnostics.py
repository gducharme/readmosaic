from __future__ import annotations

from ._artifacts import read_json, write_json_artifact
from .metrics_core import metric_hybrid_burstiness, metric_hybrid_semantic_repetition


def run_whole(ctx) -> None:
    index_payload = read_json(
        ctx,
        "preprocessed/paragraph_index.json",
        family="paragraph_index",
    )
    paragraphs = [row.get("text", "") for row in index_payload.get("paragraphs", []) if isinstance(row, dict)]

    semantic_repetition = metric_hybrid_semantic_repetition(paragraphs)
    burstiness = metric_hybrid_burstiness(paragraphs)

    write_json_artifact(
        ctx,
        "diagnostics/hybrid_semantic_repetition.json",
        semantic_repetition,
        family="hybrid_semantic_repetition",
    )
    write_json_artifact(
        ctx,
        "diagnostics/hybrid_burstiness.json",
        burstiness,
        family="hybrid_burstiness",
    )
