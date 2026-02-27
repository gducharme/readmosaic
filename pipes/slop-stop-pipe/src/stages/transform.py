from __future__ import annotations

from ._artifacts import read_jsonl, write_jsonl_artifact


def _build_rewrite_suggestion(text: str) -> str:
    # Deterministic first-pass suggestion for reviewer refinement.
    collapsed = " ".join(text.split())
    return collapsed


def run_whole(ctx) -> None:
    candidates = read_jsonl(ctx, "rewrite_candidates.jsonl", family="rewrite_candidates")
    transformed_rows: list[dict[str, object]] = []

    for candidate in candidates:
        text = str(candidate.get("text", "")).strip()
        transformed_rows.append(
            {
                "item_id": str(candidate.get("item_id", "")),
                "paragraph_id": str(candidate.get("paragraph_id", "")),
                "status": "transformed",
                "reasons": candidate.get("reasons", []),
                "instruction": candidate.get("instruction", ""),
                "original_text": text,
                "suggested_text": _build_rewrite_suggestion(text),
            }
        )

    write_jsonl_artifact(
        ctx,
        "transformed.jsonl",
        transformed_rows,
        family="transformed_rewrites",
    )
