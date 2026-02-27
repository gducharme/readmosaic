from __future__ import annotations

from collections import Counter

from ._artifacts import read_jsonl, write_json_artifact, write_jsonl_artifact


def run_whole(ctx) -> None:
    transformed_rows = read_jsonl(ctx, "transformed.jsonl", family="transformed_rewrites")

    reviewed_rows: list[dict[str, object]] = []
    reason_counter: Counter[str] = Counter()

    for row in transformed_rows:
        reasons = row.get("reasons", [])
        if isinstance(reasons, list):
            for reason in reasons:
                reason_counter[str(reason)] += 1

        reviewed_rows.append(
            {
                "item_id": str(row.get("item_id", "")),
                "paragraph_id": str(row.get("paragraph_id", "")),
                "status": "reviewed",
                "review_decision": "approve",
                "reasons": reasons if isinstance(reasons, list) else [],
                "original_text": row.get("original_text", ""),
                "suggested_text": row.get("suggested_text", ""),
            }
        )

    summary = {
        "reviewed_item_count": len(reviewed_rows),
        "approved_item_count": len(reviewed_rows),
        "rejected_item_count": 0,
        "reason_counts": dict(reason_counter),
    }

    write_jsonl_artifact(
        ctx,
        "reviewed.jsonl",
        reviewed_rows,
        family="reviewed_rewrites",
    )
    write_json_artifact(
        ctx,
        "review_summary.json",
        summary,
        family="review_summary",
    )
