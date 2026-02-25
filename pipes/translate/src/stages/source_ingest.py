from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Paragraph:
    paragraph_id: str
    text: str
    content_hash: str


def _sha256_bytes(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _sha256_text(payload: str) -> str:
    return _sha256_bytes(payload.encode("utf-8"))


def _split_markdown_paragraphs(markdown: str) -> Iterable[str]:
    blocks = [part.strip() for part in markdown.replace("\r\n", "\n").split("\n\n")]
    return [block for block in blocks if block]


def _resolve_markdown_input(ctx) -> Path:
    candidates = []

    inputs_dir = getattr(ctx, "inputs_dir", None)
    if inputs_dir:
        inputs_path = Path(inputs_dir)
        candidates.extend(
            [
                inputs_path / "markdown",
                inputs_path / "markdown.md",
                inputs_path / "markdown.markdown",
                inputs_path / "input" / "markdown",
                inputs_path / "input" / "markdown.md",
            ]
        )

    candidates.extend(
        [
            Path("artifacts/inputs/markdown"),
            Path("artifacts/inputs/markdown.md"),
            Path("artifacts/input/markdown"),
            Path("artifacts/input/markdown.md"),
        ]
    )

    for candidate in candidates:
        if candidate.is_file():
            return candidate
        if candidate.is_dir():
            matches = sorted(
                path
                for pattern in ("*.md", "*.markdown", "*.txt")
                for path in candidate.glob(pattern)
                if path.is_file()
            )
            if matches:
                return matches[0]

    raise FileNotFoundError(
        "Unable to locate input markdown. Expected one of: "
        "<inputs_dir>/markdown(.md), artifacts/inputs/markdown(.md), "
        "or artifacts/input/markdown(.md)."
    )


def _build_paragraph_rows(markdown_text: str) -> list[Paragraph]:
    rows: list[Paragraph] = []
    for index, paragraph_text in enumerate(_split_markdown_paragraphs(markdown_text), start=1):
        paragraph_id = f"p-{index:04d}"
        rows.append(
            Paragraph(
                paragraph_id=paragraph_id,
                text=paragraph_text,
                content_hash=_sha256_text(paragraph_text),
            )
        )
    return rows


def run_whole(ctx) -> None:
    """Ingest markdown source and materialize phase-1 seed artifacts."""

    run_id = getattr(ctx, "run_id", "local-run")
    pipeline_id = getattr(ctx, "pipeline_id", "translate-pipeline")
    attempt = int(getattr(ctx, "attempt", 1))
    now = datetime.now(timezone.utc).isoformat()

    markdown_path = _resolve_markdown_input(ctx)
    markdown_text = markdown_path.read_text(encoding="utf-8")
    paragraph_rows = _build_paragraph_rows(markdown_text)

    paragraphs_path = Path("paragraphs.jsonl")
    manifest_path = Path("manifest.json")

    paragraphs_path.parent.mkdir(parents=True, exist_ok=True)

    with paragraphs_path.open("w", encoding="utf-8") as paragraphs_file:
        for row in paragraph_rows:
            paragraphs_file.write(
                json.dumps(
                    {
                        "item_id": row.paragraph_id,
                        "paragraph_id": row.paragraph_id,
                        "text": row.text,
                        "content_hash": row.content_hash,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    inputs_ref = {
        "name": "input_markdown",
        "path": str(markdown_path),
        "hash": _sha256_bytes(markdown_path.read_bytes()),
        "schema_version": "phase1-v0",
        "produced_by": {
            "run_id": run_id,
            "stage_id": "external_input",
            "attempt": attempt,
        },
    }

    outputs = []
    for output_path, output_name in ((paragraphs_path, "paragraphs"),):
        outputs.append(
            {
                "name": output_name,
                "path": str(output_path),
                "hash": _sha256_bytes(output_path.read_bytes()),
                "schema_version": "phase1-v0",
                "produced_by": {
                    "run_id": run_id,
                    "stage_id": "source_ingest",
                    "attempt": attempt,
                },
            }
        )

    manifest = {
        "manifest_version": "phase1-v0",
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "code_version": "source_ingest-v1",
        "config_hash": _sha256_text("translate-pipeline:phase1")[:15],
        "created_at": now,
        "inputs": [inputs_ref],
        "stage_outputs": [
            {
                "stage_id": "source_ingest",
                "outputs": outputs,
            }
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_data["stage_outputs"][0]["outputs"].append(
        {
            "name": "run_manifest",
            "path": str(manifest_path),
            "hash": _sha256_bytes(manifest_path.read_bytes()),
            "schema_version": "phase1-v0",
            "produced_by": {
                "run_id": run_id,
                "stage_id": "source_ingest",
                "attempt": attempt,
            },
        }
    )
    manifest_path.write_text(json.dumps(manifest_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
