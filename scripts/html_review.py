#!/usr/bin/env python3
"""Render a confidence-colored manuscript as an HTML review page.

The script scans *_edits.json outputs, maps issues back to token IDs produced by
scripts/pre_processing.py, and writes a standalone HTML file with:

- token-level confidence coloring
- paragraph/sentence/word issue propagation
- per-word tooltip bubbles containing issue details
"""
from __future__ import annotations

import argparse
import html
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

CONFIDENCE_LEVELS = (
    (0.0, 1.0, "#1f7a3d"),
    (0.25, 0.8, "#3aa45e"),
    (0.5, 0.6, "#c09a2d"),
    (0.75, 0.4, "#e57a44"),
    (1.0, 0.2, "#ff6f61"),
)

PUNCTUATION_CLOSERS = {
    ".",
    ",",
    "!",
    "?",
    ";",
    ":",
    "%",
    ")",
    "]",
    "}",
    "''",
    "”",
    "’",
}
PUNCTUATION_OPENERS = {"(", "[", "{", "``", "“", "‘"}
NO_SPACE_BEFORE = PUNCTUATION_CLOSERS | {
    "'s",
    "n't",
    "'re",
    "'ve",
    "'m",
    "'ll",
    "'d",
}


@dataclass(frozen=True)
class WordRecord:
    word_id: str
    sentence_id: str
    paragraph_id: str
    text: str


@dataclass(frozen=True)
class IssueDetail:
    scope: str
    issue_id: str
    issue_type: str
    detector: str
    summary: str
    severity: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render a confidence-colored manuscript as an HTML review page using Mosaic edits outputs."
        ),
        epilog=(
            "Example: scripts/html_review.py --preprocessed /preprocessed "
            "--edits-root /mosaic/outputs --output html_review.html"
        ),
    )
    parser.add_argument(
        "--preprocessed",
        default=Path("/preprocessed"),
        type=Path,
        help="Directory containing manuscript_tokens.json and words.jsonl.",
    )
    parser.add_argument(
        "--edits-root",
        default=Path("/mosaic/outputs"),
        type=Path,
        help="Root directory containing tool outputs with *_edits.json files.",
    )
    parser.add_argument(
        "--output",
        default=Path("html_review.html"),
        type=Path,
        help="Output HTML file path.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional cap on the number of edits files to process.",
    )
    parser.add_argument(
        "--dedupe-scope",
        choices=("file", "item"),
        default="file",
        help=(
            "Deduplicate token counts within each edits file or within each item "
            "to avoid double-counting repeated token IDs."
        ),
    )
    return parser.parse_args()


def load_words(path: Path) -> List[WordRecord]:
    if not path.exists():
        raise FileNotFoundError(f"words.jsonl not found: {path}")
    records: List[WordRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            records.append(
                WordRecord(
                    word_id=payload["id"],
                    sentence_id=payload["sentence_id"],
                    paragraph_id=payload["paragraph_id"],
                    text=payload["text"],
                )
            )
    return records


def load_token_index(path: Path) -> Dict[str, int]:
    if not path.exists():
        raise FileNotFoundError(f"manuscript_tokens.json not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    index: Dict[str, int] = {}
    for paragraph in payload.get("paragraphs", []):
        for token in paragraph.get("tokens", []):
            token_id = token.get("token_id")
            global_index = token.get("global_index")
            if token_id is None or global_index is None:
                continue
            index[token_id] = int(global_index)
    return index


def find_edits_files(root: Path, max_files: Optional[int]) -> List[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Edits root not found: {root}")
    files = sorted(root.glob("*/*_edits.json"))
    if max_files is not None:
        files = files[: max_files]
    return files


def confidence_for_count(normalized_count: float) -> tuple[float, str]:
    for threshold, score, color in CONFIDENCE_LEVELS:
        if normalized_count <= threshold:
            return score, color
    return CONFIDENCE_LEVELS[-1][1], CONFIDENCE_LEVELS[-1][2]


def should_prefix_space(token: str, prev_token: Optional[str]) -> bool:
    if prev_token is None:
        return False
    if token in NO_SPACE_BEFORE:
        return False
    if prev_token in PUNCTUATION_OPENERS:
        return False
    return True


def format_issue(item: dict, scope: str) -> IssueDetail:
    evidence = item.get("evidence", {}) if isinstance(item.get("evidence"), dict) else {}
    impact = item.get("impact", {}) if isinstance(item.get("impact"), dict) else {}
    return IssueDetail(
        scope=scope,
        issue_id=str(item.get("issue_id", "unknown")),
        issue_type=str(item.get("type", "unspecified")),
        detector=str(evidence.get("detector", "unknown")),
        summary=str(evidence.get("summary", "No summary provided.")),
        severity=str(impact.get("severity", "unknown")),
    )


def sentence_ids_from_item(item: dict) -> List[str]:
    location = item.get("location", {}) if isinstance(item.get("location"), dict) else {}
    sentence_ids: List[str] = []

    direct = location.get("sentence_id")
    if isinstance(direct, str) and direct:
        sentence_ids.append(direct)

    multi = location.get("sentence_ids")
    if isinstance(multi, list):
        sentence_ids.extend(str(entry) for entry in multi if entry)

    extensions = item.get("extensions", {}) if isinstance(item.get("extensions"), dict) else {}
    ext_direct = extensions.get("sentence_id")
    if isinstance(ext_direct, str) and ext_direct:
        sentence_ids.append(ext_direct)

    ext_multi = extensions.get("sentence_ids")
    if isinstance(ext_multi, list):
        sentence_ids.extend(str(entry) for entry in ext_multi if entry)

    return sorted(set(sentence_ids))


def build_issue_maps(
    edits_files: Iterable[Path],
    words: List[WordRecord],
    token_index: Dict[str, int],
    dedupe_scope: str,
) -> tuple[
    List[int],
    List[List[IssueDetail]],
    Dict[str, List[IssueDetail]],
    Dict[str, List[IssueDetail]],
    int,
    Dict[Path, int],
]:
    total_tokens = len(words)
    counts = [0 for _ in range(total_tokens)]
    issue_lists: List[List[IssueDetail]] = [[] for _ in range(total_tokens)]

    missing_tokens: Dict[str, int] = {}
    deduped_total = 0
    deduped_by_file: Dict[Path, int] = {}
    paragraph_issue_lists: Dict[str, List[IssueDetail]] = {}
    sentence_issue_lists: Dict[str, List[IssueDetail]] = {}

    paragraph_to_indices: Dict[str, List[int]] = {}
    sentence_to_indices: Dict[str, List[int]] = {}
    for idx, word in enumerate(words):
        paragraph_to_indices.setdefault(word.paragraph_id, []).append(idx)
        sentence_to_indices.setdefault(word.sentence_id, []).append(idx)

    for edits_file in edits_files:
        payload = json.loads(edits_file.read_text(encoding="utf-8"))
        seen_token_ids: set[str] = set()
        for item in payload.get("items", []):
            if dedupe_scope == "item":
                seen_token_ids = set()
            location = item.get("location", {}) if isinstance(item.get("location"), dict) else {}

            token_ids = location.get("token_ids", []) or []
            if token_ids:
                detail = format_issue(item, "word")
                for token_id in token_ids:
                    if token_id in seen_token_ids:
                        deduped_total += 1
                        deduped_by_file[edits_file] = deduped_by_file.get(edits_file, 0) + 1
                        continue
                    seen_token_ids.add(token_id)
                    if token_id not in token_index:
                        missing_tokens[token_id] = missing_tokens.get(token_id, 0) + 1
                        continue
                    idx = token_index[token_id]
                    if 0 <= idx < total_tokens:
                        counts[idx] += 1
                        issue_lists[idx].append(detail)

            for sentence_id in sentence_ids_from_item(item):
                if sentence_id not in sentence_to_indices:
                    continue
                detail = format_issue(item, "sentence")
                sentence_issue_lists.setdefault(sentence_id, []).append(detail)
                for idx in sentence_to_indices[sentence_id]:
                    counts[idx] += 1
                    issue_lists[idx].append(detail)

            paragraph_id = location.get("paragraph_id")
            if isinstance(paragraph_id, str) and paragraph_id in paragraph_to_indices:
                detail = format_issue(item, "paragraph")
                paragraph_issue_lists.setdefault(paragraph_id, []).append(detail)
                for idx in paragraph_to_indices[paragraph_id]:
                    counts[idx] += 1
                    # Paragraph-level issues influence confidence at word-level,
                    # but paragraph issue details are shown on the paragraph label.

    if missing_tokens:
        missing_sample = ", ".join(list(missing_tokens.keys())[:5])
        raise ValueError(
            "Some token IDs referenced in edits were not found in manuscript_tokens.json: "
            f"{missing_sample}"
        )
    return (
        counts,
        issue_lists,
        sentence_issue_lists,
        paragraph_issue_lists,
        deduped_total,
        deduped_by_file,
    )


def tooltip_html(details: List[IssueDetail]) -> str:
    if not details:
        return "No detections mapped to this word."
    rows = []
    for detail in details:
        rows.append(
            "<li>"
            f"<strong>{html.escape(detail.scope.title())}</strong>"
            f" · {html.escape(detail.issue_type)}"
            f" · severity: {html.escape(detail.severity)}"
            f"<br><code>{html.escape(detail.issue_id)}</code>"
            f"<br>detector: {html.escape(detail.detector)}"
            f"<br>{html.escape(detail.summary)}"
            "</li>"
        )
    return "<ul>" + "".join(rows) + "</ul>"


def render_html(
    words: List[WordRecord],
    normalized_counts: List[float],
    issue_lists: List[List[IssueDetail]],
    sentence_issue_lists: Dict[str, List[IssueDetail]],
    paragraph_issue_lists: Dict[str, List[IssueDetail]],
    num_sources: int,
) -> str:
    total_score = 0.0
    for normalized_count in normalized_counts:
        score, _ = confidence_for_count(normalized_count)
        total_score += score
    avg_confidence = total_score / len(words) if words else 1.0

    token_chunks: List[str] = []
    current_paragraph = None
    current_sentence = None
    prev_token: Optional[str] = None
    sentence_to_indices: Dict[str, List[int]] = {}
    for idx, word in enumerate(words):
        sentence_to_indices.setdefault(word.sentence_id, []).append(idx)

    for idx, word in enumerate(words):
        if word.paragraph_id != current_paragraph:
            paragraph_label = f"Paragraph {html.escape(word.paragraph_id)}"
            paragraph_tooltip = tooltip_html(paragraph_issue_lists.get(word.paragraph_id, []))
            token_chunks.append(
                '<div class="paragraph-label">'
                f"{paragraph_label}"
                f'<span class="tooltip">{paragraph_tooltip}</span>'
                "</div>"
            )
            current_paragraph = word.paragraph_id
            current_sentence = None
            prev_token = None

        if word.sentence_id != current_sentence:
            sentence_issues = sentence_issue_lists.get(word.sentence_id, [])
            if sentence_issues:
                sentence_indices = sentence_to_indices.get(word.sentence_id, [])
                sentence_avg = (
                    sum(normalized_counts[index] for index in sentence_indices) / len(sentence_indices)
                    if sentence_indices
                    else 0.0
                )
                _, sentence_color = confidence_for_count(sentence_avg)
                sentence_tooltip = tooltip_html(sentence_issues)
                token_chunks.append(
                    '<div class="sentence-label sentence-has-issues"'
                    f' style="color:{sentence_color}; border-left-color:{sentence_color};"'
                    ">"
                    f"Sentence {html.escape(word.sentence_id)}"
                    f'<span class="tooltip">{sentence_tooltip}</span>'
                    "</div>"
                )
            else:
                token_chunks.append(
                    f'<div class="sentence-label">Sentence {html.escape(word.sentence_id)}</div>'
                )
            current_sentence = word.sentence_id
            prev_token = None

        if should_prefix_space(word.text, prev_token):
            token_chunks.append(" ")

        _, color = confidence_for_count(normalized_counts[idx])
        word_issues = issue_lists[idx]
        if word_issues:
            tooltip = tooltip_html(word_issues)
            token_chunks.append(
                "<span class=\"word word-has-issues\""
                f" style=\"color:{color}; border-bottom-color:{color};\""
                f" data-count=\"{normalized_counts[idx]:.3f}\""
                f" data-issues=\"{len(word_issues)}\""
                ">"
                f"{html.escape(word.text)}"
                f"<span class=\"tooltip\">{tooltip}</span>"
                "</span>"
            )
        else:
            token_chunks.append(
                "<span class=\"word\""
                f" style=\"color:{color};\""
                f" data-count=\"{normalized_counts[idx]:.3f}\""
                " >"
                f"{html.escape(word.text)}"
                "</span>"
            )
        prev_token = word.text

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Mosaic HTML Review</title>
  <style>
    body {{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; background: #111; color: #f7f7f7; }}
    .summary {{ margin-bottom: 1.5rem; padding: 0.75rem 1rem; background: #1b1b1b; border: 1px solid #333; border-radius: 8px; }}
    .legend {{ color: #c9c9c9; font-size: 0.92rem; margin-top: 0.35rem; }}
    .review {{ line-height: 1.85; font-size: 1.05rem; }}
    .paragraph-label {{ margin-top: 1.15rem; font-size: 0.86rem; color: #9aa0a6; font-weight: 700; position: relative; cursor: help; }}
    .sentence-label {{ margin-top: 0.5rem; font-size: 0.78rem; color: #7f8489; }}
    .sentence-has-issues {{ position: relative; cursor: help; display: inline-block; border-left: 3px solid; padding-left: 0.45rem; border-radius: 3px; }}
    .word {{ position: relative; }}
    .word-has-issues {{ cursor: help; border-bottom: 1px dotted rgba(255,255,255,0.2); }}
    .paragraph-label:hover .tooltip,
    .sentence-has-issues:hover .tooltip,
    .word:hover .tooltip {{ display: block; }}
    .tooltip {{
      display: none;
      position: absolute;
      left: 0;
      top: 1.8rem;
      z-index: 20;
      width: min(30rem, 90vw);
      max-height: 18rem;
      overflow: auto;
      background: #232427;
      color: #fff;
      border: 1px solid #444;
      border-radius: 8px;
      padding: 0.65rem 0.75rem;
      box-shadow: 0 8px 28px rgba(0,0,0,0.45);
      font-size: 0.86rem;
      line-height: 1.35;
      white-space: normal;
    }}
    .tooltip ul {{ margin: 0; padding-left: 1rem; }}
    .tooltip li {{ margin-bottom: 0.6rem; }}
    code {{ color: #ffb3a8; }}
  </style>
</head>
<body>
  <div class=\"summary\">
    <div><strong>Overall confidence:</strong> {avg_confidence:.2%} across {len(words)} tokens</div>
    <div class=\"legend\">Legend: deep green (clean) → light green → yellow → orange → coral-red (confidence based on normalized issue rate, count / {num_sources} source(s)).</div>
  </div>
  <div class=\"review\">{''.join(token_chunks)}</div>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    preprocessed_dir = args.preprocessed
    words_path = preprocessed_dir / "words.jsonl"
    tokens_path = preprocessed_dir / "manuscript_tokens.json"

    words = load_words(words_path)
    token_index = load_token_index(tokens_path)
    edits_files = find_edits_files(args.edits_root, args.max_files)

    if not edits_files:
        raise SystemExit(
            f"No *_edits.json files found under {args.edits_root}. "
            "Run the Mosaic tools to generate edits outputs first."
        )

    (
        issue_counts,
        issue_lists,
        sentence_issue_lists,
        paragraph_issue_lists,
        deduped_total,
        deduped_by_file,
    ) = build_issue_maps(
        edits_files, words, token_index, args.dedupe_scope
    )
    tool_sources = {edits_file.parent.name for edits_file in edits_files}
    num_sources = len(tool_sources) or len(edits_files)
    if num_sources == 0:
        num_sources = 1
    normalized_counts = [count / num_sources for count in issue_counts]

    if deduped_total:
        print(f"Deduped {deduped_total} repeated token reference(s) (scope: {args.dedupe_scope}).")
        for edits_file, deduped_count in sorted(
            deduped_by_file.items(), key=lambda item: item[1], reverse=True
        ):
            print(f"- {edits_file}: {deduped_count}")

    html_output = render_html(
        words,
        normalized_counts,
        issue_lists,
        sentence_issue_lists,
        paragraph_issue_lists,
        num_sources,
    )
    args.output.write_text(html_output, encoding="utf-8")
    print(f"Wrote HTML review: {args.output}")


if __name__ == "__main__":
    main()
