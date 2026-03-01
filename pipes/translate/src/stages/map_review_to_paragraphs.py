from __future__ import annotations

import json
from pathlib import Path
from typing import Any


TYPOGRAPHY_INPUT_PATTERN = "review/{lang}/typography/review.json"
CRITICS_INPUT_PATTERN = "review/{lang}/critics/review.json"
CANDIDATE_MAP_INPUT_PATTERN = "final/{lang}/candidate_map.jsonl"
TYPOGRAPHY_ROWS_OUTPUT_PATTERN = "review/{lang}/normalized/typography_paragraph_rows.jsonl"
CRITICS_ROWS_OUTPUT_PATTERN = "review/{lang}/normalized/critics_paragraph_rows.jsonl"
TYPOGRAPHY_REVIEWED_PARAGRAPHS_PATTERN = "typography_review/{lang}/paragraphs.jsonl"


def _context_keys(ctx) -> dict[str, str]:
    for attr in ("keys", "bindings"):
        key_map = getattr(ctx, attr, None)
        if isinstance(key_map, dict):
            return {str(key): str(value) for key, value in key_map.items()}
    return {}


def _binding_value(ctx, key: str) -> str | None:
    value = _context_keys(ctx).get(key)
    if value is None:
        return None
    return str(value)


def _normalize_language(language: str) -> str:
    return language.strip().strip("/")


def _resolve_inputs(ctx, language: str) -> tuple[Path, Path, Path]:
    input_candidates: list[dict[str, object]] = []
    for attr in ("inputs", "input_artifacts", "stage_inputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            input_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    typography_path: Path | None = None
    critics_path: Path | None = None
    candidate_map_path: Path | None = None

    for candidate in input_candidates:
        concrete_path = candidate.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        normalized = concrete_path.replace("\\", "/")
        candidate_bindings = candidate.get("bindings")
        if not isinstance(candidate_bindings, dict):
            candidate_bindings = candidate.get("keys")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue

        path_obj = Path(concrete_path)
        if normalized.endswith("/typography/review.json"):
            typography_path = path_obj
        elif normalized.endswith("/critics/review.json"):
            critics_path = path_obj
        elif normalized.endswith("candidate_map.jsonl"):
            candidate_map_path = path_obj

    if typography_path is None:
        typography_path = Path(TYPOGRAPHY_INPUT_PATTERN.format(lang=language))
    if critics_path is None:
        critics_path = Path(CRITICS_INPUT_PATTERN.format(lang=language))
    if candidate_map_path is None:
        candidate_map_path = Path(CANDIDATE_MAP_INPUT_PATTERN.format(lang=language))

    return typography_path, critics_path, candidate_map_path


def _resolve_outputs(ctx, language: str) -> tuple[Path, Path]:
    output_candidates: list[dict[str, object]] = []
    for attr in ("outputs", "output_artifacts", "stage_outputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            output_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    typography_rows_path: Path | None = None
    critics_rows_path: Path | None = None

    for candidate in output_candidates:
        concrete_path = candidate.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        normalized = concrete_path.replace("\\", "/")
        candidate_bindings = candidate.get("bindings")
        if not isinstance(candidate_bindings, dict):
            candidate_bindings = candidate.get("keys")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue
        if normalized.endswith("typography_paragraph_rows.jsonl"):
            typography_rows_path = Path(concrete_path)
        elif normalized.endswith("critics_paragraph_rows.jsonl"):
            critics_rows_path = Path(concrete_path)

    if typography_rows_path is None:
        typography_rows_path = Path(TYPOGRAPHY_ROWS_OUTPUT_PATTERN.format(lang=language))
    if critics_rows_path is None:
        critics_rows_path = Path(CRITICS_ROWS_OUTPUT_PATTERN.format(lang=language))

    return typography_rows_path, critics_rows_path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(parsed, dict):
        return parsed
    return {}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _paragraph_ids_from_candidate_map(path: Path) -> list[str]:
    ids: list[str] = []
    for row in _load_jsonl(path):
        paragraph_id = row.get("paragraph_id")
        if isinstance(paragraph_id, str) and paragraph_id.strip():
            ids.append(paragraph_id.strip())
    return ids


def _paragraph_ids_from_typography_rows(language: str) -> list[str]:
    fallback_path = Path(TYPOGRAPHY_REVIEWED_PARAGRAPHS_PATTERN.format(lang=language))
    ids: list[str] = []
    for row in _load_jsonl(fallback_path):
        paragraph_id = row.get("paragraph_id") or row.get("item_id")
        if isinstance(paragraph_id, str) and paragraph_id.strip():
            ids.append(paragraph_id.strip())
    return ids


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, ensure_ascii=False) + "\n")


def _normalize_typography_rows(typography_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    paragraph_reviews = typography_payload.get("paragraph_reviews")
    if not isinstance(paragraph_reviews, list):
        return rows

    for review in paragraph_reviews:
        if not isinstance(review, dict):
            continue
        paragraph_id = review.get("paragraph_id")
        if not isinstance(paragraph_id, str) or not paragraph_id.strip():
            continue
        issues = review.get("issues")
        normalized_issues: list[str] = []
        if isinstance(issues, list):
            for issue in issues:
                if isinstance(issue, str) and issue.strip():
                    normalized_issues.append(issue.strip())
        decision = str(review.get("decision") or "").strip()
        rows[paragraph_id] = {
            "paragraph_id": paragraph_id,
            "scores": {"typography": review.get("total_score")},
            "issues": normalized_issues,
            "hard_fail": decision == "major_rewrite",
            "source": "typography_review",
            "decision": decision,
            "rationale": review.get("rationale"),
        }
    return rows


def _critics_rollup(critics_payload: dict[str, Any]) -> tuple[bool, list[str], list[str], list[str]]:
    overall = critics_payload.get("overall")
    rework_required = False
    if isinstance(overall, dict):
        rework_required = bool(overall.get("rework_required"))

    issues: list[str] = []
    failing_critics: list[str] = []
    recommended_actions: list[str] = []
    for section_name in ("general_reader", "critic_results"):
        section = critics_payload.get(section_name)
        if section_name == "general_reader":
            section = [section] if isinstance(section, dict) else []
        if not isinstance(section, list):
            continue
        for critic in section:
            if not isinstance(critic, dict):
                continue
            critic_id = str(critic.get("critic_id") or "critic").strip()
            verdict = str(critic.get("verdict") or "").strip().lower()
            if verdict == "rework":
                failing_critics.append(critic_id)
            raw_issues = critic.get("issues")
            if isinstance(raw_issues, list):
                for issue in raw_issues:
                    if isinstance(issue, dict):
                        description = str(issue.get("description") or "").strip()
                        if description:
                            issues.append(f"{critic_id}: {description}")
            actions = critic.get("recommended_actions")
            if isinstance(actions, list):
                for action in actions:
                    if isinstance(action, str) and action.strip():
                        recommended_actions.append(action.strip())

    if failing_critics:
        rework_required = True

    return rework_required, issues, failing_critics, recommended_actions


def run_whole(ctx) -> None:
    language_binding = _binding_value(ctx, "lang") or _binding_value(ctx, "language")
    language = _normalize_language(language_binding or "")
    if not language:
        raise ValueError("map_review_to_paragraphs is missing required language binding ('lang').")

    typography_path, critics_path, candidate_map_path = _resolve_inputs(ctx, language)
    typography_rows_path, critics_rows_path = _resolve_outputs(ctx, language)

    typography_payload = _load_json(typography_path)
    critics_payload = _load_json(critics_path)

    paragraph_ids = _paragraph_ids_from_candidate_map(candidate_map_path)
    if not paragraph_ids:
        paragraph_ids = _paragraph_ids_from_typography_rows(language)

    typography_by_id = _normalize_typography_rows(typography_payload)
    normalized_typography_rows: list[dict[str, Any]] = []
    for paragraph_id in paragraph_ids:
        existing = typography_by_id.get(paragraph_id)
        if existing is not None:
            normalized_typography_rows.append(existing)
        else:
            normalized_typography_rows.append(
                {
                    "paragraph_id": paragraph_id,
                    "scores": {"typography": None},
                    "issues": [],
                    "hard_fail": False,
                    "source": "typography_review",
                    "decision": "missing",
                }
            )

    rework_required, critics_issues, failing_critics, recommended_actions = _critics_rollup(critics_payload)
    normalized_critics_rows: list[dict[str, Any]] = []
    for paragraph_id in paragraph_ids:
        normalized_critics_rows.append(
            {
                "paragraph_id": paragraph_id,
                "scores": {"critics_pass": 0 if rework_required else 1},
                "issues": critics_issues,
                "hard_fail": rework_required,
                "source": "critics_review",
                "failing_critics": failing_critics,
                "recommended_actions": recommended_actions,
            }
        )

    _write_jsonl(typography_rows_path, normalized_typography_rows)
    _write_jsonl(critics_rows_path, normalized_critics_rows)


def run_item(ctx, item: dict[str, object]) -> None:
    _ = item
    run_whole(ctx)
