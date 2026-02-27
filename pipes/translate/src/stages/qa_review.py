from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from ..lib.local_llm_client import (
    DEFAULT_CHAT_COMPLETIONS_URL,
    chat_completion,
)
from ..lib.progress import ProgressBar


SOURCE_PATTERN = "pass2_pre/{lang}/paragraphs.jsonl"
TARGET_PATTERN = "qa_review/{lang}/paragraphs.jsonl"
SOURCE_PARAGRAPHS_FALLBACK = Path("paragraphs.jsonl")
DEFAULT_PROMPT_ROOTS = [Path("prompts/translate/review"), Path("prompt/translate/review")]
DEFAULT_PROMPT_FILE = "qa_review.md"
STAGE_CONFIG_PATH = Path(__file__).with_name("qa_review_config.json")
RUBRIC_KEYS = [
    "intent_and_contract",
    "voice_and_style",
    "character_integrity",
    "dialogue_realism",
    "culture_and_references",
    "continuity",
    "meaning_precision",
    "sensitive_content",
    "typography_and_format",
    "edge_cases",
]


def _binding_value(ctx, key: str) -> str | None:
    bindings = _context_keys(ctx)
    value = bindings.get(key)
    if value is None:
        return None
    return str(value)


def _context_keys(ctx) -> dict[str, str]:
    for attr in ("keys", "bindings"):
        key_map = getattr(ctx, attr, None)
        if isinstance(key_map, dict):
            return {str(key): str(value) for key, value in key_map.items()}
    return {}


def _normalize_language(language: str) -> str:
    return language.strip().strip("/")


def _resolve_input_artifact_paths(ctx, language: str) -> tuple[Path, Path]:
    input_candidates: list[dict[str, object]] = []
    for attr in ("inputs", "input_artifacts", "stage_inputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            input_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    source_path: Path | None = None
    translated_path: Path | None = None

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
        if "pass2_pre/" in normalized:
            translated_path = path_obj
            continue
        if normalized.endswith("paragraphs.jsonl") and source_path is None:
            source_path = path_obj

    if translated_path is None:
        translated_path = Path(SOURCE_PATTERN.format(lang=language))
    if source_path is None:
        source_path = SOURCE_PARAGRAPHS_FALLBACK

    return source_path, translated_path


def _resolve_output_artifact_path(ctx, language: str) -> Path:
    output_candidates: list[dict[str, object]] = []
    for attr in ("outputs", "output_artifacts", "stage_outputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            output_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    for candidate in output_candidates:
        concrete_path = candidate.get("path")
        if not isinstance(concrete_path, str) or not concrete_path.strip():
            continue
        candidate_bindings = candidate.get("bindings")
        if not isinstance(candidate_bindings, dict):
            candidate_bindings = candidate.get("keys")
        if isinstance(candidate_bindings, dict):
            lang_value = candidate_bindings.get("lang") or candidate_bindings.get("language")
            if language and lang_value and str(lang_value) != language:
                continue
        return Path(concrete_path)

    return Path(TARGET_PATTERN.format(lang=language))


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise TypeError(f"Expected object JSON in {path}")
    return parsed


def _stage_config(ctx) -> dict[str, object]:
    config = _load_json(STAGE_CONFIG_PATH)
    run_config = getattr(ctx, "run_config", None)
    if isinstance(run_config, dict):
        rc = run_config.get("rc")
        if isinstance(rc, dict):
            direct = rc.get("qa_review")
            if isinstance(direct, dict):
                config.update(direct)
            nested = rc.get("stages")
            if isinstance(nested, dict):
                stage_cfg = nested.get("qa_review")
                if isinstance(stage_cfg, dict):
                    config.update(stage_cfg)
    return config


def _candidate_prompt_paths(prompt_root: Path) -> list[Path]:
    return [prompt_root / DEFAULT_PROMPT_FILE, prompt_root / "QA review markdown.md"]


def _resolve_prompt_path(configured_path: str | None) -> Path:
    if configured_path:
        explicit = Path(configured_path)
        if explicit.exists() and explicit.is_file():
            return explicit

    scope_roots = [Path.cwd(), Path(__file__).resolve().parents[3], Path(__file__).resolve().parents[4]]
    for root in DEFAULT_PROMPT_ROOTS:
        expanded_roots = [root] if root.is_absolute() else [scope / root for scope in scope_roots]
        for expanded_root in expanded_roots:
            if not expanded_root.exists() or not expanded_root.is_dir():
                continue
            for candidate in _candidate_prompt_paths(expanded_root):
                if candidate.exists() and candidate.is_file():
                    return candidate

    checked = ", ".join(str(root) for root in DEFAULT_PROMPT_ROOTS)
    raise FileNotFoundError(f"qa_review prompt not found. Checked roots: {checked}")


def _load_rows(path: Path, *, label: str) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"qa_review {label} input artifact missing: {path}")

    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _load_jsonl_prefix(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            break
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, ensure_ascii=False) + "\n")


def _row_id(row: dict[str, Any], index: int) -> str:
    for key in ("paragraph_id", "item_id"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"paragraph_{index:04d}"


def _text_value(row: dict[str, Any]) -> str:
    for key in ("text", "translation", "target_text", "content", "source_text"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("qa_review model response did not include a JSON object.")

    parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise TypeError("qa_review model response JSON must be an object.")
    return parsed


def _normalize_scores(raw_scores: dict[str, Any]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for key in RUBRIC_KEYS:
        raw_value = raw_scores.get(key, 0)
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            value = 0
        normalized[key] = max(0, min(5, value))
    return normalized


def _fallback_review(paragraph_id: str, translated_text: str, error: str) -> dict[str, Any]:
    scores = {key: 0 for key in RUBRIC_KEYS}
    return {
        "paragraph_id": paragraph_id,
        "scores": scores,
        "total_score": 0,
        "decision": "major_rewrite",
        "issues": [f"qa_review_error: {error}"],
        "revised_translation": translated_text,
        "rationale": "Model review failed; keeping pass2 translation unchanged.",
    }


def _review_paragraph(
    *,
    model: str,
    base_url: str,
    timeout: int,
    temperature: float,
    retry: int,
    system_prompt: str,
    language: str,
    paragraph_id: str,
    source_text: str,
    translated_text: str,
) -> dict[str, Any]:
    user_prompt = (
        "Evaluate this single paragraph translation and return exactly one JSON object.\n"
        "Do not return markdown fences.\n\n"
        f"paragraph_id: {paragraph_id}\n"
        f"target_language: {language}\n\n"
        "SOURCE_PARAGRAPH:\n"
        f"{source_text}\n\n"
        "TRANSLATED_PARAGRAPH:\n"
        f"{translated_text}\n\n"
        "Required JSON shape:\n"
        "{\n"
        '  "paragraph_id": "...",\n'
        '  "target_language": "...",\n'
        '  "scores": {\n'
        '    "intent_and_contract": 0,\n'
        '    "voice_and_style": 0,\n'
        '    "character_integrity": 0,\n'
        '    "dialogue_realism": 0,\n'
        '    "culture_and_references": 0,\n'
        '    "continuity": 0,\n'
        '    "meaning_precision": 0,\n'
        '    "sensitive_content": 0,\n'
        '    "typography_and_format": 0,\n'
        '    "edge_cases": 0\n'
        "  },\n"
        '  "total_score": 0,\n'
        '  "decision": "approved|minor_rewrite|major_rewrite",\n'
        '  "issues": ["..."],\n'
        '  "revised_translation": "...",\n'
        '  "rationale": "..."\n'
        "}"
    )

    last_error = "unknown_error"
    for _ in range(retry + 1):
        try:
            response = chat_completion(
                base_url=base_url,
                model=model,
                system_prompt=system_prompt,
                user_content=user_prompt,
                timeout=timeout,
                temperature=temperature,
            )
            parsed = _extract_json_object(response)
            raw_scores = parsed.get("scores")
            if not isinstance(raw_scores, dict):
                raise ValueError("qa_review response missing 'scores' object.")
            scores = _normalize_scores(raw_scores)
            total_score = sum(scores.values())

            revised = parsed.get("revised_translation")
            revised_translation = translated_text
            if isinstance(revised, str) and revised.strip():
                revised_translation = revised.strip()

            issues = parsed.get("issues")
            normalized_issues: list[str] = []
            if isinstance(issues, list):
                for issue in issues:
                    if isinstance(issue, str) and issue.strip():
                        normalized_issues.append(issue.strip())

            decision = parsed.get("decision")
            decision_value = "minor_rewrite"
            if isinstance(decision, str) and decision in {"approved", "minor_rewrite", "major_rewrite"}:
                decision_value = decision

            rationale = parsed.get("rationale")
            rationale_value = ""
            if isinstance(rationale, str):
                rationale_value = rationale.strip()

            return {
                "paragraph_id": paragraph_id,
                "target_language": language,
                "scores": scores,
                "total_score": total_score,
                "decision": decision_value,
                "issues": normalized_issues,
                "revised_translation": revised_translation,
                "rationale": rationale_value,
            }
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    return _fallback_review(paragraph_id, translated_text, last_error)


def run_whole(ctx) -> None:
    cfg = _stage_config(ctx)

    language_binding = _binding_value(ctx, "lang") or _binding_value(ctx, "language")
    language = _normalize_language(language_binding or "")
    if not language:
        raise ValueError("qa_review is missing required language binding ('lang').")

    model = str(cfg.get("model", "")).strip()
    if not model:
        raise ValueError("qa_review config is missing required 'model'.")

    prompt_path = _resolve_prompt_path(cfg.get("prompt_path") and str(cfg.get("prompt_path")))
    system_prompt = prompt_path.read_text(encoding="utf-8")

    base_url = str(cfg.get("base_url", DEFAULT_CHAT_COMPLETIONS_URL))
    timeout = int(cfg.get("timeout", 180))
    retry = int(cfg.get("retry", 1))
    temperature = float(cfg.get("temperature", 0.1))
    concurrency = max(1, int(cfg.get("concurrency", 1)))

    source_path, translated_path = _resolve_input_artifact_paths(ctx, language)
    source_rows = _load_rows(source_path, label="source")
    translated_rows = _load_rows(translated_path, label="translated")
    output_path = _resolve_output_artifact_path(ctx, language)

    source_by_id: dict[str, dict[str, Any]] = {}
    for idx, source_row in enumerate(source_rows, start=1):
        source_by_id[_row_id(source_row, idx)] = source_row

    def process_row(index_and_row: tuple[int, dict[str, Any]]) -> tuple[int, dict[str, Any], dict[str, Any]]:
        idx, translated_row = index_and_row
        paragraph_id = _row_id(translated_row, idx)
        translated_text = _text_value(translated_row)

        source_row = source_by_id.get(paragraph_id, {})
        source_text = _text_value(source_row)
        if not source_text:
            source_text = str(translated_row.get("source_text") or "").strip()

        review = _review_paragraph(
            model=model,
            base_url=base_url,
            timeout=timeout,
            temperature=temperature,
            retry=retry,
            system_prompt=system_prompt,
            language=language,
            paragraph_id=paragraph_id,
            source_text=source_text,
            translated_text=translated_text,
        )

        revised_translation = str(review.get("revised_translation") or translated_text).strip()
        out_row = dict(translated_row)
        out_row["paragraph_id"] = paragraph_id
        out_row["item_id"] = str(translated_row.get("item_id") or paragraph_id)
        out_row["source_text"] = source_text
        out_row["text"] = revised_translation
        out_row["translation"] = revised_translation
        out_row["language"] = language
        out_row["qa_decision"] = review.get("decision")
        out_row["qa_total_score"] = review.get("total_score")
        out_row["qa_prompt"] = str(prompt_path)
        return idx, out_row, review

    ordered_rows = list(enumerate(translated_rows, start=1))
    progress = ProgressBar(
        len(ordered_rows),
        label=f"qa_review:{language}",
        color=True,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    scores_path = output_path.with_name("scores.jsonl")
    existing_output_rows = _load_jsonl_prefix(output_path)
    existing_score_rows = _load_jsonl_prefix(scores_path)
    resumable_count = min(len(existing_output_rows), len(existing_score_rows), len(ordered_rows))
    resume_from = 0
    for idx in range(resumable_count):
        expected_id = _row_id(translated_rows[idx], idx + 1)
        output_id = _row_id(existing_output_rows[idx], idx + 1)
        score_id = _row_id(existing_score_rows[idx], idx + 1)
        if expected_id != output_id or expected_id != score_id:
            break
        resume_from += 1

    if resume_from != len(existing_output_rows):
        _write_jsonl(output_path, existing_output_rows[:resume_from])
        existing_output_rows = existing_output_rows[:resume_from]
    if resume_from != len(existing_score_rows):
        _write_jsonl(scores_path, existing_score_rows[:resume_from])
        existing_score_rows = existing_score_rows[:resume_from]

    failed_count = 0
    for review in existing_score_rows[:resume_from]:
        issues = review.get("issues")
        if isinstance(issues, list) and any(
            isinstance(issue, str) and issue.startswith("qa_review_error:") for issue in issues
        ):
            failed_count += 1

    progress.print(resume_from, failed=failed_count)
    if resume_from >= len(ordered_rows):
        progress.done(len(ordered_rows), failed=failed_count)
        return

    rows_to_process = ordered_rows[resume_from:]
    with output_path.open("a" if resume_from else "w", encoding="utf-8") as output_file, scores_path.open("a" if resume_from else "w", encoding="utf-8") as scores_file:
        if concurrency == 1:
            completed = resume_from
            for entry in rows_to_process:
                _, out_row, review = process_row(entry)
                output_file.write(json.dumps(out_row, ensure_ascii=False) + "\n")
                scores_file.write(json.dumps(review, ensure_ascii=False) + "\n")
                output_file.flush()
                scores_file.flush()
                completed += 1
                issues = review.get("issues")
                if isinstance(issues, list) and any(
                    isinstance(issue, str) and issue.startswith("qa_review_error:") for issue in issues
                ):
                    failed_count += 1
                progress.print(completed, failed=failed_count)
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                future_to_idx = {pool.submit(process_row, entry): entry[0] for entry in rows_to_process}
                buffered: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
                next_idx = resume_from + 1
                completed = resume_from
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    _, out_row, review = future.result()
                    buffered[idx] = (out_row, review)
                    while next_idx in buffered:
                        ordered_out_row, ordered_review = buffered.pop(next_idx)
                        output_file.write(json.dumps(ordered_out_row, ensure_ascii=False) + "\n")
                        scores_file.write(json.dumps(ordered_review, ensure_ascii=False) + "\n")
                        output_file.flush()
                        scores_file.flush()
                        completed += 1
                        ordered_issues = ordered_review.get("issues")
                        if isinstance(ordered_issues, list) and any(
                            isinstance(issue, str) and issue.startswith("qa_review_error:")
                            for issue in ordered_issues
                        ):
                            failed_count += 1
                        progress.print(completed, failed=failed_count)
                        next_idx += 1
    progress.done(len(ordered_rows), failed=failed_count)


def run_item(ctx, item: dict[str, object]) -> None:
    _ = item
    run_whole(ctx)
