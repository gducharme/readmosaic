from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..lib.local_llm_client import DEFAULT_CHAT_COMPLETIONS_URL, chat_completion
from ..lib.progress import ProgressBar


SOURCE_PATTERN = "final/{lang}/candidate.md"
TARGET_PATTERN = "review/{lang}/critics/review.json"
TYPOGRAPHY_REVIEW_PATTERN = "review/{lang}/typography/review.json"
CANDIDATE_MAP_PATTERN = "final/{lang}/candidate_map.jsonl"
TYPOGRAPHY_ROWS_PATTERN = "review/{lang}/normalized/typography_paragraph_rows.jsonl"
CRITICS_ROWS_PATTERN = "review/{lang}/normalized/critics_paragraph_rows.jsonl"
DEFAULT_CRITICS_DIR = Path("prompts/critics")
STAGE_CONFIG_PATH = Path(__file__).with_name("review_critics_config.json")


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


def _resolve_input_artifact_path(ctx, language: str) -> Path:
    input_candidates: list[dict[str, object]] = []
    for attr in ("inputs", "input_artifacts", "stage_inputs", "artifacts"):
        value = getattr(ctx, attr, None)
        if isinstance(value, list):
            input_candidates.extend(candidate for candidate in value if isinstance(candidate, dict))

    for candidate in input_candidates:
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
        if concrete_path.endswith(".md"):
            return Path(concrete_path)

    return Path(SOURCE_PATTERN.format(lang=language))


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
            direct = rc.get("review_critics")
            if isinstance(direct, dict):
                config.update(direct)
            nested = rc.get("stages")
            if isinstance(nested, dict):
                stage_cfg = nested.get("review_critics")
                if isinstance(stage_cfg, dict):
                    config.update(stage_cfg)
    return config


def _resolve_critics_dir(configured_path: str | None) -> Path:
    if configured_path:
        explicit = Path(configured_path)
        if explicit.exists() and explicit.is_dir():
            return explicit

    scope_roots = [Path.cwd(), Path(__file__).resolve().parents[3], Path(__file__).resolve().parents[4]]
    for scope in scope_roots:
        candidate = scope / DEFAULT_CRITICS_DIR
        if candidate.exists() and candidate.is_dir():
            return candidate
    return DEFAULT_CRITICS_DIR


def _load_critic_files(critics_dir: Path) -> list[Path]:
    if not critics_dir.exists() or not critics_dir.is_dir():
        return []
    return sorted(path for path in critics_dir.iterdir() if path.is_file() and path.suffix.lower() == ".md")


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("review_critics model response did not include a JSON object.")

    parsed = json.loads(text[start : end + 1])
    if not isinstance(parsed, dict):
        raise TypeError("review_critics model response JSON must be an object.")
    return parsed


def _normalize_issues(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    issues: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity") or "medium").strip().lower()
        if severity not in {"low", "medium", "high", "critical"}:
            severity = "medium"
        issue_type = str(item.get("type") or "other").strip().lower()
        description = str(item.get("description") or "").strip()
        evidence_quote = str(item.get("evidence_quote") or "").strip()
        if not description:
            continue
        issues.append(
            {
                "severity": severity,
                "type": issue_type,
                "description": description,
                "evidence_quote": evidence_quote,
            }
        )
    return issues


def _review_with_profile(
    *,
    model: str,
    base_url: str,
    timeout: int,
    temperature: float,
    retry: int,
    language: str,
    manuscript_text: str,
    critic_id: str,
    profile_name: str,
    profile_text: str,
    focus: str,
) -> dict[str, Any]:
    system_prompt = (
        "You are a literary review assistant. Return strict JSON only. "
        "Be concrete, evidence-based, and concise."
    )
    user_prompt = (
        "Review the manuscript with this critic profile.\n\n"
        f"CRITIC_ID: {critic_id}\n"
        f"CRITIC_NAME: {profile_name}\n"
        f"TARGET_LANGUAGE: {language}\n"
        f"FOCUS: {focus}\n\n"
        "CRITIC_PROFILE:\n"
        f"{profile_text}\n\n"
        "MANUSCRIPT:\n"
        f"{manuscript_text}\n\n"
        "Return exactly one JSON object with shape:\n"
        "{\n"
        '  "critic_id": "...",\n'
        '  "critic_name": "...",\n'
        '  "verdict": "pass|rework",\n'
        '  "fundamental_problem": false,\n'
        '  "summary": "...",\n'
        '  "issues": [\n'
        "    {\n"
        '      "severity": "low|medium|high|critical",\n'
        '      "type": "coherence|continuity|style|sensitivity|other",\n'
        '      "description": "...",\n'
        '      "evidence_quote": "..."\n'
        "    }\n"
        "  ],\n"
        '  "recommended_actions": ["..."]\n'
        "}\n"
        "Set verdict to rework if issues are substantial. "
        "Set fundamental_problem=true only when the manuscript does not make sense or has a severe structural issue."
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
            verdict = str(parsed.get("verdict") or "pass").strip().lower()
            if verdict not in {"pass", "rework"}:
                verdict = "pass"
            fundamental_problem = bool(parsed.get("fundamental_problem"))
            summary = str(parsed.get("summary") or "").strip()
            recommended_actions_raw = parsed.get("recommended_actions")
            recommended_actions: list[str] = []
            if isinstance(recommended_actions_raw, list):
                for action in recommended_actions_raw:
                    if isinstance(action, str) and action.strip():
                        recommended_actions.append(action.strip())
            return {
                "critic_id": critic_id,
                "critic_name": profile_name,
                "verdict": verdict,
                "fundamental_problem": fundamental_problem,
                "summary": summary,
                "issues": _normalize_issues(parsed.get("issues")),
                "recommended_actions": recommended_actions,
            }
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    return {
        "critic_id": critic_id,
        "critic_name": profile_name,
        "verdict": "rework",
        "fundamental_problem": False,
        "summary": f"critic execution failed: {last_error}",
        "issues": [
            {
                "severity": "high",
                "type": "other",
                "description": f"review_critics_error: {last_error}",
                "evidence_quote": "",
            }
        ],
        "recommended_actions": ["Retry critic evaluation."],
    }


def _general_reader_profile() -> tuple[str, str, str]:
    return (
        "general_reader",
        "General Reader Sanity Check",
        (
            "You are a careful, neutral reader. Determine if the manuscript is understandable and coherent. "
            "Flag only true structural comprehension failures as fundamental problems."
        ),
    )


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_whole(ctx) -> None:
    cfg = _stage_config(ctx)

    language_binding = _binding_value(ctx, "lang") or _binding_value(ctx, "language")
    language = _normalize_language(language_binding or "")
    if not language:
        raise ValueError("review_critics is missing required language binding ('lang').")

    model = str(cfg.get("model", "")).strip()
    if not model:
        raise ValueError("review_critics config is missing required 'model'.")

    base_url = str(cfg.get("base_url", DEFAULT_CHAT_COMPLETIONS_URL))
    timeout = int(cfg.get("timeout", 240))
    retry = int(cfg.get("retry", 1))
    temperature = float(cfg.get("temperature", 0.1))
    concurrency = max(1, int(cfg.get("concurrency", 2)))

    critics_dir = _resolve_critics_dir(cfg.get("critics_dir") and str(cfg.get("critics_dir")))
    critic_files = _load_critic_files(critics_dir)

    input_path = _resolve_input_artifact_path(ctx, language)
    output_path = _resolve_output_artifact_path(ctx, language)

    if not input_path.exists():
        raise FileNotFoundError(f"review_critics input manuscript missing: {input_path}")
    manuscript_text = input_path.read_text(encoding="utf-8")

    profiles: list[tuple[str, str, str, str]] = []
    general_id, general_name, general_text = _general_reader_profile()
    profiles.append((general_id, general_name, general_text, "global_sanity"))
    for critic_file in critic_files:
        critic_id = critic_file.stem
        profiles.append(
            (
                critic_id,
                critic_file.stem.replace("_", " "),
                critic_file.read_text(encoding="utf-8"),
                "persona_critique",
            )
        )

    progress = ProgressBar(len(profiles), label=f"review_critics:{language}", color=True)
    results: list[dict[str, Any]] = []
    progress.print(0, failed=0)

    if concurrency == 1:
        for idx, (critic_id, critic_name, profile_text, focus) in enumerate(profiles, start=1):
            result = _review_with_profile(
                model=model,
                base_url=base_url,
                timeout=timeout,
                temperature=temperature,
                retry=retry,
                language=language,
                manuscript_text=manuscript_text,
                critic_id=critic_id,
                profile_name=critic_name,
                profile_text=profile_text,
                focus=focus,
            )
            results.append(result)
            progress.print(idx, failed=0)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            future_map = {
                pool.submit(
                    _review_with_profile,
                    model=model,
                    base_url=base_url,
                    timeout=timeout,
                    temperature=temperature,
                    retry=retry,
                    language=language,
                    manuscript_text=manuscript_text,
                    critic_id=critic_id,
                    profile_name=critic_name,
                    profile_text=profile_text,
                    focus=focus,
                ): idx
                for idx, (critic_id, critic_name, profile_text, focus) in enumerate(profiles)
            }
            ordered: dict[int, dict[str, Any]] = {}
            completed = 0
            for future in as_completed(future_map):
                idx = future_map[future]
                ordered[idx] = future.result()
                completed += 1
                progress.print(completed, failed=0)
            results = [ordered[idx] for idx in sorted(ordered.keys())]

    progress.done(len(profiles), failed=0)

    general_reader = next((result for result in results if result.get("critic_id") == "general_reader"), None)
    persona_results = [result for result in results if result.get("critic_id") != "general_reader"]

    rework_required = any(
        bool(result.get("fundamental_problem")) or str(result.get("verdict") or "") == "rework"
        for result in results
    )
    fundamental_problem_count = sum(1 for result in results if bool(result.get("fundamental_problem")))
    failing_critics = sum(1 for result in results if str(result.get("verdict") or "") == "rework")
    passing_critics = len(results) - failing_critics

    payload = {
        "target_language": language,
        "manuscript_path": str(input_path),
        "generated_at": _now_rfc3339(),
        "overall": {
            "rework_required": rework_required,
            "fundamental_problem_count": fundamental_problem_count,
            "total_critics": len(results),
            "passing_critics": passing_critics,
            "failing_critics": failing_critics,
        },
        "general_reader": general_reader,
        "critic_results": persona_results,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Emit normalized paragraph rows here so the generated no-op map stage still has
    # deterministic artifacts to register.
    candidate_map_rows = _load_jsonl(Path(CANDIDATE_MAP_PATTERN.format(lang=language)))
    paragraph_ids = [
        str(row.get("paragraph_id")).strip()
        for row in candidate_map_rows
        if isinstance(row.get("paragraph_id"), str) and str(row.get("paragraph_id")).strip()
    ]
    typography_payload = _load_json(Path(TYPOGRAPHY_REVIEW_PATTERN.format(lang=language)))
    typography_reviews = typography_payload.get("paragraph_reviews")
    typography_by_id: dict[str, dict[str, Any]] = {}
    if isinstance(typography_reviews, list):
        for review in typography_reviews:
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
            typography_by_id[paragraph_id] = {
                "paragraph_id": paragraph_id,
                "scores": {"typography": review.get("total_score")},
                "issues": normalized_issues,
                "hard_fail": decision == "major_rewrite",
                "source": "typography_review",
                "decision": decision,
            }

    typography_rows: list[dict[str, Any]] = []
    critics_rows: list[dict[str, Any]] = []
    rework_required = bool(payload.get("overall", {}).get("rework_required"))
    critics_issues = [
        issue.get("description")
        for section_name in ("general_reader", "critic_results")
        for item in (
            [payload.get("general_reader")] if section_name == "general_reader" else payload.get("critic_results", [])
        )
        if isinstance(item, dict)
        for issue in item.get("issues", [])
        if isinstance(issue, dict) and isinstance(issue.get("description"), str) and issue.get("description")
    ]
    failing_critics = [
        str(item.get("critic_id"))
        for item in ([payload.get("general_reader")] + list(payload.get("critic_results") or []))
        if isinstance(item, dict) and str(item.get("verdict") or "") == "rework"
    ]

    for paragraph_id in paragraph_ids:
        typography_rows.append(
            typography_by_id.get(
                paragraph_id,
                {
                    "paragraph_id": paragraph_id,
                    "scores": {"typography": None},
                    "issues": [],
                    "hard_fail": False,
                    "source": "typography_review",
                    "decision": "missing",
                },
            )
        )
        critics_rows.append(
            {
                "paragraph_id": paragraph_id,
                "scores": {"critics_pass": 0 if rework_required else 1},
                "issues": critics_issues,
                "hard_fail": rework_required,
                "source": "critics_review",
                "failing_critics": failing_critics,
            }
        )

    _write_jsonl(Path(TYPOGRAPHY_ROWS_PATTERN.format(lang=language)), typography_rows)
    _write_jsonl(Path(CRITICS_ROWS_PATTERN.format(lang=language)), critics_rows)


def run_item(ctx, item: dict[str, object]) -> None:
    _ = item
    run_whole(ctx)
