from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from ._artifacts import default_input_candidates, output_artifact_dir, stage_config

DEFAULT_INPUT_MANUSCRIPT_NAMES = ('manuscript.markdown', 'manuscript.md')
PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n+")


def _clean_markdown_text(markdown_text: str) -> str:
    text = re.sub(r"```.*?```", "", markdown_text, flags=re.DOTALL)
    text = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^\)]*\)", r"\1", text)
    text = re.sub(r"^\s{0,3}#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*>\s?", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*[-*+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*\d+\.\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"[*_]{1,3}", "", text)
    return text.strip()


def _resolve_manuscript_path(ctx, manuscript_config: str | None) -> Path:
    if manuscript_config:
        configured = Path(manuscript_config)
        if configured.is_absolute():
            return configured
        return Path.cwd() / configured

    fallback_candidates: list[Path] = []
    for manuscript_name in DEFAULT_INPUT_MANUSCRIPT_NAMES:
        candidates = default_input_candidates(ctx, manuscript_name)
        fallback_candidates.extend(candidates)
        for candidate in candidates:
            if candidate.exists():
                return candidate

    return fallback_candidates[0]


def _load_manuscript_text(manuscript_path: Path) -> str:
    if not manuscript_path.exists():
        raise FileNotFoundError(
            f"Expected manuscript markdown at '{manuscript_path}'. "
            "Provide input artifact 'artifacts/inputs/manuscript.markdown' or 'artifacts/inputs/manuscript.md', "
            "or set run_config.rc.voice.input_manuscript."
        )

    cleaned = _clean_markdown_text(manuscript_path.read_text(encoding='utf-8'))
    paragraphs = [p.strip() for p in PARAGRAPH_SPLIT_RE.split(cleaned) if p.strip()]
    if not paragraphs:
        raise ValueError('Manuscript is empty after markdown normalization.')
    return '\n\n'.join(paragraphs)


def _build_kokoro_pipeline(lang_code: str):
    try:
        from kokoro import KPipeline
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Kokoro is not installed. Install it first, e.g. `pip install kokoro`."
        ) from exc

    return KPipeline(lang_code=lang_code)


def _synthesize_wav(pipeline, text: str, *, voice: str, speed: float, wav_path: Path) -> None:
    try:
        import soundfile as sf
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "soundfile is required to serialize Kokoro output. Install with `pip install soundfile`."
        ) from exc

    segments = list(pipeline(text, voice=voice, speed=speed))
    if not segments:
        raise RuntimeError('Kokoro returned no audio for the provided manuscript.')

    with sf.SoundFile(wav_path, mode='w', samplerate=24000, channels=1, subtype='PCM_16') as out_file:
        for _, _, audio in segments:
            out_file.write(audio)


def _convert_wav_to_mp3(wav_path: Path, mp3_path: Path) -> None:
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError('ffmpeg is required to convert Kokoro WAV output into manuscript.mp3.')

    cmd = [
        ffmpeg,
        '-y',
        '-i',
        str(wav_path),
        '-vn',
        '-codec:a',
        'libmp3lame',
        '-q:a',
        '2',
        str(mp3_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def run_whole(ctx) -> None:
    cfg = stage_config(ctx, 'voice')

    manuscript_path = _resolve_manuscript_path(
        ctx,
        cfg.get('input_manuscript') or cfg.get('manuscript_path'),
    )

    voice = str(cfg.get('voice', 'af_heart'))
    lang_code = str(cfg.get('lang_code', 'a'))
    speed = float(cfg.get('speed', 1.0))
    output_name = str(cfg.get('output_name', 'manuscript.mp3'))

    manuscript_text = _load_manuscript_text(manuscript_path)
    pipeline = _build_kokoro_pipeline(lang_code)

    out_dir = output_artifact_dir(ctx)
    out_dir.mkdir(parents=True, exist_ok=True)
    mp3_path = out_dir / output_name

    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp_wav:
        wav_path = Path(tmp_wav.name)

    try:
        _synthesize_wav(pipeline, manuscript_text, voice=voice, speed=speed, wav_path=wav_path)
        _convert_wav_to_mp3(wav_path, mp3_path)
    finally:
        wav_path.unlink(missing_ok=True)
