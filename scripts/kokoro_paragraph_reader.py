#!/usr/bin/env python3
"""Read pre-processed manuscript paragraphs with Kokoro TTS.

Consumes a pre-processing output directory (expects paragraphs.jsonl), then
reads each paragraph and prompts whether to continue.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Use Kokoro TTS to read pre-processed paragraphs one by one and ask "
            "whether to continue after each paragraph."
        )
    )
    parser.add_argument(
        "preprocessed_dir",
        type=Path,
        help="Pre-processing directory containing paragraphs.jsonl.",
    )
    parser.add_argument(
        "--voice",
        default="af_heart",
        help="Kokoro voice ID (default: af_heart).",
    )
    parser.add_argument(
        "--lang-code",
        default="a",
        help="Kokoro language code for KPipeline (default: a).",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Speech speed multiplier passed to Kokoro (default: 1.0).",
    )
    parser.add_argument(
        "--no-playback",
        action="store_true",
        help="Generate audio but do not play it through the system speaker.",
    )
    return parser.parse_args()


def load_paragraphs(preprocessed_dir: Path) -> list[str]:
    paragraphs_path = preprocessed_dir / "paragraphs.jsonl"
    if not paragraphs_path.exists():
        raise RuntimeError(
            f"Missing file: {paragraphs_path}. Run scripts/pre_processing.py first."
        )

    records: list[dict[str, Any]] = []
    with paragraphs_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSON in {paragraphs_path} at line {line_number}."
                ) from exc
            if "text" not in record:
                raise RuntimeError(
                    f"Missing 'text' field in {paragraphs_path} at line {line_number}."
                )
            records.append(record)

    records.sort(key=lambda r: r.get("order", 0))
    return [str(record["text"]).strip() for record in records if str(record["text"]).strip()]


def build_pipeline(lang_code: str):
    try:
        from kokoro import KPipeline
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Kokoro is not installed. Install it first, e.g. `pip install kokoro`."
        ) from exc
    return KPipeline(lang_code=lang_code)


def synthesize_wav_bytes(pipeline, text: str, voice: str, speed: float) -> bytes:
    try:
        import soundfile as sf
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "soundfile is required to serialize Kokoro output. Install with `pip install soundfile`."
        ) from exc

    segments = list(pipeline(text, voice=voice, speed=speed))
    if not segments:
        raise RuntimeError("Kokoro returned no audio for the provided paragraph.")

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with sf.SoundFile(tmp_path, mode="w", samplerate=24000, channels=1, subtype="PCM_16") as out_file:
            for _, _, audio in segments:
                out_file.write(audio)
        return tmp_path.read_bytes()
    finally:
        tmp_path.unlink(missing_ok=True)


def play_audio(wav_bytes: bytes) -> None:
    try:
        import sounddevice as sd
        import soundfile as sf
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Playback requires sounddevice and soundfile. Install with `pip install sounddevice soundfile`."
        ) from exc

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp.write(wav_bytes)
        tmp_path = Path(tmp.name)

    try:
        data, sample_rate = sf.read(tmp_path, dtype="float32")
        sd.play(data, sample_rate)
        sd.wait()
    finally:
        tmp_path.unlink(missing_ok=True)


def should_continue() -> bool:
    while True:
        answer = input("Continue? (Yes, no): ").strip().lower()
        if answer in {"", "c", "continue", "y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please respond with Continue, Yes, or No.")


def main() -> int:
    args = parse_args()
    if not args.preprocessed_dir.exists():
        print(f"Directory not found: {args.preprocessed_dir}", file=sys.stderr)
        return 1

    try:
        paragraphs = load_paragraphs(args.preprocessed_dir)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1

    if not paragraphs:
        print("No readable paragraphs found in paragraphs.jsonl.")
        return 0

    try:
        pipeline = build_pipeline(args.lang_code)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1

    total = len(paragraphs)
    for idx, paragraph in enumerate(paragraphs, start=1):
        print(f"\n--- Paragraph {idx}/{total} ---\n")
        print(paragraph)

        try:
            wav_bytes = synthesize_wav_bytes(pipeline, paragraph, args.voice, args.speed)
            if not args.no_playback:
                play_audio(wav_bytes)
        except RuntimeError as error:
            print(str(error), file=sys.stderr)
            return 1

        if idx < total and not should_continue():
            print("Stopped by user.")
            return 0

    print("Finished reading all paragraphs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
