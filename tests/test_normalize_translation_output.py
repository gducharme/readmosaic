from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.assemble_candidate import assemble_candidate
from scripts.normalize_translation_output import normalize_translation_output


class NormalizeTranslationOutputTests(unittest.TestCase):
    def test_global_token_offsets_are_global_not_local(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            (source_pre / "paragraphs.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "paragraph_id": "p1",
                                "content_hash": "sha256:" + "a" * 64,
                                "text": "alpha.",
                                "manuscript_id": "m1",
                                "source": "src.md",
                            }
                        ),
                        json.dumps(
                            {
                                "paragraph_id": "p2",
                                "content_hash": "sha256:" + "b" * 64,
                                "text": "beta.",
                                "manuscript_id": "m1",
                                "source": "src.md",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            translation_json = root / "translation.json"
            translation_json.write_text(
                json.dumps({"paragraph_translations": ["Hi.", "Yo."]}),
                encoding="utf-8",
            )

            output_pre = root / "pass1_pre"
            normalize_translation_output(source_pre, translation_json, output_pre)

            manuscript_tokens = json.loads((output_pre / "manuscript_tokens.json").read_text(encoding="utf-8"))
            p1_token_start = manuscript_tokens["paragraphs"][0]["tokens"][0]["start_char"]
            p2_token_start = manuscript_tokens["paragraphs"][1]["tokens"][0]["start_char"]
            self.assertEqual(p1_token_start, 0)
            self.assertEqual(p2_token_start, len("Hi.") + 2)

    def test_missing_source_paragraphs_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_pre = root / "source_pre"
            source_pre.mkdir(parents=True, exist_ok=True)
            translation_json = root / "translation.json"
            translation_json.write_text(json.dumps({"paragraph_translations": []}), encoding="utf-8")

            with self.assertRaises(FileNotFoundError):
                normalize_translation_output(source_pre, translation_json, root / "pass1_pre")

    def test_assembler_map_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paragraphs = root / "paragraphs.jsonl"
            paragraphs.write_text(
                "\n".join(
                    [
                        json.dumps({"paragraph_id": "p1", "text": "line1\nline2"}),
                        json.dumps({"paragraph_id": "p2", "text": "line3"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            candidate_md = root / "candidate.md"
            candidate_map = root / "candidate_map.jsonl"
            assemble_candidate(paragraphs, candidate_md, candidate_map)

            candidate_lines = candidate_md.read_text(encoding="utf-8").splitlines()
            map_rows = [
                json.loads(line)
                for line in candidate_map.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

            expected = ["line1\nline2", "line3"]
            for row, expected_text in zip(map_rows, expected):
                start = int(row["start_line"]) - 1
                end = int(row["end_line"])
                reconstructed = "\n".join(candidate_lines[start:end])
                self.assertEqual(reconstructed, expected_text)


if __name__ == "__main__":
    unittest.main()
