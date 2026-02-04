#!/usr/bin/env python3
"""Download NLTK data resources required by the diagnostics scripts."""
from __future__ import annotations

import argparse

import nltk


RESOURCE_MAP = {
    "tokenizers/punkt": "punkt",
    "corpora/stopwords": "stopwords",
    "taggers/averaged_perceptron_tagger": "averaged_perceptron_tagger",
    "corpora/wordnet": "wordnet",
    "corpora/omw-1.4": "omw-1.4",
}


def ensure_nltk_resources(quiet: bool) -> None:
    for path, name in RESOURCE_MAP.items():
        try:
            nltk.data.find(path)
        except LookupError:
            nltk.download(name, quiet=quiet)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download NLTK resources used by ReadMosaic diagnostics."
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress verbose NLTK downloader output.",
    )
    args = parser.parse_args()
    ensure_nltk_resources(quiet=args.quiet)


if __name__ == "__main__":
    main()
