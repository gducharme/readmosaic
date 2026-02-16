# Neo4j local container setup

This repository provides a `docker-compose.yaml` for running a local Neo4j instance with the APOC plugin enabled.

## Text semantic repetition analyzer

The `scripts/analyzer.py` CLI analyzes `.txt` or `.md` files for semantic echoes and redundancy. Run the script with `--help` for usage details and options.


## Conceptual Theme Mapper (CTM)

The `scripts/theme_mapper.py` CLI discovers thematic clusters across manuscript chunks using LDA, and exports a topic heatmap plus an interactive pyLDAvis HTML report. Run the script with `--help` for usage details.

## Linguistic Pattern Extractor (LPE)

The `scripts/pattern_extractor.py` CLI uses spaCy's Dependency Matcher to extract phrasal verbs, action chains, descriptive pairs, and adverbial intent patterns from `.txt` or `.md` manuscripts. It can compare files, report pattern density, and surface stylistic entropy. Install the transformer model with:

```bash
scripts/download_spacy_model.sh
```

Then run:

```bash
scripts/pattern_extractor.py --help
```

## Neutrino Surprisal Scout (NSS)

The `scripts/surprisal_scout.py` CLI computes per-sentence surprisal scores using a local
transformers causal language model (GPT-2 by default). It flags high-probability
"slop zone" sentences above a configurable percentile threshold and highlights
stock AI transition phrases. The tool outputs a Surprisal Map plot plus optional
CSV/JSON exports.

```bash
python scripts/surprisal_scout.py manuscript.txt --model gpt2 --percentile 90 \
  --plot surprisal_map.png --output-csv surprisal.csv --output-json surprisal.json
```

## Semantic Entropy Evaluator (SEE)

The `scripts/entropy_evaluator.py` CLI calculates Shannon entropy across unigrams and bigrams, tracks entropy drift over sliding windows, and generates an entropy heatmap plus JSON stats. Run the script with `--help` for usage details and options.

```bash
python scripts/entropy_evaluator.py path/to/manuscript.txt --output results/entropy
```

## Word Frequency Benchmark (WFB)

The `scripts/word_frequency_benchmark.py` CLI lists the top manuscript words (default top 10), includes raw counts, and compares each term against the NLTK Brown corpus as a rough “humanity average” baseline. The output includes per-million rates and a relative ratio versus corpus usage.

```bash
python scripts/word_frequency_benchmark.py path/to/manuscript.txt --top-n 10
```

Use `--include-stopwords` if you want function words in the ranking, `--output-json` to persist the benchmark payload, and `--frequency-report-file` to emit a compact word-frequency JSON object (`word -> count`).

## Lexical Entropy Amplifier (LEA)

The `scripts/lexical_entropy_amplifier.py` CLI takes an overuse/frequency report plus a preprocessing directory and builds rewrite bundles to reduce lexical attractors while preserving narrative profile.

For each overused word, it:

- finds paragraphs containing that word,
- generates suggestions from four methods:
  - **A**: frequency band jump (prefer lower Zipf frequency candidates when available),
  - **B**: embedding drift with SentenceTransformers,
  - **C**: WordNet lateral expansion (coordinate terms / hyponyms),
  - **D**: Datamuse thesaurus suggestions,
- sends paragraph + suggestion bundle to a local LLM with instruction: _"escape the lexical attractor while preserving semantic identity"_,
- records before/after paragraph replacements keyed by target word.

Example:

```bash
python scripts/lexical_entropy_amplifier.py \
  --preprocessing /preprocessed \
  --overuse-report overuse_report.json \
  --model llama3:8b-instruct-q8_0 \
  --output-json lexical_bundles.json \
  --output-markdown lexical_bundles.md
```

`--overuse-report` supports either the `word_frequency_benchmark.py --output-json` payload (`top_words`) or a compact frequency map (`word -> count`).


## Lexical Enhancer (Interactive LEA v2)

The `scripts/lexical_enhancer.py` CLI is an interactive version of lexical expansion. It loads preprocessing once, then loops on user-selected target words.

For each target word, it:

- scans all preprocessing paragraphs for every occurrence of the target word (including repeated hits within a paragraph),
- generates the same four suggestion methods as LEA,
- runs LLM rewrite proposals occurrence-by-occurrence,
- requires manual review (`accept`, `edit`, or `skip`) for each occurrence review,
- only then returns control so you can choose the next target word.

Example:

```bash
python scripts/lexical_enhancer.py \
  --preprocessing /preprocessed \
  --model llama3:8b-instruct-q8_0 \
  --output-json lexical_enhancer_review.json
```

Type `quit` at the target word prompt to end and save the session artifact.

## Narrative Burst Monitor (NBM)

The `scripts/burst_monitor.py` CLI scans manuscripts for statistically significant bursts of terms (uni/bi/tri-grams) using a sliding-window Z-score model. It focuses on content words (nouns, verbs, adjectives) and ignores stop words so you can spot concept clumping without noise.

Run the script with:

```bash
scripts/burst_monitor.py --help
```

Example:

```bash
python scripts/burst_monitor.py docs/burst_monitor_sample.txt --window-size 50 --step-size 10 --threshold 2.0 --top-n 5
```

Sample hot zones from `docs/burst_monitor_sample.txt` (using the command above):

- **entropy** and **capitalism** spike together in the early-middle passage where the Founder lectures on doctrine (roughly the 20–50% progress band).
- **engineered resonance** shows a later burst near the mid-to-late portion of the text (around the 60–80% progress band).

## NLTK data setup

Some diagnostics (notably `scripts/burst_monitor.py` and `scripts/entropy_evaluator.py`) rely on NLTK corpora and taggers that are not bundled with the pip package. If you see `LookupError` messages for missing NLTK data, run:

```bash
python scripts/setup_nltk_data.py
```

Run the script once per environment (or after clearing your NLTK data directory) to ensure the required resources are available.

## Cliché Wrap-Up Scrubber (CWS)

The `scripts/slop_scrubber.py` CLI inspects the last 1–2 paragraphs of a scene and flags AI-style “hopeful” wrap-ups. It scores a custom lexicon of abstract nouns, vague adjectives, togetherness phrases, detects reflective clause patterns, and identifies sentiment pivots that signal moralizing drift. The curated lexicon lives in `scripts/ai_ending_lexicon.json` so you can edit it without touching code.

Run the script with:

```bash
python scripts/slop_scrubber.py --help
```

## Direct Signal Filter (DSF)

The `scripts/direct_signal_filter.py` CLI hunts for negation-led pacing stalls,
quiet/silent intensity clichés, and hedge-heavy paragraphs. It uses spaCy's
transformer pipeline and emits optional edits payloads for the Mosaic
orchestrator.

Run the script with:

```bash
python scripts/direct_signal_filter.py --help
```


## Vivid Verb Upgrader (VVU)

The `scripts/vivid_verb_upgrader.py` CLI detects generic/light verbs with direct objects and suggests more vivid troponym alternatives using WordNet candidates ranked by semantic fit, concreteness, frequency penalty, and specificity depth. VerbNet filtering is optional via `--strict-verbnet`.

By default, VVU reads `./concreteness.csv` using the schema: `verb,category,concreteness_hint`.

Run the script with:

```bash
python scripts/vivid_verb_upgrader.py --help
```

Examples:

```bash
python scripts/vivid_verb_upgrader.py manuscript.md --concreteness-csv concreteness.csv
python scripts/vivid_verb_upgrader.py --preprocessed /preprocessed/sentences.jsonl --concreteness-csv concreteness.csv --output-json vvu.json
```

`--preprocessed` accepts either a `sentences.jsonl` file or a preprocessing directory containing `sentences.jsonl`.


Helpful output flags:

- `--print-text` prints one human-readable block per occurrence in the form `Original / Generic / Suggestions`.
- `--include-empty` keeps occurrences even when all candidates are filtered out (for full auditing).
- `--strict-verbnet` re-enables strict VerbNet gating for every candidate.

## Manuscript Pre-Processing

The `scripts/pre_processing.py` CLI segments a manuscript into paragraph, sentence, and word JSONL artifacts with stable IDs, order, offsets, and cross-references. It also emits a schema-aligned `manuscript_tokens.json` artifact for downstream token processing. Run the script with `--help` for usage details and options.

```bash
python scripts/pre_processing.py path/to/manuscript.md --output-dir /preprocessed --manuscript-id draft-01
```

Outputs:

- `/preprocessed/paragraphs.jsonl` for paragraph IDs, ordering, offsets, and prev/next links.
- `/preprocessed/sentences.jsonl` for sentence IDs, ordering, offsets, and paragraph links.
- `/preprocessed/words.jsonl` for word IDs, ordering, offsets, and sentence/paragraph links.
- `/preprocessed/manuscript_tokens.json` for paragraph-ordered tokens with stable `paragraph_id`/`token_id` strings and paragraph-relative offsets.

Schemas for the JSONL record shapes live in:

- `schemas/paragraph_ids.schema.json`
- `schemas/sentence_ids.schema.json`
- `schemas/word_ids.schema.json`
- `schemas/manuscript_tokens.schema.json`

## Confidence Review Script

The `scripts/confidence_review.py` CLI scans Mosaic `_edits.json` outputs, aligns
them to the pre-processed manuscript token IDs, and prints the full manuscript
with a five-band confidence color scale (deep green → light green → yellow → orange → red).
Run the script with `--help` for usage details and options.

```bash
python scripts/confidence_review.py --preprocessed /preprocessed --edits-root /mosaic/outputs
```


## HTML Review Script

The `scripts/html_review.py` CLI generates a standalone HTML review page from Mosaic
`_edits.json` outputs. It applies confidence coloring (green → yellow → orange → coral-red),
tracks detections by word/sentence/paragraph scope, and adds a per-word hover bubble
with issue details (`type`, `severity`, detector, and evidence summary).

```bash
python scripts/html_review.py --preprocessed /preprocessed --edits-root /mosaic/outputs --output html_review.html
```

## Mosaic Orchestrator (MO)

The Mosaic Orchestrator ties the full tool stack together, runs the analysis sweep,
and produces a unified Simulation Fidelity Report plus editorial culling directives
using a local LM Studio model. It emits two artifacts:

- `mosaic_outputs/fidelity_context.json` (the Fidelity Context artifact).
- `mosaic_outputs/culling_directives.md` (the Archivist "Culling Directives" report).

The system prompt for the Archivist lives at `prompts/Archivist_Core_V1.txt` so you can
edit the tone or instructions without touching code.

Run the orchestrator with:

```bash
python mosaic_mo.py --file manuscript_v1.md --model llama3:8b-instruct-q8_0
```

Use `--help` for full CLI options, including the LM Studio base URL override and output directory.


## Culling Resolver Pipeline

The `scripts/culling_resolver.py` CLI resolves **one culling node at a time** into a strict JSON deletion action. It is deletion-only: if a directive asks for rewrite/edit behavior, the model is instructed to refuse and the resolver continues to the next item.

Default inputs:

- `--sentences`: `./mosaic_outputs/preprocessing/sentences.jsonl`
- `--culling-directives`: `./mosaic_outputs/culling_directives.md`
- `--output-dir`: `./mosaic_outputs/culling_items/`

Per-item flow:

1. Initial resolution against the indexed manuscript.
2. Retry with narrower context if schema validation fails or confidence is below threshold.
3. If still unresolved, mark `manual_review_required`.

Each per-item artifact stores attempt-level `request_id` and model `response_id` plus raw prompt/response payloads for auditability.

```bash
python scripts/culling_resolver.py --model llama3:8b-instruct-q8_0
```

## Mosaic Recursive Engine (MRE) Minimal Prototype

The `mre_minimal.py` script is a greenfield, single-pass engine that can forge new tools,
hot-reload them, and apply edits to a manuscript based on a diagnostics JSON file.

```bash
python mre_minimal.py --file manuscript.md --diagnostics diagnostics.json --model llama3:8b-instruct-q8_0
```

## Prompt Transformer Script

The `scripts/prompt_transformer.py` CLI applies a selected prompt to each line or paragraph
of a manuscript using a local LM Studio model.

Use it when you want prompt-driven rewrites at a chosen resolution.

```bash
python scripts/prompt_transformer.py \
  --file manuscript.md \
  --prompt Revision_Assistant_Template.txt \
  --model llama3:8b-instruct-q8_0 \
  --resolution paragraph \
  --output-dir prompt_outputs \
  --preprocessed preprocessed
```

Key flags:

- `--prompt`: prompt filename (resolved inside `prompts/`) or full path.
- `--model`: LM Studio model identifier.
- `--resolution`: `line` or `paragraph`.
- `--output-dir`: directory for JSONL + Markdown outputs.
- `--preprocessed`: optional pre-processing directory; paragraph mode uses `paragraphs.jsonl` when present.

## Kokoro Paragraph Reader

The `scripts/kokoro_paragraph_reader.py` CLI reads paragraphs from a pre-processing directory (`paragraphs.jsonl`) using Kokoro TTS, then asks whether to continue (`Continue`, `Yes`, or `No`) after each paragraph.

```bash
python scripts/kokoro_paragraph_reader.py /preprocessed --voice af_heart
```

Use `--no-playback` if you only want synthesis flow without speaker output.

## Schemas

The `schemas/` directory captures lightweight JSON Schema definitions used to stabilize
the editing pipeline. The initial set covers:

- `manuscript_tokens.schema.json` for word-level IDs and offsets.
- `paragraph_ids.schema.json`, `sentence_ids.schema.json`, and `word_ids.schema.json` for pre-processed manuscript IDs and cross-references.
- `edits.schema.json` for normalized issue payloads.
- `demonstration.schema.json` for problem demonstrations.
- `proposal.schema.json` plus `patch.schema.json` for targeted fixes and verification.


## Interactive Script Menu

The `scripts/script_menu.py` CLI discovers Python tools in a scripts directory, reads each tool's `--help` output, and builds an interactive TUI menu.

- Navigate script selection with **↑/↓** and **Enter**.
- Fill or toggle arguments in a second screen.
- Use **Esc** to back out to the previous menu.
- Use **r** or select **Run script** to execute with your chosen arguments.

```bash
python scripts/script_menu.py
```

You can also inspect discovery without opening the interactive UI:

```bash
python scripts/script_menu.py --list
```

## Mosaic Signal Density (MSD)

The `scripts/signal_density.py` CLI estimates lexical density and signal concentration
for a manuscript. It powers the MSD tool inside the orchestrator.

```bash
python scripts/signal_density.py path/to/manuscript.md --top-n 10 --output-json msd.json
```

## Prerequisites

- Docker Desktop or Docker Engine
- Docker Compose (v2 plugin or the `docker-compose` binary)

## First-time setup

1. Create the bind-mount directories (only needed once):

   ```bash
   mkdir -p neo4j/data neo4j/logs neo4j/import neo4j/plugins
   ```

2. Start the container:

   ```bash
   docker compose up -d
   ```

## Verify the service

- Neo4j Browser: http://localhost:7474
- Bolt protocol: `bolt://localhost:7687`

Use the credentials from the compose file:

- Username: `neo4j`
- Password: `mosaic_founding`

## Stop the container

```bash
docker compose down
```

## Reset data

If you need to wipe the database, stop the container and remove the data directory:

```bash
docker compose down
rm -rf neo4j/data
```

## Critics Runner

The `scripts/critics_runner.py` CLI scans `prompts/critics` for markdown files, sends each file's full contents to a local LM Studio chat-completions endpoint, and writes a single unstructured JSON object where each key is the critic filename stem and each value is the model response.

```bash
python scripts/critics_runner.py --model llama3:8b-instruct-q8_0
```

Use `--output` to control the destination path, or let it default to `critics_outputs/critics_responses_<timestamp>.json`.

## Ultra-Precision Grammar Auditor (UPGA)

The `scripts/grammar_auditor.py` CLI runs a strict grammar-only audit against a local LM Studio model using `prompts/Ultra_Precision_Grammar_Auditor.txt` and writes all detected issues to a single JSON file. It prefers `--preprocessed` sentence artifacts so chunking aligns to sentence boundaries and never cuts final sentences mid-chunk.

Key behavior:

- Uses sentence-boundary chunked analysis (`--chunk-size`, default 1000 words) to keep chunks grammatically complete.
- Supports strictness flags for optional preferences, style choices, ambiguity, CMOS strictness, and clause-level parse output.
- Aggregates all issues from all chunks into one report file in `grammar_outputs/`.
- Validates output against `schemas/grammar_audit_report.schema.json`.

Example:

```bash
python scripts/grammar_auditor.py \
  --preprocessed mosaic_outputs/preprocessing \
  --model llama3:8b-instruct-q8_0 \
  --include-ambiguities \
  --strict-cmos \
  --clause-level-parse
```

Preview without model calls:

```bash
python scripts/grammar_auditor.py --preprocessed mosaic_outputs/preprocessing --model stub --preview
```

Fallback mode (when preprocessed artifacts are unavailable):

```bash
python scripts/grammar_auditor.py --file docs/sample.md --model llama3:8b-instruct-q8_0
```

## Quotation & Delimiter Precision Auditor

The `scripts/quotation_delimiter_auditor.py` CLI runs a delimiter-focused audit (quotes, apostrophes, brackets/parentheses, nesting, punctuation placement consistency, prime misuse, and encoding artifacts) against a local LM Studio model using `prompts/Quotation_Delimiter_Precision_Auditor.txt`.

Key behavior:

- Uses sentence-boundary chunked analysis (`--chunk-size`, default 1000 words).
- Aggregates all issues from all chunks into one report file in `quotation_audit_outputs/`.
- Validates output against `schemas/quotation_delimiter_audit_report.schema.json`.
- Supports optional em/en dash misuse detection inside quotes via `--flag-em-en-dash-misuse`.

Example:

```bash
python scripts/quotation_delimiter_auditor.py \
  --preprocessed mosaic_outputs/preprocessing \
  --model llama3:8b-instruct-q8_0 \
  --flag-em-en-dash-misuse
```

Preview without model calls:

```bash
python scripts/quotation_delimiter_auditor.py --preprocessed mosaic_outputs/preprocessing --model stub --preview
```

Fallback mode (when preprocessed artifacts are unavailable):

```bash
python scripts/quotation_delimiter_auditor.py --file docs/sample.md --model llama3:8b-instruct-q8_0
```

## Typographic Precision Review (Interactive)

The `scripts/typographic_precision_review.py` CLI reviews auditor issue output from the top-level `issues` key and aligns each issue to preprocessed sentence lines.

Key behavior:

- Accepts report JSON objects that contain an `issues` array.
- Requires `--preprocessed` and reads `sentences.jsonl` so each issue is shown against the corresponding line.
- Reorders issues by ascending `sentence_index` before review to ensure line-by-line processing.
- Displays a before-line and after-line preview per issue, with interactive default **accept** behavior (`Y` on Enter), plus reject (`n`) or edit correction (`e`).
- Writes decisions JSON and also writes a merged final `sentences.jsonl`-style file with accepted edits applied.

Example:

```bash
python scripts/typographic_precision_review.py \
  --audit quotation_audit_outputs/quotation_audit_20260101_120000.json \
  --preprocessed mosaic_outputs/preprocessing \
  --output quotation_audit_review.json \
  --final-output quotation_audit_final_sentences.jsonl
```
