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

## Cliché Wrap-Up Scrubber (CWS)

The `scripts/slop_scrubber.py` CLI inspects the last 1–2 paragraphs of a scene and flags AI-style “hopeful” wrap-ups. It scores a custom lexicon of abstract nouns, vague adjectives, togetherness phrases, detects reflective clause patterns, and identifies sentiment pivots that signal moralizing drift. The curated lexicon lives in `scripts/ai_ending_lexicon.json` so you can edit it without touching code.

Run the script with:

```bash
python scripts/slop_scrubber.py --help
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
