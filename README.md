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
