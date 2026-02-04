# Neo4j local container setup

This repository provides a `docker-compose.yaml` for running a local Neo4j instance with the APOC plugin enabled.

## Text semantic repetition analyzer

The `scripts/analyzer.py` CLI analyzes `.txt` or `.md` files for semantic echoes and redundancy. Run the script with `--help` for usage details and options.

## Linguistic Pattern Extractor (LPE)

The `scripts/pattern_extractor.py` CLI uses spaCy's Dependency Matcher to extract phrasal verbs, action chains, descriptive pairs, and adverbial intent patterns from `.txt` or `.md` manuscripts. It can compare files, report pattern density, and surface stylistic entropy. Install the transformer model with:

```bash
scripts/download_spacy_model.sh
```

Then run:

```bash
scripts/pattern_extractor.py --help
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
