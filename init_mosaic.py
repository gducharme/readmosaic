from __future__ import annotations

import os
import time
from pathlib import Path
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from neo4j import GraphDatabase


DEFAULT_URI = "bolt://localhost:7687"
DEFAULT_USER = "neo4j"
DEFAULT_PASSWORD = "neo4j"
DEFAULT_SCHEMA_FILE = "schema.cypher"
DEFAULT_MIGRATIONS_DIR = "migrations"


def wait_for_neo4j(driver, max_attempts: int = 30, delay_s: float = 2.0) -> None:
    attempt = 0
    while True:
        attempt += 1
        try:
            with driver.session() as session:
                session.run("RETURN 1").consume()
            return
        except Exception:
            if attempt >= max_attempts:
                raise
            time.sleep(delay_s)


def run_statements(session, statements: Iterable[str]) -> None:
    for statement in statements:
        session.run(statement).consume()


def _normalize_year(timestamp: int | str) -> int:
    if isinstance(timestamp, int):
        return timestamp
    try:
        return datetime.fromisoformat(timestamp).year
    except ValueError as exc:
        raise ValueError(f"Unsupported timestamp format: {timestamp}") from exc


def log_event(
    tx,
    uid: str,
    type: str,
    timestamp: int | str,
    description: str,
    location_name: str,
    actors: Mapping[str, str],
    parent_event_uid: str | None = None,
) -> None:
    year = _normalize_year(timestamp)
    actor_rows = [{"uid": char_uid, "role": role} for char_uid, role in actors.items()]
    tx.run(
        """
        MERGE (year:Year {value: $year})
        MERGE (location:Location {name: $location_name})
        CREATE (event:Event {
            uid: $uid,
            type: $type,
            description: $description,
            timestamp: $timestamp
        })
        MERGE (event)-[:OCCURRED_IN]->(year)
        MERGE (event)-[:TOOK_PLACE_AT]->(location)
        WITH event
        UNWIND $actors AS actor
        MATCH (character:Character {uid: actor.uid})
        MERGE (character)-[:PARTICIPATED {role: actor.role}]->(event)
        WITH event
        OPTIONAL MATCH (parent:Event {uid: $parent_event_uid})
        FOREACH (_ IN CASE WHEN parent IS NULL THEN [] ELSE [1] END |
            MERGE (event)-[:CAUSED_BY]->(parent)
        )
        """,
        year=year,
        location_name=location_name,
        uid=uid,
        type=type,
        description=description,
        timestamp=timestamp,
        actors=actor_rows,
        parent_event_uid=parent_event_uid,
    ).consume()


def seed_mosaic_genesis(session) -> None:
    if migration_applied(session, "seed_mosaic_genesis"):
        return

    session.run(
        """
        MERGE (founder:Character {uid: $founder_uid})
        SET founder.name = $founder_name
        MERGE (subject:Character {uid: $subject_uid})
        SET subject.name = $subject_name
        """,
        founder_uid="CHAR-001",
        founder_name="The Architect",
        subject_uid="CHAR-002",
        subject_name="Subject 0",
    ).consume()

    session.execute_write(
        log_event,
        uid="EVT-001",
        type="SOCIAL_RITUAL",
        timestamp=2024,
        description="Initial contact initiated by Founder.",
        location_name="Sector 4 Apartment",
        actors={"CHAR-001": "Initiator", "CHAR-002": "Target"},
    )
    session.execute_write(
        log_event,
        uid="EVT-002",
        type="BIOLOGICAL_IMPRINT",
        timestamp=2024,
        description="Consent protocols bypassed via epigenetic trigger.",
        location_name="Seaside",
        actors={"CHAR-001": "Apex", "CHAR-002": "Vessel"},
        parent_event_uid="EVT-001",
    )
    record_migration(session, "seed_mosaic_genesis")


def parse_cypher_statements(raw: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).rstrip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            buffer = []

    if buffer:
        statement = "\n".join(buffer).strip()
        if statement:
            statements.append(statement)

    return statements


def load_cypher_file(path: Path) -> list[str]:
    raw = path.read_text(encoding="utf-8")
    return parse_cypher_statements(raw)


def migration_applied(session, version: str) -> bool:
    result = session.run(
        "MATCH (m:SchemaMigration {version: $version}) RETURN count(m) AS count",
        version=version,
    )
    record = result.single()
    return bool(record and record["count"] > 0)


def record_migration(session, version: str) -> None:
    session.run(
        "MERGE (:SchemaMigration {version: $version, appliedAt: datetime()})",
        version=version,
    ).consume()


def apply_migration(session, version: str, statements: Sequence[str]) -> None:
    if not statements or migration_applied(session, version):
        return
    run_statements(session, statements)
    record_migration(session, version)


def main() -> None:
    uri = os.getenv("NEO4J_URI", DEFAULT_URI)
    user = os.getenv("NEO4J_USER", DEFAULT_USER)
    password = os.getenv("NEO4J_PASSWORD", DEFAULT_PASSWORD)
    schema_file = Path(os.getenv("SCHEMA_CYPHER", DEFAULT_SCHEMA_FILE))
    migrations_dir = Path(os.getenv("MIGRATIONS_DIR", DEFAULT_MIGRATIONS_DIR))

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        wait_for_neo4j(driver)
        with driver.session() as session:
            if schema_file.exists():
                apply_migration(session, "schema", load_cypher_file(schema_file))

            if migrations_dir.exists():
                for path in sorted(migrations_dir.glob("*.cypher")):
                    apply_migration(
                        session,
                        path.stem,
                        load_cypher_file(path),
                    )
            seed_mosaic_genesis(session)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
