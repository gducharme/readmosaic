from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Iterable, Sequence

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
    finally:
        driver.close()


if __name__ == "__main__":
    main()
