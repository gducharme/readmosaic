from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from reality_ingestor.config import Config

try:
    from neo4j import Neo4jError  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    Neo4jError = Exception


Q_NODE_COUNTS = """
MATCH (n)
UNWIND labels(n) AS label
RETURN label, count(*) AS count
ORDER BY count DESC, label ASC
"""

Q_REL_COUNTS = """
MATCH ()-[r]->()
RETURN type(r) AS rel_type, count(*) AS count
ORDER BY count DESC, rel_type ASC
"""

Q_ENTITIES = """
MATCH (e:Entity)
WHERE $name_filter = '' OR toLower(e.name) CONTAINS toLower($name_filter)
RETURN
  e.uuid AS uuid,
  e.name AS name,
  labels(e) AS labels,
  e.aliases AS aliases,
  CASE
    WHEN 'baseline_state' IN keys(e) THEN e.baseline_state
    ELSE NULL
  END AS baseline_state
ORDER BY e.name ASC
LIMIT $limit
"""


def _print_kv_table(title: str, rows: list[dict[str, Any]], key: str, value: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    if not rows:
        print("(none)")
        return
    max_key = max(len(str(r.get(key, ""))) for r in rows)
    for row in rows:
        lhs = str(row.get(key, ""))
        rhs = str(row.get(value, ""))
        print(f"{lhs.ljust(max_key)}  {rhs}")


def _print_entities(rows: list[dict[str, Any]]) -> None:
    print("\nEntities")
    print("--------")
    if not rows:
        print("(none)")
        return
    for row in rows:
        labels = [label for label in row.get("labels", []) if label != "Entity"]
        label_str = ",".join(labels) if labels else "Entity"
        aliases = row.get("aliases") or []
        aliases_str = ", ".join(aliases) if aliases else "-"
        baseline = row.get("baseline_state")
        baseline_str = baseline if baseline else "-"
        print(f"- {row.get('name', '')} [{label_str}]")
        print(f"  uuid: {row.get('uuid', '')}")
        print(f"  aliases: {aliases_str}")
        print(f"  baseline_state: {baseline_str}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect Neo4j Reality graph contents.")
    parser.add_argument("--limit", type=int, default=25, help="Max number of entities to show (default: 25).")
    parser.add_argument("--name-filter", default="", help="Case-insensitive substring filter for entity name.")
    args = parser.parse_args()

    cfg = Config.from_env()
    try:
        with cfg.driver().session(database="neo4j") as session:
            node_counts = session.run(Q_NODE_COUNTS).data()
            rel_counts = session.run(Q_REL_COUNTS).data()
            entities = session.run(Q_ENTITIES, limit=args.limit, name_filter=args.name_filter).data()
    except Neo4jError as exc:
        print(f"Failed to query Neo4j: {exc}", file=sys.stderr)
        return 1
    finally:
        cfg.close()

    print(f"Connected to: {cfg.neo4j_uri}")
    _print_kv_table("Node Counts by Label", node_counts, "label", "count")
    _print_kv_table("Relationship Counts by Type", rel_counts, "rel_type", "count")
    _print_entities(entities)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
