from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from neo4j import GraphDatabase


@dataclass
class Config:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    model: str
    embed_model: str
    adapter: str
    artifact_dir: Path
    conflict_threshold: float
    diff_decision: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    _driver: Optional[object] = field(default=None, init=False, repr=False)

    @classmethod
    def from_env(cls) -> "Config":
        artifact_dir = Path(os.getenv("REALITY_ARTIFACT_DIR", "pipes/neo4j-engine/artifacts"))
        return cls(
            neo4j_uri=os.getenv("REALITY_NEO4J_URI", "bolt://localhost:7687"),
            neo4j_user=os.getenv("REALITY_NEO4J_USER", "neo4j"),
            neo4j_password=os.getenv("REALITY_NEO4J_PASSWORD", "mosaic_founding"),
            model=os.getenv("REALITY_MODEL", "stub"),
            embed_model=os.getenv("REALITY_EMBED_MODEL", "text-embedding-3-large"),
            adapter=os.getenv("REALITY_ADAPTER", "stub"),
            artifact_dir=artifact_dir,
            conflict_threshold=float(os.getenv("REALITY_CONFLICT_THRESHOLD", "0.88")),
            diff_decision=os.getenv("REALITY_DIFF_DECISION", "prompt"),
        )

    def ensure_artifact_dir(self) -> Path:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        return self.artifact_dir

    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None
