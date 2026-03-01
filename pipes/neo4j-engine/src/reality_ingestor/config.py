from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    from neo4j import GraphDatabase  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - fallback when neo4j driver is unavailable
    GraphDatabase = None


@dataclass
class Config:
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    model: str
    embed_model: str
    adapter: str
    llm_base_url: str
    llm_api_key: str
    artifact_dir: Path
    conflict_threshold: float
    diff_decision: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    _driver: Optional[object] = field(default=None, init=False, repr=False)

    @classmethod
    def from_env(cls) -> "Config":
        artifact_dir = Path(os.getenv("REALITY_ARTIFACT_DIR", "pipes/neo4j-engine/artifacts"))
        adapter = os.getenv("REALITY_ADAPTER", "litellm")
        default_model = "stub" if adapter.lower() == "stub" else "lfm2-24b-a2b"
        return cls(
            neo4j_uri=os.getenv("REALITY_NEO4J_URI", "bolt://localhost:7687"),
            neo4j_user=os.getenv("REALITY_NEO4J_USER", "neo4j"),
            neo4j_password=os.getenv("REALITY_NEO4J_PASSWORD", "mosaic_founding"),
            model=os.getenv("REALITY_MODEL", default_model),
            embed_model=os.getenv("REALITY_EMBED_MODEL", "text-embedding-3-large"),
            adapter=adapter,
            llm_base_url=os.getenv("REALITY_LLM_BASE_URL", "http://127.0.0.1:1234/v1"),
            llm_api_key=os.getenv("REALITY_LLM_API_KEY", "lm-studio"),
            artifact_dir=artifact_dir,
            conflict_threshold=float(os.getenv("REALITY_CONFLICT_THRESHOLD", "0.88")),
            diff_decision=os.getenv("REALITY_DIFF_DECISION", "prompt"),
        )

    def ensure_artifact_dir(self) -> Path:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        return self.artifact_dir

    def driver(self):
        if GraphDatabase is None:
            raise ModuleNotFoundError(
                "Neo4j driver is not installed. Install the 'neo4j' package in this environment."
            )
        if self._driver is None:
            self._driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None
