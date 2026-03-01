from __future__ import annotations

from .config import Config
from .committer import Committer
from .diff_validator import DiffValidator
from .extractor import Extractor
from .llm.langchain_adapter import LangChainAdapter
from .llm.llm_adapter import LiteLLMAdapter
from .llm.stub_adapter import StubAdapter
from .markdown_parser import parse_markdown
from .ontology_retriever import OntologyRetriever
from .resolver import Resolver
from .schemas import EXTRACTION_SCHEMA
from .errors import ExtractionAdapterError


class RealityIngestor:
    def __init__(self, config: Config):
        self.config = config
        self.driver_factory = config.driver
        self.config.ensure_artifact_dir()
        self.ontology_retriever = OntologyRetriever(self.driver_factory)
        self.adapter = self._build_adapter()
        self.extractor = Extractor(self.adapter)
        self.diff_validator = DiffValidator(self.config)
        self.committer = Committer(self.driver_factory, self.config)

    @classmethod
    def from_env(cls) -> "RealityIngestor":
        return cls(Config.from_env())

    def parse_markdown(self, markdown_path: str):
        return parse_markdown(markdown_path)

    def build_ontology_context(self, parsed):
        return self.ontology_retriever.build_context(parsed, self.config)

    def extract_graph_json(self, parsed, ontology):
        return self.extractor.extract_graph_json(parsed, ontology, self.config)

    def resolve_entities(self, payload, ontology):
        resolver = Resolver(ontology, self.config)
        return resolver.resolve_entities(payload.data if hasattr(payload, "data") else payload)

    def commit_to_graph(self, parsed, payload, plan):
        return self.committer.commit_to_graph(parsed, payload.data if hasattr(payload, "data") else payload, plan)

    def _build_adapter(self):
        adapter_choice = self.config.adapter.lower()
        if adapter_choice == "stub":
            return StubAdapter()
        if adapter_choice == "litellm":
            return LiteLLMAdapter(base_url=self.config.llm_base_url, api_key=self.config.llm_api_key)
        if adapter_choice == "langchain":
            return LangChainAdapter()
        raise ExtractionAdapterError(
            f"Unsupported REALITY_ADAPTER='{self.config.adapter}'. "
            "Use one of: litellm, langchain, stub."
        )
