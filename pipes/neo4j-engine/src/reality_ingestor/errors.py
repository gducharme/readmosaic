from __future__ import annotations


class RealityError(Exception):
    """Base error for Reality Ingestor."""


class MarkdownParseError(RealityError):
    pass


class OntologyBuildError(RealityError):
    pass


class ExtractionSchemaError(RealityError):
    pass


class ResolutionError(RealityError):
    pass


class ResolutionConflictError(RealityError):
    pass


class GraphCommitError(RealityError):
    pass


class CommitRejected(RealityError):
    pass
