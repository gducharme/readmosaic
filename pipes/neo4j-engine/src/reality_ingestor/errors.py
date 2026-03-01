from __future__ import annotations


class RealityError(Exception):
    """Base error for Reality Ingestor."""


class MarkdownParseError(RealityError):
    pass


class OntologyBuildError(RealityError):
    pass


class ExtractionSchemaError(RealityError):
    pass


class ExtractionAdapterError(RealityError):
    pass


class ResolutionError(RealityError):
    pass


class ResolutionConflictError(RealityError):
    def __init__(self, message: str, *, conflicts=None):
        super().__init__(message)
        self.conflicts = conflicts or []


class GraphCommitError(RealityError):
    pass


class CommitRejected(RealityError):
    pass
