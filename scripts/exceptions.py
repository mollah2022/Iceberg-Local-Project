"""
Domain-specific exceptions for the Iceberg local pipeline.
Using custom exceptions instead of generic Exception makes failures
explicit and catchable at the right layer.
"""


class PipelineError(Exception):
    """Base exception for all pipeline-related errors."""


class MappingLoadError(PipelineError):
    """Raised when a reference mapping file cannot be loaded or parsed."""


class RecordTransformError(PipelineError):
    """Raised when a single record cannot be transformed into the target schema."""