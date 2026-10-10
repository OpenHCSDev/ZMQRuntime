"""Generic progress registry + projection primitives."""

from .projection import (
    GenericAxisProjection,
    GenericExecutionProjection,
    GenericSubjectProjection,
    ProgressProjectionAdapterABC,
    build_execution_projection,
)
from .registry import (
    EventRegistryABC,
    EventRegistryMutation,
    EventRegistryMutationKind,
    LatestEventRegistry,
)

__all__ = [
    "EventRegistryABC",
    "EventRegistryMutation",
    "EventRegistryMutationKind",
    "LatestEventRegistry",
    "GenericAxisProjection",
    "GenericSubjectProjection",
    "GenericExecutionProjection",
    "ProgressProjectionAdapterABC",
    "build_execution_projection",
]
