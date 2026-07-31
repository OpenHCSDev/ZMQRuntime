"""Generic progress registry + projection primitives."""

from .projection import (
    GenericAxisProjection,
    GenericExecutionProjection,
    GenericPlateProjection,
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
    "GenericPlateProjection",
    "GenericExecutionProjection",
    "ProgressProjectionAdapterABC",
    "build_execution_projection",
]
