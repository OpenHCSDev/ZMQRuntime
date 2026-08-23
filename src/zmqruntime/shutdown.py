"""Batch endpoint shutdown owned by the generic ZMQ runtime."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from zmqruntime.client import (
    EndpointShutdownMode,
    EndpointShutdownResult,
    ZMQClient,
)
from zmqruntime.config import ZMQConfig
from zmqruntime.queue_tracker import GlobalQueueTrackerRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EndpointShutdownOutcome:
    """Authoritative outcome for one requested endpoint shutdown."""

    port: int
    result: EndpointShutdownResult | None = None
    error: Exception | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None and self.result is not None and self.result.succeeded

    @property
    def endpoint_terminated(self) -> bool:
        return self.succeeded and self.result is not None and self.result.endpoint_terminated


@dataclass(frozen=True, slots=True)
class EndpointShutdownBatchResult:
    """Complete typed outcomes for one requested shutdown batch."""

    outcomes: tuple[EndpointShutdownOutcome, ...] = ()

    @property
    def succeeded(self) -> bool:
        return all(outcome.succeeded for outcome in self.outcomes)

    @property
    def succeeded_ports(self) -> tuple[int, ...]:
        return tuple(outcome.port for outcome in self.outcomes if outcome.succeeded)

    @property
    def terminated_ports(self) -> tuple[int, ...]:
        return tuple(
            outcome.port for outcome in self.outcomes if outcome.endpoint_terminated
        )

    @property
    def failed_ports(self) -> tuple[int, ...]:
        return tuple(outcome.port for outcome in self.outcomes if not outcome.succeeded)

    @property
    def failure_message(self) -> str:
        return f"Failed to shut down endpoints on ports: {list(self.failed_ports)}"

    @property
    def message(self) -> str:
        """Return the generic user-facing outcome of this exact batch."""

        if self.succeeded:
            return "All endpoints shut down successfully"
        return self.failure_message


class EndpointShutdownService:
    """Execute endpoint shutdowns and retire their generic progress trackers."""

    @classmethod
    def for_config(cls, config: ZMQConfig) -> EndpointShutdownService:
        tracker_registry = GlobalQueueTrackerRegistry()
        return cls(
            shutdown_endpoint=partial(
                ZMQClient.shutdown_endpoint_on_port,
                config=config,
            ),
            retire_progress_tracker=tracker_registry.remove_tracker,
        )

    def __init__(
        self,
        *,
        shutdown_endpoint: Callable[
            [int, EndpointShutdownMode],
            EndpointShutdownResult,
        ],
        retire_progress_tracker: Callable[[int], None],
    ) -> None:
        self._shutdown_endpoint = shutdown_endpoint
        self._retire_progress_tracker = retire_progress_tracker

    def shutdown_ports(
        self,
        *,
        ports: list[int],
        mode: EndpointShutdownMode,
    ) -> EndpointShutdownBatchResult:
        """Execute one shutdown policy for every requested endpoint."""

        outcomes: list[EndpointShutdownOutcome] = []
        for port in ports:
            try:
                logger.info(
                    "Shutting down endpoint on port %s (mode=%s)",
                    port,
                    mode.value,
                )
                result = self._shutdown_endpoint(port, mode)
            except Exception as error:
                logger.exception("Error shutting down endpoint on port %s: %s", port, error)
                outcomes.append(EndpointShutdownOutcome(port=port, error=error))
                continue
            outcomes.append(EndpointShutdownOutcome(port=port, result=result))
            if not result.succeeded:
                logger.warning(
                    "Endpoint shutdown failed on port %s (mode=%s)",
                    port,
                    mode.value,
                )
                continue
            self._retire_progress_tracker(port)
        return EndpointShutdownBatchResult(tuple(outcomes))
