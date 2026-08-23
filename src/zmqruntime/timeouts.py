"""Monotonic operation deadlines shared by ZMQ lifecycle owners."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


class OperationTimeoutError(TimeoutError):
    """Raised before an operation can enter its next side-effecting phase."""


class OperationCancellation:
    """One explicit, thread-safe cancellation authority for an operation."""

    __slots__ = ("_requested",)

    def __init__(self) -> None:
        self._requested = threading.Event()

    def cancel(self) -> None:
        self._requested.set()

    def requested(self) -> bool:
        return self._requested.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until cancellation or ``timeout`` and report the owned state."""

        return self._requested.wait(timeout)


@dataclass(frozen=True, slots=True)
class OperationDeadline:
    """One monotonic timeout budget spanning a multi-phase operation."""

    operation: str
    timeout_ms: int
    expires_at: float

    @classmethod
    def after_milliseconds(
        cls,
        timeout_ms: int,
        *,
        operation: str,
    ) -> OperationDeadline:
        """Start one positive millisecond budget for ``operation``."""

        if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int):
            raise TypeError("Operation timeout must be an integer number of milliseconds.")
        if timeout_ms <= 0:
            raise ValueError("Operation timeout must be positive.")
        return cls(
            operation=operation,
            timeout_ms=timeout_ms,
            expires_at=time.monotonic() + timeout_ms / 1000.0,
        )

    def expired(self) -> bool:
        """Return whether this operation has exhausted its total budget."""

        return time.monotonic() >= self.expires_at

    def remaining_seconds(self) -> float:
        """Return positive remaining seconds or raise the owned timeout."""

        remaining = self.remaining_seconds_or_zero()
        if remaining <= 0:
            raise self.timeout_error()
        return remaining

    def remaining_seconds_or_zero(self) -> float:
        """Return the non-negative budget without changing control flow."""

        return max(0.0, self.expires_at - time.monotonic())

    def remaining_milliseconds(self) -> int:
        """Return positive remaining milliseconds for a downstream boundary."""

        return max(1, int(self.remaining_seconds() * 1000))

    def cap_seconds(self, timeout: float) -> float:
        """Limit an inactivity timeout by this operation's remaining budget."""

        return min(timeout, self.remaining_seconds())

    def timeout_error(self) -> OperationTimeoutError:
        """Build the canonical exception for this deadline."""

        return OperationTimeoutError(
            f"Timed out waiting for {self.operation} after {self.timeout_ms}ms."
        )
