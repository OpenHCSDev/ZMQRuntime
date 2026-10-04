"""Background subscriber for execution progress stream."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

import zmq

from zmqruntime.execution.responses import (
    WireResponse,
    WireValue,  # noqa: F401 -- resolves WireResponse's recursive annotation in this module
)
from zmqruntime.messages import MessageFields, validate_progress_payload

logger = logging.getLogger(__name__)


def _freeze_wire_value(value):
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_wire_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_wire_value(item) for item in value)
    return value


def _thaw_wire_value(value):
    if isinstance(value, Mapping):
        return {str(key): _thaw_wire_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_wire_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class ExecutionProgressObservation:
    """Immutable latest progress event and its per-execution sequence."""

    sequence: int
    event: WireResponse
    delivery_error: str | None = None

    def __post_init__(self) -> None:
        if self.sequence < 1:
            raise ValueError("Execution progress sequence must be positive")
        object.__setattr__(
            self,
            "event",
            _freeze_wire_value(self.event),
        )

    def as_wire(self) -> dict:
        """Return a detached JSON-compatible projection."""

        return {
            type(self).sequence.__name__: self.sequence,
            type(self).event.__name__: _thaw_wire_value(self.event),
            type(self).delivery_error.__name__: self.delivery_error,
        }

    @classmethod
    def from_wire(cls, payload: WireResponse) -> ExecutionProgressObservation:
        """Parse the projection emitted by :meth:`as_wire`."""

        sequence = payload[cls.sequence.__name__]
        event = payload[cls.event.__name__]
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise TypeError("Execution progress sequence must be an integer")
        if not isinstance(event, Mapping):
            raise TypeError("Execution progress event must be a mapping")
        return cls(
            sequence=sequence, event=event, delivery_error=payload.get(cls.delivery_error.__name__)
        )


class ProgressStreamSubscriber:
    """Owns progress listener thread lifecycle and callback dispatch."""

    def __init__(
        self,
        socket_provider: Callable[[], zmq.Socket | None],
        callback: Callable[[dict], None],
    ) -> None:
        self._socket_provider = socket_provider
        self._callback = callback
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lifecycle_lock = threading.RLock()
        self._delivery_condition = threading.Condition()
        self._dispatching_execution_id: str | None = None

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        with self._lifecycle_lock:
            thread = self._thread
            if thread is None:
                return
            self._stop_event.set()
        if thread is threading.current_thread():
            return
        if thread.is_alive():
            thread.join(timeout=timeout)
        with self._lifecycle_lock:
            if thread.is_alive():
                raise TimeoutError(f"Progress listener did not stop within {timeout:.3f} seconds")
            if self._thread is thread:
                self._thread = None

    def wait_for_delivery(
        self,
        execution_id: str,
        delivered: Callable[[], bool],
        timeout: float,
    ) -> None:
        """Wait for this execution's callback dispatch, not merely its socket read."""
        with self._delivery_condition:
            complete = self._delivery_condition.wait_for(
                lambda: self._dispatching_execution_id != execution_id and delivered(),
                timeout=timeout,
            )
        if not complete:
            raise TimeoutError(
                f"Progress delivery for {execution_id} did not complete within {timeout:.3f}s"
            )

    def _listen_loop(self) -> None:
        logger.info("Progress listener loop started")
        message_count = 0
        try:
            while not self._stop_event.is_set():
                socket = self._socket_provider()
                if socket is None:
                    self._stop_event.wait(0.1)
                    continue
                if not socket.poll(timeout=50, flags=zmq.POLLIN):
                    continue
                try:
                    message = socket.recv_string(zmq.NOBLOCK)
                except zmq.Again:
                    continue

                if self._dispatch_message(message):
                    message_count += 1
        finally:
            with self._lifecycle_lock:
                if self._thread is threading.current_thread():
                    self._thread = None
            logger.info(
                "Progress listener loop exited (received %s messages total)",
                message_count,
            )

    def _dispatch_message(self, message: str) -> bool:
        """Validate and dispatch one message without terminating the stream."""

        try:
            data = json.loads(message)
            validate_progress_payload(data)
            with self._delivery_condition:
                self._dispatching_execution_id = data.get(MessageFields.EXECUTION_ID)
            try:
                self._callback(data)
            finally:
                with self._delivery_condition:
                    self._dispatching_execution_id = None
                    self._delivery_condition.notify_all()
        except Exception as error:
            logger.exception("Progress message dispatch failed: %s", error)
            return False
        return True
