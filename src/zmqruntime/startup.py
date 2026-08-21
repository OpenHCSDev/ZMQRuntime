"""Typed endpoint connection and child-process startup status."""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path


class EndpointStartupPresentationTarget(ABC):
    """Nominal rendering port consumed by endpoint-phase presentation leaves."""

    @abstractmethod
    def present_checking(self, message: str) -> None:
        """Present in-progress endpoint activity."""

    @abstractmethod
    def present_connected(self, message: str) -> None:
        """Present a connected endpoint."""

    @abstractmethod
    def present_disconnected(self, message: str) -> None:
        """Present an unavailable endpoint."""

    @abstractmethod
    def present_warning(self, message: str) -> None:
        """Present endpoint activity that may take noticeable time."""


EndpointStartupPresenter = Callable[
    [EndpointStartupPresentationTarget, str],
    None,
]


def _present_checking(
    target: EndpointStartupPresentationTarget,
    message: str,
) -> None:
    target.present_checking(message)


def _present_connected(
    target: EndpointStartupPresentationTarget,
    message: str,
) -> None:
    target.present_connected(message)


def _present_disconnected(
    target: EndpointStartupPresentationTarget,
    message: str,
) -> None:
    target.present_disconnected(message)


def _present_warning(
    target: EndpointStartupPresentationTarget,
    message: str,
) -> None:
    target.present_warning(message)


class EndpointStartupPhase(str, Enum):
    """Closed lifecycle vocabulary for a client-managed endpoint."""

    def __new__(
        cls,
        value: str,
        presenter: EndpointStartupPresenter,
        expects_endpoint_presence: bool = True,
        startup_failure: bool = False,
    ) -> EndpointStartupPhase:
        member = str.__new__(cls, value)
        member._value_ = value
        member._presenter = presenter
        member._expects_endpoint_presence = expects_endpoint_presence
        member._startup_failure = startup_failure
        return member

    DISCONNECTED = (
        "disconnected",
        _present_disconnected,
        False,
    )
    CHECKING_ENDPOINT = (
        "checking_endpoint",
        _present_checking,
    )
    STARTING_PROCESS = (
        "starting_process",
        _present_checking,
    )
    LOADING_CONFIG = (
        "loading_config",
        _present_checking,
    )
    IMPORTING_RUNTIME = (
        "importing_runtime",
        _present_checking,
    )
    CREATING_SERVER = (
        "creating_server",
        _present_checking,
    )
    BINDING_ENDPOINT = (
        "binding_endpoint",
        _present_checking,
    )
    SERVER_READY = (
        "server_ready",
        _present_checking,
    )
    CONNECTED = (
        "connected",
        _present_connected,
    )
    PREPARING_CAPABILITIES = (
        "preparing_capabilities",
        _present_warning,
    )
    FAILED = (
        "failed",
        _present_disconnected,
        False,
        True,
    )

    @property
    def expects_endpoint_presence(self) -> bool:
        """Whether this phase proves an endpoint attempt should remain observable."""

        return self._expects_endpoint_presence

    @property
    def startup_failed(self) -> bool:
        """Whether this phase terminates an in-progress startup attempt."""

        return self._startup_failure

    def present(
        self,
        target: EndpointStartupPresentationTarget,
        message: str,
    ) -> None:
        """Execute this phase member's presentation leaf."""

        self._presenter(target, message)


@dataclass(frozen=True, slots=True)
class EndpointStartupStatus:
    """One monotonic lifecycle update suitable for logs, IPC, and UI views."""

    phase: EndpointStartupPhase
    message: str
    sequence: int = 0
    timestamp: float = 0.0

    def present(
        self,
        target: EndpointStartupPresentationTarget,
        endpoint_name: str,
    ) -> None:
        """Present through the owning phase without consumer-side dispatch."""

        self.phase.present(target, f"{endpoint_name}: {self.message}")

    def to_json(self) -> str:
        payload = asdict(self)
        payload["phase"] = self.phase.value
        return json.dumps(payload, separators=(",", ":"))

    @classmethod
    def from_json(cls, source: str) -> EndpointStartupStatus:
        payload = json.loads(source)
        return cls(
            phase=EndpointStartupPhase(payload["phase"]),
            message=str(payload["message"]),
            sequence=int(payload["sequence"]),
            timestamp=float(payload["timestamp"]),
        )


EndpointStartupStatusCallback = Callable[[EndpointStartupStatus], None]
EndpointStartupPhaseCallback = Callable[[EndpointStartupPhase, str], None]

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EndpointStartupStatusRead:
    """New status events and the offset for the next channel read."""

    statuses: tuple[EndpointStartupStatus, ...]
    next_offset: int


class EndpointStartupStatusWriter:
    """Append child startup events to a parent-owned JSONL channel."""

    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._sequence = 0

    def emit(
        self,
        phase: EndpointStartupPhase,
        message: str,
    ) -> EndpointStartupStatus:
        self._sequence += 1
        status = EndpointStartupStatus(
            phase=phase,
            message=message,
            sequence=self._sequence,
            timestamp=time.time(),
        )
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as stream:
                stream.write(status.to_json() + "\n")
                stream.flush()
        return status


class EndpointStartupStatusReader:
    """Own incremental reads from one child startup status journal."""

    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._offset = 0

    def read(self) -> EndpointStartupStatusRead:
        """Read complete newly appended events and advance the owned offset."""

        if self._path is None or not self._path.exists():
            return EndpointStartupStatusRead((), self._offset)
        with self._path.open("r", encoding="utf-8") as stream:
            stream.seek(self._offset)
            lines = stream.readlines()
            self._offset = stream.tell()
        return EndpointStartupStatusRead(
            statuses=tuple(EndpointStartupStatus.from_json(line) for line in lines if line.strip()),
            next_offset=self._offset,
        )


class EndpointStartupObserver(ABC):
    """Nominal readiness-loop view of endpoint startup activity."""

    @abstractmethod
    def poll_activity(self) -> bool:
        """Publish newly observed activity and report whether any occurred."""

    @abstractmethod
    def should_abort(self) -> bool:
        """Return whether readiness can no longer be reached."""


class IdleEndpointStartupObserver(EndpointStartupObserver):
    """Observer for endpoints without a child startup journal."""

    def poll_activity(self) -> bool:
        return False

    def should_abort(self) -> bool:
        return False


IDLE_ENDPOINT_STARTUP_OBSERVER = IdleEndpointStartupObserver()


class EndpointStartupStatusMonitor(EndpointStartupObserver):
    """Relay child journal events and terminal state into client readiness."""

    def __init__(
        self,
        path: Path | None,
        *,
        status_emitter: EndpointStartupPhaseCallback,
        process_has_exited: Callable[[], bool],
    ) -> None:
        self._reader = EndpointStartupStatusReader(path)
        self._status_emitter = status_emitter
        self._process_has_exited = process_has_exited
        self._startup_failed = False

    def poll_activity(self) -> bool:
        status_read = self._reader.read()
        for status in status_read.statuses:
            logger.info("Endpoint startup: %s", status.message)
            self._status_emitter(status.phase, status.message)
        self._startup_failed = self._startup_failed or any(
            status.phase.startup_failed for status in status_read.statuses
        )
        return bool(status_read.statuses)

    def should_abort(self) -> bool:
        return self._startup_failed or self._process_has_exited()
