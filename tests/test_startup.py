"""Endpoint startup lifecycle contracts."""

from __future__ import annotations

import threading
from functools import partialmethod

import pytest

from zmqruntime.client import (
    EndpointConnectionCancelledError,
    EndpointConnectionPolicy,
    EndpointProcess,
    ZMQClient,
)
from zmqruntime.messages import PongResponse, ProcessExit, ServerRole
from zmqruntime.startup import (
    EndpointStartupCancellationObserver,
    EndpointStartupObserver,
    EndpointStartupPhase,
    EndpointStartupPresentationTarget,
    EndpointStartupStatusMonitor,
    EndpointStartupStatusReader,
    EndpointStartupStatusWriter,
)
from zmqruntime.timeouts import OperationCancellation
from zmqruntime.transport import get_default_transport_mode, wait_for_server_ready


class _PresentationTarget(EndpointStartupPresentationTarget):
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def _present(self, state: str, message: str) -> None:
        self.events.append((state, message))

    present_checking = partialmethod(_present, "checking")
    present_connected = partialmethod(_present, "connected")
    present_disconnected = partialmethod(_present, "disconnected")
    present_warning = partialmethod(_present, "warning")


class _EndpointProcess(EndpointProcess):
    def is_alive(self) -> bool:
        return True

    def exit(self) -> ProcessExit | None:
        return None

    def wait_for_exit(self, timeout: float) -> ProcessExit | None:
        del timeout
        return None

    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        return False


class _StartupClient(ZMQClient):
    def __init__(self, statuses, *, spawn_error: Exception | None = None) -> None:
        super().__init__(5555, connection_status_callback=statuses.append)
        self._spawn_error = spawn_error

    def _is_port_in_use(self, port: int) -> bool:
        return False

    def _spawn_server_process(self):
        if self._spawn_error is not None:
            raise self._spawn_error
        return _EndpointProcess()

    def _wait_for_endpoint_ready(
        self,
        process,
        timeout: float = 10.0,
    ) -> PongResponse:
        return PongResponse(
            port=self.port,
            control_port=self.control_port,
            ready=True,
            server=type(self).__name__,
            server_role=ServerRole.GENERIC,
        )

    def _setup_client_sockets(self) -> None:
        return None

    def send_data(self, data) -> None:
        return None


class _LegacyStartupClient(ZMQClient):
    """Client exercising the readiness hook published before typed PONGs."""

    def __init__(self, statuses) -> None:
        super().__init__(5555, connection_status_callback=statuses.append)
        self.readiness_observed = False

    def _is_port_in_use(self, port: int) -> bool:
        return False

    def _spawn_server_process(self):
        return _EndpointProcess()

    def _wait_for_server_ready(
        self,
        process,
        timeout: float = 10.0,
    ) -> bool:
        self.readiness_observed = True
        return True

    def _try_connect_to_existing(
        self,
        port: int,
        timeout_ms: int = 500,
    ) -> PongResponse:
        return PongResponse(
            port=self.port,
            control_port=self.control_port,
            ready=True,
            server=type(self).__name__,
            server_role=ServerRole.GENERIC,
        )

    def _setup_client_sockets(self) -> None:
        return None

    def send_data(self, data) -> None:
        return None


class _ActivityObserver(EndpointStartupObserver):
    def __init__(self, activity: list[bool]) -> None:
        self._activity = iter(activity)

    def poll_activity(self) -> bool:
        return next(self._activity, False)

    def should_abort(self) -> bool:
        return False


def test_startup_status_channel_roundtrips_incremental_typed_events(tmp_path) -> None:
    path = tmp_path / "startup.jsonl"
    writer = EndpointStartupStatusWriter(path)
    writer.emit(EndpointStartupPhase.IMPORTING_RUNTIME, "Importing runtime")

    reader = EndpointStartupStatusReader(path)
    first = reader.read()
    assert [status.phase for status in first.statuses] == [EndpointStartupPhase.IMPORTING_RUNTIME]

    writer.emit(EndpointStartupPhase.SERVER_READY, "Server ready")
    second = reader.read()
    assert [status.phase for status in second.statuses] == [EndpointStartupPhase.SERVER_READY]
    assert second.next_offset > first.next_offset


def test_startup_monitor_owns_relay_failure_and_process_exit_state(tmp_path) -> None:
    path = tmp_path / "startup.jsonl"
    writer = EndpointStartupStatusWriter(path)
    writer.emit(EndpointStartupPhase.IMPORTING_RUNTIME, "Importing runtime")
    relayed = []
    process_exited = [False]
    monitor = EndpointStartupStatusMonitor(
        path,
        status_emitter=lambda phase, message: relayed.append((phase, message)),
        process_has_exited=lambda: process_exited[0],
    )

    assert monitor.poll_activity() is True
    assert relayed == [(EndpointStartupPhase.IMPORTING_RUNTIME, "Importing runtime")]
    assert monitor.should_abort() is False
    process_exited[0] = True
    assert monitor.should_abort() is True
    process_exited[0] = False

    writer.emit(EndpointStartupPhase.FAILED, "Import failed")
    assert monitor.poll_activity() is True
    assert monitor.should_abort() is True


def test_cancellation_observer_composes_with_startup_monitor(tmp_path) -> None:
    cancellation = OperationCancellation()
    path = tmp_path / "startup.jsonl"
    EndpointStartupStatusWriter(path).emit(
        EndpointStartupPhase.IMPORTING_RUNTIME,
        "Importing runtime",
    )
    relayed = []
    monitor = EndpointStartupCancellationObserver(
        cancellation,
        EndpointStartupStatusMonitor(
            path,
            status_emitter=lambda phase, message: relayed.append((phase, message)),
            process_has_exited=lambda: False,
        ),
    )

    assert monitor.poll_activity() is True
    assert relayed == [(EndpointStartupPhase.IMPORTING_RUNTIME, "Importing runtime")]
    assert monitor.should_abort() is False

    cancellation.cancel()

    assert monitor.should_abort() is True


def test_cancelled_client_does_not_spawn_an_endpoint() -> None:
    statuses = []
    client = _StartupClient(statuses)
    attempt = client.new_connection_attempt()
    attempt.cancel()

    with pytest.raises(EndpointConnectionCancelledError):
        attempt.connect(EndpointConnectionPolicy.ATTACH_OR_START, 1)
    assert [status.phase for status in statuses] == [
        EndpointStartupPhase.CHECKING_ENDPOINT,
        EndpointStartupPhase.DISCONNECTED,
    ]

    assert client.connect(timeout=1) is True
    assert client.connected_endpoint is not None


def test_connection_attempt_can_share_its_callers_cancellation_authority() -> None:
    statuses = []
    client = _StartupClient(statuses)
    cancellation = OperationCancellation()
    attempt = client.new_connection_attempt(cancellation=cancellation)

    cancellation.cancel()

    with pytest.raises(EndpointConnectionCancelledError):
        attempt.connect(EndpointConnectionPolicy.ATTACH_OR_START, 1)
    assert [status.phase for status in statuses] == [
        EndpointStartupPhase.CHECKING_ENDPOINT,
        EndpointStartupPhase.DISCONNECTED,
    ]
    assert client.connected_endpoint is None


def test_concurrent_attempts_use_their_exact_cancellation_tokens() -> None:
    statuses = []
    client = _StartupClient(statuses)
    first_started = threading.Event()
    release_first = threading.Event()
    original_connect_locked = client._connect_locked

    def delayed_connect_locked(timeout, *, operation_deadline=None):
        first_started.set()
        release_first.wait(timeout=1.0)
        return original_connect_locked(
            timeout,
            operation_deadline=operation_deadline,
        )

    client._connect_locked = delayed_connect_locked
    results: list[bool | type[BaseException]] = []
    first_attempt = client.new_connection_attempt()
    second_attempt = client.new_connection_attempt()

    def connect(attempt) -> None:
        try:
            results.append(attempt.connect(EndpointConnectionPolicy.ATTACH_OR_START, 1))
        except EndpointConnectionCancelledError:
            results.append(EndpointConnectionCancelledError)

    first = threading.Thread(
        target=connect,
        args=(first_attempt,),
    )
    second = threading.Thread(
        target=connect,
        args=(second_attempt,),
    )

    first.start()
    assert first_started.wait(timeout=1.0)
    second.start()
    first_attempt.cancel()
    release_first.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert len(results) == 2
    assert EndpointConnectionCancelledError in results
    assert True in results


def test_typed_handshake_preserves_legacy_readiness_extension_point() -> None:
    client = _LegacyStartupClient([])

    assert client.connect(timeout=1.0) is True
    assert client.readiness_observed is True
    assert client.connected_endpoint is not None
    assert client.connected_endpoint.server == "_LegacyStartupClient"


def test_each_startup_phase_executes_its_owned_presentation_leaf() -> None:
    expected = {
        EndpointStartupPhase.DISCONNECTED: ("disconnected", "ZMQ message"),
        EndpointStartupPhase.CHECKING_ENDPOINT: ("checking", "ZMQ message"),
        EndpointStartupPhase.STARTING_PROCESS: ("checking", "ZMQ message"),
        EndpointStartupPhase.LOADING_CONFIG: ("checking", "ZMQ message"),
        EndpointStartupPhase.IMPORTING_RUNTIME: ("checking", "ZMQ message"),
        EndpointStartupPhase.CREATING_SERVER: ("checking", "ZMQ message"),
        EndpointStartupPhase.BINDING_ENDPOINT: ("checking", "ZMQ message"),
        EndpointStartupPhase.SERVER_READY: ("checking", "ZMQ message"),
        EndpointStartupPhase.CONNECTED: ("connected", "ZMQ message"),
        EndpointStartupPhase.PREPARING_CAPABILITIES: (
            "warning",
            "ZMQ message",
        ),
        EndpointStartupPhase.FAILED: ("disconnected", "ZMQ message"),
    }

    for phase in EndpointStartupPhase:
        target = _PresentationTarget()
        phase.present(target, "ZMQ message")
        assert target.events == [expected[phase]]


def test_startup_failure_semantics_are_owned_by_phase_members() -> None:
    assert EndpointStartupPhase.FAILED.startup_failed is True
    assert all(
        not phase.startup_failed
        for phase in EndpointStartupPhase
        if phase is not EndpointStartupPhase.FAILED
    )


def test_endpoint_presence_semantics_are_owned_by_phase_members() -> None:
    assert not EndpointStartupPhase.DISCONNECTED.expects_endpoint_presence
    assert not EndpointStartupPhase.FAILED.expects_endpoint_presence
    assert all(
        phase.expects_endpoint_presence
        for phase in EndpointStartupPhase
        if phase
        not in {
            EndpointStartupPhase.DISCONNECTED,
            EndpointStartupPhase.FAILED,
        }
    )


def test_client_publishes_generic_connection_lifecycle() -> None:
    statuses = []
    client = _StartupClient(statuses)

    assert client.connect(timeout=1)

    assert [status.phase for status in statuses] == [
        EndpointStartupPhase.CHECKING_ENDPOINT,
        EndpointStartupPhase.STARTING_PROCESS,
        EndpointStartupPhase.CONNECTED,
    ]


def test_client_publishes_terminal_startup_exception() -> None:
    statuses = []
    client = _StartupClient(statuses, spawn_error=RuntimeError("boom"))

    try:
        client.connect(timeout=1)
    except RuntimeError as error:
        assert str(error) == "boom"
    else:
        raise AssertionError("Startup error was not propagated")

    assert statuses[-1].phase is EndpointStartupPhase.FAILED
    assert "boom" in statuses[-1].message


def test_readiness_timeout_is_an_inactivity_deadline(
    monkeypatch,
) -> None:
    """Real child activity extends startup without increasing the base timeout."""

    import zmqruntime.transport as transport

    now = [0.0]
    monkeypatch.setattr(transport.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        transport.time,
        "sleep",
        lambda duration: now.__setitem__(0, now[0] + duration),
    )
    monkeypatch.setattr(
        get_default_transport_mode().declaration,
        "endpoint_in_use",
        staticmethod(lambda _port, _host, _config: False),
    )

    ready = wait_for_server_ready(
        5555,
        None,
        timeout=0.1,
        poll_interval=0.06,
        startup_observer=_ActivityObserver([False, True, False, False]),
    )

    assert ready is False
    assert now[0] >= 0.16
