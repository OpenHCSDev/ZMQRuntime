"""Endpoint startup lifecycle contracts."""

from __future__ import annotations

from functools import partialmethod

from zmqruntime.client import EndpointProcess, ZMQClient
from zmqruntime.messages import PongResponse, ProcessExit, ServerRole
from zmqruntime.startup import (
    EndpointStartupObserver,
    EndpointStartupPhase,
    EndpointStartupPresentationTarget,
    EndpointStartupStatusMonitor,
    EndpointStartupStatusReader,
    EndpointStartupStatusWriter,
)
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

    def stop(self, timeout: float = 5.0) -> None:
        return None


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
        get_default_transport_mode(),
        "endpoint_in_use",
        lambda _port, _host, _config: False,
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
