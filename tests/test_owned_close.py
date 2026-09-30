"""Source-only lifecycle regressions; no process, native endpoint or JVM starts."""

from dataclasses import replace
from unittest.mock import Mock
import pickle

import psutil
import pytest

from zmqruntime.client import EndpointShutdownMode, ZMQClient
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    EndpointControlCapability,
    EndpointShutdownRequest,
    ControlMessageType,
    MessageFields,
    PongResponse,
    ProcessIdentity,
    ResponseType,
    ServerRole,
)
from zmqruntime.timeouts import OperationDeadline, OperationTimeoutError
from zmqruntime.transport import TransportEndpoint
from zmqruntime.execution.server import ExecutionServer


IDENTITY = ProcessIdentity(41, 100.0)


class Client(ZMQClient):
    def _spawn_server_process(self):
        raise AssertionError("Close must not start a process")

    def send_data(self, data):
        raise AssertionError("Close must not submit source")


class Server(ExecutionServer):
    _server_type = "owned-close-source-fixture"

    def execute_task(self, execution_id, request):
        raise AssertionError("No task execution in source lifecycle tests")


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    client = Client(
        port=5913, host="127.0.0.1", transport_mode=TransportMode.TCP, config=ZMQConfig()
    )
    pong = PongResponse(
        port=5913,
        control_port=client.control_port,
        server="fixture",
        server_role=ServerRole.EXECUTION,
        ready=True,
        process_identity=IDENTITY,
        control_capabilities=frozenset(
            (EndpointControlCapability.SHUTDOWN, EndpointControlCapability.FORCE_SHUTDOWN)
        ),
    )
    ping = Mock(side_effect=[pong, None])
    monkeypatch.setattr(TransportEndpoint, "ping", ping)
    monkeypatch.setattr(TransportEndpoint, "occupied_ports", lambda *_: frozenset())
    cleanup = Mock()
    monkeypatch.setattr(TransportEndpoint, "cleanup_stale_addresses", cleanup)
    monkeypatch.setattr(ProcessIdentity, "is_alive", lambda _self: True)
    terminate = Mock(return_value=False)
    monkeypatch.setattr(ProcessIdentity, "terminate", terminate)
    socket = Mock()
    socket.recv.return_value = pickle.dumps({MessageFields.TYPE: ResponseType.SHUTDOWN_ACK.value})
    context = Mock()
    context.socket.return_value = socket
    monkeypatch.setattr("zmqruntime.client.zmq.Context.instance", lambda: context)
    declaration = client.transport_mode.declaration
    for port in client.endpoint.port_pair(client.config).ports:
        path = declaration.startup_lock_path(port, client.config)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        declaration.record_startup_owner(port, client.config, IDENTITY)
    return client, ping, socket, terminate, cleanup


def close(client, mode=EndpointShutdownMode.FORCE):
    return client.close_owned_process(
        IDENTITY,
        mode=mode,
        operation_deadline=OperationDeadline.after_milliseconds(500, operation="owned close test"),
    )


def test_listener_gone_is_not_process_exit(fixture):
    client, ping, socket, terminate, cleanup = fixture
    result = close(client)
    assert not result.succeeded
    assert result.endpoint_terminated
    assert result.process_exited is False
    assert result.process_identity == IDENTITY
    assert result.acknowledged and result.request_attempted
    socket.send.assert_called_once()
    terminate.assert_called_once()
    cleanup.assert_not_called()


def test_force_completes_only_after_exact_process_exit(fixture, monkeypatch):
    client, ping, socket, terminate, cleanup = fixture
    alive = [True]
    monkeypatch.setattr(ProcessIdentity, "is_alive", lambda _self: alive[0])
    terminate.side_effect = lambda **_kwargs: alive.__setitem__(0, False) or True
    result = close(client)
    assert result.succeeded and result.endpoint_terminated and result.process_exited
    socket.send.assert_called_once()
    assert EndpointShutdownRequest.from_wire_payload(
        socket.send.call_args.args[0]
    ) == EndpointShutdownRequest(ControlMessageType.FORCE_SHUTDOWN, IDENTITY)
    cleanup.assert_called_once()
    assert terminate.call_args.kwargs["timeout"] <= 0.5


def test_absent_listener_reconciles_exact_process_without_rpc_replay(fixture):
    client, ping, socket, terminate, cleanup = fixture
    ping.side_effect = None
    ping.return_value = None
    result = close(client)
    socket.send.assert_not_called()
    assert not result.request_attempted and not result.acknowledged
    assert not result.succeeded and result.process_exited is False
    terminate.assert_called_once()


def test_graceful_ack_clears_workers_without_process_close(fixture):
    client, ping, socket, terminate, cleanup = fixture
    result = close(client, EndpointShutdownMode.GRACEFUL)
    assert result.succeeded and result.acknowledged
    assert not result.endpoint_terminated and result.process_exited is False
    socket.send.assert_called_once()
    assert EndpointShutdownRequest.from_wire_payload(
        socket.send.call_args.args[0]
    ) == EndpointShutdownRequest(ControlMessageType.SHUTDOWN, IDENTITY)
    terminate.assert_not_called()
    cleanup.assert_not_called()


@pytest.mark.parametrize("control", [False, True])
def test_different_native_reservation_rejects_before_control(fixture, control):
    client, ping, socket, terminate, cleanup = fixture
    port = client.control_port if control else client.port
    client.transport_mode.declaration.record_startup_owner(
        port, client.config, replace(IDENTITY, create_time=101.0)
    )
    with pytest.raises(RuntimeError, match="not reserved"):
        close(client)
    ping.assert_not_called()
    socket.send.assert_not_called()
    terminate.assert_not_called()


def test_different_endpoint_incarnation_rejects_before_control(fixture):
    client, ping, socket, terminate, cleanup = fixture
    ping.side_effect = [
        PongResponse(
            port=5913,
            control_port=client.control_port,
            ready=True,
            server="foreign",
            server_role=ServerRole.EXECUTION,
            process_identity=replace(IDENTITY, create_time=101.0),
        )
    ]
    with pytest.raises(RuntimeError, match="before shutdown"):
        close(client)
    socket.send.assert_not_called()
    terminate.assert_not_called()
    cleanup.assert_not_called()


def test_unknown_incarnation_liveness_never_signals_or_dispatches(fixture, monkeypatch):
    client, ping, socket, terminate, cleanup = fixture
    monkeypatch.setattr(ProcessIdentity, "is_alive", lambda _self: None)
    result = close(client)
    assert result.process_exited is None and not result.succeeded
    socket.send.assert_not_called()
    terminate.assert_not_called()
    cleanup.assert_not_called()


def test_missing_unidentified_endpoint_is_not_process_exit_proof(fixture):
    client, ping, socket, terminate, cleanup = fixture
    ping.side_effect = None
    ping.return_value = None
    result = ZMQClient.shutdown_endpoint_on_port(
        client.port,
        EndpointShutdownMode.FORCE,
        config=client.config,
        transport_mode=TransportMode.TCP,
    )
    assert result.endpoint_terminated and result.process_exited is None
    socket.send.assert_not_called()
    terminate.assert_not_called()


def test_expired_close_never_dispatches(fixture):
    client, ping, socket, terminate, cleanup = fixture
    deadline = replace(OperationDeadline.after_milliseconds(500, operation="test"), expires_at=0)
    with pytest.raises(OperationTimeoutError):
        client.close_owned_process(
            IDENTITY, mode=EndpointShutdownMode.FORCE, operation_deadline=deadline
        )
    ping.assert_not_called()
    socket.send.assert_not_called()
    terminate.assert_not_called()


def test_missing_ack_never_resends_shutdown(fixture):
    client, ping, socket, terminate, cleanup = fixture
    socket.recv.side_effect = TimeoutError("unknown delivery")
    result = close(client)
    assert result.request_attempted and not result.acknowledged
    assert not result.succeeded and result.process_exited is False
    socket.send.assert_called_once()


def test_expiry_after_dispatch_retains_handle_without_late_signal(fixture, monkeypatch):
    client, ping, socket, terminate, cleanup = fixture
    clock = [100.0]
    monkeypatch.setattr("zmqruntime.timeouts.time.monotonic", lambda: clock[0])

    def delayed_ack():
        clock[0] += 1.0
        return pickle.dumps({MessageFields.TYPE: ResponseType.SHUTDOWN_ACK.value})

    socket.recv.side_effect = delayed_ack
    result = close(client)
    assert result.process_identity == IDENTITY
    assert result.request_attempted and result.acknowledged
    assert result.process_exited is False and not result.succeeded
    socket.send.assert_called_once()
    terminate.assert_not_called()


def test_changed_endpoint_after_dispatch_preserves_uncertainty_without_takeover(fixture):
    client, ping, socket, terminate, cleanup = fixture
    original = PongResponse(
        port=5913,
        control_port=client.control_port,
        ready=True,
        server="fixture",
        server_role=ServerRole.EXECUTION,
        process_identity=IDENTITY,
        control_capabilities=frozenset((EndpointControlCapability.FORCE_SHUTDOWN,)),
    )
    ping.side_effect = [
        original,
        replace(original, process_identity=replace(IDENTITY, create_time=101.0)),
    ]
    result = close(client)
    assert result.request_attempted and result.acknowledged
    assert not result.succeeded and not result.endpoint_terminated
    assert result.process_identity == IDENTITY
    socket.send.assert_called_once()
    terminate.assert_not_called()
    cleanup.assert_not_called()


def test_foreign_host_never_enters_lifecycle(fixture):
    client, ping, socket, terminate, cleanup = fixture
    client.endpoint = replace(client.endpoint, host="203.0.113.7")
    with pytest.raises(ValueError, match="local"):
        close(client)
    ping.assert_not_called()
    socket.send.assert_not_called()
    terminate.assert_not_called()


def test_pid_reuse_does_not_signal_replacement(monkeypatch):
    process = Mock()
    process.create_time.return_value = 101.0
    monkeypatch.setattr("zmqruntime.messages.psutil.Process", lambda *_: process)
    assert IDENTITY.terminate()
    process.terminate.assert_not_called()
    process.kill.assert_not_called()


@pytest.mark.parametrize("matching", [False, True])
@pytest.mark.parametrize("mode", [EndpointShutdownMode.GRACEFUL, EndpointShutdownMode.FORCE])
def test_native_shutdown_handler_checks_incarnation_before_worker_mutation(
    monkeypatch, matching, mode
):
    server = Server(port=5913, transport_mode=TransportMode.TCP)
    server._cancel_all_executions = Mock()
    server._kill_worker_processes = Mock(return_value=0)
    server.request_shutdown = Mock()
    monkeypatch.setattr(ProcessIdentity, "current", classmethod(lambda _cls: IDENTITY))
    identity = IDENTITY if matching else replace(IDENTITY, create_time=101.0)
    request = EndpointShutdownRequest(mode.control_message_type, identity)
    assert EndpointShutdownRequest.from_wire_payload(request.to_wire_payload()) == request
    response = server.handle_control_message(request.to_dict())
    if not matching:
        assert response[MessageFields.STATUS] == ResponseType.ERROR.value
        server._cancel_all_executions.assert_not_called()
        server._kill_worker_processes.assert_not_called()
        server.request_shutdown.assert_not_called()
    else:
        assert response[MessageFields.TYPE] == ResponseType.SHUTDOWN_ACK.value
        server._cancel_all_executions.assert_called_once()
        server._kill_worker_processes.assert_called_once()
        assert server.request_shutdown.call_count == int(mode is EndpointShutdownMode.FORCE)


@pytest.mark.parametrize("stale", [False, True])
def test_ipc_cleanup_uses_existing_stale_address_owner(tmp_path, monkeypatch, stale):
    monkeypatch.setenv("HOME", str(tmp_path))
    client = Client(port=5913, transport_mode=TransportMode.IPC, config=ZMQConfig())
    declaration = client.transport_mode.declaration
    paths = []
    for port in client.endpoint.port_pair(client.config).ports:
        lock = declaration.startup_lock_path(port, client.config)
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.touch()
        declaration.record_startup_owner(port, client.config, IDENTITY)
        socket_path = declaration.socket_path(port, client.config)
        socket_path.write_text("socket sentinel", encoding="utf-8")
        paths.append(socket_path)
    monkeypatch.setattr(TransportEndpoint, "ping", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(declaration, "endpoint_is_stale", lambda *_: stale)
    monkeypatch.setattr(ProcessIdentity, "is_alive", lambda _self: False)
    terminate = Mock(side_effect=AssertionError("Terminal incarnation must not be signalled"))
    monkeypatch.setattr(ProcessIdentity, "terminate", terminate)
    result = close(client)
    assert result.process_exited is True and not result.request_attempted
    assert result.succeeded is stale
    assert all(path.exists() is not stale for path in paths)
    terminate.assert_not_called()


def test_process_owner_uses_single_total_budget_for_term_kill_wait(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr("zmqruntime.timeouts.time.monotonic", lambda: clock[0])
    process = Mock()
    process.create_time.return_value = IDENTITY.create_time
    process.is_running.return_value = True
    process.status.return_value = psutil.STATUS_RUNNING
    waits = []

    def wait(*, timeout):
        waits.append(timeout)
        clock[0] += timeout
        if len(waits) == 1:
            raise psutil.TimeoutExpired(timeout)
        return 0

    process.wait.side_effect = wait
    monkeypatch.setattr("zmqruntime.messages.psutil.Process", lambda *_: process)
    assert IDENTITY.terminate(timeout=5.0)
    assert waits == [2.5, 2.5]
    process.terminate.assert_called_once()
    process.kill.assert_called_once()


def test_process_owner_does_not_kill_reused_pid_after_term_wait(monkeypatch):
    process = Mock()
    process.create_time.return_value = IDENTITY.create_time
    process.is_running.side_effect = [True, False]
    process.status.return_value = psutil.STATUS_RUNNING
    process.wait.side_effect = psutil.TimeoutExpired(1)
    monkeypatch.setattr("zmqruntime.messages.psutil.Process", lambda *_: process)
    assert IDENTITY.terminate(timeout=5.0)
    process.kill.assert_not_called()
