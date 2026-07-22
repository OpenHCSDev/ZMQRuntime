import pickle
import platform
import subprocess
import sys
import threading
import time
import pytest

from zmqruntime import ProcessExit
from zmqruntime.client import ZMQClient
from zmqruntime.execution.client import ExecutionClient
from zmqruntime.execution.responses import ExecutionSubmissionResponse
from zmqruntime.execution.server import ExecutionServer
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ControlMessageType,
    ExecuteRequest,
    ExecutionStatus,
    MessageFields,
    PongResponse,
    ProcessIdentity,
    ResponseType,
    TaskProgress,
)
from zmqruntime.transport import get_ipc_socket_path


class DummyExecutionServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        return {"result": 1}


class FailingExecutionServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        raise RuntimeError("boom")


def test_execution_server_pong_projects_its_process_identity():
    pong = PongResponse.from_dict(
        DummyExecutionServer(port=5555)._create_pong_response()
    )

    assert pong.process_identity == ProcessIdentity.current()


def test_execution_server_handle_execute_and_run():
    server = DummyExecutionServer(port=5555)
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"x": 1},
    )
    response = server._handle_execute(request.to_dict())
    assert response[MessageFields.STATUS] == "accepted"
    execution_id = response[MessageFields.EXECUTION_ID]
    record = server.active_executions[execution_id]
    assert record.status == ExecutionStatus.QUEUED.value

    server._run_execution(execution_id, request, record)
    assert record.status == ExecutionStatus.COMPLETE.value

    status_response = server._handle_status(
        {
            MessageFields.TYPE: ControlMessageType.STATUS.value,
            MessageFields.EXECUTION_ID: execution_id,
        }
    )
    assert status_response[MessageFields.STATUS] == "ok"
    assert status_response[MessageFields.EXECUTION][MessageFields.EXECUTION_ID] == execution_id


def test_execution_status_response_is_picklable_with_non_transport_values():
    server = DummyExecutionServer(port=5555)
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"x": 1},
    )
    response = server._handle_execute(request.to_dict())
    execution_id = response[MessageFields.EXECUTION_ID]
    record = server.active_executions[execution_id]

    class Unserializable:
        pass

    record.client_address = Unserializable
    record.results_summary = {"raw": Unserializable, "nested": [Unserializable]}

    status_response = server._handle_status(
        {
            MessageFields.TYPE: ControlMessageType.STATUS.value,
            MessageFields.EXECUTION_ID: execution_id,
        }
    )
    encoded = pickle.dumps(status_response)
    decoded = pickle.loads(encoded)

    execution = decoded[MessageFields.EXECUTION]
    assert isinstance(execution[MessageFields.CLIENT_ADDRESS], str)
    assert isinstance(execution[MessageFields.RESULTS_SUMMARY]["raw"], str)
    assert isinstance(execution[MessageFields.RESULTS_SUMMARY]["nested"][0], str)


def test_failed_execution_exposes_traceback_field():
    server = FailingExecutionServer(port=5555)
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"x": 1},
    )
    response = server._handle_execute(request.to_dict())
    execution_id = response[MessageFields.EXECUTION_ID]
    record = server.active_executions[execution_id]

    server._run_execution(execution_id, request, record)

    status_response = server._handle_status(
        {
            MessageFields.TYPE: ControlMessageType.STATUS.value,
            MessageFields.EXECUTION_ID: execution_id,
        }
    )
    execution = status_response[MessageFields.EXECUTION]
    assert execution[MessageFields.STATUS] == ExecutionStatus.FAILED.value
    assert "RuntimeError: boom" in execution[MessageFields.TRACEBACK]


def test_task_progress_roundtrip_supports_execution_id_and_task_id():
    progress = TaskProgress(
        task_id="exec-123",
        phase="running",
        status="running",
        percent=10.0,
        timestamp=1.0,
        completed=1,
        total=10,
    )

    payload = progress.to_dict()
    parsed = TaskProgress.from_dict(payload)
    assert parsed.task_id == "exec-123"

    legacy_payload = dict(payload)
    del legacy_payload["execution_id"]
    legacy_parsed = TaskProgress.from_dict(legacy_payload)
    assert legacy_parsed.task_id == "exec-123"


class DummyExecutionClient(ExecutionClient):
    def __init__(self):
        super().__init__(port=5555)
        self._connected = True

    def _spawn_server_process(self):
        return None

    def send_data(self, data):
        return None

    def serialize_task(self, task, config):
        return {"task": task}

    def connect(self, timeout: float = 10.0):
        self._connected = True
        return True

    def _send_control_request(self, request, timeout_ms=5000):
        if request[MessageFields.TYPE] == ControlMessageType.REGISTER_PROGRESS.value:
            return {MessageFields.STATUS: ResponseType.OK.value}
        return request


def test_execution_client_submit_adds_type():
    client = DummyExecutionClient()
    response = client.submit_execution({"hello": "world"})
    assert response[MessageFields.TYPE] == ControlMessageType.EXECUTE.value


def test_execution_server_progress_registration_roundtrip():
    server = DummyExecutionServer(port=5555)
    register = server._handle_register_progress(
        {
            MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
            MessageFields.CLIENT_ID: "client-1",
        }
    )
    assert register[MessageFields.STATUS] == ResponseType.OK.value
    assert register[MessageFields.PROGRESS_SUBSCRIBERS] == 1

    duplicate = server._handle_register_progress(
        {
            MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
            MessageFields.CLIENT_ID: "client-1",
        }
    )
    assert duplicate[MessageFields.PROGRESS_SUBSCRIBERS] == 1

    unregister = server._handle_unregister_progress(
        {
            MessageFields.TYPE: ControlMessageType.UNREGISTER_PROGRESS.value,
            MessageFields.CLIENT_ID: "client-1",
        }
    )
    assert unregister[MessageFields.STATUS] == ResponseType.OK.value
    assert unregister[MessageFields.PROGRESS_SUBSCRIBERS] == 0


class ProgressAwareExecutionClient(DummyExecutionClient):
    def __init__(self):
        super().__init__()
        self.progress_callback = lambda _msg: None
        self.sent_requests = []
        self.listener_started = False

    def _start_progress_listener(self):
        self.listener_started = True

    def _send_control_request(self, request, timeout_ms=5000):
        self.sent_requests.append(request)
        if request.get(MessageFields.TYPE) == ControlMessageType.REGISTER_PROGRESS.value:
            return {MessageFields.STATUS: ResponseType.OK.value}
        if (
            request.get(MessageFields.TYPE)
            == ControlMessageType.UNREGISTER_PROGRESS.value
        ):
            return {MessageFields.STATUS: ResponseType.OK.value}
        return request


def test_execution_client_registers_progress_before_execute():
    client = ProgressAwareExecutionClient()
    response = client.submit_execution({"hello": "world"})
    assert response[MessageFields.TYPE] == ControlMessageType.EXECUTE.value
    assert client.listener_started is True
    assert client.sent_requests[0][MessageFields.TYPE] == ControlMessageType.REGISTER_PROGRESS.value
    assert client.sent_requests[1][MessageFields.TYPE] == ControlMessageType.EXECUTE.value

    client.disconnect()
    assert client.sent_requests[2][MessageFields.TYPE] == ControlMessageType.UNREGISTER_PROGRESS.value


class EndpointPolicyExecutionClient(ExecutionClient):
    def __init__(self, *, transport_mode=TransportMode.IPC, endpoint_stale=False):
        super().__init__(
            port=5555,
            transport_mode=transport_mode,
        )
        self.endpoint_stale = endpoint_stale
        self.killed_ports = []
        self.spawned = False
        self.setup_called = False

    def _is_port_in_use(self, port: int):
        return True

    def _try_connect_to_existing(self, port: int, timeout_ms: int = 500):
        return False

    def _should_preserve_unresponsive_endpoint(self, port: int):
        return self.transport_mode == TransportMode.IPC and not self.endpoint_stale

    def _kill_processes_on_port(self, port: int):
        self.killed_ports.append(port)

    def _spawn_server_process(self):
        self.spawned = True
        return object()

    def _wait_for_server_ready(self, timeout: float = 10.0):
        return True

    def _setup_client_sockets(self):
        self.setup_called = True

    def send_data(self, data):
        return None

    def serialize_task(self, task, config):
        return {"task": task}


def test_ipc_connect_preserves_unresponsive_live_server_endpoint():
    client = EndpointPolicyExecutionClient(endpoint_stale=False)

    connected = client.connect(timeout=1)

    assert connected is False
    assert client.killed_ports == []
    assert client.spawned is False
    assert client.setup_called is False


def test_ipc_connect_removes_stale_endpoint_before_spawning():
    client = EndpointPolicyExecutionClient(endpoint_stale=True)

    connected = client.connect(timeout=1)

    assert connected is True
    assert client.killed_ports == [client.port, client.control_port]
    assert client.spawned is True
    assert client.setup_called is True


def test_tcp_connect_keeps_existing_spawn_cleanup_policy():
    client = EndpointPolicyExecutionClient(
        transport_mode=TransportMode.TCP,
        endpoint_stale=False,
    )

    connected = client.connect(timeout=1)

    assert connected is True
    assert client.killed_ports == [client.port, client.control_port]
    assert client.spawned is True
    assert client.setup_called is True


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_owned_server_shutdown_removes_exact_ipc_endpoints():
    config = ZMQConfig(
        app_name="zmqruntime-owned-process-test",
        ipc_socket_prefix="owned",
    )
    client = EndpointPolicyExecutionClient()
    client.config = config
    client.port = 45556
    client.control_port = client.port + config.control_port_offset
    paths = (
        get_ipc_socket_path(client.port, config),
        get_ipc_socket_path(client.control_port, config),
    )
    for path in paths:
        assert path is not None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
    )
    ZMQClient._stop_owned_server_process(client, process)

    assert process.poll() is not None
    assert all(not path.exists() for path in paths if path is not None)


def test_owned_server_process_liveness_distinguishes_process_ownership():
    client = EndpointPolicyExecutionClient()
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
    )
    client.server_process = process
    try:
        assert client.owned_server_process_is_alive() is True

        client._connected_to_existing = True
        assert client.owned_server_process_is_alive() is None
        client._connected_to_existing = False

        process.terminate()
        process.wait(timeout=5)
        assert client.owned_server_process_is_alive() is False
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_owned_server_process_exit_retains_exact_terminal_status():
    client = EndpointPolicyExecutionClient()
    process = subprocess.Popen([sys.executable, "-c", "raise SystemExit(7)"])
    client.server_process = process

    process.wait(timeout=5)

    assert client.owned_server_process_exit() == ProcessExit(7)
    client._connected_to_existing = True
    assert client.owned_server_process_exit() is None


def test_process_exit_describes_exit_codes_and_signals():
    assert ProcessExit(7).describe() == "exit code 7"
    assert ProcessExit(-9).describe() == "signal SIGKILL (-9)"


def test_known_server_process_liveness_includes_identified_local_server():
    client = EndpointPolicyExecutionClient(transport_mode=TransportMode.IPC)
    client._connected_to_existing = True
    process_identity = ProcessIdentity.current()
    client._connected_server_process_identity = process_identity

    assert client.known_server_process_is_alive() is True

    client._connected_server_process_identity = ProcessIdentity(
        pid=process_identity.pid,
        create_time=process_identity.create_time - 1,
    )
    assert client.known_server_process_is_alive() is False


def test_known_server_process_liveness_leaves_remote_identity_unknown(monkeypatch):
    client = EndpointPolicyExecutionClient(transport_mode=TransportMode.TCP)
    client._connected_to_existing = True
    client._connected_server_process_identity = ProcessIdentity.current()
    monkeypatch.setattr(client, "_endpoint_is_local", lambda: False)

    assert client.known_server_process_is_alive() is None


def test_existing_connection_retains_typed_server_process_identity(monkeypatch):
    process_identity = ProcessIdentity.current()
    monkeypatch.setattr(
        "zmqruntime.client.request_control_ping",
        lambda *_args, **_kwargs: PongResponse(
            port=5555,
            control_port=6555,
            ready=True,
            server="DummyExecutionServer",
            process_identity=process_identity,
        ).to_dict(),
    )
    client = EndpointPolicyExecutionClient()

    assert ZMQClient._try_connect_to_existing(client, 5555) is True
    assert client._connected_server_process_identity == process_identity


def test_disconnect_stops_owned_server_when_socket_cleanup_fails(monkeypatch):
    client = EndpointPolicyExecutionClient()
    process = object()
    stopped = []
    client._connected = True
    client._connected_to_existing = False
    client.persistent = False
    client.server_process = process

    def fail_cleanup():
        raise RuntimeError("socket cleanup failed")

    monkeypatch.setattr(client, "_cleanup_sockets", fail_cleanup)
    monkeypatch.setattr(client, "_stop_owned_server_process", stopped.append)

    with pytest.raises(RuntimeError, match="socket cleanup failed"):
        client.disconnect()

    assert stopped == [process]
    assert client.server_process is None
    assert client._connected is False
    assert client._connected_to_existing is False


class ConcurrentStartupExecutionClient(ExecutionClient):
    def __init__(self, *, port, config, state):
        super().__init__(
            port=port,
            transport_mode=TransportMode.IPC,
            config=config,
        )
        self.state = state

    def _try_connect_to_existing(self, port: int, timeout_ms: int = 500):
        return self.state["ready"].is_set()

    def _spawn_server_process(self):
        with self.state["lock"]:
            self.state["spawn_count"] += 1
        return object()

    def _wait_for_server_ready(self, timeout: float = 10.0):
        for port in (self.port, self.control_port):
            path = get_ipc_socket_path(port, self.config)
            assert path is not None
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        self.state["ready"].set()
        time.sleep(0.1)
        return True

    def _setup_client_sockets(self):
        return None

    def send_data(self, data):
        return None

    def serialize_task(self, task, config):
        return {"task": task}


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_concurrent_clients_spawn_one_ipc_server():
    config = ZMQConfig(
        app_name="zmqruntime-concurrent-startup-test",
        ipc_socket_prefix="concurrent",
    )
    port = 45557
    state = {
        "lock": threading.Lock(),
        "ready": threading.Event(),
        "spawn_count": 0,
    }
    clients = tuple(
        ConcurrentStartupExecutionClient(port=port, config=config, state=state)
        for _ in range(2)
    )
    barrier = threading.Barrier(len(clients))

    def connect(client):
        barrier.wait()
        assert client.connect(timeout=1) is True

    threads = tuple(threading.Thread(target=connect, args=(client,)) for client in clients)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    try:
        assert all(not thread.is_alive() for thread in threads)
        assert state["spawn_count"] == 1
        assert sum(client._connected_to_existing for client in clients) == 1
    finally:
        for endpoint_port in (port, port + config.control_port_offset):
            path = get_ipc_socket_path(endpoint_port, config)
            if path is not None and path.exists():
                path.unlink()


def test_execution_waiter_surfaces_error_field_when_message_absent():
    waiter = ExecutionWaiter(
        lambda _execution_id: {
            MessageFields.STATUS: ResponseType.ERROR.value,
            MessageFields.ERROR: "Execution missing from restarted server",
        }
    )

    result = waiter.wait("compile-1", WaitPolicy(poll_interval=0))

    assert result[MessageFields.STATUS] == ResponseType.ERROR.value
    assert result[MessageFields.MESSAGE] == "Execution missing from restarted server"


def test_execution_waiter_treats_progress_as_liveness_during_status_timeouts():
    calls = 0
    progress_sequence = 0

    def poll_status(_execution_id):
        nonlocal calls, progress_sequence
        calls += 1
        if calls <= 3:
            progress_sequence += 1
            raise TimeoutError("status endpoint busy")
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.EXECUTION: {
                MessageFields.EXECUTION_ID: "compile-1",
                MessageFields.PLATE_ID: "plate-1",
                MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
            },
        }

    waiter = ExecutionWaiter(
        poll_status,
        progress_sequence=lambda _execution_id: progress_sequence,
    )

    result = waiter.wait(
        "compile-1",
        WaitPolicy(
            poll_interval=0,
            max_consecutive_errors=2,
            retry_backoff_seconds=0,
        ),
    )

    assert result[MessageFields.STATUS] == ExecutionStatus.COMPLETE.value


def test_execution_waiter_treats_known_server_process_as_exact_liveness():
    calls = 0

    def poll_status(_execution_id):
        nonlocal calls
        calls += 1
        if calls <= 3:
            raise TimeoutError("interpreter busy")
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.EXECUTION: {
                MessageFields.EXECUTION_ID: "compile-1",
                MessageFields.PLATE_ID: "plate-1",
                MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
            },
        }

    waiter = ExecutionWaiter(
        poll_status,
        known_server_process_is_alive=lambda: True,
    )
    result = waiter.wait(
        "compile-1",
        WaitPolicy(
            poll_interval=0,
            max_consecutive_errors=2,
            retry_backoff_seconds=0,
        ),
    )

    assert calls == 4
    assert result[MessageFields.STATUS] == ExecutionStatus.COMPLETE.value


def test_execution_waiter_stops_when_known_server_process_exits():
    waiter = ExecutionWaiter(
        lambda _execution_id: (_ for _ in ()).throw(TimeoutError("no response")),
        known_server_process_is_alive=lambda: False,
        owned_server_process_exit=lambda: ProcessExit(-9),
    )

    result = waiter.wait(
        "compile-1",
        WaitPolicy(
            poll_interval=0,
            max_consecutive_errors=5,
            retry_backoff_seconds=0,
        ),
    )

    assert result == {
        MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
        MessageFields.EXECUTION_ID: "compile-1",
        MessageFields.MESSAGE: (
            "Lost connection to server (server process exited with signal "
            "SIGKILL (-9); last status error: TimeoutError: no response)"
        ),
    }


def test_execution_client_composes_known_server_liveness_into_waiter(monkeypatch):
    client = DummyExecutionClient()
    calls = 0

    def poll_status(_execution_id, *, timeout_ms):
        nonlocal calls
        assert timeout_ms == WaitPolicy.status_timeout_ms
        calls += 1
        if calls <= 3:
            raise TimeoutError("interpreter busy")
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.EXECUTION: {
                MessageFields.EXECUTION_ID: "compile-1",
                MessageFields.PLATE_ID: "plate-1",
                MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
            },
        }

    monkeypatch.setattr(client, "poll_status", poll_status)
    monkeypatch.setattr(client, "known_server_process_is_alive", lambda: True)
    monkeypatch.setattr(client, "owned_server_process_exit", lambda: None)

    result = client.wait_for_completion(
        "compile-1",
        poll_interval=0,
        max_consecutive_errors=2,
    )

    assert calls == 4
    assert result[MessageFields.STATUS] == ExecutionStatus.COMPLETE.value


def test_submission_response_requires_explicit_tracking_and_diagnostics():
    accepted_without_id = ExecutionSubmissionResponse.from_wire(
        {MessageFields.STATUS: ResponseType.ACCEPTED.value}
    )
    with pytest.raises(RuntimeError, match="without execution_id"):
        accepted_without_id.require_execution_id("submission")

    failed_without_diagnostic = ExecutionSubmissionResponse.from_wire(
        {MessageFields.STATUS: ResponseType.ERROR.value}
    )
    with pytest.raises(RuntimeError, match="message or error"):
        failed_without_diagnostic.require_failure_text("submission")

    failed_with_both = ExecutionSubmissionResponse.from_wire(
        {
            MessageFields.STATUS: ResponseType.ERROR.value,
            MessageFields.MESSAGE: "bad request",
            MessageFields.ERROR: "missing plate",
        }
    )
    assert failed_with_both.require_failure_text("submission") == (
        "bad request (missing plate)"
    )
