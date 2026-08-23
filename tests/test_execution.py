import json
import multiprocessing
import pickle
import platform
import subprocess
import sys
import threading
import time

import pytest

from zmqruntime import ProcessExit
from zmqruntime.client import (
    AttachedEndpointConnection,
    EndpointConnectionPolicy,
    EndpointProcess,
    EndpointProcessGroup,
    EndpointShutdownMode,
    EndpointShutdownResult,
    OwnedEndpointConnection,
    ZMQClient,
    endpoint_process,
)
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.execution.client import ExecutionClient
from zmqruntime.execution.responses import ExecutionSubmissionResponse
from zmqruntime.execution.server import ExecutionServer
from zmqruntime.execution.status_poller import (
    CallbackExecutionStatusPollPolicy,
    ExecutionStatusPoller,
)
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.messages import (
    ControlMessageType,
    EndpointApplication,
    EndpointControlCapability,
    ExecuteRequest,
    ExecutionStatus,
    MessageFields,
    PongResponse,
    ProcessIdentity,
    ResponseType,
    ServerRole,
    TaskProgress,
)
from zmqruntime.transport import (
    TcpDataControlPortPairAuthority,
    TransportEndpoint,
    get_ipc_socket_path,
    ping_control_port,
)


class DummyExecutionServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        return {"result": 1}


def _return_immediately() -> None:
    return None


class FailingExecutionServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        raise RuntimeError("boom")


class BlockingExecutionServer(ExecutionServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.started = threading.Event()
        self.release = threading.Event()
        self.interrupted_execution_ids = []

    def execute_task(self, execution_id: str, request: ExecuteRequest):
        self.started.set()
        assert self.release.wait(timeout=5)
        return {"late": 1}

    def _interrupt_execution(self, execution_id: str) -> int:
        self.interrupted_execution_ids.append(execution_id)
        return 0


class StubEndpointProcess(EndpointProcess):
    def __init__(self) -> None:
        self.alive = True
        self.stop_count = 0
        self.wait_count = 0

    def is_alive(self) -> bool:
        return self.alive

    def exit(self) -> ProcessExit | None:
        return None if self.alive else ProcessExit(0)

    def wait_for_exit(self, timeout: float) -> ProcessExit | None:
        del timeout
        self.wait_count += 1
        return self.exit()

    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        self.stop_count += 1
        self.alive = False
        return False


def test_endpoint_process_adapts_subprocess_lifecycle():
    source = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        process = endpoint_process(source)

        assert process.is_alive()
        assert process.stop(timeout=1, kill_timeout=1) is False
        assert not process.is_alive()
    finally:
        if source.poll() is None:
            source.kill()
            source.wait(timeout=1)


def test_endpoint_process_reaps_external_subprocess_exit_without_polling():
    source = subprocess.Popen(
        [sys.executable, "-c", "pass"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    process = endpoint_process(source)

    assert process.wait_for_exit(timeout=2) == ProcessExit(0)
    assert source.returncode == 0
    assert not process.is_alive()


def test_endpoint_process_reaps_external_multiprocessing_exit_without_polling():
    source = multiprocessing.get_context("spawn").Process(target=_return_immediately)
    source.start()
    process = endpoint_process(source)

    assert process.wait_for_exit(timeout=5) == ProcessExit(0)
    assert source.exitcode == 0
    assert not process.is_alive()


def test_endpoint_process_rejects_structural_process_lookalike():
    class ProcessLike:
        def is_alive(self):
            return True

    with pytest.raises(TypeError, match="Unsupported ZMQ server process handle"):
        endpoint_process(ProcessLike())


def test_endpoint_process_group_stops_every_owned_process_once():
    group = EndpointProcessGroup()
    first = StubEndpointProcess()
    second = StubEndpointProcess()

    assert group.own(first) is first
    assert group.own(second) is second
    assert group.own(first) is first
    assert group.active_count == 2

    group.stop_all()

    assert first.stop_count == 1
    assert second.stop_count == 1
    assert group.active_count == 0


def test_endpoint_process_group_stops_owned_processes_concurrently():
    group = EndpointProcessGroup()
    stop_barrier = threading.Barrier(2)

    class CoordinatedEndpointProcess(StubEndpointProcess):
        def stop(
            self,
            timeout: float = 5.0,
            kill_timeout: float = 2.0,
        ) -> bool:
            del timeout, kill_timeout
            stop_barrier.wait(timeout=1)
            return super().stop()

    first = CoordinatedEndpointProcess()
    second = CoordinatedEndpointProcess()
    group.own(first)
    group.own(second)

    group.stop_all()

    assert first.stop_count == 1
    assert second.stop_count == 1
    assert group.active_count == 0


def test_endpoint_process_group_disowns_without_stopping():
    group = EndpointProcessGroup()
    process = StubEndpointProcess()
    group.own(process)

    assert group.disown(process) is process
    group.stop_all()

    assert process.stop_count == 0


def owned_connection(client, process) -> OwnedEndpointConnection:
    endpoint = PongResponse(
        port=client.port,
        control_port=client.control_port,
        ready=True,
        server=type(client).__name__,
        server_role=ServerRole.EXECUTION,
    )
    return OwnedEndpointConnection(
        process=endpoint_process(process),
        target=TransportEndpoint(
            host=client.host,
            port=client.port,
            transport_mode=client.transport_mode,
        ),
        config=client.config,
        endpoint=endpoint,
    )


def test_owned_endpoint_requests_shutdown_and_waits_for_exact_process(
    monkeypatch,
):
    client = EndpointPolicyExecutionClient()
    process = StubEndpointProcess()
    connection = owned_connection(client, process)
    shutdown_calls = []

    def shutdown_endpoint_on_port(**kwargs):
        shutdown_calls.append(kwargs)
        process.alive = False
        return EndpointShutdownResult(succeeded=True, endpoint_terminated=True)

    monkeypatch.setattr(
        ZMQClient,
        "shutdown_endpoint_on_port",
        shutdown_endpoint_on_port,
    )

    connection.terminate_endpoint()

    assert shutdown_calls == [
        {
            "port": client.port,
            "mode": EndpointShutdownMode.FORCE,
            "timeout": connection.shutdown_timeout_seconds,
            "transport_mode": client.transport_mode,
            "host": client.host,
            "config": client.config,
        }
    ]
    assert process.wait_count == 1
    assert process.stop_count == 0


def test_owned_endpoint_uses_exact_process_when_shutdown_is_not_admitted(
    monkeypatch,
):
    client = EndpointPolicyExecutionClient()
    process = StubEndpointProcess()
    connection = owned_connection(client, process)
    monkeypatch.setattr(
        ZMQClient,
        "shutdown_endpoint_on_port",
        lambda **_kwargs: EndpointShutdownResult(
            succeeded=False,
            endpoint_terminated=False,
        ),
    )

    connection.terminate_endpoint()

    assert process.wait_count == 0
    assert process.stop_count == 1


def attached_connection(
    client,
    process_identity: ProcessIdentity | None = None,
) -> AttachedEndpointConnection:
    return AttachedEndpointConnection(
        PongResponse(
            port=client.port,
            control_port=client.control_port,
            ready=True,
            server=type(client).__name__,
            server_role=ServerRole.EXECUTION,
            process_identity=process_identity,
        )
    )


def test_execution_server_pong_projects_its_process_identity():
    pong = DummyExecutionServer(port=5555)._create_pong_response()

    assert pong.process_identity == ProcessIdentity.current()
    assert EndpointControlCapability.FORCE_SHUTDOWN in pong.control_capabilities


def test_execution_server_pong_preserves_base_application_identity():
    application = EndpointApplication(identifier="example", version="2.0")

    pong = DummyExecutionServer(
        port=5555,
        application=application,
    )._create_pong_response()

    assert pong.application == application


def test_shutdown_rejects_endpoint_without_advertised_capability(monkeypatch):
    endpoint = PongResponse(
        port=5555,
        control_port=6555,
        ready=True,
        server="InProcessEndpoint",
        server_role=ServerRole.GENERIC,
        process_identity=ProcessIdentity.current(),
    )
    process_terminations = []
    monkeypatch.setattr(
        TransportEndpoint,
        "ping",
        lambda *_args, **_kwargs: endpoint,
    )
    monkeypatch.setattr(
        TransportEndpoint,
        "is_in_use",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        ProcessIdentity,
        "terminate",
        lambda *_args, **_kwargs: process_terminations.append(True) or True,
    )

    result = ZMQClient.shutdown_endpoint_on_port(
        endpoint.port,
        mode=EndpointShutdownMode.FORCE,
    )

    assert result == EndpointShutdownResult(
        succeeded=False,
        endpoint_terminated=False,
    )
    assert process_terminations == []


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


def test_cancelled_execution_is_terminal_when_running_task_returns():
    server = BlockingExecutionServer(port=5555)
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"x": 1},
    )
    response = server._handle_execute(request.to_dict())
    execution_id = response[MessageFields.EXECUTION_ID]
    record = server.active_executions[execution_id]
    execution_thread = threading.Thread(
        target=server.run_execution,
        args=(execution_id, request, record),
    )
    execution_thread.start()
    assert server.started.wait(timeout=5)

    cancellation = server._handle_cancel(
        {
            MessageFields.TYPE: ControlMessageType.CANCEL.value,
            MessageFields.EXECUTION_ID: execution_id,
        }
    )
    server.release.set()
    execution_thread.join(timeout=5)

    assert not execution_thread.is_alive()
    assert cancellation[MessageFields.STATUS] == ResponseType.OK.value
    assert record.status == ExecutionStatus.CANCELLED.value
    assert record.results_summary is None
    assert server.interrupted_execution_ids == [execution_id]


def test_cancel_targets_queued_execution_without_interrupting_running_execution():
    server = BlockingExecutionServer(port=5555)
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"x": 1},
    )
    running_id = server._handle_execute(request.to_dict())[MessageFields.EXECUTION_ID]
    queued_id = server._handle_execute(request.to_dict())[MessageFields.EXECUTION_ID]
    server._lifecycle.mark_running(running_id)

    cancellation = server._handle_cancel(
        {
            MessageFields.TYPE: ControlMessageType.CANCEL.value,
            MessageFields.EXECUTION_ID: queued_id,
        }
    )

    assert cancellation[MessageFields.STATUS] == ResponseType.OK.value
    assert server.active_executions[running_id].status == ExecutionStatus.RUNNING.value
    assert server.active_executions[queued_id].status == ExecutionStatus.CANCELLED.value
    assert server.interrupted_execution_ids == []


def test_cancel_rejects_terminal_execution_without_rewriting_status():
    server = DummyExecutionServer(port=5555)
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"x": 1},
    )
    response = server._handle_execute(request.to_dict())
    execution_id = response[MessageFields.EXECUTION_ID]
    record = server.active_executions[execution_id]
    server.run_execution(execution_id, request, record)

    cancellation = server._handle_cancel(
        {
            MessageFields.TYPE: ControlMessageType.CANCEL.value,
            MessageFields.EXECUTION_ID: execution_id,
        }
    )

    assert cancellation[MessageFields.STATUS] == ResponseType.ERROR.value
    assert record.status == ExecutionStatus.COMPLETE.value


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
        self._connection = attached_connection(self)

    def _spawn_server_process(self):
        return None

    def send_data(self, data):
        return None

    def serialize_task(self, task, config):
        return {"task": task}

    def connect(self, timeout: float = 10.0):
        self._connection = attached_connection(self)
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
        self.sent_requests.append((request, timeout_ms))
        if request.get(MessageFields.TYPE) == ControlMessageType.REGISTER_PROGRESS.value:
            return {MessageFields.STATUS: ResponseType.OK.value}
        if request.get(MessageFields.TYPE) == ControlMessageType.UNREGISTER_PROGRESS.value:
            return {MessageFields.STATUS: ResponseType.OK.value}
        return request


def test_execution_client_registers_progress_before_execute():
    client = ProgressAwareExecutionClient()
    response = client.submit_execution({"hello": "world"}, timeout_ms=15000)
    assert response[MessageFields.TYPE] == ControlMessageType.EXECUTE.value
    assert client.listener_started is True
    assert client.sent_requests[0] == (
        {
            MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
            MessageFields.CLIENT_ID: client._progress_client_id,
        },
        15000,
    )
    assert client.sent_requests[1] == (
        {
            "task": {"hello": "world"},
            MessageFields.TYPE: ControlMessageType.EXECUTE.value,
        },
        15000,
    )

    client.disconnect()
    assert (
        client.sent_requests[2][0][MessageFields.TYPE]
        == ControlMessageType.UNREGISTER_PROGRESS.value
    )


def test_execution_client_retains_immutable_progress_observations_per_execution():
    client = ProgressAwareExecutionClient()
    first = TaskProgress(
        task_id="execution-1",
        phase="compile",
        status="running",
        percent=10.0,
        timestamp=1.0,
        completed=1,
        total=10,
    ).to_dict()
    second = TaskProgress(
        task_id="execution-1",
        phase="execute",
        status="running",
        percent=20.0,
        timestamp=2.0,
        completed=2,
        total=10,
    ).to_dict()
    second["nested"] = {"values": [1, 2]}
    other = TaskProgress(
        task_id="execution-2",
        phase="compile",
        status="running",
        percent=50.0,
        timestamp=3.0,
        completed=1,
        total=2,
    ).to_dict()

    client._record_progress(first)
    client._record_progress(second)
    client._record_progress(other)
    second["phase"] = "mutated"
    second["nested"]["values"][0] = 99

    observation = client.progress_observation("execution-1")
    other_observation = client.progress_observation("execution-2")

    assert observation is not None
    assert observation.sequence == 2
    assert observation.event["phase"] == "execute"
    assert observation.event["nested"]["values"] == (1, 2)
    assert type(observation).from_wire(observation.as_wire()) == observation
    assert other_observation is not None
    assert other_observation.sequence == 1
    assert client.progress_observation("missing") is None


def test_execution_client_disconnect_closes_base_after_listener_failure(monkeypatch):
    client = ProgressAwareExecutionClient()
    base_disconnect_called = []

    monkeypatch.setattr(
        client,
        "_stop_progress_listener",
        lambda: (_ for _ in ()).throw(TimeoutError("listener still running")),
    )
    monkeypatch.setattr(
        ZMQClient,
        "disconnect",
        lambda _client: base_disconnect_called.append(True),
    )

    with pytest.raises(TimeoutError, match="listener still running"):
        client.disconnect()

    assert base_disconnect_called == [True]


def test_execution_client_disconnect_chains_listener_and_base_failures(
    monkeypatch,
):
    client = ProgressAwareExecutionClient()

    monkeypatch.setattr(
        client,
        "_stop_progress_listener",
        lambda: (_ for _ in ()).throw(TimeoutError("listener still running")),
    )
    monkeypatch.setattr(
        ZMQClient,
        "disconnect",
        lambda _client: (_ for _ in ()).throw(RuntimeError("base close failed")),
    )

    with pytest.raises(RuntimeError, match="base close failed") as error:
        client.disconnect()

    assert isinstance(error.value.__cause__, TimeoutError)


class EndpointPolicyExecutionClient(ExecutionClient):
    def __init__(
        self,
        *,
        port=5555,
        transport_mode=TransportMode.IPC,
        config=None,
    ):
        super().__init__(
            port=port,
            transport_mode=transport_mode,
            config=config,
        )
        self.killed_ports = []
        self.spawned = False
        self.setup_called = False

    def _is_port_in_use(self, port: int):
        return True

    def _try_connect_to_existing(self, port: int, timeout_ms: int = 500):
        if not self.spawned:
            return None
        return PongResponse(
            port=self.port,
            control_port=self.control_port,
            ready=True,
            server=type(self).__name__,
            server_role=ServerRole.EXECUTION,
        )

    def _kill_processes_on_port(self, port: int):
        self.killed_ports.append(port)

    def _spawn_server_process(self):
        self.spawned = True
        return StubEndpointProcess()

    def _wait_for_endpoint_ready(
        self,
        process: EndpointProcess,
        timeout: float = 10.0,
    ):
        return self._try_connect_to_existing(self.port)

    def _setup_client_sockets(self):
        self.setup_called = True

    def send_data(self, data):
        return None

    def serialize_task(self, task, config):
        return {"task": task}


def test_ipc_connect_preserves_unresponsive_live_server_endpoint(monkeypatch):
    monkeypatch.setattr(
        TransportMode.IPC.declaration,
        "preserve_unresponsive_endpoint",
        classmethod(lambda _cls, _port, _config: True),
    )
    client = EndpointPolicyExecutionClient()

    connected = client.connect(timeout=1)

    assert connected is False
    assert client.killed_ports == []
    assert client.spawned is False
    assert client.setup_called is False


def test_ipc_connect_removes_stale_endpoint_before_spawning(monkeypatch):
    monkeypatch.setattr(
        TransportMode.IPC.declaration,
        "preserve_unresponsive_endpoint",
        classmethod(lambda _cls, _port, _config: False),
    )
    client = EndpointPolicyExecutionClient()

    connected = client.connect(timeout=1)

    assert connected is True
    assert client.killed_ports == [client.port, client.control_port]
    assert client.spawned is True
    assert client.setup_called is True


def test_tcp_connect_keeps_existing_spawn_cleanup_policy():
    client = EndpointPolicyExecutionClient(
        transport_mode=TransportMode.TCP,
    )

    connected = client.connect(timeout=1)

    assert connected is True
    assert client.killed_ports == [client.port, client.control_port]
    assert client.spawned is True
    assert client.setup_called is True


def test_connect_cleans_owned_process_when_readiness_hook_raises():
    class FailingReadinessClient(EndpointPolicyExecutionClient):
        def __init__(self) -> None:
            super().__init__(transport_mode=TransportMode.TCP)
            self.spawned_process: StubEndpointProcess | None = None

        def _spawn_server_process(self):
            self.spawned = True
            self.spawned_process = StubEndpointProcess()
            return self.spawned_process

        def _wait_for_endpoint_ready(
            self,
            process: EndpointProcess,
            timeout: float = 10.0,
        ):
            del process, timeout
            raise RuntimeError("readiness hook failed")

    client = FailingReadinessClient()

    with pytest.raises(RuntimeError, match="readiness hook failed"):
        client.connect(timeout=1)

    assert client.spawned_process is not None
    assert client.spawned_process.stop_count == 1
    assert client.is_connected() is False


def test_attach_existing_never_replaces_an_unresponsive_endpoint():
    client = EndpointPolicyExecutionClient(
        transport_mode=TransportMode.TCP,
    )

    connected = EndpointConnectionPolicy.ATTACH_EXISTING.connect(client, timeout=1)

    assert connected is False
    assert client.killed_ports == []
    assert client.spawned is False
    assert client.setup_called is False


def test_attach_existing_connects_a_ready_endpoint_without_spawning(monkeypatch):
    client = EndpointPolicyExecutionClient()
    endpoint = PongResponse(
        port=client.port,
        control_port=client.control_port,
        ready=True,
        server="EndpointPolicyExecutionClient",
        server_role=ServerRole.EXECUTION,
    )
    monkeypatch.setattr(
        client,
        "_try_connect_to_existing",
        lambda *_args, **_kwargs: endpoint,
    )

    connected = EndpointConnectionPolicy.ATTACH_EXISTING.connect(client, timeout=1)

    assert connected is True
    assert client.is_connected()
    assert isinstance(client._connection, AttachedEndpointConnection)
    assert client.connected_endpoint is endpoint
    assert client.killed_ports == []
    assert client.spawned is False
    assert client.setup_called is True


def test_shutdown_result_distinguishes_worker_stop_from_endpoint_termination():
    config = ZMQConfig(default_port=47777, control_port_offset=1000)
    endpoint = TcpDataControlPortPairAuthority.acquire(config)
    server = DummyExecutionServer(
        port=endpoint.data_port,
        host="127.0.0.1",
        transport_mode=TransportMode.TCP,
        config=config,
    )
    started = threading.Event()

    def serve() -> None:
        server.start()
        server._ready = True
        started.set()
        while server.is_running():
            server.process_messages()
            time.sleep(0.01)
        server.stop()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert started.wait(timeout=2)

    graceful = ZMQClient.shutdown_endpoint_on_port(
        endpoint.data_port,
        mode=EndpointShutdownMode.GRACEFUL,
        transport_mode=TransportMode.TCP,
        host="127.0.0.1",
        config=config,
    )

    assert graceful.succeeded is True
    assert graceful.endpoint_terminated is False
    assert ping_control_port(
        endpoint.data_port,
        TransportMode.TCP,
        host="127.0.0.1",
        config=config,
    )

    forced = ZMQClient.shutdown_endpoint_on_port(
        endpoint.data_port,
        mode=EndpointShutdownMode.FORCE,
        transport_mode=TransportMode.TCP,
        host="127.0.0.1",
        config=config,
    )

    thread.join(timeout=2)
    assert forced.succeeded is True
    assert forced.endpoint_terminated is True
    assert not thread.is_alive()


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_owned_server_shutdown_removes_exact_ipc_endpoints():
    config = ZMQConfig(
        app_name="zmqruntime-owned-process-test",
        ipc_socket_prefix="owned",
    )
    client = EndpointPolicyExecutionClient(port=45556, config=config)
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
    owned_connection(client, process).terminate_endpoint()

    assert process.poll() is not None
    assert all(not path.exists() for path in paths if path is not None)


def test_owned_server_process_liveness_distinguishes_process_ownership():
    client = EndpointPolicyExecutionClient()
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
    )
    client._connection = owned_connection(client, process)
    try:
        assert client.owned_server_process_is_alive() is True

        client._connection = attached_connection(client)
        assert client.owned_server_process_is_alive() is None
        client._connection = owned_connection(client, process)

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
    client._connection = owned_connection(client, process)

    process.wait(timeout=5)

    assert client.owned_server_process_exit() == ProcessExit(7)
    client._connection = attached_connection(client)
    assert client.owned_server_process_exit() is None


@pytest.mark.skipif(
    platform.system() == "Windows",
    reason="multiprocessing resource tracker is a child process on POSIX",
)
def test_execution_cleanup_preserves_resource_tracker_and_kills_worker():
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json
import subprocess
import sys
from multiprocessing import resource_tracker, shared_memory

import psutil

from zmqruntime.execution.server import ExecutionServer


class ProbeExecutionServer(ExecutionServer):
    def execute_task(self, execution_id, request):
        return {}


first = shared_memory.SharedMemory(create=True, size=1)
tracker_pid = resource_tracker._resource_tracker._pid
worker = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(60)"]
)
try:
    killed = ProbeExecutionServer(port=5555)._kill_worker_processes()
    tracker_alive = psutil.pid_exists(tracker_pid)
    second = shared_memory.SharedMemory(create=True, size=1)
    second.close()
    second.unlink()
    print(
        json.dumps(
            {
                "killed": killed,
                "tracker_alive": tracker_alive,
                "worker_alive": psutil.pid_exists(worker.pid),
            }
        )
    )
finally:
    if worker.poll() is None:
        worker.kill()
        worker.wait(timeout=5)
    first.close()
    first.unlink()
""",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert probe.returncode == 0, probe.stderr
    payload = json.loads(probe.stdout)
    assert payload == {
        "killed": 1,
        "tracker_alive": True,
        "worker_alive": False,
    }
    assert "process died unexpectedly" not in probe.stderr
    assert "KeyError" not in probe.stderr


@pytest.mark.parametrize(
    "command",
    (
        "from multiprocessing.resource_tracker import main; main(5)",
        "from joblib.externals.loky.backend.resource_tracker import main; main(5)",
        "from multiprocessing.semaphore_tracker import main; main(5)",
        "from multiprocessing.forkserver import main; main(5, 6, [], **{})",
        "import napari; napari.run()",
        "from fiji_bridge import main; main()",
    ),
)
def test_execution_process_ownership_preserves_infrastructure_and_viewers(command):
    process = type(
        "FakeProcess",
        (),
        {"cmdline": lambda self: [sys.executable, "-c", command]},
    )()

    assert DummyExecutionServer._is_execution_worker_process(process) is False


def test_execution_process_ownership_identifies_python_worker():
    process = type(
        "FakeProcess",
        (),
        {
            "cmdline": lambda self: [
                sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ]
        },
    )()

    assert DummyExecutionServer._is_execution_worker_process(process) is True


def test_process_exit_describes_exit_codes_and_signals():
    assert ProcessExit(7).describe() == "exit code 7"
    assert ProcessExit(-9).describe() == "signal SIGKILL (-9)"


def test_known_server_process_liveness_includes_identified_local_server():
    client = EndpointPolicyExecutionClient(transport_mode=TransportMode.IPC)
    process_identity = ProcessIdentity.current()
    client._connection = attached_connection(client, process_identity)

    assert client.known_server_process_is_alive() is True

    client._connection = attached_connection(
        client,
        ProcessIdentity(
            pid=process_identity.pid,
            create_time=process_identity.create_time - 1,
        ),
    )
    assert client.known_server_process_is_alive() is False


def test_known_server_process_liveness_leaves_remote_identity_unknown(monkeypatch):
    client = EndpointPolicyExecutionClient(transport_mode=TransportMode.TCP)
    client._connection = attached_connection(client, ProcessIdentity.current())
    monkeypatch.setattr(
        TransportMode.TCP.declaration,
        "endpoint_is_local",
        classmethod(lambda _cls, _host, _port: False),
    )

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
            server_role=ServerRole.EXECUTION,
            process_identity=process_identity,
        ),
    )
    client = EndpointPolicyExecutionClient()

    endpoint = ZMQClient._try_connect_to_existing(client, 5555)
    assert endpoint is not None
    assert endpoint.process_identity == process_identity


def test_disconnect_stops_owned_server_when_socket_cleanup_fails(monkeypatch):
    client = EndpointPolicyExecutionClient()
    process = StubEndpointProcess()
    client._connection = owned_connection(client, process)
    client.persistent = False

    def fail_cleanup():
        raise RuntimeError("socket cleanup failed")

    monkeypatch.setattr(client, "_cleanup_sockets", fail_cleanup)
    with pytest.raises(RuntimeError, match="socket cleanup failed"):
        client.disconnect()

    assert process.stop_count == 1
    assert client._connection is None


class ConcurrentStartupExecutionClient(ExecutionClient):
    def __init__(self, *, port, config, state):
        super().__init__(
            port=port,
            transport_mode=TransportMode.IPC,
            config=config,
        )
        self.state = state

    def _try_connect_to_existing(self, port: int, timeout_ms: int = 500):
        if not self.state["ready"].is_set():
            return None
        return PongResponse(
            port=self.port,
            control_port=self.control_port,
            ready=True,
            server="ConcurrentStartupExecutionClient",
            server_role=ServerRole.EXECUTION,
        )

    def _spawn_server_process(self):
        with self.state["lock"]:
            self.state["spawn_count"] += 1
        return StubEndpointProcess()

    def _wait_for_endpoint_ready(
        self,
        process: EndpointProcess,
        timeout: float = 10.0,
    ):
        for port in (self.port, self.control_port):
            path = get_ipc_socket_path(port, self.config)
            assert path is not None
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        self.state["ready"].set()
        time.sleep(0.1)
        return self._try_connect_to_existing(self.port)

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
        ConcurrentStartupExecutionClient(port=port, config=config, state=state) for _ in range(2)
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
        assert (
            sum(isinstance(client._connection, AttachedEndpointConnection) for client in clients)
            == 1
        )
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


def test_execution_status_declaration_owns_polling_transitions(monkeypatch):
    responses = iter(
        (
            {
                "status": "ok",
                "execution": {"status": "future-status"},
            },
            {
                "status": "ok",
                "execution": {"status": ExecutionStatus.RUNNING.value},
            },
            {
                "status": "ok",
                "execution": {"status": ExecutionStatus.COMPLETED.value},
            },
        )
    )
    transitions = []
    monkeypatch.setattr("zmqruntime.execution.status_poller.time.sleep", lambda _delay: None)

    ExecutionStatusPoller().run(
        "execution-1",
        CallbackExecutionStatusPollPolicy(
            poll_status_fn=lambda _execution_id: next(responses),
            on_running_fn=lambda execution_id, _payload: transitions.append(
                ("running", execution_id)
            ),
            on_terminal_fn=lambda execution_id, status, _payload: transitions.append(
                (status, execution_id)
            ),
        ),
    )

    assert transitions == [
        ("running", "execution-1"),
        ("completed", "execution-1"),
    ]


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
    assert failed_with_both.require_failure_text("submission") == ("bad request (missing plate)")
