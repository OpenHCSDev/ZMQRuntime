import pickle
import pytest

from zmqruntime.execution.client import ExecutionClient
from zmqruntime.execution.responses import ExecutionSubmissionResponse
from zmqruntime.execution.server import ExecutionServer
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.config import TransportMode
from zmqruntime.messages import (
    ControlMessageType,
    ExecuteRequest,
    ExecutionStatus,
    MessageFields,
    ResponseType,
    TaskProgress,
)


class DummyExecutionServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        return {"result": 1}


class FailingExecutionServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        raise RuntimeError("boom")


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
    def __init__(self, *, transport_mode=TransportMode.IPC, process_exists=False):
        super().__init__(
            port=5555,
            transport_mode=transport_mode,
        )
        self.process_exists = process_exists
        self.killed_ports = []
        self.spawned = False
        self.setup_called = False

    def _is_port_in_use(self, port: int):
        return True

    def _try_connect_to_existing(self, port: int, timeout_ms: int = 500):
        return False

    def _ipc_server_process_exists(self, port: int):
        return self.process_exists

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
    client = EndpointPolicyExecutionClient(process_exists=True)

    connected = client.connect(timeout=1)

    assert connected is False
    assert client.killed_ports == []
    assert client.spawned is False
    assert client.setup_called is False


def test_ipc_connect_removes_stale_endpoint_before_spawning():
    client = EndpointPolicyExecutionClient(process_exists=False)

    connected = client.connect(timeout=1)

    assert connected is True
    assert client.killed_ports == [client.port, client.control_port]
    assert client.spawned is True
    assert client.setup_called is True


def test_tcp_connect_keeps_existing_spawn_cleanup_policy():
    client = EndpointPolicyExecutionClient(
        transport_mode=TransportMode.TCP,
        process_exists=True,
    )

    connected = client.connect(timeout=1)

    assert connected is True
    assert client.killed_ports == [client.port, client.control_port]
    assert client.spawned is True
    assert client.setup_called is True


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
