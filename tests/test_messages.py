from dataclasses import dataclass

import pytest
from metaclass_registry import AutoRegisterMeta

from zmqruntime.client import EndpointClientSession, EndpointCompatibilityClientABC
from zmqruntime.messages import (
    CancelRequest,
    ControlMessageType,
    ControlRequestHeader,
    EndpointApplication,
    EndpointApplicationCompatibilityError,
    ExecuteRequest,
    MessageFields,
    PongResponse,
    ProcessIdentity,
    ProcessResourceUsage,
    ResponseType,
    ServerRole,
    TaskProgress,
    WorkerState,
)


def test_control_request_header_owns_dict_and_pickle_wire_round_trip() -> None:
    request = ControlRequestHeader(ControlMessageType.PING)

    assert ControlRequestHeader.from_dict(request.to_dict()) == request
    assert ControlRequestHeader.from_wire_payload(request.to_wire_payload()) == request


def test_endpoint_application_owns_exact_compatibility() -> None:
    expected = EndpointApplication(identifier="test-app", version="1.2.3")

    compatible = expected.compatibility_with(
        EndpointApplication(identifier="test-app", version="1.2.3")
    )
    incompatible = expected.compatibility_with(
        EndpointApplication(identifier="test-app", version="1.2.2")
    )
    missing = expected.compatibility_with(None)

    assert compatible.matches is True
    compatible.require_match()
    assert incompatible.matches is False
    assert incompatible.observed_version_label == "1.2.2"
    assert missing.observed_version_label == "not reported"
    with pytest.raises(
        EndpointApplicationCompatibilityError,
        match=("expected test-app 1.2.3, observed test-app 1.2.2"),
    ):
        incompatible.require_match()


def test_endpoint_client_session_exposes_only_an_admitted_client() -> None:
    expected = EndpointApplication(identifier="test-app", version="1.2.3")

    class Client(EndpointCompatibilityClientABC):
        def __init__(self, observed: EndpointApplication) -> None:
            self.observed = observed

        def endpoint_compatibility(self):
            return expected.compatibility_with(self.observed)

    admitted_client = Client(expected)
    admitted_session = EndpointClientSession(admitted_client)
    assert admitted_session.admitted_client is None
    admitted_session.observe_compatibility()
    assert admitted_session.require_admitted_client() is admitted_client
    assert admitted_session.admitted_client is admitted_client

    foreign_client = Client(EndpointApplication("foreign", "1.2.3"))
    foreign_session = EndpointClientSession(foreign_client)
    foreign_session.observe_compatibility()
    assert foreign_session.admitted_client is None
    with pytest.raises(EndpointApplicationCompatibilityError):
        foreign_session.require_admitted_client()


def test_execute_request_roundtrip():
    request = ExecuteRequest(
        subject_id="subject-1",
        pipeline_code="print('hi')",
        config_params={"a": 1},
        client_address="127.0.0.1",
    )
    data = request.to_dict()
    assert data[MessageFields.TYPE] == "execute"
    roundtrip = ExecuteRequest.from_dict(data)
    assert roundtrip.subject_id == "subject-1"
    assert roundtrip.config_params == {"a": 1}


def test_cancel_request_roundtrip():
    request = CancelRequest(execution_id="exec-1")
    data = request.to_dict()
    assert data[MessageFields.TYPE] == "cancel"
    roundtrip = CancelRequest.from_dict(data)
    assert roundtrip.execution_id == "exec-1"


def test_pong_response_dict():
    process_identity = ProcessIdentity.current()
    pong = PongResponse(
        port=5555,
        control_port=6555,
        ready=True,
        server="Test",
        server_type="test",
        server_role=ServerRole.VIEWER,
        progress_subscribers=2,
        process_identity=process_identity,
        process_usage=ProcessResourceUsage(memory_mb=12.5, cpu_percent=3.0),
        application=EndpointApplication(identifier="test-app", version="1.2.3"),
    )
    data = pong.to_dict()
    assert data[MessageFields.TYPE] == ResponseType.PONG.value
    assert data[MessageFields.PORT] == 5555
    assert data[MessageFields.SERVER_TYPE] == "test"
    assert data[MessageFields.PROGRESS_SUBSCRIBERS] == 2
    parsed = PongResponse.from_dict(data)
    assert parsed.server_type == "test"
    assert parsed.server_role is ServerRole.VIEWER
    assert parsed.process_usage == ProcessResourceUsage(
        memory_mb=12.5,
        cpu_percent=3.0,
    )
    assert parsed.process_identity == process_identity
    assert parsed.application == EndpointApplication(
        identifier="test-app",
        version="1.2.3",
    )
    assert process_identity.is_alive() is True


def test_pong_response_rejects_legacy_running_execution_shape():
    payload = {
        MessageFields.TYPE: ResponseType.PONG.value,
        MessageFields.PORT: 5555,
        MessageFields.CONTROL_PORT: 6555,
        MessageFields.READY: True,
        MessageFields.SERVER: "ExecutionServer",
        MessageFields.RUNNING_EXECUTIONS: ["legacy-exec-id"],
    }
    with pytest.raises(TypeError):
        PongResponse.from_dict(payload)


def test_pong_response_requires_protocol_discriminator():
    payload = {
        MessageFields.PORT: 5555,
        MessageFields.CONTROL_PORT: 6555,
        MessageFields.READY: True,
        MessageFields.SERVER: "ExecutionServer",
        MessageFields.SERVER_ROLE: ServerRole.EXECUTION.value,
    }

    with pytest.raises(ValueError):
        PongResponse.from_dict(payload)


def test_pong_response_rejects_wrong_protocol_discriminator():
    payload = {
        MessageFields.TYPE: ResponseType.ERROR.value,
        MessageFields.PORT: 5555,
        MessageFields.CONTROL_PORT: 6555,
        MessageFields.READY: True,
        MessageFields.SERVER: "ExecutionServer",
        MessageFields.SERVER_ROLE: ServerRole.EXECUTION.value,
    }

    with pytest.raises(ValueError, match="requires type='pong'"):
        PongResponse.from_dict(payload)


def test_pong_response_requires_explicit_server_role():
    payload = {
        MessageFields.TYPE: ResponseType.PONG.value,
        MessageFields.PORT: 5555,
        MessageFields.CONTROL_PORT: 6555,
        MessageFields.READY: True,
        MessageFields.SERVER: "ExecutionServer",
    }

    with pytest.raises(ValueError):
        PongResponse.from_dict(payload)


def test_worker_state_projects_declared_process_identity_without_metadata_bag():
    state = WorkerState(
        pid=123,
        status="running",
        cpu_percent=4.0,
        memory_mb=256.0,
        create_time=12.5,
    )

    assert WorkerState.from_dict(state.to_dict()) == state
    with pytest.raises(ValueError, match="undeclared field"):
        WorkerState.from_dict({**state.to_dict(), "legacy_extra": "value"})


def _declared_message_examples():
    import uuid

    from zmqruntime import messages

    identity = ProcessIdentity.current()
    route = messages.AckReturnRoute("tcp://127.0.0.1:1", str(uuid.uuid4()), identity)
    record = messages.ExecutionRecord("e-1", "subject-1", "running", start_time=1.0)
    running = messages.RunningExecutionInfo("e-1", "subject-1", 1.0, 2.0)
    queued = messages.QueuedExecutionInfo("e-2", "subject-1", 1)
    worker = WorkerState(1, "running", 0.5, 10.0, create_time=3.0)
    return (
        worker,
        identity,
        route,
        record,
        running,
        queued,
        ProcessResourceUsage(1.0, 2.0),
        EndpointApplication("app", "1.0"),
        messages.EndpointLaunch("/code", identity, "owner_only"),
        messages.ImageTransferIdentity("image-1", route, identity),
        messages.ImageAck("image-1", 1, "viewer", return_route=route, producer=identity),
        messages.TaskProgress("e-1", "running", "running", 10.0, 1.0, 1, 10, subject_id="s"),
        messages.ControlErrorResponse("boom"),
        messages.ExecuteResponse(ResponseType.ACCEPTED, execution_id="e-1"),
        messages.CancelResponse(ResponseType.OK, 2, message="cancelled"),
        messages.ShutdownAck("stopping"),
        messages.ProgressSubscriptionResponse("client", 1, "registered", {"e-1": {"x": 1}}),
        messages.ExecutionStatusSnapshot(
            ResponseType.OK,
            execution=record,
            running_executions=(running,),
            queued_executions=(queued,),
            executions=("e-1",),
        ),
        PongResponse(
            port=1,
            control_port=2,
            ready=True,
            server="server",
            server_role=ServerRole.EXECUTION,
            workers=(worker,),
            running_executions=(running,),
            process_identity=identity,
            process_usage=ProcessResourceUsage(1.0, 2.0),
            application=EndpointApplication("app", "1.0"),
            launch=messages.EndpointLaunch("/code", identity, "any_session"),
        ),
        ControlRequestHeader(ControlMessageType.PING),
        messages.EndpointShutdownRequest(ControlMessageType.SHUTDOWN, identity),
        ExecuteRequest(subject_id="subject-1", pipeline_code="pass", config_params={"a": [1]}),
        messages.StatusRequest(execution_id="e-1"),
        CancelRequest(execution_id="e-1"),
        messages.ProgressRegistrationRequest(client_id="client"),
        messages.ProgressUnregistrationRequest(client_id="client"),
    )


def _concrete_declared_messages():
    from dataclasses import is_dataclass

    from zmqruntime import messages

    return {
        value
        for value in vars(messages).values()
        if isinstance(value, type)
        and value.__module__ == messages.__name__
        and is_dataclass(value)
        and issubclass(value, (messages.WireMessage, ControlRequestHeader))
    }


def test_every_declared_message_round_trips_through_its_declaration() -> None:
    examples = _declared_message_examples()

    assert {type(example) for example in examples} == _concrete_declared_messages()
    for example in examples:
        assert type(example).from_dict(example.to_dict()) == example


def test_typed_messages_reject_another_kind() -> None:
    from zmqruntime import messages

    with pytest.raises(ValueError, match="type='image_ack'"):
        messages.ImageAck.from_dict(messages.ShutdownAck("stopping").to_dict())
    with pytest.raises(ValueError, match="message_type"):
        messages.StatusRequest.from_dict(CancelRequest(execution_id="e-1").to_dict())


class _Phase(metaclass=AutoRegisterMeta):
    __registry_key__ = "name"
    __skip_if_no_key__ = True
    name = None


class _Compiling(_Phase):
    name = "compiling"


@dataclass(frozen=True)
class _ApplicationProgress(TaskProgress):
    phase: type[_Phase]
    status: type[_Phase]


def test_task_progress_carries_the_application_phase_vocabulary() -> None:
    event = _ApplicationProgress("e-1", _Compiling, _Compiling, 10.0, 1.0, 1, 10)
    wire = event.to_dict()

    assert wire["phase"] == wire["status"] == "compiling"
    assert TaskProgress.from_dict(wire).phase == "compiling"


@dataclass(frozen=True)
class _ByName:
    name: str


@dataclass(frozen=True)
class _ByAlias:
    name: str


@dataclass(frozen=True, slots=True)
class _SelectRequest(ControlRequestHeader):
    selector: _ByName | _ByAlias


def test_request_body_field_admitting_several_classes_is_tagged_on_the_wire() -> None:
    request = _SelectRequest(ControlMessageType.STATUS, _ByAlias("a"))

    payload = request.to_dict()

    assert payload == {"type": "status", "selector": {"_ByAlias": {"name": "a"}}}
    assert _SelectRequest.from_dict(payload) == request
    assert _SelectRequest.from_wire_payload(request.to_wire_payload()) == request
    with pytest.raises(ValueError, match="_ByName"):
        _SelectRequest.from_dict({"type": "status", "selector": {"Unknown": {"name": "a"}}})


@dataclass(frozen=True)
class _NamedProcessIdentity(ProcessIdentity):
    name: str = ""


def test_field_declared_as_one_class_refuses_a_subclass_with_more_fields() -> None:
    from zmqruntime import messages

    launch = messages.EndpointLaunch("/code", _NamedProcessIdentity(1, 2.0, "n"), "any_session")

    with pytest.raises(ValueError, match="name"):
        messages.EndpointLaunch.from_dict(launch.to_dict())
