import pytest

from zmqruntime.messages import (
    CancelRequest,
    ExecuteRequest,
    MessageFields,
    ProcessIdentity,
    PongResponse,
    ResponseType,
)


def test_execute_request_roundtrip():
    request = ExecuteRequest(
        plate_id="plate-1",
        pipeline_code="print('hi')",
        config_params={"a": 1},
        client_address="127.0.0.1",
    )
    data = request.to_dict()
    assert data[MessageFields.TYPE] == "execute"
    roundtrip = ExecuteRequest.from_dict(data)
    assert roundtrip.plate_id == "plate-1"
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
        progress_subscribers=2,
        process_identity=process_identity,
    )
    data = pong.to_dict()
    assert data[MessageFields.TYPE] == ResponseType.PONG.value
    assert data[MessageFields.PORT] == 5555
    assert data[MessageFields.SERVER_TYPE] == "test"
    assert data[MessageFields.PROGRESS_SUBSCRIBERS] == 2
    parsed = PongResponse.from_dict(data)
    assert parsed.server_type == "test"
    assert parsed.process_identity == process_identity
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
