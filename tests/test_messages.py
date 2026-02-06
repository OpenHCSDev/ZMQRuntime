from zmqruntime.messages import (
    CancelRequest,
    ExecuteRequest,
    MessageFields,
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
    pong = PongResponse(
        port=5555,
        control_port=6555,
        ready=True,
        server="Test",
        progress_subscribers=2,
    )
    data = pong.to_dict()
    assert data[MessageFields.TYPE] == ResponseType.PONG.value
    assert data[MessageFields.PORT] == 5555
    assert data[MessageFields.PROGRESS_SUBSCRIBERS] == 2
