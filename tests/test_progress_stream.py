import json

from zmqruntime.execution.progress_stream import ProgressStreamSubscriber


def _message() -> str:
    return json.dumps(
        {
            "type": "progress",
            "execution_id": "exec-1",
            "phase": "running",
            "status": "running",
            "percent": 50.0,
            "timestamp": 1.0,
            "completed": 1,
            "total": 2,
            "plate_id": "plate-1",
            "axis_id": "A01",
        }
    )


def test_dispatch_message_validates_and_delivers_payload():
    received = []
    subscriber = ProgressStreamSubscriber(lambda: None, received.append)

    assert subscriber._dispatch_message(_message()) is True
    assert received[0]["execution_id"] == "exec-1"


def test_dispatch_message_isolates_callback_failure_from_listener_loop():
    def fail(_payload):
        raise RuntimeError("consumer failed")

    subscriber = ProgressStreamSubscriber(lambda: None, fail)

    assert subscriber._dispatch_message(_message()) is False


def test_dispatch_message_rejects_malformed_payload_without_escaping():
    subscriber = ProgressStreamSubscriber(lambda: None, lambda _payload: None)

    assert subscriber._dispatch_message("not-json") is False
    assert subscriber._dispatch_message("{}") is False
