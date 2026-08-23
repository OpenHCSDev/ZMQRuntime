import json
import threading
import time

import pytest

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


def test_stop_retains_live_listener_ownership_after_timeout():
    callback_started = threading.Event()
    release_callback = threading.Event()

    class OneMessageSocket:
        def __init__(self):
            self.sent = False

        def recv_string(self, _flags):
            if self.sent:
                import zmq

                raise zmq.Again()
            self.sent = True
            return _message()

    socket = OneMessageSocket()

    def blocking_callback(_payload):
        callback_started.set()
        release_callback.wait(timeout=1.0)

    subscriber = ProgressStreamSubscriber(lambda: socket, blocking_callback)
    subscriber.start()
    assert callback_started.wait(timeout=1.0)
    owned_thread = subscriber._thread

    with pytest.raises(TimeoutError, match="Progress listener did not stop"):
        subscriber.stop(timeout=0.01)

    assert subscriber._thread is owned_thread
    subscriber.start()
    assert subscriber._thread is owned_thread

    release_callback.set()
    deadline = time.monotonic() + 1.0
    while subscriber._thread is not None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert subscriber._thread is None
