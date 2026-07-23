import pickle
import platform
import socket
import threading
import time
import uuid

import pytest
import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ControlMessageType,
    MessageFields,
    PongResponse,
)
from zmqruntime.server import ZMQServer
from zmqruntime.transport import (
    TcpDataControlPortPairAuthority,
    get_default_transport_mode,
    get_ipc_socket_path,
    get_zmq_transport_url,
    ipc_socket_is_stale,
    remove_ipc_socket,
    wait_for_server_ready,
)


def test_tcp_port_pair_authority_returns_free_configured_pair():
    config = ZMQConfig(default_port=47777, control_port_offset=1000)

    pair = TcpDataControlPortPairAuthority.acquire(config)

    assert pair.control_port == pair.data_port + config.control_port_offset
    assert pair.ports == frozenset((pair.data_port, pair.control_port))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as data_socket:
        data_socket.bind(("127.0.0.1", pair.data_port))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as control_socket:
        control_socket.bind(("127.0.0.1", pair.control_port))


def test_tcp_port_pair_authority_scans_both_ports_together(monkeypatch):
    config = ZMQConfig(default_port=47777, control_port_offset=1000)
    attempted_ports = []

    class FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def bind(self, address):
            port = int(address[1])
            attempted_ports.append(port)
            if port == config.default_port + config.control_port_offset:
                raise OSError("simulated reserved control port")

    monkeypatch.setattr("zmqruntime.transport.socket.socket", lambda *_args: FakeSocket())

    pair = TcpDataControlPortPairAuthority.acquire(config)

    assert pair.data_port == config.default_port + 1
    assert pair.control_port == (
        config.default_port + 1 + config.control_port_offset
    )
    assert attempted_ports == [
        config.default_port,
        config.default_port + config.control_port_offset,
        config.default_port + 1,
        config.default_port + 1 + config.control_port_offset,
    ]


def test_get_default_transport_mode():
    mode = get_default_transport_mode()
    assert mode in (TransportMode.TCP, TransportMode.IPC)


def test_control_response_payload_accepts_an_external_dispatch_owner():
    class TestServer(ZMQServer):
        def __init__(self):
            super().__init__(5555)
            self.handled_messages = []

        def handle_control_message(self, message):
            self.handled_messages.append(message)
            return {"status": "unexpected"}

        def handle_data_message(self, message):
            raise AssertionError(message)

    server = TestServer()
    payload = server.control_response_payload(
        {MessageFields.TYPE: "thread_owned"},
        response_factory=lambda: {"status": "success", "owner": "transport"},
    )

    assert pickle.loads(payload) == {
        "status": "success",
        "owner": "transport",
    }
    assert server.handled_messages == []


def test_get_zmq_transport_url_tcp():
    url = get_zmq_transport_url(5555, host="localhost", mode=TransportMode.TCP)
    assert url == "tcp://localhost:5555"


def test_ipc_socket_path_and_url():
    config = ZMQConfig(app_name="zmqruntime-test", ipc_socket_prefix="test")
    if platform.system() == "Windows":
        assert get_ipc_socket_path(5555, config) is None
        with pytest.raises(ValueError):
            get_zmq_transport_url(5555, mode=TransportMode.IPC, config=config)
        return

    path = get_ipc_socket_path(5555, config)
    assert path is not None
    assert str(path).endswith(".sock")
    url = get_zmq_transport_url(5555, mode=TransportMode.IPC, config=config)
    assert url.startswith("ipc://")


def test_remove_ipc_socket(tmp_path):
    config = ZMQConfig(app_name="zmqruntime-test", ipc_socket_prefix="test")
    if platform.system() == "Windows":
        assert remove_ipc_socket(5555, config) is False
        return

    socket_path = get_ipc_socket_path(5555, config)
    assert socket_path is not None
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path.write_text("test")
    assert socket_path.exists()
    assert remove_ipc_socket(5555, config) is True
    assert not socket_path.exists()


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_ipc_socket_staleness_uses_kernel_socket_ownership():
    config = ZMQConfig(
        app_name=f"zmqruntime-stale-{uuid.uuid4().hex}",
        ipc_socket_prefix="test",
    )
    port = 45555
    socket_path = get_ipc_socket_path(port, config)
    assert socket_path is not None
    remove_ipc_socket(port, config)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(get_zmq_transport_url(port, mode=TransportMode.IPC, config=config))
    try:
        assert socket_path.exists()
        assert ipc_socket_is_stale(port, config) is False
    finally:
        socket.close(linger=0)
        context.term()

    socket_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path.touch()
    try:
        assert ipc_socket_is_stale(port, config) is True
    finally:
        remove_ipc_socket(port, config)


def test_wait_for_server_ready_retries_until_server_reports_ready(
    monkeypatch,
):
    ping_calls = []

    monkeypatch.setattr(
        "zmqruntime.transport.is_port_in_use",
        lambda *args, **kwargs: True,
    )

    def ping(*args, **kwargs):
        ping_calls.append((args, kwargs))
        return len(ping_calls) == 3

    monkeypatch.setattr("zmqruntime.transport.ping_control_port", ping)

    assert wait_for_server_ready(
        5555,
        TransportMode.IPC,
        timeout=2.5,
        poll_interval=0.001,
    )
    assert len(ping_calls) == 3
    assert all(1 <= call[1]["timeout_ms"] <= 2500 for call in ping_calls)
    assert ping_calls[0][1]["timeout_ms"] > 250
    assert all(call[1]["require_ready"] is True for call in ping_calls)


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_wait_for_server_ready_retains_request_for_delayed_ready_reply():
    config = ZMQConfig(
        app_name=f"zmqruntime-ready-{uuid.uuid4().hex}",
        ipc_socket_prefix="test",
    )
    port = 45557
    control_port = port + config.control_port_offset
    server_started = threading.Event()
    release_server = threading.Event()
    server_errors = []

    def delayed_ready_reply():
        context = zmq.Context()
        data_socket = context.socket(zmq.REP)
        control_socket = context.socket(zmq.REP)
        try:
            data_socket.bind(
                get_zmq_transport_url(
                    port,
                    mode=TransportMode.IPC,
                    config=config,
                )
            )
            control_socket.bind(
                get_zmq_transport_url(
                    control_port,
                    mode=TransportMode.IPC,
                    config=config,
                )
            )
            server_started.set()
            if not control_socket.poll(2000, zmq.POLLIN):
                raise TimeoutError("Readiness test server received no control PING")
            request = pickle.loads(control_socket.recv())
            assert request == {
                MessageFields.TYPE: ControlMessageType.PING.value,
            }
            time.sleep(0.4)
            control_socket.send(
                pickle.dumps(
                    PongResponse(
                        port=port,
                        control_port=control_port,
                        ready=True,
                        server="DelayedReadyTestServer",
                    ).to_dict()
                )
            )
            release_server.wait(timeout=2.0)
        except Exception as error:
            server_errors.append(error)
        finally:
            server_started.set()
            data_socket.close(linger=0)
            control_socket.close(linger=0)
            context.term()

    server_thread = threading.Thread(target=delayed_ready_reply)
    server_thread.start()
    try:
        assert server_started.wait(timeout=1.0)
        assert not server_errors
        assert wait_for_server_ready(
            port,
            TransportMode.IPC,
            config=config,
            timeout=1.5,
            poll_interval=0.01,
        )
    finally:
        release_server.set()
        server_thread.join(timeout=3.0)
        remove_ipc_socket(port, config)
        remove_ipc_socket(control_port, config)
    assert not server_thread.is_alive()
    assert not server_errors
