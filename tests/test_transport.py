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
    ControlErrorResponse,
    ControlMessageType,
    MessageFields,
    PongResponse,
    ServerRole,
    SocketType,
)
from zmqruntime.server import ZMQServer
from zmqruntime.timeouts import OperationDeadline, OperationTimeoutError
from zmqruntime.transport import (
    DataControlPortPairAuthority,
    TcpDataControlPortPairAuthority,
    TransportEndpoint,
    endpoint_startup_lock,
    get_default_transport_mode,
    get_ipc_socket_path,
    get_zmq_transport_url,
    ipc_socket_is_stale,
    is_port_in_use,
    remove_ipc_socket,
    resolve_transport_mode,
    wait_for_endpoint_ready,
    wait_for_server_ready,
)


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_ipc_startup_lock_obeys_total_operation_deadline() -> None:
    config = ZMQConfig(
        app_name=f"zmqruntime-lock-{uuid.uuid4().hex}",
        ipc_socket_prefix="test",
    )
    port = 45554
    lock_entered = threading.Event()
    release_lock = threading.Event()

    def hold_lock() -> None:
        with endpoint_startup_lock(port, TransportMode.IPC, config):
            lock_entered.set()
            release_lock.wait(timeout=5.0)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert lock_entered.wait(timeout=1.0)
    try:
        deadline = OperationDeadline.after_milliseconds(
            20,
            operation="IPC startup lock",
        )
        with pytest.raises(OperationTimeoutError, match="IPC startup lock"):
            with endpoint_startup_lock(
                port,
                TransportMode.IPC,
                config,
                operation_deadline=deadline,
            ):
                raise AssertionError("contended lock must not be entered")
    finally:
        release_lock.set()
        holder.join(timeout=1.0)
        assert not holder.is_alive()
        socket_path = get_ipc_socket_path(port, config)
        assert socket_path is not None
        socket_path.with_name(f"{socket_path.name}.startup.lock").unlink()
        socket_path.parent.rmdir()


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

    monkeypatch.setattr(
        "zmqruntime.transport_modes.socket.socket",
        lambda *_args: FakeSocket(),
    )

    pair = TcpDataControlPortPairAuthority.acquire(config)

    assert pair.data_port == config.default_port + 1
    assert pair.control_port == (config.default_port + 1 + config.control_port_offset)
    assert attempted_ports == [
        config.default_port,
        config.default_port + config.control_port_offset,
        config.default_port + 1,
        config.default_port + 1 + config.control_port_offset,
    ]


@pytest.mark.skipif(platform.system() == "Windows", reason="IPC is POSIX-only")
def test_data_control_port_pair_authority_uses_ipc_path_occupancy() -> None:
    config = ZMQConfig(
        default_port=47777,
        control_port_offset=1000,
        app_name=f"zmqruntime-pair-{uuid.uuid4().hex}",
    )
    occupied_path = TransportMode.IPC.socket_path(config.default_port, config)
    assert occupied_path is not None
    occupied_path.parent.mkdir(parents=True)
    occupied_path.touch()
    try:
        pair = DataControlPortPairAuthority.acquire(
            config,
            transport_mode=TransportMode.IPC,
        )

        assert pair.data_port == config.default_port + 1
        assert pair.control_port == (config.default_port + 1 + config.control_port_offset)
    finally:
        occupied_path.unlink()
        occupied_path.parent.rmdir()


def test_tcp_port_probe_reports_active_listener_then_ignores_time_wait() -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener,
        socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client,
    ):
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.listen()
        client.connect(("127.0.0.1", port))
        accepted, _address = listener.accept()
        with accepted:
            assert is_port_in_use(port, TransportMode.TCP, host="127.0.0.1")
        listener.close()
        client.close()

    assert not is_port_in_use(port, TransportMode.TCP, host="127.0.0.1")


def test_get_default_transport_mode():
    mode = get_default_transport_mode()
    assert mode in (TransportMode.TCP, TransportMode.IPC)


def test_transport_declarations_own_platform_support_and_default(monkeypatch):
    monkeypatch.setattr("zmqruntime.transport_modes.platform.system", lambda: "Windows")

    assert TransportMode.TCP.is_supported() is True
    assert TransportMode.IPC.is_supported() is False
    assert get_default_transport_mode() is TransportMode.TCP


def test_resolve_transport_mode_preserves_exact_enum_or_uses_default():
    assert resolve_transport_mode(TransportMode.TCP) is TransportMode.TCP
    assert resolve_transport_mode(None) is get_default_transport_mode()


def test_resolve_transport_mode_rejects_textual_mirror():
    with pytest.raises(TypeError, match="TransportMode"):
        resolve_transport_mode("tcp")


def test_transport_endpoint_reports_exact_occupied_pair_ports() -> None:
    config = ZMQConfig(default_port=47777, control_port_offset=1000)
    pair = TcpDataControlPortPairAuthority.acquire(config)
    endpoint = TransportEndpoint(
        host="127.0.0.1",
        port=pair.data_port,
        transport_mode=TransportMode.TCP,
    )

    assert endpoint.port_pair(config) == pair
    assert endpoint.occupied_ports(config) == frozenset()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as control_socket:
        control_socket.bind((endpoint.host, pair.control_port))
        control_socket.listen()

        assert endpoint.occupied_ports(config) == frozenset((pair.control_port,))


def test_server_projects_topology_from_its_single_endpoint_owner() -> None:
    class TestServer(ZMQServer):
        def handle_control_message(self, message):
            raise AssertionError(message)

        def handle_data_message(self, message):
            raise AssertionError(message)

    config = ZMQConfig(default_port=23000, control_port_offset=321)
    server = TestServer(
        config.default_port,
        host="127.0.0.1",
        config=config,
        transport_mode=TransportMode.TCP,
    )

    assert server.endpoint == TransportEndpoint(
        host="127.0.0.1",
        port=23000,
        transport_mode=TransportMode.TCP,
    )
    assert server.port == server.endpoint.port
    assert server.host == server.endpoint.host
    assert server.control_port == server.endpoint.control_port(config)
    assert server.data_transport_url() == "tcp://127.0.0.1:23000"
    assert server.control_transport_url() == "tcp://127.0.0.1:23321"


def test_socket_type_rejects_unknown_zmq_constant():
    with pytest.raises(ValueError, match="Unsupported ZMQ socket type"):
        SocketType.from_zmq_constant(object())


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


def test_control_response_payload_serializes_nominal_ping_response():
    class TestServer(ZMQServer):
        def handle_control_message(self, message):
            raise AssertionError(message)

        def handle_data_message(self, message):
            raise AssertionError(message)

    server = TestServer(5555)
    payload = pickle.loads(
        server.control_response_payload(
            {MessageFields.TYPE: ControlMessageType.PING.value},
        )
    )

    assert PongResponse.from_dict(payload).server_role is ServerRole.GENERIC


def test_control_failure_is_owned_by_nominal_wire_response():
    class TestServer(ZMQServer):
        def handle_control_message(self, message):
            raise RuntimeError("broken control request")

        def handle_data_message(self, message):
            raise AssertionError(message)

    server = TestServer(5555)
    response = server.control_response({MessageFields.TYPE: "unknown"})

    assert isinstance(response, ControlErrorResponse)
    assert pickle.loads(server.serialize_control_response(response)) == {
        MessageFields.STATUS: "error",
        MessageFields.TYPE: "error",
        MessageFields.MESSAGE: "broken control request",
    }


def test_serialization_failure_uses_the_same_nominal_error_projection():
    class TestServer(ZMQServer):
        def handle_control_message(self, message):
            raise AssertionError(message)

        def handle_data_message(self, message):
            raise AssertionError(message)

    payload = pickle.loads(TestServer(5555).serialize_control_response(lambda: None))

    assert payload == ControlErrorResponse(message="Internal server serialization error").to_dict()


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
        TransportMode.IPC,
        "endpoint_in_use",
        lambda _port, _host, _config: True,
    )

    def ping(endpoint, config, *, timeout_ms):
        ping_calls.append((endpoint, config, timeout_ms))
        if len(ping_calls) < 3:
            return None
        return PongResponse(
            port=endpoint.port,
            control_port=endpoint.control_port(config),
            ready=True,
            server="ReadinessTestServer",
            server_role=ServerRole.GENERIC,
        )

    monkeypatch.setattr(TransportEndpoint, "ping", ping)

    assert wait_for_server_ready(
        5555,
        TransportMode.IPC,
        timeout=2.5,
        poll_interval=0.001,
    )
    assert len(ping_calls) == 3
    assert all(1 <= call[2] <= 2500 for call in ping_calls)
    assert ping_calls[0][2] > 250


def test_startup_activity_cannot_extend_an_operation_deadline(monkeypatch):
    class AlwaysActiveObserver:
        def poll_activity(self) -> bool:
            return True

        def should_abort(self) -> bool:
            return False

    monkeypatch.setattr(
        TransportMode.TCP,
        "endpoint_in_use",
        lambda _port, _host, _config: False,
    )
    observer = AlwaysActiveObserver()
    operation_deadline = OperationDeadline.after_milliseconds(
        20,
        operation="endpoint startup",
    )
    started = time.monotonic()

    ready = wait_for_server_ready(
        5555,
        TransportMode.TCP,
        timeout=1.0,
        poll_interval=0.001,
        startup_observer=observer,
        operation_deadline=operation_deadline,
    )

    assert ready is False
    assert time.monotonic() - started < 0.2


def test_wait_for_endpoint_ready_returns_the_authoritative_handshake(monkeypatch):
    endpoint_response = PongResponse(
        port=5555,
        control_port=6555,
        ready=True,
        server="ReadinessTestServer",
        server_role=ServerRole.GENERIC,
    )
    monkeypatch.setattr(
        TransportMode.IPC,
        "endpoint_in_use",
        lambda _port, _host, _config: True,
    )
    monkeypatch.setattr(
        TransportEndpoint,
        "ping",
        lambda _endpoint, _config, *, timeout_ms: endpoint_response,
    )

    assert (
        wait_for_endpoint_ready(
            5555,
            TransportMode.IPC,
            timeout=1.0,
            poll_interval=0.001,
        )
        is endpoint_response
    )


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
                        server_role=ServerRole.GENERIC,
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
