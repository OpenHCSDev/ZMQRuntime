import platform
import uuid

import pytest
import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.transport import (
    get_default_transport_mode,
    get_ipc_socket_path,
    get_zmq_transport_url,
    ipc_socket_is_stale,
    remove_ipc_socket,
)


def test_get_default_transport_mode():
    mode = get_default_transport_mode()
    assert mode in (TransportMode.TCP, TransportMode.IPC)


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
