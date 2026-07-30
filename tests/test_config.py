import pytest

from zmqruntime.config import TransportMode, ZMQConfig


def test_transport_mode_values():
    assert TransportMode.TCP.value == "tcp"
    assert TransportMode.IPC.value == "ipc"
    assert TransportMode.optional_from_text("tcp") is TransportMode.TCP
    assert TransportMode.optional_from_text(None) is None
    assert TransportMode.optional_to_text(TransportMode.IPC) == "ipc"
    assert TransportMode.optional_to_text(None) is None


def test_zmq_config_defaults():
    config = ZMQConfig()
    assert config.control_port_offset == 1000
    assert config.default_port == 7777
    assert config.ipc_socket_dir == "ipc"
    assert config.ipc_socket_prefix == "zmq"
    assert config.ipc_socket_extension == ".sock"
    assert config.shared_ack_port == 7555
    assert config.app_name == "zmqruntime"


def test_zmq_config_rejects_invalid_declared_values():
    with pytest.raises(ValueError, match="default_port must be at least 1"):
        ZMQConfig(default_port=0)

    with pytest.raises(ValueError, match="ipc_socket_dir does not satisfy strip"):
        ZMQConfig(ipc_socket_dir=" ")

    with pytest.raises(
        ValueError,
        match="default_port plus control_port_offset must not exceed",
    ):
        ZMQConfig(default_port=65000, control_port_offset=1000)
