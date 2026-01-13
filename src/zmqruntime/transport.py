"""Transport utilities for ZMQ communication."""
from __future__ import annotations

import pickle
import platform
import socket
import time
from pathlib import Path
from typing import Optional

import zmq

from zmqruntime.config import TransportMode, ZMQConfig

_default_config = ZMQConfig()


def get_default_transport_mode() -> TransportMode:
    """Get platform-appropriate transport mode."""
    return TransportMode.TCP if platform.system() == "Windows" else TransportMode.IPC


def coerce_transport_mode(transport_mode) -> TransportMode | None:
    """Normalize transport mode inputs to a zmqruntime TransportMode."""
    if transport_mode is None:
        return None
    if isinstance(transport_mode, TransportMode):
        return transport_mode
    try:
        value = transport_mode.value
    except Exception:
        value = transport_mode
    try:
        return TransportMode(value)
    except Exception:
        try:
            return TransportMode(str(value))
        except Exception:
            return None


def get_ipc_socket_path(port: int, config: ZMQConfig | None = None) -> Optional[Path]:
    """Get IPC socket path for a given port (Unix/Mac only)."""
    config = config or _default_config
    if platform.system() == "Windows":
        return None
    ipc_dir = Path.home() / f".{config.app_name}" / config.ipc_socket_dir
    socket_name = f"{config.ipc_socket_prefix}-{port}{config.ipc_socket_extension}"
    return ipc_dir / socket_name


def get_zmq_transport_url(
    port: int,
    host: str = "localhost",
    mode: TransportMode | None = None,
    config: ZMQConfig | None = None,
) -> str:
    """Get ZMQ transport URL for given port/host/mode."""
    config = config or _default_config
    mode = coerce_transport_mode(mode) or get_default_transport_mode()
    if mode == TransportMode.IPC:
        if platform.system() == "Windows":
            raise ValueError(
                "IPC transport mode is not supported on Windows. "
                "Use TransportMode.TCP instead, or use get_default_transport_mode()."
            )
        socket_path = get_ipc_socket_path(port, config)
        if socket_path is None:
            raise ValueError("IPC socket path could not be determined.")
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        return f"ipc://{socket_path}"
    if mode == TransportMode.TCP:
        return f"tcp://{host}:{port}"
    raise ValueError(f"Invalid transport mode: {mode}")


def get_control_port(port: int, config: ZMQConfig | None = None) -> int:
    """Get control port for a data port."""
    config = config or _default_config
    return port + config.control_port_offset


def get_control_url(
    port: int,
    transport_mode,
    host: str = "localhost",
    config: ZMQConfig | None = None,
) -> str:
    """Get control socket URL for a given data port."""
    config = config or _default_config
    mode = coerce_transport_mode(transport_mode) or get_default_transport_mode()
    return get_zmq_transport_url(
        get_control_port(port, config),
        host=host,
        mode=mode,
        config=config,
    )


def remove_ipc_socket(port: int, config: ZMQConfig | None = None) -> bool:
    """Remove stale IPC socket file."""
    socket_path = get_ipc_socket_path(port, config)
    if socket_path and socket_path.exists():
        socket_path.unlink()
        return True
    return False


def is_port_in_use(
    port: int,
    transport_mode,
    host: str = "localhost",
    config: ZMQConfig | None = None,
) -> bool:
    """Check whether the given port is in use for the chosen transport."""
    config = config or _default_config
    mode = coerce_transport_mode(transport_mode) or get_default_transport_mode()
    if mode == TransportMode.IPC:
        socket_path = get_ipc_socket_path(port, config)
        return socket_path.exists() if socket_path else False

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.1)
    try:
        sock.bind((host, port))
        return False
    except OSError:
        return True
    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def ping_control_port(
    port: int,
    transport_mode,
    host: str = "localhost",
    config: ZMQConfig | None = None,
    timeout_ms: int = 500,
    require_ready: bool = True,
) -> bool:
    """Ping the control socket for a given data port."""
    config = config or _default_config
    control_url = get_control_url(port, transport_mode, host=host, config=config)
    ctx = None
    sock = None
    try:
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        sock.connect(control_url)
        sock.send(pickle.dumps({"type": "ping"}))
        response = pickle.loads(sock.recv())
        if response.get("type") != "pong":
            return False
        if require_ready:
            return bool(response.get("ready"))
        return True
    except Exception:
        return False
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        if ctx is not None:
            try:
                ctx.term()
            except Exception:
                pass


def wait_for_server_ready(
    port: int,
    transport_mode,
    host: str = "localhost",
    config: ZMQConfig | None = None,
    timeout: float = 10.0,
    require_ready: bool = True,
    poll_interval: float = 0.2,
) -> bool:
    """Wait for a server to bind its data/control sockets and respond to ping."""
    config = config or _default_config
    start_time = time.time()
    control_port = get_control_port(port, config)

    while time.time() - start_time < timeout:
        if is_port_in_use(port, transport_mode, host=host, config=config) and is_port_in_use(
            control_port,
            transport_mode,
            host=host,
            config=config,
        ):
            break
        time.sleep(poll_interval)
    else:
        return False

    start_time = time.time()
    while time.time() - start_time < timeout:
        if ping_control_port(
            port,
            transport_mode,
            host=host,
            config=config,
            timeout_ms=1000,
            require_ready=require_ready,
        ):
            return True
        time.sleep(0.5)

    return False
