"""Nominal transport declarations with member-owned endpoint behavior."""

from __future__ import annotations

import platform
import socket
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from ipaddress import ip_address
from pathlib import Path

from .timeouts import OperationDeadline


class _TransportConfigBase:
    """Nominal configuration surface consumed by transport declarations."""

    app_name: str
    ipc_socket_dir: str
    ipc_socket_prefix: str
    ipc_socket_extension: str


TransportSupport = Callable[[], bool]
TransportUrlBuilder = Callable[[int, str, _TransportConfigBase], str]
TransportOccupancyProbe = Callable[[int, str, _TransportConfigBase], bool]
TransportLocalityProbe = Callable[[str, int], bool]
TransportEndpointCleanup = Callable[[int, _TransportConfigBase], bool]
TransportPreservationPolicy = Callable[[int, _TransportConfigBase], bool]
TransportProcessTerminator = Callable[[int, _TransportConfigBase], int]
TransportSocketPathBuilder = Callable[[int, _TransportConfigBase], Path | None]
TransportStalenessProbe = Callable[[int, _TransportConfigBase], bool]
TransportStartupLockFactory = Callable[
    [int, _TransportConfigBase, OperationDeadline | None],
    AbstractContextManager[None],
]
TransportDataControlPairAvailabilityProbe = Callable[
    [int, int, str, _TransportConfigBase],
    bool,
]


def _tcp_is_supported() -> bool:
    return True


def _tcp_endpoint_url(port: int, host: str, config: _TransportConfigBase) -> str:
    del config
    return f"tcp://{host}:{port}"


def _tcp_endpoint_in_use(
    port: int,
    host: str,
    config: _TransportConfigBase,
) -> bool:
    del config
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as endpoint_socket:
        endpoint_socket.settimeout(0.1)
        try:
            return endpoint_socket.connect_ex((host, port)) == 0
        except OSError:
            return False


def _tcp_endpoint_is_local(host: str, port: int) -> bool:
    try:
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return bool(addresses) and all(
            ip_address(address[4][0]).is_loopback for address in addresses
        )
    except (OSError, ValueError):
        return False


def _tcp_cleanup_endpoint(port: int, config: _TransportConfigBase) -> bool:
    del port, config
    return False


def _tcp_preserve_unresponsive_endpoint(
    port: int,
    config: _TransportConfigBase,
) -> bool:
    del port, config
    return False


def _tcp_kill_processes_on_port(port: int, config: _TransportConfigBase) -> int:
    del config
    import psutil

    try:
        connections = psutil.net_connections(kind="inet")
    except (OSError, psutil.Error):
        return 0

    killed = 0
    for connection in connections:
        if (
            connection.pid is None
            or connection.status != psutil.CONN_LISTEN
            or not connection.laddr
            or connection.laddr.port != port
        ):
            continue
        try:
            psutil.Process(connection.pid).kill()
            killed += 1
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return killed


def _tcp_socket_path(port: int, config: _TransportConfigBase) -> Path | None:
    del port, config
    return None


def _tcp_endpoint_is_stale(port: int, config: _TransportConfigBase) -> bool:
    del port, config
    return False


def _tcp_data_control_pair_is_available(
    data_port: int,
    control_port: int,
    host: str,
    config: _TransportConfigBase,
) -> bool:
    """Return whether both TCP addresses can be bound together."""

    del config
    try:
        with (
            socket.socket(socket.AF_INET, socket.SOCK_STREAM) as data_socket,
            socket.socket(socket.AF_INET, socket.SOCK_STREAM) as control_socket,
        ):
            data_socket.bind((host, data_port))
            control_socket.bind((host, control_port))
    except OSError:
        return False
    return True


@contextmanager
def _tcp_startup_lock(
    port: int,
    config: _TransportConfigBase,
    operation_deadline: OperationDeadline | None = None,
) -> Iterator[None]:
    del port, config, operation_deadline
    yield


def _ipc_is_supported() -> bool:
    return platform.system() != "Windows"


def _ipc_socket_path(port: int, config: _TransportConfigBase) -> Path | None:
    if not _ipc_is_supported():
        return None
    ipc_dir = Path.home() / f".{config.app_name}" / config.ipc_socket_dir
    socket_name = f"{config.ipc_socket_prefix}-{port}{config.ipc_socket_extension}"
    return ipc_dir / socket_name


def _ipc_endpoint_url(port: int, host: str, config: _TransportConfigBase) -> str:
    del host
    socket_path = _ipc_socket_path(port, config)
    if socket_path is None:
        raise ValueError("IPC transport is not supported on this host")
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    return f"ipc://{socket_path}"


def _ipc_endpoint_in_use(
    port: int,
    host: str,
    config: _TransportConfigBase,
) -> bool:
    del host
    socket_path = _ipc_socket_path(port, config)
    return socket_path.exists() if socket_path is not None else False


def _ipc_endpoint_is_local(host: str, port: int) -> bool:
    del host, port
    return True


def _ipc_cleanup_endpoint(port: int, config: _TransportConfigBase) -> bool:
    socket_path = _ipc_socket_path(port, config)
    if socket_path is None or not socket_path.exists():
        return False
    socket_path.unlink()
    return True


def _ipc_endpoint_is_stale(port: int, config: _TransportConfigBase) -> bool:
    socket_path = _ipc_socket_path(port, config)
    if socket_path is None or not socket_path.exists():
        return False
    try:
        import psutil

        connections = psutil.net_connections(kind="unix")
    except (ImportError, OSError):
        return False
    except psutil.Error:
        return False
    path = str(socket_path)
    return all(connection.laddr != path for connection in connections)


def _ipc_data_control_pair_is_available(
    data_port: int,
    control_port: int,
    host: str,
    config: _TransportConfigBase,
) -> bool:
    """Return whether neither IPC address has an owning filesystem path."""

    return not any(_ipc_endpoint_in_use(port, host, config) for port in (data_port, control_port))


def _ipc_preserve_unresponsive_endpoint(
    port: int,
    config: _TransportConfigBase,
) -> bool:
    return not _ipc_endpoint_is_stale(port, config)


def _ipc_kill_processes_on_port(port: int, config: _TransportConfigBase) -> int:
    import psutil

    socket_path = _ipc_socket_path(port, config)
    killed = 0
    if socket_path is not None:
        try:
            connections = psutil.net_connections(kind="unix")
        except (OSError, psutil.Error):
            connections = ()
        for connection in connections:
            if connection.laddr != str(socket_path) or connection.pid is None:
                continue
            try:
                psutil.Process(connection.pid).kill()
                killed += 1
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
    _ipc_cleanup_endpoint(port, config)
    return killed


@contextmanager
def _ipc_startup_lock(
    port: int,
    config: _TransportConfigBase,
    operation_deadline: OperationDeadline | None = None,
) -> Iterator[None]:
    import fcntl

    socket_path = _ipc_socket_path(port, config)
    if socket_path is None:
        raise ValueError("IPC endpoint lock requires an IPC socket path")
    lock_path = socket_path.with_name(f"{socket_path.name}.startup.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_file:
        if operation_deadline is None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        else:
            while True:
                try:
                    fcntl.flock(
                        lock_file.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    break
                except BlockingIOError:
                    time.sleep(min(0.05, operation_deadline.remaining_seconds()))
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
