"""Nominal transport declarations owning complete endpoint behavior."""

from __future__ import annotations

import platform
import socket
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from ipaddress import ip_address
from pathlib import Path
from typing import ClassVar

import zmq
from metaclass_registry import AutoRegisterMeta

from .config import TransportMode, ZMQConfig
from .timeouts import OperationCancellation, OperationDeadline


class TransportDeclaration(ABC, metaclass=AutoRegisterMeta):
    """Complete behavior owned by one transport implementation."""

    __registry_key__ = "mode"
    __skip_if_no_key__ = True

    mode: ClassVar[TransportMode | None] = None
    default_priority: ClassVar[int]

    @classmethod
    @abstractmethod
    def is_supported(cls) -> bool:
        """Return whether this transport can run on the current platform."""

    @classmethod
    @abstractmethod
    def endpoint_url(
        cls,
        port: int,
        host: str,
        config: ZMQConfig,
    ) -> str:
        """Build one transport endpoint URL."""

    @classmethod
    @abstractmethod
    def bind_socket(
        cls,
        endpoint_socket: zmq.Socket,
        host: str,
        port: int | None,
        config: ZMQConfig,
    ) -> int:
        """Bind a socket and return its resolved data port."""

    @classmethod
    @abstractmethod
    def endpoint_in_use(
        cls,
        port: int,
        host: str,
        config: ZMQConfig,
    ) -> bool:
        """Return whether an endpoint identity is occupied."""

    @classmethod
    @abstractmethod
    def endpoint_is_local(cls, host: str, port: int) -> bool:
        """Return whether the endpoint belongs to the local host."""

    @classmethod
    @abstractmethod
    def cleanup_endpoint(cls, port: int, config: ZMQConfig) -> bool:
        """Remove stale transport resources when applicable."""

    @classmethod
    @abstractmethod
    def preserve_unresponsive_endpoint(
        cls,
        port: int,
        config: ZMQConfig,
    ) -> bool:
        """Return whether an unresponsive endpoint must be preserved."""

    @classmethod
    @abstractmethod
    def kill_processes_on_port(cls, port: int, config: ZMQConfig) -> int:
        """Terminate local processes owning the endpoint."""

    @classmethod
    @abstractmethod
    def socket_path(
        cls,
        port: int,
        config: ZMQConfig,
    ) -> Path | None:
        """Return the filesystem identity when the transport has one."""

    @classmethod
    @abstractmethod
    def endpoint_is_stale(cls, port: int, config: ZMQConfig) -> bool:
        """Return whether a filesystem endpoint has no live owner."""

    @classmethod
    @abstractmethod
    def startup_lock(
        cls,
        port: int,
        config: ZMQConfig,
        operation_deadline: OperationDeadline | None,
        cancellation: OperationCancellation,
    ) -> AbstractContextManager[bool]:
        """Serialize startup and yield whether the operation acquired ownership."""

    @classmethod
    @abstractmethod
    def data_control_pair_is_available(
        cls,
        data_port: int,
        control_port: int,
        host: str,
        config: ZMQConfig,
    ) -> bool:
        """Return whether both endpoint identities can be acquired together."""


class TcpTransportDeclaration(TransportDeclaration):
    """Complete TCP endpoint semantics."""

    mode = TransportMode.TCP
    default_priority = 1

    @classmethod
    def is_supported(cls) -> bool:
        return True

    @classmethod
    def endpoint_url(
        cls,
        port: int,
        host: str,
        config: ZMQConfig,
    ) -> str:
        del config
        return f"tcp://{host}:{port}"

    @classmethod
    def bind_socket(
        cls,
        endpoint_socket: zmq.Socket,
        host: str,
        port: int | None,
        config: ZMQConfig,
    ) -> int:
        del config
        base_url = f"tcp://{host}"
        if port in (None, 0):
            return endpoint_socket.bind_to_random_port(base_url)
        endpoint_socket.bind(f"{base_url}:{port}")
        return port

    @classmethod
    def endpoint_in_use(
        cls,
        port: int,
        host: str,
        config: ZMQConfig,
    ) -> bool:
        del config
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as endpoint_socket:
            endpoint_socket.settimeout(0.1)
            try:
                return endpoint_socket.connect_ex((host, port)) == 0
            except OSError:
                return False

    @classmethod
    def endpoint_is_local(cls, host: str, port: int) -> bool:
        try:
            addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            return bool(addresses) and all(
                ip_address(address[4][0]).is_loopback for address in addresses
            )
        except (OSError, ValueError):
            return False

    @classmethod
    def cleanup_endpoint(cls, port: int, config: ZMQConfig) -> bool:
        del port, config
        return False

    @classmethod
    def preserve_unresponsive_endpoint(
        cls,
        port: int,
        config: ZMQConfig,
    ) -> bool:
        del port, config
        return False

    @classmethod
    def kill_processes_on_port(cls, port: int, config: ZMQConfig) -> int:
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

    @classmethod
    def socket_path(
        cls,
        port: int,
        config: ZMQConfig,
    ) -> None:
        del port, config
        return None

    @classmethod
    def endpoint_is_stale(cls, port: int, config: ZMQConfig) -> bool:
        del port, config
        return False

    @classmethod
    @contextmanager
    def startup_lock(
        cls,
        port: int,
        config: ZMQConfig,
        operation_deadline: OperationDeadline | None,
        cancellation: OperationCancellation,
    ) -> Iterator[bool]:
        del port, config, operation_deadline
        yield not cancellation.requested()

    @classmethod
    def data_control_pair_is_available(
        cls,
        data_port: int,
        control_port: int,
        host: str,
        config: ZMQConfig,
    ) -> bool:
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


class IpcTransportDeclaration(TransportDeclaration):
    """Complete IPC endpoint semantics."""

    mode = TransportMode.IPC
    default_priority = 0

    @classmethod
    def is_supported(cls) -> bool:
        return platform.system() != "Windows"

    @classmethod
    def socket_path(
        cls,
        port: int,
        config: ZMQConfig,
    ) -> Path | None:
        if not cls.is_supported():
            return None
        ipc_dir = Path.home() / f".{config.app_name}" / config.ipc_socket_dir
        socket_name = f"{config.ipc_socket_prefix}-{port}{config.ipc_socket_extension}"
        return ipc_dir / socket_name

    @classmethod
    def endpoint_url(
        cls,
        port: int,
        host: str,
        config: ZMQConfig,
    ) -> str:
        del host
        socket_path = cls.socket_path(port, config)
        if socket_path is None:
            raise ValueError("IPC transport is not supported on this host")
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        return f"ipc://{socket_path}"

    @classmethod
    def bind_socket(
        cls,
        endpoint_socket: zmq.Socket,
        host: str,
        port: int | None,
        config: ZMQConfig,
    ) -> int:
        if port is None:
            raise ValueError("IPC socket binding requires an explicit port")
        endpoint_socket.bind(cls.endpoint_url(port, host, config))
        return port

    @classmethod
    def endpoint_in_use(
        cls,
        port: int,
        host: str,
        config: ZMQConfig,
    ) -> bool:
        del host
        socket_path = cls.socket_path(port, config)
        return socket_path.exists() if socket_path is not None else False

    @classmethod
    def endpoint_is_local(cls, host: str, port: int) -> bool:
        del host, port
        return True

    @classmethod
    def cleanup_endpoint(cls, port: int, config: ZMQConfig) -> bool:
        socket_path = cls.socket_path(port, config)
        if socket_path is None:
            return False
        try:
            socket_path.unlink()
        except FileNotFoundError:
            return False
        return True

    @classmethod
    def endpoint_is_stale(cls, port: int, config: ZMQConfig) -> bool:
        socket_path = cls.socket_path(port, config)
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

    @classmethod
    def preserve_unresponsive_endpoint(
        cls,
        port: int,
        config: ZMQConfig,
    ) -> bool:
        return not cls.endpoint_is_stale(port, config)

    @classmethod
    def kill_processes_on_port(cls, port: int, config: ZMQConfig) -> int:
        import psutil

        socket_path = cls.socket_path(port, config)
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
        cls.cleanup_endpoint(port, config)
        return killed

    @classmethod
    @contextmanager
    def startup_lock(
        cls,
        port: int,
        config: ZMQConfig,
        operation_deadline: OperationDeadline | None,
        cancellation: OperationCancellation,
    ) -> Iterator[bool]:
        import fcntl

        socket_path = cls.socket_path(port, config)
        if socket_path is None:
            raise ValueError("IPC endpoint lock requires an IPC socket path")
        lock_path = socket_path.with_name(f"{socket_path.name}.startup.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock_file:
            while True:
                if cancellation.requested():
                    yield False
                    return
                try:
                    fcntl.flock(
                        lock_file.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    break
                except BlockingIOError:
                    wait_seconds = 0.05
                    if operation_deadline is not None:
                        wait_seconds = min(
                            wait_seconds,
                            operation_deadline.remaining_seconds(),
                        )
                    time.sleep(wait_seconds)
            try:
                yield True
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @classmethod
    def data_control_pair_is_available(
        cls,
        data_port: int,
        control_port: int,
        host: str,
        config: ZMQConfig,
    ) -> bool:
        return not any(
            cls.endpoint_in_use(port, host, config) for port in (data_port, control_port)
        )
