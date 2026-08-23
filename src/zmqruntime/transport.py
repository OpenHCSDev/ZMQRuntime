"""Transport utilities for ZMQ communication."""

from __future__ import annotations

import pickle
import time
from collections.abc import Collection
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ControlMessageType, ControlRequestHeader, PongResponse
from zmqruntime.startup import (
    IDLE_ENDPOINT_STARTUP_OBSERVER,
    EndpointStartupObserver,
)
from zmqruntime.timeouts import OperationCancellation, OperationDeadline

_default_config = ZMQConfig()


@dataclass(frozen=True, slots=True)
class DataControlPortPair:
    """One transport data/control port pair derived from a ZMQ configuration."""

    data_port: int
    control_port: int

    @property
    def ports(self) -> frozenset[int]:
        """Return both ports for subsequent allocation exclusion."""
        return frozenset((self.data_port, self.control_port))


@dataclass(frozen=True, slots=True)
class TransportEndpoint:
    """Nominal address authority for one ZMQ data/control endpoint pair."""

    host: str
    port: int
    transport_mode: TransportMode

    def data_url(self, config: ZMQConfig | None = None) -> str:
        """Return this endpoint's data socket URL."""

        return get_zmq_transport_url(
            self.port,
            host=self.host,
            mode=self.transport_mode,
            config=config,
        )

    def control_port(self, config: ZMQConfig) -> int:
        """Return this endpoint's derived control port."""

        return get_control_port(self.port, config)

    def port_pair(self, config: ZMQConfig) -> DataControlPortPair:
        """Return the configured data/control pair owned by this endpoint."""

        return DataControlPortPair(
            data_port=self.port,
            control_port=self.control_port(config),
        )

    def occupied_ports(self, config: ZMQConfig) -> frozenset[int]:
        """Return the addresses in this endpoint pair that are currently bound."""

        return frozenset(
            port
            for port in self.port_pair(config).ports
            if self.transport_mode.declaration.endpoint_in_use(
                port,
                self.host,
                config,
            )
        )

    def control_url(self, config: ZMQConfig) -> str:
        """Return this endpoint's control socket URL."""

        return get_zmq_transport_url(
            self.control_port(config),
            host=self.host,
            mode=self.transport_mode,
            config=config,
        )

    def is_in_use(self, config: ZMQConfig) -> bool:
        """Return whether this endpoint's data address is occupied."""

        return self.transport_mode.declaration.endpoint_in_use(
            self.port,
            self.host,
            config,
        )

    def ping(
        self,
        config: ZMQConfig,
        *,
        timeout_ms: int = 500,
    ) -> PongResponse | None:
        """Return this endpoint's typed control heartbeat."""

        return request_control_ping(
            self.port,
            self.transport_mode,
            host=self.host,
            config=config,
            timeout_ms=timeout_ms,
        )

    def cleanup(self, config: ZMQConfig) -> None:
        """Remove residue for both addresses owned by this endpoint."""

        self.transport_mode.declaration.cleanup_endpoint(self.port, config)
        self.transport_mode.declaration.cleanup_endpoint(
            self.control_port(config),
            config,
        )

    def wait_until_ready(
        self,
        config: ZMQConfig,
        *,
        timeout: float,
        require_ready: bool,
        poll_interval: float,
        startup_observer: EndpointStartupObserver,
    ) -> bool:
        """Wait for this endpoint's declared addresses and heartbeat readiness."""

        return (
            self.wait_for_ready_response(
                config,
                timeout=timeout,
                require_ready=require_ready,
                poll_interval=poll_interval,
                startup_observer=startup_observer,
            )
            is not None
        )

    def wait_for_ready_response(
        self,
        config: ZMQConfig,
        *,
        timeout: float,
        require_ready: bool,
        poll_interval: float,
        startup_observer: EndpointStartupObserver,
        operation_deadline: OperationDeadline | None = None,
    ) -> PongResponse | None:
        """Return the first heartbeat that proves this endpoint is ready."""

        inactivity_deadline = time.monotonic() + timeout
        control_port = self.control_port(config)
        while True:
            if startup_observer.poll_activity():
                inactivity_deadline = time.monotonic() + timeout
            now = time.monotonic()
            if (
                startup_observer.should_abort()
                or now >= inactivity_deadline
                or (operation_deadline is not None and operation_deadline.expired())
            ):
                return None
            addresses_ready = self.transport_mode.declaration.endpoint_in_use(
                self.port,
                self.host,
                config,
            ) and self.transport_mode.declaration.endpoint_in_use(
                control_port,
                self.host,
                config,
            )
            if addresses_ready:
                remaining = inactivity_deadline - time.monotonic()
                if operation_deadline is not None:
                    remaining = min(
                        remaining,
                        operation_deadline.remaining_seconds_or_zero(),
                    )
                if remaining <= 0:
                    return None
                response = self.ping(
                    config,
                    timeout_ms=max(1, int(remaining * 1000)),
                )
                if response is not None and (response.ready or not require_ready):
                    return response
            sleep_deadline = inactivity_deadline
            if operation_deadline is not None:
                sleep_deadline = min(sleep_deadline, operation_deadline.expires_at)
            time.sleep(min(poll_interval, max(0.0, sleep_deadline - time.monotonic())))


class DataControlPortPairAuthority:
    """Acquire free data/control pairs through the selected transport owner."""

    @staticmethod
    def acquire(
        config: ZMQConfig,
        *,
        transport_mode: TransportMode,
        excluded: Collection[int] = (),
        host: str = "127.0.0.1",
    ) -> DataControlPortPair:
        """Return the first available configured data/control endpoint pair."""

        mode = resolve_transport_mode(transport_mode)
        if not mode.declaration.is_supported():
            raise ValueError(f"Transport mode {mode.value!r} is not supported.")
        first_port = config.default_port
        last_port = 65535 - config.control_port_offset
        for data_port in range(first_port, last_port + 1):
            control_port = get_control_port(data_port, config)
            if data_port in excluded or control_port in excluded:
                continue
            if not mode.declaration.data_control_pair_is_available(
                data_port,
                control_port,
                host,
                config,
            ):
                continue
            return DataControlPortPair(
                data_port=data_port,
                control_port=control_port,
            )
        raise RuntimeError(
            f"Could not allocate a free {mode.value.upper()} data/control port pair."
        )


class TcpDataControlPortPairAuthority:
    """Compatibility declaration for acquiring loopback TCP endpoint pairs."""

    @staticmethod
    def acquire(
        config: ZMQConfig,
        *,
        excluded: Collection[int] = (),
        host: str = "127.0.0.1",
    ) -> DataControlPortPair:
        return DataControlPortPairAuthority.acquire(
            config,
            transport_mode=TransportMode.TCP,
            excluded=excluded,
            host=host,
        )


TcpDataControlPortPair = DataControlPortPair


def get_default_transport_mode() -> TransportMode:
    """Get platform-appropriate transport mode."""
    return TransportMode.default()


def resolve_transport_mode(mode: TransportMode | None) -> TransportMode:
    """Resolve an omitted mode without accepting alternate representations."""

    return TransportMode.resolve(mode)


def get_ipc_socket_path(port: int, config: ZMQConfig | None = None) -> Path | None:
    """Get IPC socket path for a given port (Unix/Mac only)."""
    config = config or _default_config
    return TransportMode.IPC.declaration.socket_path(port, config)


def get_zmq_transport_url(
    port: int,
    host: str = "localhost",
    mode: TransportMode | None = None,
    config: ZMQConfig | None = None,
) -> str:
    """Get ZMQ transport URL for given port/host/mode."""
    config = config or _default_config
    mode = resolve_transport_mode(mode)
    return mode.declaration.endpoint_url(port, host, config)


def get_control_port(port: int, config: ZMQConfig | None = None) -> int:
    """Get control port for a data port."""
    config = config or _default_config
    return port + config.control_port_offset


def get_control_url(
    port: int,
    transport_mode: TransportMode | None,
    host: str = "localhost",
    config: ZMQConfig | None = None,
) -> str:
    """Get control socket URL for a given data port."""
    config = config or _default_config
    mode = resolve_transport_mode(transport_mode)
    return get_zmq_transport_url(
        get_control_port(port, config),
        host=host,
        mode=mode,
        config=config,
    )


@contextmanager
def endpoint_startup_lock(
    port: int,
    transport_mode: TransportMode | None,
    config: ZMQConfig | None = None,
    *,
    operation_deadline: OperationDeadline | None = None,
    cancellation: OperationCancellation | None = None,
):
    """Serialize discovery and startup for one IPC endpoint across clients."""

    config = config or _default_config
    mode = resolve_transport_mode(transport_mode)
    cancellation = cancellation or OperationCancellation()
    with mode.declaration.startup_lock(
        port,
        config,
        operation_deadline,
        cancellation,
    ) as acquired:
        yield acquired


def remove_ipc_socket(port: int, config: ZMQConfig | None = None) -> bool:
    """Remove stale IPC socket file."""
    return TransportMode.IPC.declaration.cleanup_endpoint(
        port,
        config or _default_config,
    )


def ipc_socket_is_stale(port: int, config: ZMQConfig | None = None) -> bool:
    """Return whether an IPC path exists without a kernel-owned Unix socket."""
    return TransportMode.IPC.declaration.endpoint_is_stale(
        port,
        config or _default_config,
    )


def is_port_in_use(
    port: int,
    transport_mode: TransportMode | None,
    host: str = "localhost",
    config: ZMQConfig | None = None,
) -> bool:
    """Check whether the given port is in use for the chosen transport."""
    config = config or _default_config
    mode = resolve_transport_mode(transport_mode)
    return mode.declaration.endpoint_in_use(port, host, config)


def ping_control_port(
    port: int,
    transport_mode: TransportMode | None,
    host: str = "localhost",
    config: ZMQConfig | None = None,
    timeout_ms: int = 500,
    require_ready: bool = True,
) -> bool:
    """Ping the control socket for a given data port."""
    response = request_control_ping(
        port,
        transport_mode,
        host=host,
        config=config,
        timeout_ms=timeout_ms,
    )
    if response is None:
        return False
    if require_ready:
        return response.ready
    return True


def request_control_ping(
    port: int,
    transport_mode: TransportMode | None,
    host: str = "localhost",
    config: ZMQConfig | None = None,
    timeout_ms: int = 500,
) -> PongResponse | None:
    """Return the typed control PONG for a data port, or None when unreachable."""
    config = config or _default_config
    control_url = get_control_url(port, transport_mode, host=host, config=config)
    deadline = time.monotonic() + max(0, timeout_ms) / 1000.0

    def remaining_timeout_ms() -> int:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return 0
        return max(1, int(remaining * 1000))

    ctx = zmq.Context.instance()
    sock = None
    try:
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.IMMEDIATE, 1)
        sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        sock.connect(control_url)
        send_timeout_ms = remaining_timeout_ms()
        if send_timeout_ms <= 0 or not sock.poll(send_timeout_ms, zmq.POLLOUT):
            return None
        sock.send(
            ControlRequestHeader(ControlMessageType.PING).to_wire_payload(),
            flags=zmq.NOBLOCK,
        )
        receive_timeout_ms = remaining_timeout_ms()
        if receive_timeout_ms <= 0 or not sock.poll(
            receive_timeout_ms,
            zmq.POLLIN,
        ):
            return None
        response = pickle.loads(sock.recv(flags=zmq.NOBLOCK))
        if not isinstance(response, dict):
            return None
        return PongResponse.from_dict(response)
    except Exception:
        return None
    finally:
        if sock is not None:
            try:
                sock.close(linger=0)
            except Exception:
                pass


def wait_for_server_ready(
    port: int,
    transport_mode: TransportMode | None,
    host: str = "localhost",
    config: ZMQConfig | None = None,
    timeout: float = 10.0,
    require_ready: bool = True,
    poll_interval: float = 0.2,
    startup_observer: EndpointStartupObserver = IDLE_ENDPOINT_STARTUP_OBSERVER,
    operation_deadline: OperationDeadline | None = None,
) -> bool:
    """Wait for readiness, optionally treating child updates as startup activity."""
    return (
        wait_for_endpoint_ready(
            port,
            transport_mode,
            host=host,
            config=config,
            timeout=timeout,
            require_ready=require_ready,
            poll_interval=poll_interval,
            startup_observer=startup_observer,
            operation_deadline=operation_deadline,
        )
        is not None
    )


def wait_for_endpoint_ready(
    port: int,
    transport_mode: TransportMode | None,
    host: str = "localhost",
    config: ZMQConfig | None = None,
    timeout: float = 10.0,
    require_ready: bool = True,
    poll_interval: float = 0.2,
    startup_observer: EndpointStartupObserver = IDLE_ENDPOINT_STARTUP_OBSERVER,
    operation_deadline: OperationDeadline | None = None,
) -> PongResponse | None:
    """Return the first typed heartbeat that proves endpoint readiness."""

    config = config or _default_config
    endpoint = TransportEndpoint(
        host=host,
        port=port,
        transport_mode=resolve_transport_mode(transport_mode),
    )
    return endpoint.wait_for_ready_response(
        config,
        timeout=timeout,
        require_ready=require_ready,
        poll_interval=poll_interval,
        startup_observer=startup_observer,
        operation_deadline=operation_deadline,
    )
