"""Configuration types for ZMQ transport."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated

from annotated_types import Ge, Gt, Le, MinLen, Predicate
from python_introspect import validate_annotated_dataclass

from zmqruntime.transport_modes import (
    TransportDataControlPairAvailabilityProbe,
    TransportEndpointCleanup,
    TransportLocalityProbe,
    TransportOccupancyProbe,
    TransportPreservationPolicy,
    TransportProcessTerminator,
    TransportSocketPathBuilder,
    TransportStalenessProbe,
    TransportStartupLockFactory,
    TransportSupport,
    TransportUrlBuilder,
    _ipc_cleanup_endpoint,
    _ipc_data_control_pair_is_available,
    _ipc_endpoint_in_use,
    _ipc_endpoint_is_local,
    _ipc_endpoint_is_stale,
    _ipc_endpoint_url,
    _ipc_is_supported,
    _ipc_kill_processes_on_port,
    _ipc_preserve_unresponsive_endpoint,
    _ipc_socket_path,
    _ipc_startup_lock,
    _tcp_cleanup_endpoint,
    _tcp_data_control_pair_is_available,
    _tcp_endpoint_in_use,
    _tcp_endpoint_is_local,
    _tcp_endpoint_is_stale,
    _tcp_endpoint_url,
    _tcp_is_supported,
    _tcp_kill_processes_on_port,
    _tcp_preserve_unresponsive_endpoint,
    _tcp_socket_path,
    _tcp_startup_lock,
    _TransportConfigBase,
)


class TransportMode(Enum):
    """Canonical transport declarations with member-owned leaf behavior."""

    is_supported: TransportSupport
    endpoint_url: TransportUrlBuilder
    endpoint_in_use: TransportOccupancyProbe
    endpoint_is_local: TransportLocalityProbe
    cleanup_endpoint: TransportEndpointCleanup
    preserve_unresponsive_endpoint: TransportPreservationPolicy
    kill_processes_on_port: TransportProcessTerminator
    socket_path: TransportSocketPathBuilder
    endpoint_is_stale: TransportStalenessProbe
    startup_lock: TransportStartupLockFactory
    data_control_pair_is_available: TransportDataControlPairAvailabilityProbe

    def __new__(
        cls,
        value: str,
        default_priority: int,
        is_supported: TransportSupport,
        endpoint_url: TransportUrlBuilder,
        endpoint_in_use: TransportOccupancyProbe,
        endpoint_is_local: TransportLocalityProbe,
        cleanup_endpoint: TransportEndpointCleanup,
        preserve_unresponsive_endpoint: TransportPreservationPolicy,
        kill_processes_on_port: TransportProcessTerminator,
        socket_path: TransportSocketPathBuilder,
        endpoint_is_stale: TransportStalenessProbe,
        startup_lock: TransportStartupLockFactory,
        data_control_pair_is_available: TransportDataControlPairAvailabilityProbe,
    ) -> TransportMode:
        member = object.__new__(cls)
        member._value_ = value
        member.default_priority = default_priority
        member.is_supported = is_supported
        member.endpoint_url = endpoint_url
        member.endpoint_in_use = endpoint_in_use
        member.endpoint_is_local = endpoint_is_local
        member.cleanup_endpoint = cleanup_endpoint
        member.preserve_unresponsive_endpoint = preserve_unresponsive_endpoint
        member.kill_processes_on_port = kill_processes_on_port
        member.socket_path = socket_path
        member.endpoint_is_stale = endpoint_is_stale
        member.startup_lock = startup_lock
        member.data_control_pair_is_available = data_control_pair_is_available
        return member

    TCP = (
        "tcp",
        1,
        _tcp_is_supported,
        _tcp_endpoint_url,
        _tcp_endpoint_in_use,
        _tcp_endpoint_is_local,
        _tcp_cleanup_endpoint,
        _tcp_preserve_unresponsive_endpoint,
        _tcp_kill_processes_on_port,
        _tcp_socket_path,
        _tcp_endpoint_is_stale,
        _tcp_startup_lock,
        _tcp_data_control_pair_is_available,
    )
    IPC = (
        "ipc",
        0,
        _ipc_is_supported,
        _ipc_endpoint_url,
        _ipc_endpoint_in_use,
        _ipc_endpoint_is_local,
        _ipc_cleanup_endpoint,
        _ipc_preserve_unresponsive_endpoint,
        _ipc_kill_processes_on_port,
        _ipc_socket_path,
        _ipc_endpoint_is_stale,
        _ipc_startup_lock,
        _ipc_data_control_pair_is_available,
    )

    @classmethod
    def default(cls) -> TransportMode:
        """Select the highest-priority supported transport declaration."""

        return min(
            (mode for mode in cls if mode.is_supported()),
            key=lambda mode: mode.default_priority,
        )

    @classmethod
    def resolve(cls, value: TransportMode | None) -> TransportMode:
        """Resolve an omitted declaration and reject alternate representations."""

        if value is None:
            return cls.default()
        if not isinstance(value, cls):
            raise TypeError(
                "Transport mode must be a TransportMode instance or None, "
                f"not {type(value).__name__}."
            )
        return value

    @classmethod
    def optional_from_text(cls, value: str | None) -> TransportMode | None:
        """Parse an optional value at a text serialization boundary."""

        return None if value is None else cls(value)

    @classmethod
    def optional_to_text(cls, value: TransportMode | None) -> str | None:
        """Project an optional mode at a text serialization boundary."""

        if value is None:
            return None
        if not isinstance(value, cls):
            raise TypeError(f"Transport mode must be {cls.__name__}.")
        return value.value


NonBlankString = Annotated[str, MinLen(1), Predicate(str.strip)]
PositiveFloat = Annotated[float, Gt(0)]
PositiveInteger = Annotated[int, Gt(0)]
SocketPort = Annotated[int, Ge(0), Le(65535)]
TcpPort = Annotated[int, Ge(1), Le(65535)]


@dataclass(frozen=True, slots=True)
class ZMQConfig(_TransportConfigBase):
    """Shared ZMQ data/control endpoint topology and IPC naming policy."""

    control_port_offset: PositiveInteger = 1000
    """Positive offset added to a data port to derive its paired control port.

    The resulting control port must remain within the TCP port range. Port-pair
    allocation and endpoint discovery both use this same relationship.
    """

    default_port: TcpPort = 7777
    """First data port used when a caller does not provide an explicit endpoint.

    Discovery and allocation may inspect subsequent ports, but every paired
    control port is still derived through ``control_port_offset``.
    """

    ipc_socket_dir: NonBlankString = "ipc"
    """Directory containing IPC socket files.

    Relative values are resolved beneath the runtime socket root; absolute
    values select an explicit directory. TCP transport ignores this field.
    """

    ipc_socket_prefix: NonBlankString = "zmq"
    """Filename prefix used to namespace generated IPC data and control sockets."""

    ipc_socket_extension: NonBlankString = ".sock"
    """Filename suffix appended to generated IPC data and control socket paths."""

    shared_ack_port: TcpPort = 7555
    """Data port reserved for the shared acknowledgement endpoint used by streamers."""

    app_name: NonBlankString = "zmqruntime"
    """Application namespace included in generated transport identities and paths."""

    def __post_init__(self) -> None:
        """Reject invalid transport topology at its declaration boundary."""

        validate_annotated_dataclass(self)
        if self.default_port + self.control_port_offset > 65535:
            raise ValueError("default_port plus control_port_offset must not exceed 65535.")
