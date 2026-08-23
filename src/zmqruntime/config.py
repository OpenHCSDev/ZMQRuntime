"""Configuration types for ZMQ transport."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Annotated

from annotated_types import Ge, Gt, Le, MinLen, Predicate
from python_introspect import validate_annotated_dataclass

if TYPE_CHECKING:
    from zmqruntime.transport_modes import TransportDeclaration


class TransportMode(Enum):
    """Closed serialized identities resolved through transport declarations."""

    TCP = "tcp"
    IPC = "ipc"

    @property
    def declaration(self) -> type[TransportDeclaration]:
        """Resolve this identity through the declaration registry."""

        from zmqruntime.transport_modes import TransportDeclaration

        return TransportDeclaration.__registry__[self]

    @classmethod
    def default(cls) -> TransportMode:
        """Select the highest-priority supported transport declaration."""

        return min(
            (mode for mode in cls if mode.declaration.is_supported()),
            key=lambda mode: mode.declaration.default_priority,
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
class ZMQConfig:
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
