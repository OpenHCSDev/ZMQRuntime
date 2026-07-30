"""Configuration types for ZMQ transport."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, TypeVar

from annotated_types import Ge, Gt, Le, MinLen, Predicate
from python_introspect import validate_annotated_dataclass

TransportModeT = TypeVar("TransportModeT", bound="TransportMode")


class TransportMode(Enum):
    """Transport mode for ZMQ communication."""

    TCP = "tcp"
    IPC = "ipc"

    @classmethod
    def optional_from_text(
        cls: type[TransportModeT],
        value: str | None,
    ) -> TransportModeT | None:
        """Parse an optional value at a text serialization boundary."""

        if value is None:
            return None
        return cls(value)

    @classmethod
    def optional_to_text(
        cls: type[TransportModeT],
        value: TransportModeT | None,
    ) -> str | None:
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
    """Configuration for ZMQ transport."""

    control_port_offset: PositiveInteger = 1000
    default_port: TcpPort = 7777
    ipc_socket_dir: NonBlankString = "ipc"
    ipc_socket_prefix: NonBlankString = "zmq"
    ipc_socket_extension: NonBlankString = ".sock"
    shared_ack_port: TcpPort = 7555
    app_name: NonBlankString = "zmqruntime"

    def __post_init__(self) -> None:
        """Reject invalid transport topology at its declaration boundary."""

        validate_annotated_dataclass(self)
        if self.default_port + self.control_port_offset > 65535:
            raise ValueError("default_port plus control_port_offset must not exceed 65535.")
