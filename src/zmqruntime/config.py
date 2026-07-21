"""Configuration types for ZMQ transport."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TransportMode(Enum):
    """Transport mode for ZMQ communication."""
    TCP = "tcp"
    IPC = "ipc"


@dataclass(frozen=True, slots=True)
class ZMQConfig:
    """Configuration for ZMQ transport."""
    control_port_offset: int = 1000
    default_port: int = 7777
    ipc_socket_dir: str = "ipc"
    ipc_socket_prefix: str = "zmq"
    ipc_socket_extension: str = ".sock"
    shared_ack_port: int = 7555
    app_name: str = "zmqruntime"
