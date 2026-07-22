"""ZMQ server base class and utilities."""
from __future__ import annotations

import logging
import pickle
import platform
import subprocess
import threading
from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Callable, Mapping

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ControlMessageType,
    MessageFields,
    ProcessIdentity,
    PongResponse,
    ResponseType,
    SocketType,
)
from zmqruntime.transport import (
    get_default_transport_mode,
    get_zmq_transport_url,
    remove_ipc_socket,
)

logger = logging.getLogger(__name__)


try:
    from metaclass_registry import AutoRegisterMeta  # type: ignore
except Exception:  # pragma: no cover - fallback for optional dependency

    class AutoRegisterMeta(ABCMeta):
        """Fallback registry metaclass when metaclass-registry is unavailable."""

        def __new__(mcls, name, bases, namespace, **kwargs):
            cls = super().__new__(mcls, name, bases, namespace, **kwargs)
            registry_key = getattr(cls, "__registry_key__", None)
            if registry_key is None:
                return cls
            registry_owner = None
            for base in bases:
                if hasattr(base, "__registry__"):
                    registry_owner = base
                    break
            if registry_owner is None:
                cls.__registry__ = {}
                registry_owner = cls
            key_value = getattr(cls, registry_key, None)
            if key_value:
                registry_owner.__registry__[key_value] = cls
            return cls


class ZMQServer(ABC, metaclass=AutoRegisterMeta):
    """
    ABC for ZMQ servers - dual-channel pattern with ping/pong handshake.

    Registry auto-created and stored as ZMQServer.__registry__.
    Subclasses auto-register by setting _server_type class attribute.
    """

    __registry_key__ = "_server_type"

    _server_type: str | None = None  # Override in subclasses for registration

    @classmethod
    def server_type(cls) -> str | None:
        """Return this server class's registered runtime role."""
        return cls._server_type

    def __init__(
        self,
        port: int,
        host: str = "*",
        log_file_path: str | None = None,
        data_socket_type=None,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
    ):
        import zmq

        self.config = config or ZMQConfig()
        self.port = port
        self.host = host
        self.control_port = port + self.config.control_port_offset
        self.log_file_path = log_file_path
        self.data_socket_type = data_socket_type if data_socket_type is not None else zmq.PUB
        # Windows doesn't support IPC (POSIX named pipes), so use TCP with localhost
        self.transport_mode = transport_mode or get_default_transport_mode()
        self.zmq_context = None
        self.data_socket = None
        self.control_socket = None
        self._running = False
        self._ready = False
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self._running:
                return
            self.zmq_context = zmq.Context()
            self.data_socket = self.bind_data_socket(self.zmq_context)
            self.control_socket = self.bind_control_socket(self.zmq_context)
            self._running = True
            logger.info(
                "ZMQ Server started on %s (%s), control %s",
                self.data_transport_url(),
                SocketType.from_zmq_constant(self.data_socket_type).get_display_name(),
                self.control_transport_url(),
            )

    def data_transport_url(self) -> str:
        """Return the configured data endpoint for this server."""

        return get_zmq_transport_url(
            self.port,
            host=self.host,
            mode=self.transport_mode,
            config=self.config,
        )

    def control_transport_url(self) -> str:
        """Return the configured control endpoint for this server."""

        return get_zmq_transport_url(
            self.control_port,
            host=self.host,
            mode=self.transport_mode,
            config=self.config,
        )

    def bind_data_socket(self, context: zmq.Context) -> zmq.Socket:
        """Create and bind the data socket in its calling thread."""

        socket = context.socket(self.data_socket_type)
        socket.setsockopt(zmq.LINGER, 0)

        # Set high water mark for SUB/PULL sockets to prevent message drops.
        if self.data_socket_type in (zmq.SUB, zmq.PULL):
            socket.setsockopt(zmq.RCVHWM, 100000)
            socket_type_name = "SUB" if self.data_socket_type == zmq.SUB else "PULL"
            logger.info(
                "ZMQ %s socket RCVHWM set to 100000 to prevent drops during blocking operations",
                socket_type_name,
            )

        socket.bind(self.data_transport_url())
        if self.data_socket_type == zmq.SUB:
            socket.setsockopt(zmq.SUBSCRIBE, b"")
        return socket

    def bind_control_socket(self, context: zmq.Context) -> zmq.Socket:
        """Create and bind the control socket in its calling thread."""

        socket = context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(self.control_transport_url())
        return socket

    def stop(self):
        with self._lock:
            self._running = False
            if self.data_socket:
                self.data_socket.close()
                self.data_socket = None
            if self.control_socket:
                self.control_socket.close()
                self.control_socket = None
            if self.zmq_context:
                self.zmq_context.term()
                self.zmq_context = None
            if self.transport_mode == TransportMode.IPC:
                remove_ipc_socket(self.port, self.config)
                remove_ipc_socket(self.control_port, self.config)
            logger.info("ZMQ Server stopped")

    def is_running(self):
        return self._running

    def process_messages(self):
        if not self._running:
            return

        # CRITICAL: ZMQ REP sockets require strict recv->send alternation.
        try:
            control_data = pickle.loads(self.control_socket.recv(zmq.NOBLOCK))
        except zmq.Again:
            return

        payload = self.control_response_payload(control_data)

        try:
            self.control_socket.send(payload)
        except Exception as e:
            logger.error("Failed to send response on control socket: %s", e, exc_info=True)

    def control_response(
        self,
        control_data: Mapping[str, object],
        *,
        response_factory: Callable[[], object] | None = None,
    ) -> object:
        """Return one control response independently of socket ownership."""

        try:
            if control_data.get(MessageFields.TYPE) == ControlMessageType.PING.value:
                if not self._ready:
                    self._ready = True
                    logger.info("Server ready")
                response = self._create_pong_response()
            elif response_factory is not None:
                response = response_factory()
            else:
                response = self.handle_control_message(control_data)
        except Exception as e:
            response = self.control_error_response(e)
        return response

    def control_error_response(self, error: Exception) -> dict[str, object]:
        """Return the canonical control error reply for a dispatch failure."""

        logger.error(
            "Error processing control message: %s",
            error,
            exc_info=(type(error), error, error.__traceback__),
        )
        return {"status": "error", "message": str(error), "type": "error"}

    def serialize_control_response(self, response: object) -> bytes:
        """Serialize a control response with the canonical error fallback."""

        try:
            return pickle.dumps(response)
        except Exception as e:
            logger.error(
                "Failed to serialize control response: %s (response_type=%s)",
                e,
                type(response).__name__,
                exc_info=True,
            )
            return pickle.dumps(
                {
                    MessageFields.STATUS: ResponseType.ERROR.value,
                    MessageFields.TYPE: ResponseType.ERROR.value,
                    MessageFields.MESSAGE: "Internal server serialization error",
                }
            )

    def control_response_payload(
        self,
        control_data: Mapping[str, object],
        *,
        response_factory: Callable[[], object] | None = None,
    ) -> bytes:
        """Create and serialize one control response without touching a socket."""

        return self.serialize_control_response(
            self.control_response(
                control_data,
                response_factory=response_factory,
            )
        )

    def _create_pong_response(self):
        return (
            PongResponse(
                port=self.port,
                control_port=self.control_port,
                ready=self._ready,
                server=self.__class__.__name__,
                server_type=self.__class__.server_type(),
                log_file_path=self.log_file_path,
                process_identity=ProcessIdentity.current(),
            ).to_dict()
        )

    def get_status_info(self):
        return {
            "port": self.port,
            "control_port": self.control_port,
            "running": self._running,
            "ready": self._ready,
            "server_type": self.__class__.__name__,
            "log_file": self.log_file_path,
        }

    def request_shutdown(self):
        self._running = False

    @staticmethod
    def kill_processes_on_port(port):
        killed = 0
        try:
            system = platform.system()
            if system in ["Linux", "Darwin"]:
                result = subprocess.run(
                    ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                if result.returncode == 0 and result.stdout.strip():
                    for pid in result.stdout.strip().split("\n"):
                        try:
                            subprocess.run(["kill", "-9", pid], timeout=1)
                            killed += 1
                        except Exception:
                            pass
            elif system == "Windows":
                result = subprocess.run(
                    ["netstat", "-ano"], capture_output=True, text=True, timeout=2
                )
                for line in result.stdout.split("\n"):
                    if f":{port}" in line and "LISTENING" in line:
                        try:
                            subprocess.run(["taskkill", "/PID", line.split()[-1]], timeout=1)
                            killed += 1
                        except Exception:
                            pass
        except Exception:
            pass
        return killed

    @staticmethod
    def load_images_from_shared_memory(images, error_callback=None):
        """Load images from shared memory and clean up."""
        from multiprocessing import shared_memory

        import numpy as np

        image_data_list = []
        for image_info in images:
            shm_name = image_info.get("shm_name")
            shape = tuple(image_info.get("shape"))
            dtype = np.dtype(image_info.get("dtype"))
            metadata = image_info.get("metadata", {})
            image_id = image_info.get("image_id")

            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                np_data = np.ndarray(shape, dtype=dtype, buffer=shm.buf).copy()
                shm.close()
                shm.unlink()

                image_data_list.append(
                    {"data": np_data, "metadata": metadata, "image_id": image_id}
                )
            except Exception as e:
                logger.error("Failed to read shared memory %s: %s", shm_name, e)
                if error_callback and image_id:
                    error_callback(image_id, "error", f"Failed to read shared memory: {e}")
                continue

        return image_data_list

    @staticmethod
    def collect_dimension_values(images, components):
        """Collect unique dimension value tuples from images."""
        if not components:
            return [()]

        values = set()
        for img_data in images:
            meta = img_data["metadata"]
            value_tuple = tuple(meta[comp] for comp in components)
            values.add(value_tuple)

        return sorted(values)

    @staticmethod
    def organize_components_by_mode(
        component_order,
        component_modes,
        component_unique_values,
        always_include_window=True,
        skip_flat_dimensions=True,
    ):
        """Organize components by their display mode."""
        result = {"window": [], "channel": [], "slice": [], "frame": []}

        for comp_name in component_order:
            mode = component_modes[comp_name]
            is_flat = len(component_unique_values.get(comp_name, set())) <= 1

            if mode == "window":
                result["window"].append(comp_name)
            elif skip_flat_dimensions and is_flat:
                continue
            else:
                result[mode].append(comp_name)

        return result

    @abstractmethod
    def handle_control_message(self, message):
        pass

    @abstractmethod
    def handle_data_message(self, message):
        pass


# Registry export
ZMQ_SERVERS = getattr(ZMQServer, "__registry__", {})
