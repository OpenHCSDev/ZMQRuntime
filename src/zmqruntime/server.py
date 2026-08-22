"""ZMQ server base class and utilities."""

from __future__ import annotations

import logging
import pickle
import platform
import subprocess
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping

import zmq
from metaclass_registry import AutoRegisterMeta

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ControlErrorResponse,
    ControlMessageType,
    ControlResponse,
    EndpointApplication,
    MessageFields,
    PongResponse,
    ProcessIdentity,
    ServerRole,
    SocketType,
)
from zmqruntime.transport import (
    TransportEndpoint,
    resolve_transport_mode,
)

logger = logging.getLogger(__name__)


class ZMQServer(ABC, metaclass=AutoRegisterMeta):
    """
    ABC for ZMQ servers - dual-channel pattern with ping/pong handshake.

    Registry auto-created and stored as ZMQServer.__registry__.
    Subclasses auto-register by setting _server_type class attribute.
    """

    __registry_key__ = "_server_type"

    _server_type: str | None = None  # Override in subclasses for registration
    _server_role = ServerRole.GENERIC

    @classmethod
    def server_type(cls) -> str | None:
        """Return this server class's registered runtime role."""
        return cls._server_type

    @classmethod
    def server_role(cls) -> ServerRole:
        """Return the protocol-level category of this server."""

        return cls._server_role

    def __init__(
        self,
        port: int,
        host: str = "*",
        log_file_path: str | None = None,
        data_socket_type=None,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
        application: EndpointApplication | None = None,
    ):
        import zmq

        self.config = config or ZMQConfig()
        self.transport_mode = resolve_transport_mode(transport_mode)
        self.endpoint = TransportEndpoint(
            host=host,
            port=port,
            transport_mode=self.transport_mode,
        )
        self.log_file_path = log_file_path
        self.data_socket_type = data_socket_type if data_socket_type is not None else zmq.PUB
        self.application = application
        self.zmq_context = None
        self.data_socket = None
        self.control_socket = None
        self._running = False
        self._ready = False
        self._lock = threading.Lock()

    @property
    def port(self) -> int:
        """Return the endpoint-owned data port."""

        return self.endpoint.port

    @property
    def host(self) -> str:
        """Return the endpoint-owned bind host."""

        return self.endpoint.host

    @property
    def control_port(self) -> int:
        """Return the endpoint-owned configured control port."""

        return self.endpoint.control_port(self.config)

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

        return self.endpoint.data_url(self.config)

    def control_transport_url(self) -> str:
        """Return the configured control endpoint for this server."""

        return self.endpoint.control_url(self.config)

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
            self.endpoint.cleanup(self.config)
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

    def control_error_response(self, error: Exception) -> ControlErrorResponse:
        """Return the canonical control error reply for a dispatch failure."""

        logger.error(
            "Error processing control message: %s",
            error,
            exc_info=(type(error), error, error.__traceback__),
        )
        return ControlErrorResponse.from_exception(error)

    def serialize_control_response(self, response: object) -> bytes:
        """Serialize a control response with the canonical error fallback."""

        try:
            if isinstance(response, ControlResponse):
                response = response.to_dict()
            return pickle.dumps(response)
        except Exception as e:
            logger.error(
                "Failed to serialize control response: %s (response_type=%s)",
                e,
                type(response).__name__,
                exc_info=True,
            )
            return pickle.dumps(
                ControlErrorResponse(
                    message="Internal server serialization error",
                ).to_dict()
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

    def _create_pong_response(self) -> PongResponse:
        """Return the typed heartbeat; serialization owns wire projection."""

        return PongResponse(
            port=self.port,
            control_port=self.control_port,
            ready=self._ready,
            server=self.__class__.__name__,
            server_type=self.__class__.server_type(),
            server_role=self.__class__.server_role(),
            application=self.application,
            log_file_path=self.log_file_path,
            process_identity=ProcessIdentity.current(),
        )

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

    @abstractmethod
    def handle_control_message(self, message):
        pass

    @abstractmethod
    def handle_data_message(self, message):
        pass
