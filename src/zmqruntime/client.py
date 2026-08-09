"""ZMQ client base class."""

from __future__ import annotations

import logging
import pickle
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from enum import Enum
from functools import singledispatch
from multiprocessing.process import BaseProcess

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ControlMessageType,
    EndpointControlCapability,
    MessageFields,
    PongResponse,
    ProcessExit,
    ResponseType,
)
from zmqruntime.startup import (
    EndpointStartupPhase,
    EndpointStartupStatus,
    EndpointStartupStatusCallback,
)
from zmqruntime.transport import (
    TransportEndpoint,
    endpoint_startup_lock,
    get_control_port,
    is_port_in_use,
    request_control_ping,
    resolve_transport_mode,
    wait_for_server_ready,
)


class EndpointConnectionPolicy(Enum):
    """Closed endpoint-connection policies with member-owned execution."""

    def __new__(
        cls,
        value: str,
        connector: Callable[[ZMQClient, float], bool],
    ) -> EndpointConnectionPolicy:
        member = object.__new__(cls)
        member._value_ = value
        member._connector = connector
        return member

    ATTACH_OR_START = (
        "attach_or_start",
        lambda client, timeout: client.connect(timeout=timeout),
    )
    ATTACH_EXISTING = (
        "attach_existing",
        lambda client, timeout: client.connect_existing(timeout=timeout),
    )

    def connect(self, client: ZMQClient, timeout: float) -> bool:
        """Execute this policy's exact connection leaf."""

        return self._connector(client, timeout)


class ClientEndpointConnection(ABC):
    """Single authoritative state for one established client connection."""

    @abstractmethod
    def close_client(self, persistent: bool) -> None:
        """Release client ownership according to endpoint persistence policy."""

    @abstractmethod
    def owned_process_is_alive(self) -> bool | None:
        """Return exact owned-process liveness, or unknown when not owned."""

    @abstractmethod
    def owned_process_exit(self) -> ProcessExit | None:
        """Return an exact owned-process exit, or none when unavailable."""

    @abstractmethod
    def known_process_is_alive(self, endpoint_is_local: bool) -> bool | None:
        """Return exact endpoint-process liveness when it can be proven."""


class EndpointProcess(ABC):
    """Nominal process operations required by an owned endpoint connection."""

    @abstractmethod
    def is_alive(self) -> bool:
        """Return whether the exact spawned process remains alive."""

    @abstractmethod
    def exit(self) -> ProcessExit | None:
        """Return the exact process exit when it has terminated."""

    @abstractmethod
    def stop(self, timeout: float = 5.0) -> None:
        """Terminate the exact spawned process, escalating when necessary."""


@dataclass(frozen=True, slots=True)
class MultiprocessingEndpointProcess(EndpointProcess):
    """Endpoint process backed by multiprocessing."""

    process: BaseProcess

    def is_alive(self) -> bool:
        return self.process.is_alive()

    def exit(self) -> ProcessExit | None:
        returncode = self.process.exitcode
        return None if returncode is None else ProcessExit(returncode)

    def stop(self, timeout: float = 5.0) -> None:
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=timeout)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=timeout)
        if self.process.is_alive():
            raise TimeoutError("Multiprocessing endpoint process did not terminate")


@dataclass(frozen=True, slots=True)
class SubprocessEndpointProcess(EndpointProcess):
    """Endpoint process backed by subprocess.Popen."""

    process: subprocess.Popen

    def is_alive(self) -> bool:
        return self.process.poll() is None

    def exit(self) -> ProcessExit | None:
        returncode = self.process.poll()
        return None if returncode is None else ProcessExit(returncode)

    def stop(self, timeout: float = 5.0) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=timeout)


EndpointProcessSource = EndpointProcess | BaseProcess | subprocess.Popen


@singledispatch
def endpoint_process(source: EndpointProcessSource) -> EndpointProcess:
    """Resolve external process handles at one nominal adapter boundary."""

    raise TypeError(f"Unsupported ZMQ server process handle: {type(source).__name__}")


@endpoint_process.register
def _(source: EndpointProcess) -> EndpointProcess:
    return source


@endpoint_process.register
def _(source: BaseProcess) -> EndpointProcess:
    return MultiprocessingEndpointProcess(source)


@endpoint_process.register
def _(source: subprocess.Popen) -> EndpointProcess:
    return SubprocessEndpointProcess(source)


@dataclass(frozen=True, slots=True)
class OwnedEndpointConnection(ClientEndpointConnection):
    """Established connection to the endpoint process spawned by this client."""

    process: EndpointProcess
    target: TransportEndpoint
    config: ZMQConfig

    def close_client(self, persistent: bool) -> None:
        if not persistent:
            self.terminate_endpoint()

    def terminate_endpoint(self) -> None:
        self.process.stop()
        self.target.cleanup(self.config)

    def owned_process_is_alive(self) -> bool | None:
        return self.process.is_alive()

    def owned_process_exit(self) -> ProcessExit | None:
        return self.process.exit()

    def known_process_is_alive(self, endpoint_is_local: bool) -> bool | None:
        return self.owned_process_is_alive()


@dataclass(frozen=True, slots=True)
class AttachedEndpointConnection(ClientEndpointConnection):
    """Established connection to an endpoint owned outside this client."""

    endpoint: PongResponse

    def close_client(self, persistent: bool) -> None:
        return None

    def owned_process_is_alive(self) -> bool | None:
        return None

    def owned_process_exit(self) -> ProcessExit | None:
        return None

    def known_process_is_alive(self, endpoint_is_local: bool) -> bool | None:
        if not endpoint_is_local or self.endpoint.process_identity is None:
            return None
        return self.endpoint.process_identity.is_alive()


@dataclass(frozen=True, slots=True)
class EndpointShutdownResult:
    """Observed outcome of one endpoint shutdown operation."""

    succeeded: bool
    endpoint_terminated: bool


@dataclass(slots=True)
class _EndpointShutdownOperation:
    """State and mechanics for one endpoint shutdown request."""

    target: TransportEndpoint
    timeout: float
    config: ZMQConfig
    endpoint: PongResponse | None
    acknowledged: bool

    def acknowledgement_result(self) -> EndpointShutdownResult:
        """Report whether a non-terminating shutdown request was acknowledged."""

        return EndpointShutdownResult(
            succeeded=self.acknowledged,
            endpoint_terminated=False,
        )

    def termination_result(self) -> EndpointShutdownResult:
        """Prove endpoint termination, escalating through owned process identity."""

        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if not self._endpoint_responds():
                return self._terminated_result()
            time.sleep(0.05)

        process_identity = None if self.endpoint is None else self.endpoint.process_identity
        if (
            process_identity is not None
            and self.target.transport_mode.endpoint_is_local(
                self.target.host,
                self.target.control_port(self.config),
            )
            and process_identity.terminate(timeout=self.timeout)
        ):
            return self._terminated_result()

        return EndpointShutdownResult(succeeded=False, endpoint_terminated=False)

    def _endpoint_responds(self) -> bool:
        return self.target.ping(self.config, timeout_ms=100) is not None

    def _terminated_result(self) -> EndpointShutdownResult:
        self.target.cleanup(self.config)
        return EndpointShutdownResult(succeeded=True, endpoint_terminated=True)


class EndpointShutdownMode(str, Enum):
    """Endpoint shutdown modes with member-owned wire and completion leaves."""

    def __new__(
        cls,
        value: str,
        control_message_type: ControlMessageType,
        required_capability: EndpointControlCapability,
        completion: Callable[[_EndpointShutdownOperation], EndpointShutdownResult],
    ) -> EndpointShutdownMode:
        member = str.__new__(cls, value)
        member._value_ = value
        member.control_message_type = control_message_type
        member.required_capability = required_capability
        member._completion = completion
        return member

    GRACEFUL = (
        "graceful",
        ControlMessageType.SHUTDOWN,
        EndpointControlCapability.SHUTDOWN,
        _EndpointShutdownOperation.acknowledgement_result,
    )
    FORCE = (
        "force",
        ControlMessageType.FORCE_SHUTDOWN,
        EndpointControlCapability.FORCE_SHUTDOWN,
        _EndpointShutdownOperation.termination_result,
    )

    @classmethod
    def from_graceful(cls, graceful: bool) -> EndpointShutdownMode:
        """Resolve a legacy Boolean only at the nominal declaration boundary."""

        return cls.GRACEFUL if graceful else cls.FORCE

    def complete(
        self,
        operation: _EndpointShutdownOperation,
    ) -> EndpointShutdownResult:
        """Execute this member's completion leaf."""

        return self._completion(operation)


class ZMQClient(ABC):
    """ABC for ZMQ clients - dual-channel pattern with auto-spawning."""

    def __init__(
        self,
        port: int,
        host: str = "localhost",
        persistent: bool = True,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
        connection_status_callback: EndpointStartupStatusCallback | None = None,
    ):
        self.config = config or ZMQConfig()
        self.endpoint = TransportEndpoint(
            host=host,
            port=port,
            transport_mode=resolve_transport_mode(transport_mode),
        )
        self.persistent = persistent
        self.zmq_context = None
        self.data_socket = None
        self.control_socket = None
        self._connection: ClientEndpointConnection | None = None
        self._lock = threading.Lock()
        self._connection_status_callback = connection_status_callback
        self._connection_status_sequence = 0

    @property
    def port(self) -> int:
        return self.endpoint.port

    @property
    def host(self) -> str:
        return self.endpoint.host

    @property
    def control_port(self) -> int:
        return self.endpoint.control_port(self.config)

    @property
    def transport_mode(self) -> TransportMode:
        return self.endpoint.transport_mode

    def _emit_connection_status(
        self,
        phase: EndpointStartupPhase,
        message: str,
    ) -> None:
        """Publish one client-owned lifecycle transition."""

        self._connection_status_sequence += 1
        status = EndpointStartupStatus(
            phase=phase,
            message=message,
            sequence=self._connection_status_sequence,
            timestamp=time.time(),
        )
        if self._connection_status_callback is not None:
            self._connection_status_callback(status)

    def connect(self, timeout: float = 10.0):
        self._emit_connection_status(
            EndpointStartupPhase.CHECKING_ENDPOINT,
            f"Checking server endpoint on port {self.port}",
        )
        try:
            with self._lock:
                return self._connect_locked(timeout)
        except BaseException as error:
            self._emit_connection_status(
                EndpointStartupPhase.FAILED,
                f"Server endpoint connection failed: {error}",
            )
            raise

    def connect_existing(self, timeout: float = 1.0) -> bool:
        """Attach to a ready endpoint without starting or replacing a server."""

        self._emit_connection_status(
            EndpointStartupPhase.CHECKING_ENDPOINT,
            f"Checking existing server endpoint on port {self.port}",
        )
        try:
            with self._lock:
                if self.is_connected():
                    self._emit_connected_status()
                    return True
                with endpoint_startup_lock(
                    self.port,
                    self.transport_mode,
                    self.config,
                ):
                    if not self._is_port_in_use(self.port):
                        self._emit_connection_status(
                            EndpointStartupPhase.DISCONNECTED,
                            f"No server endpoint available on port {self.port}",
                        )
                        return False
                    if self._attach_existing_endpoint(timeout):
                        return True
                    self._emit_connection_status(
                        EndpointStartupPhase.FAILED,
                        f"Server endpoint on port {self.port} is unresponsive",
                    )
                    return False
        except BaseException as error:
            self._emit_connection_status(
                EndpointStartupPhase.FAILED,
                f"Existing server endpoint connection failed: {error}",
            )
            raise

    def _connect_locked(self, timeout: float) -> bool:
        """Connect while the caller owns the client lifecycle lock."""

        if self.is_connected():
            self._emit_connected_status()
            return True
        with endpoint_startup_lock(
            self.port,
            self.transport_mode,
            self.config,
        ):
            if self._is_port_in_use(self.port):
                if self._attach_existing_endpoint(timeout):
                    return True
                if self.transport_mode.preserve_unresponsive_endpoint(
                    self.port,
                    self.config,
                ):
                    self._emit_connection_status(
                        EndpointStartupPhase.FAILED,
                        f"Server endpoint on port {self.port} is unresponsive",
                    )
                    return False
                self._kill_processes_on_port(self.port)
                self._kill_processes_on_port(self.control_port)
                time.sleep(0.5)
            self._emit_connection_status(
                EndpointStartupPhase.STARTING_PROCESS,
                f"Starting server process for port {self.port}",
            )
            owned_connection = OwnedEndpointConnection(
                process=endpoint_process(self._spawn_server_process()),
                target=self.endpoint,
                config=self.config,
            )
            if not self._wait_for_server_ready(
                owned_connection.process,
                timeout=timeout,
            ):
                owned_connection.terminate_endpoint()
                self._emit_connection_status(
                    EndpointStartupPhase.FAILED,
                    f"Server endpoint on port {self.port} did not become ready",
                )
                return False
            try:
                self._setup_client_sockets()
            except Exception:
                owned_connection.terminate_endpoint()
                raise
            self._connection = owned_connection
            self._emit_connected_status()
            return True

    def _attach_existing_endpoint(self, timeout: float) -> bool:
        endpoint = self._try_connect_to_existing(
            self.port,
            timeout_ms=self._existing_endpoint_probe_timeout_ms(timeout),
        )
        if endpoint is None:
            return False
        self._setup_client_sockets()
        self._connection = AttachedEndpointConnection(endpoint)
        self._emit_connected_status()
        return True

    def _emit_connected_status(self) -> None:
        self._emit_connection_status(
            EndpointStartupPhase.CONNECTED,
            f"Connected to server endpoint on port {self.port}",
        )

    def disconnect(self):
        with self._lock:
            connection = self._connection
            if connection is None:
                return
            try:
                try:
                    self._cleanup_sockets()
                finally:
                    connection.close_client(self.persistent)
            finally:
                self._connection = None
                self._emit_connection_status(
                    EndpointStartupPhase.DISCONNECTED,
                    f"Disconnected from server endpoint on port {self.port}",
                )

    def is_connected(self):
        return self._connection is not None

    def owned_server_process_is_alive(self) -> bool | None:
        """Return exact liveness when this client owns the server process."""
        connection = self._connection
        return None if connection is None else connection.owned_process_is_alive()

    def owned_server_process_exit(self) -> ProcessExit | None:
        """Return an exact terminal status when this client owns the process."""

        connection = self._connection
        return None if connection is None else connection.owned_process_exit()

    def known_server_process_is_alive(self) -> bool | None:
        """Return exact liveness for an owned or identified local server."""

        connection = self._connection
        if connection is None:
            return None
        return connection.known_process_is_alive(
            self.transport_mode.endpoint_is_local(
                self.host,
                self.control_port,
            )
        )

    def _setup_client_sockets(self):
        import zmq

        logger = logging.getLogger(__name__)
        self.zmq_context = zmq.Context()
        data_url = self.endpoint.data_url(self.config)

        self.data_socket = self.zmq_context.socket(zmq.SUB)
        self.data_socket.setsockopt(zmq.LINGER, 0)
        self.data_socket.connect(data_url)
        self.data_socket.setsockopt(zmq.SUBSCRIBE, b"")
        time.sleep(0.1)
        logger.info(f"Set up ZMQ SUB socket connected to {data_url}")

    def _cleanup_sockets(self):
        if self.data_socket:
            self.data_socket.close()
            self.data_socket = None
        if self.control_socket:
            self.control_socket.close()
            self.control_socket = None

        if self.zmq_context:
            self.zmq_context.term()
            self.zmq_context = None

    def _try_connect_to_existing(
        self,
        port: int,
        timeout_ms: int = 500,
    ) -> PongResponse | None:
        response = request_control_ping(
            port,
            self.transport_mode,
            host=self.host,
            config=self.config,
            timeout_ms=timeout_ms,
        )
        if response is None or not response.ready:
            return None
        return response

    @staticmethod
    def _existing_endpoint_probe_timeout_ms(timeout: float) -> int:
        return max(500, min(int(timeout * 1000), 5000))

    def _wait_for_server_ready(
        self,
        process: EndpointProcess,
        timeout: float = 10.0,
    ) -> bool:
        return wait_for_server_ready(
            self.port,
            self.transport_mode,
            host=self.host,
            config=self.config,
            timeout=timeout,
        )

    def _is_port_in_use(self, port: int) -> bool:
        return is_port_in_use(
            port,
            self.transport_mode,
            host=self.host,
            config=self.config,
        )

    def _kill_processes_on_port(self, port: int):
        self.transport_mode.kill_processes_on_port(port, self.config)

    @staticmethod
    def scan_servers(
        ports,
        host: str = "localhost",
        timeout_ms: int = 200,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
    ):
        config = config or ZMQConfig()
        transport_mode = resolve_transport_mode(transport_mode)
        ports = tuple(ports)
        if not ports:
            return []

        def scan_port(port):
            pong = request_control_ping(
                port,
                transport_mode,
                host=host,
                config=config,
                timeout_ms=timeout_ms,
            )
            if pong is None:
                return None
            return replace(
                pong,
                port=port,
                control_port=get_control_port(port, config),
            )

        servers = []
        worker_count = min(len(ports), 32)
        executor = ThreadPoolExecutor(max_workers=worker_count)
        try:
            futures = tuple(executor.submit(scan_port, port) for port in ports)
            done, _ = wait(futures, timeout=max(timeout_ms / 1000, 0.001))
            for future in done:
                server = future.result()
                if server is not None:
                    servers.append(server)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return sorted(servers, key=lambda server: ports.index(server.port))

    @staticmethod
    def shutdown_endpoint_on_port(
        port: int,
        mode: EndpointShutdownMode,
        timeout: float = 5.0,
        transport_mode: TransportMode | None = None,
        host: str = "localhost",
        config: ZMQConfig | None = None,
    ) -> EndpointShutdownResult:
        config = config or ZMQConfig()
        transport_mode = resolve_transport_mode(transport_mode)
        if not isinstance(mode, EndpointShutdownMode):
            raise TypeError("Shutdown mode must be an EndpointShutdownMode instance.")
        target = TransportEndpoint(
            host=host,
            port=port,
            transport_mode=transport_mode,
        )
        endpoint = target.ping(
            config,
            timeout_ms=min(max(int(timeout * 1000), 100), 1000),
        )
        if endpoint is None and not target.is_in_use(config):
            return EndpointShutdownResult(succeeded=True, endpoint_terminated=True)
        if endpoint is None or mode.required_capability not in endpoint.control_capabilities:
            return EndpointShutdownResult(succeeded=False, endpoint_terminated=False)

        acknowledged = False
        sock = None
        try:
            control_url = target.control_url(config)

            ctx = zmq.Context.instance()
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(control_url)
            sock.setsockopt(zmq.SNDTIMEO, min(int(timeout * 1000), 1000))
            sock.setsockopt(zmq.RCVTIMEO, min(int(timeout * 1000), 1000))
            sock.send(
                pickle.dumps(
                    {MessageFields.TYPE: mode.control_message_type.value},
                )
            )
            ack = pickle.loads(sock.recv())
            acknowledged = ack.get(MessageFields.TYPE) == ResponseType.SHUTDOWN_ACK.value
        except (EOFError, KeyError, OSError, TypeError, pickle.PickleError, zmq.ZMQError):
            acknowledged = False
        finally:
            if sock is not None:
                sock.close(linger=0)

        return mode.complete(
            _EndpointShutdownOperation(
                target=target,
                timeout=timeout,
                config=config,
                endpoint=endpoint,
                acknowledged=acknowledged,
            )
        )

    @staticmethod
    def shutdown_server_on_port(
        port: int,
        graceful: bool = True,
        timeout: float = 5.0,
        transport_mode: TransportMode | None = None,
        host: str = "localhost",
        config: ZMQConfig | None = None,
    ) -> EndpointShutdownResult:
        """Compatibility boundary for callers still supplying a Boolean mode."""

        return ZMQClient.shutdown_endpoint_on_port(
            port=port,
            mode=EndpointShutdownMode.from_graceful(graceful),
            timeout=timeout,
            transport_mode=transport_mode,
            host=host,
            config=config,
        )

    @staticmethod
    def kill_server_on_port(
        port: int,
        graceful: bool = True,
        timeout: float = 5.0,
        transport_mode: TransportMode | None = None,
        host: str = "localhost",
        config: ZMQConfig | None = None,
    ) -> bool:
        """Compatibility wrapper for callers that only consume success."""

        return ZMQClient.shutdown_server_on_port(
            port=port,
            graceful=graceful,
            timeout=timeout,
            transport_mode=transport_mode,
            host=host,
            config=config,
        ).succeeded

    @abstractmethod
    def _spawn_server_process(self):
        pass

    @abstractmethod
    def send_data(self, data):
        pass
