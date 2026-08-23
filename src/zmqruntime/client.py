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
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import Enum
from functools import singledispatch
from multiprocessing.process import BaseProcess
from typing import Generic, TypeVar

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ControlMessageType,
    EndpointApplicationCompatibility,
    EndpointControlCapability,
    MessageFields,
    PongResponse,
    ProcessExit,
    ResponseType,
)
from zmqruntime.startup import (
    IDLE_ENDPOINT_STARTUP_OBSERVER,
    EndpointStartupCancellationObserver,
    EndpointStartupObserver,
    EndpointStartupPhase,
    EndpointStartupStatus,
    EndpointStartupStatusCallback,
)
from zmqruntime.timeouts import OperationCancellation, OperationDeadline
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

    def connect(
        self,
        client: ZMQClient,
        timeout: float,
    ) -> bool:
        """Execute this policy's exact connection leaf."""

        return self._connector(client, timeout)


class EndpointConnectionAttempt:
    """One cancellable invocation of a declared endpoint connection policy."""

    __slots__ = ("_cancellation", "_client")

    def __init__(self, client: ZMQClient) -> None:
        self._client = client
        self._cancellation = OperationCancellation()

    def cancel(self) -> None:
        """Request cancellation of this exact attempt."""

        self._cancellation.cancel()

    def connect(self, policy: EndpointConnectionPolicy, timeout: float) -> bool:
        """Execute the selected connection leaf under this attempt's authority."""

        with self._client._bind_connection_attempt(self._cancellation):
            return policy.connect(self._client, timeout)


class EndpointCompatibilityClientABC(ABC):
    """Nominal client contract able to prove endpoint application identity."""

    @abstractmethod
    def endpoint_compatibility(self) -> EndpointApplicationCompatibility:
        """Return compatibility with the application's local declaration."""


CompatibleClientT = TypeVar("CompatibleClientT", bound=EndpointCompatibilityClientABC)


@dataclass(slots=True, eq=False)
class EndpointClientSession(Generic[CompatibleClientT]):
    """One observed transport client and its application admission proof."""

    client: CompatibleClientT
    compatibility: EndpointApplicationCompatibility | None = None

    def observe_compatibility(self) -> EndpointApplicationCompatibility:
        self.compatibility = self.client.endpoint_compatibility()
        return self.compatibility

    def require_admitted_client(self) -> CompatibleClientT:
        if self.compatibility is None:
            raise RuntimeError("Endpoint compatibility has not been observed")
        self.compatibility.require_match()
        return self.client

    @property
    def admitted_client(self) -> CompatibleClientT | None:
        if self.compatibility is None or not self.compatibility.matches:
            return None
        return self.client


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

    @abstractmethod
    def handshake_response(self) -> PongResponse:
        """Return the handshake that established this connection."""


class EndpointProcess(ABC):
    """Nominal process operations required by an owned endpoint connection."""

    @abstractmethod
    def is_alive(self) -> bool:
        """Return whether the exact spawned process remains alive."""

    @abstractmethod
    def exit(self) -> ProcessExit | None:
        """Return the exact process exit when it has terminated."""

    @abstractmethod
    def wait_for_exit(self, timeout: float) -> ProcessExit | None:
        """Wait for the exact process and return its exit, or none on timeout."""

    @abstractmethod
    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        """Terminate the exact process and report whether escalation was required."""


@dataclass(frozen=True, slots=True)
class MultiprocessingEndpointProcess(EndpointProcess):
    """Endpoint process backed by multiprocessing."""

    process: BaseProcess

    def is_alive(self) -> bool:
        return self.process.is_alive()

    def exit(self) -> ProcessExit | None:
        returncode = self.process.exitcode
        return None if returncode is None else ProcessExit(returncode)

    def wait_for_exit(self, timeout: float) -> ProcessExit | None:
        self.process.join(timeout=timeout)
        return self.exit()

    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        forced = False
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=timeout)
        if self.process.is_alive():
            forced = True
            self.process.kill()
            self.process.join(timeout=kill_timeout)
        if self.process.is_alive():
            raise TimeoutError("Multiprocessing endpoint process did not terminate")
        return forced


@dataclass(frozen=True, slots=True)
class SubprocessEndpointProcess(EndpointProcess):
    """Endpoint process backed by subprocess.Popen."""

    process: subprocess.Popen

    def is_alive(self) -> bool:
        return self.process.poll() is None

    def exit(self) -> ProcessExit | None:
        returncode = self.process.poll()
        return None if returncode is None else ProcessExit(returncode)

    def wait_for_exit(self, timeout: float) -> ProcessExit | None:
        try:
            return ProcessExit(self.process.wait(timeout=timeout))
        except subprocess.TimeoutExpired:
            return None

    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=kill_timeout)
                return True
        return False


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


class EndpointProcessGroup:
    """Authoritative owner for every exact endpoint process added to the group."""

    def __init__(self) -> None:
        self._processes: dict[int, EndpointProcess] = {}
        self._lock = threading.Lock()

    def own(self, source: EndpointProcessSource) -> EndpointProcess:
        """Retain ownership of one process source until release or group shutdown."""

        process = endpoint_process(source)
        with self._lock:
            self._discard_terminated_locked()
            self._processes[id(source)] = process
        return process

    def disown(self, source: EndpointProcessSource) -> EndpointProcess | None:
        """Release this group's ownership without stopping the process."""

        with self._lock:
            return self._processes.pop(id(source), None)

    @property
    def active_count(self) -> int:
        """Return the number of processes this group still owns and observes alive."""

        with self._lock:
            self._discard_terminated_locked()
            return len(self._processes)

    def stop_all(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> None:
        """Stop and release every process currently owned by this group."""

        with self._lock:
            owned_processes = tuple(self._processes.items())

        if not owned_processes:
            return

        def stop_owned_process(
            owned_process: tuple[int, EndpointProcess],
        ) -> list[BaseException]:
            source_id, process = owned_process
            process_failures: list[BaseException] = []
            try:
                if process.is_alive():
                    process.stop(timeout=timeout, kill_timeout=kill_timeout)
            except BaseException as exc:
                process_failures.append(exc)
            finally:
                try:
                    alive = process.is_alive()
                except BaseException as exc:
                    process_failures.append(exc)
                    alive = True
                if not alive:
                    with self._lock:
                        if self._processes.get(source_id) is process:
                            self._processes.pop(source_id)
            return process_failures

        failures: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=len(owned_processes)) as executor:
            for process_failures in executor.map(
                stop_owned_process,
                owned_processes,
            ):
                failures.extend(process_failures)

        if failures:
            raise RuntimeError(
                f"Failed to stop {len(failures)} owned endpoint process operation(s)."
            ) from failures[0]

    def _discard_terminated_locked(self) -> None:
        terminated_ids = [
            source_id for source_id, process in self._processes.items() if not process.is_alive()
        ]
        for source_id in terminated_ids:
            self._processes.pop(source_id)


@dataclass(frozen=True, slots=True)
class OwnedEndpointConnection(ClientEndpointConnection):
    """Established connection to the endpoint process spawned by this client."""

    process: EndpointProcess
    target: TransportEndpoint
    config: ZMQConfig
    endpoint: PongResponse
    shutdown_timeout_seconds: float = 10.0

    def close_client(self, persistent: bool) -> None:
        if not persistent:
            self.terminate_endpoint()

    def terminate_endpoint(self) -> None:
        shutdown = ZMQClient.shutdown_endpoint_on_port(
            port=self.target.port,
            mode=EndpointShutdownMode.FORCE,
            timeout=self.shutdown_timeout_seconds,
            transport_mode=self.target.transport_mode,
            host=self.target.host,
            config=self.config,
        )
        if shutdown.succeeded:
            process_exit = self.process.wait_for_exit(timeout=self.shutdown_timeout_seconds)
            if process_exit is not None:
                self.target.cleanup(self.config)
                return
        self.process.stop(timeout=self.shutdown_timeout_seconds)
        self.target.cleanup(self.config)

    def owned_process_is_alive(self) -> bool | None:
        return self.process.is_alive()

    def owned_process_exit(self) -> ProcessExit | None:
        return self.process.exit()

    def known_process_is_alive(self, endpoint_is_local: bool) -> bool | None:
        return self.owned_process_is_alive()

    def handshake_response(self) -> PongResponse:
        return self.endpoint


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

    def handshake_response(self) -> PongResponse:
        return self.endpoint


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
            and self.target.transport_mode.declaration.endpoint_is_local(
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

    @classmethod
    def from_force(cls, force: bool) -> EndpointShutdownMode:
        """Resolve a force flag only at the nominal declaration boundary."""

        return cls.FORCE if force else cls.GRACEFUL

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
        self._connection_cancellation: ContextVar[OperationCancellation | None] = ContextVar(
            f"{type(self).__qualname__}.connection_cancellation",
            default=None,
        )
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

    @contextmanager
    def _bind_connection_attempt(
        self,
        cancellation: OperationCancellation,
    ):
        """Bind the exact cancellation authority for one connection attempt."""

        if self._connection_cancellation.get() is not None:
            raise RuntimeError("A connection attempt is already active in this context")
        token = self._connection_cancellation.set(cancellation)
        try:
            yield
        finally:
            self._connection_cancellation.reset(token)

    @contextmanager
    def _ensure_connection_attempt(self):
        """Give direct client calls an exact local cancellation authority."""

        if self._connection_cancellation.get() is not None:
            yield
            return
        with self._bind_connection_attempt(OperationCancellation()):
            yield

    def new_connection_attempt(self) -> EndpointConnectionAttempt:
        """Create the exact cancellable authority for one future connection call."""

        return EndpointConnectionAttempt(self)

    def _connection_cancelled(self) -> bool:
        cancellation = self._connection_cancellation.get()
        return cancellation is not None and cancellation.requested()

    def connect(
        self,
        timeout: float = 10.0,
        *,
        operation_deadline: OperationDeadline | None = None,
    ):
        with self._ensure_connection_attempt():
            self._emit_connection_status(
                EndpointStartupPhase.CHECKING_ENDPOINT,
                f"Checking server endpoint on port {self.port}",
            )
            try:
                with self._lock:
                    return self._connect_locked(
                        timeout,
                        operation_deadline=operation_deadline,
                    )
            except BaseException as error:
                self._emit_connection_status(
                    EndpointStartupPhase.FAILED,
                    f"Server endpoint connection failed: {error}",
                )
                raise

    def connect_existing(
        self,
        timeout: float = 1.0,
    ) -> bool:
        """Attach to a ready endpoint without starting or replacing a server."""

        with self._ensure_connection_attempt():
            self._emit_connection_status(
                EndpointStartupPhase.CHECKING_ENDPOINT,
                f"Checking existing server endpoint on port {self.port}",
            )
            try:
                with self._lock:
                    if self.is_connected():
                        self._emit_connected_status()
                        return True
                    if self._connection_cancelled():
                        return self._cancelled_connection_result()
                    with endpoint_startup_lock(
                        self.port,
                        self.transport_mode,
                        self.config,
                        cancellation=self._connection_cancellation.get(),
                    ) as lock_acquired:
                        if not lock_acquired:
                            return self._cancelled_connection_result()
                        if not self._is_port_in_use(self.port):
                            self._emit_connection_status(
                                EndpointStartupPhase.DISCONNECTED,
                                f"No server endpoint available on port {self.port}",
                            )
                            return False
                        if self._attach_existing_endpoint(timeout):
                            return True
                        if self._connection_cancelled():
                            return self._cancelled_connection_result()
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

    def _connect_locked(
        self,
        timeout: float,
        *,
        operation_deadline: OperationDeadline | None = None,
    ) -> bool:
        """Connect while the caller owns the client lifecycle lock."""

        if self.is_connected():
            self._emit_connected_status()
            return True
        if self._connection_cancelled():
            return self._cancelled_connection_result()
        with endpoint_startup_lock(
            self.port,
            self.transport_mode,
            self.config,
            operation_deadline=operation_deadline,
            cancellation=self._connection_cancellation.get(),
        ) as lock_acquired:
            if not lock_acquired:
                return self._cancelled_connection_result()
            if self._is_port_in_use(self.port):
                attach_timeout = (
                    timeout
                    if operation_deadline is None
                    else operation_deadline.cap_seconds(timeout)
                )
                if self._attach_existing_endpoint(attach_timeout):
                    return True
                if self._connection_cancelled():
                    return self._cancelled_connection_result()
                if self.transport_mode.declaration.preserve_unresponsive_endpoint(
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
                if operation_deadline is None:
                    time.sleep(0.5)
                else:
                    time.sleep(min(0.5, operation_deadline.remaining_seconds()))
            if self._connection_cancelled():
                return self._cancelled_connection_result()
            self._emit_connection_status(
                EndpointStartupPhase.STARTING_PROCESS,
                f"Starting server process for port {self.port}",
            )
            process = endpoint_process(self._spawn_server_process())
            try:
                if operation_deadline is None:
                    endpoint = self._wait_for_endpoint_ready(
                        process,
                        timeout=timeout,
                    )
                else:
                    endpoint = self._wait_for_endpoint_ready_before_deadline(
                        process,
                        timeout=timeout,
                        operation_deadline=operation_deadline,
                    )
            except BaseException:
                process.stop()
                self.endpoint.cleanup(self.config)
                raise
            if endpoint is None:
                process.stop()
                self.endpoint.cleanup(self.config)
                if self._connection_cancelled():
                    return self._cancelled_connection_result()
                self._emit_connection_status(
                    EndpointStartupPhase.FAILED,
                    f"Server endpoint on port {self.port} did not become ready",
                )
                return False
            if self._connection_cancelled():
                process.stop()
                self.endpoint.cleanup(self.config)
                return self._cancelled_connection_result()
            owned_connection = OwnedEndpointConnection(
                process=process,
                target=self.endpoint,
                config=self.config,
                endpoint=endpoint,
            )
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
        if endpoint is None or self._connection_cancelled():
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

    def _cancelled_connection_result(self) -> bool:
        self._emit_connection_status(
            EndpointStartupPhase.DISCONNECTED,
            f"Connection attempt cancelled for port {self.port}",
        )
        return False

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

    @property
    def connected_endpoint(self) -> PongResponse | None:
        """Return the handshake owned by the established connection, if any."""

        connection = self._connection
        return None if connection is None else connection.handshake_response()

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
            self.transport_mode.declaration.endpoint_is_local(
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
        return max(1, min(int(timeout * 1000), 5000))

    def _wait_for_endpoint_ready_before_deadline(
        self,
        process: EndpointProcess,
        *,
        timeout: float,
        operation_deadline: OperationDeadline,
    ) -> PongResponse | None:
        """Preserve the readiness hook while applying a total caller deadline."""

        return self._wait_for_endpoint_ready(
            process,
            timeout=operation_deadline.cap_seconds(timeout),
        )

    def _wait_for_endpoint_ready(
        self,
        process: EndpointProcess,
        timeout: float = 10.0,
    ) -> PongResponse | None:
        """Return the handshake after the established readiness extension point.

        ``_wait_for_server_ready`` remains the lifecycle hook so clients built
        against earlier zmqruntime releases keep their startup observers.  New
        clients that need the first typed PONG can override this adapter
        directly.
        """

        if not self._wait_for_server_ready(process, timeout=timeout):
            return None
        return self._try_connect_to_existing(
            self.port,
            timeout_ms=self._existing_endpoint_probe_timeout_ms(timeout),
        )

    def _wait_for_server_ready(
        self,
        process: EndpointProcess,
        timeout: float = 10.0,
    ) -> bool:
        """Wait for readiness through the stable client extension point."""

        return wait_for_server_ready(
            self.port,
            self.transport_mode,
            host=self.host,
            config=self.config,
            timeout=timeout,
            startup_observer=self._connection_startup_observer(),
        )

    def _connection_startup_observer(
        self,
        observed: EndpointStartupObserver = IDLE_ENDPOINT_STARTUP_OBSERVER,
    ) -> EndpointStartupObserver:
        """Compose client cancellation with an optional startup observer."""

        cancellation = self._connection_cancellation.get()
        return EndpointStartupCancellationObserver(
            cancellation or OperationCancellation(),
            observed,
        )

    def _is_port_in_use(self, port: int) -> bool:
        return is_port_in_use(
            port,
            self.transport_mode,
            host=self.host,
            config=self.config,
        )

    def _kill_processes_on_port(self, port: int):
        self.transport_mode.declaration.kill_processes_on_port(port, self.config)

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
