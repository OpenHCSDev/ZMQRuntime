"""ZMQ client base class."""

from __future__ import annotations

import asyncio
import logging
import pickle
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
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
    EndpointShutdownRequest,
    MessageFields,
    PongResponse,
    ProcessExit,
    ProcessIdentity,
    ResponseType,
)
from zmqruntime.startup import (
    IDLE_ENDPOINT_STARTUP_OBSERVER,
    EndpointStartupCancellationObserver,
    EndpointStartupObserver,
    EndpointStartupPhase,
    EndpointStartupProcessObserver,
    EndpointStartupStatus,
    EndpointStartupStatusCallback,
)
from zmqruntime.timeouts import OperationCancellation, OperationDeadline
from zmqruntime.transport import (
    TransportEndpoint,
    endpoint_startup_lock,
    is_port_in_use,
    request_control_ping,
    resolve_transport_mode,
    wait_for_endpoint_ready,
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


class EndpointConnectionCancelledError(RuntimeError):
    """Raised when the owner cancels one exact endpoint connection attempt."""


class EndpointStartupUncertainError(RuntimeError):
    """The child exists, but publishing its pre-bind reservation failed."""

    def __init__(self, process: EndpointProcess) -> None:
        self.process = process
        super().__init__("Child spawned; reservation publication uncertain. Do not replay startup.")


class EndpointConnectionAttempt:
    """One cancellable invocation of a declared endpoint connection policy."""

    __slots__ = ("_cancellation", "_client")

    def __init__(
        self,
        client: ZMQClient,
        *,
        cancellation: OperationCancellation | None = None,
    ) -> None:
        self._client = client
        self._cancellation = OperationCancellation() if cancellation is None else cancellation

    def cancel(self) -> None:
        """Request cancellation of this exact attempt."""

        self._cancellation.cancel()

    def connect(self, policy: EndpointConnectionPolicy, timeout: float) -> bool:
        """Execute the selected connection leaf under this attempt's authority."""

        with self._client._bind_connection_attempt(self._cancellation):
            connected = policy.connect(self._client, timeout)
        if not self._cancellation.requested():
            return connected
        self._client.disconnect()
        raise EndpointConnectionCancelledError(
            "Endpoint connection attempt was cancelled by its owner."
        )

    async def connect_async(self, policy: EndpointConnectionPolicy, timeout: float) -> bool:
        """Await this same attempt without abandoning its worker on cancellation."""
        worker = asyncio.get_running_loop().run_in_executor(
            None, self.connect, policy, timeout
        )
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError as cancelled:
            self.cancel()
            reaped = asyncio.create_task(asyncio.wait((worker,)))
            while not worker.done():
                try:
                    await asyncio.shield(reaped)
                except asyncio.CancelledError:
                    self.cancel()
            try:
                worker.result()
            except EndpointConnectionCancelledError:
                pass
            raise cancelled


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

    def startup_observer(self, observed: EndpointStartupObserver) -> EndpointStartupObserver:
        """Give every native process leaf the same exact-work startup mechanism."""
        return EndpointStartupProcessObserver(self.identity, observed)

    @property
    @abstractmethod
    def identity(self) -> ProcessIdentity:
        """Return the exact process incarnation captured from the native handle."""

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


class _EndpointProcessExitObservation:
    """Observe and retain one exact child until its platform wait completes."""

    def __init__(self, wait_for_exit: Callable[[], ProcessExit]) -> None:
        self._wait_for_exit = wait_for_exit
        self._completed = threading.Event()
        self._exit: ProcessExit | None = None
        self._failure: BaseException | None = None
        threading.Thread(
            target=self._observe,
            name="zmqruntime-endpoint-process-reaper",
            daemon=True,
        ).start()

    def _observe(self) -> None:
        try:
            self._exit = self._wait_for_exit()
        except BaseException as error:
            self._failure = error
        finally:
            self._completed.set()

    def exit(self) -> ProcessExit | None:
        if not self._completed.is_set():
            return None
        if self._failure is not None:
            raise RuntimeError("Failed to observe endpoint process exit") from self._failure
        return self._exit

    def wait(self, timeout: float) -> ProcessExit | None:
        if not self._completed.wait(timeout=timeout):
            return None
        return self.exit()


@dataclass(frozen=True, slots=True)
class _ObservedEndpointProcess(EndpointProcess, ABC):
    """Template owner for exact child exit observation and bounded waits."""

    _exit_observation: _EndpointProcessExitObservation = field(
        init=False,
        repr=False,
        compare=False,
    )
    _identity: ProcessIdentity = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_identity", self._capture_identity())
        object.__setattr__(
            self,
            "_exit_observation",
            _EndpointProcessExitObservation(self._wait_and_resolve_exit),
        )

    @property
    def identity(self) -> ProcessIdentity:
        return self._identity

    @abstractmethod
    def _capture_identity(self) -> ProcessIdentity:
        """Capture identity before the owned process reaper can release the PID."""

    @abstractmethod
    def _wait_and_resolve_exit(self) -> ProcessExit:
        """Perform the platform wait and resolve its exact terminal status."""

    def exit(self) -> ProcessExit | None:
        return self._exit_observation.exit()

    def wait_for_exit(self, timeout: float) -> ProcessExit | None:
        return self._exit_observation.wait(timeout)


@dataclass(frozen=True, slots=True)
class MultiprocessingEndpointProcess(_ObservedEndpointProcess):
    """Endpoint process backed by multiprocessing."""

    process: BaseProcess

    def _capture_identity(self) -> ProcessIdentity:
        return ProcessIdentity.for_pid(self.process.pid)

    def _wait_and_resolve_exit(self) -> ProcessExit:
        self.process.join()
        returncode = self.process.exitcode
        if returncode is None:
            raise RuntimeError("Multiprocessing endpoint exited without a return code")
        return ProcessExit(returncode)

    def is_alive(self) -> bool:
        return self.process.is_alive()

    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        forced = False
        if self.is_alive():
            self.process.terminate()
        if self.wait_for_exit(timeout) is None:
            forced = True
            self.process.kill()
        if self.wait_for_exit(kill_timeout) is None:
            raise TimeoutError("Multiprocessing endpoint process did not terminate")
        return forced


@dataclass(frozen=True, slots=True)
class SubprocessEndpointProcess(_ObservedEndpointProcess):
    """Endpoint process backed by subprocess.Popen."""

    process: subprocess.Popen

    def _capture_identity(self) -> ProcessIdentity:
        return ProcessIdentity.for_pid(self.process.pid)

    def _wait_and_resolve_exit(self) -> ProcessExit:
        return ProcessExit(self.process.wait())

    def is_alive(self) -> bool:
        return self.process.poll() is None

    def stop(
        self,
        timeout: float = 5.0,
        kill_timeout: float = 2.0,
    ) -> bool:
        if self.is_alive():
            self.process.terminate()
        if self.wait_for_exit(timeout) is not None:
            return False
        self.process.kill()
        if self.wait_for_exit(kill_timeout) is None:
            raise TimeoutError("Subprocess endpoint process did not terminate")
        return True


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
    """Endpoint disappearance and exact process exit are separate observations.

    request_attempted records one wire send attempt, not proof of delivery.
    process_exited=None means no local incarnation proof, never success at exit.
    """

    succeeded: bool
    endpoint_terminated: bool
    process_identity: ProcessIdentity | None = None
    process_exited: bool | None = None
    request_attempted: bool = False
    acknowledged: bool = False


@dataclass(slots=True)
class _EndpointShutdownOperation:
    """State and mechanics for one endpoint shutdown request."""

    target: TransportEndpoint
    deadline: OperationDeadline
    config: ZMQConfig
    process_identity: ProcessIdentity | None
    acknowledged: bool
    request_attempted: bool

    @classmethod
    def run(
        cls,
        target: TransportEndpoint,
        config: ZMQConfig,
        mode: EndpointShutdownMode,
        *,
        deadline: OperationDeadline,
        expected_process_identity: ProcessIdentity | None,
    ) -> EndpointShutdownResult:
        """Admit one native request and complete it through the selected mode.

        Dispatch, acknowledgement uncertainty and terminal observation belong
        to this same operation. Neither a client nor a completion leaf resends.
        """
        endpoint = target.ping(
            config, timeout_ms=min(deadline.remaining_milliseconds(), 1000),
        )
        if (
            expected_process_identity is not None
            and endpoint is not None
            and endpoint.process_identity != expected_process_identity
        ):
            raise RuntimeError("Endpoint incarnation changed before shutdown; no dispatch")
        process_identity = (
            expected_process_identity
            if expected_process_identity is not None
            else (None if endpoint is None else endpoint.process_identity)
        )
        if endpoint is None and process_identity is None and not target.occupied_ports(config):
            return EndpointShutdownResult(succeeded=True, endpoint_terminated=True)
        if endpoint is not None and mode.required_capability not in endpoint.control_capabilities:
            return EndpointShutdownResult(succeeded=False, endpoint_terminated=False)
        if endpoint is None and expected_process_identity is None:
            return EndpointShutdownResult(succeeded=False, endpoint_terminated=False)

        acknowledged = False
        request_attempted = False
        sock = None
        try:
            # A missing listener cannot receive an RPC. Reconcile the proven
            # incarnation without another request or implicit replacement.
            if endpoint is not None and (
                expected_process_identity is None or expected_process_identity.is_alive() is True
            ):
                ctx = zmq.Context.instance()
                sock = ctx.socket(zmq.REQ)
                sock.setsockopt(zmq.LINGER, 0)
                sock.connect(target.control_url(config))
                sock.setsockopt(zmq.SNDTIMEO, min(deadline.remaining_milliseconds(), 1000))
                request_attempted = True
                sock.send(EndpointShutdownRequest(
                    mode.control_message_type, process_identity,
                ).to_wire_payload())
                sock.setsockopt(zmq.RCVTIMEO, min(deadline.remaining_milliseconds(), 1000))
                ack = pickle.loads(sock.recv())
                acknowledged = ack.get(MessageFields.TYPE) == ResponseType.SHUTDOWN_ACK.value
        except (
            EOFError, KeyError, OSError, TypeError, pickle.PickleError,
            TimeoutError, zmq.ZMQError,
        ):
            acknowledged = False
        finally:
            if sock is not None:
                sock.close(linger=0)
        return mode.complete(cls(
            target=target, deadline=deadline, config=config,
            process_identity=process_identity,
            acknowledged=acknowledged, request_attempted=request_attempted,
        ))

    def acknowledgement_result(self) -> EndpointShutdownResult:
        """Report whether a non-terminating shutdown request was acknowledged."""

        return EndpointShutdownResult(
            succeeded=self.acknowledged,
            endpoint_terminated=False,
            process_identity=self.process_identity,
            process_exited=self._process_exited(),
            request_attempted=self.request_attempted,
            acknowledged=self.acknowledged,
        )

    def termination_result(self) -> EndpointShutdownResult:
        """Complete FORCE through the existing exact-process owner, never replay."""

        # Reserve part of this SAME deadline for process-owner escalation.
        grace_end = time.monotonic() + self.deadline.remaining_seconds_or_zero() / 2
        while time.monotonic() < grace_end:
            if self._process_exited() is True:
                break
            remaining = self.deadline.remaining_seconds_or_zero()
            if remaining <= 0:
                break
            pong = self.target.ping(
                self.config,
                timeout_ms=min(100, max(1, int(remaining * 1000))),
            )
            if pong is None:
                break
            if self.process_identity is not None and pong.process_identity != self.process_identity:
                # A shutdown request may already have arrived at the original
                # child. Retain that disposition, but never signal a successor.
                return EndpointShutdownResult(
                    succeeded=False,
                    endpoint_terminated=False,
                    process_identity=self.process_identity,
                    process_exited=self._process_exited(),
                    request_attempted=self.request_attempted,
                    acknowledged=self.acknowledged,
                )
            time.sleep(min(0.05, self.deadline.remaining_seconds_or_zero()))

        exited = self._process_exited()
        remaining = self.deadline.remaining_seconds_or_zero()
        if self.process_identity is not None and exited is False and remaining > 0:
            # This is exact-incarnation OS cleanup, NOT another shutdown RPC.
            self.process_identity.terminate(timeout=remaining)
            exited = self._process_exited()
        if exited is True:
            # IPC address files can outlive their listener. Remove only what
            # the existing transport owner proves stale, not a foreign socket.
            self.target.cleanup_stale_addresses(self.config)
        endpoint_terminated = not self.target.occupied_ports(self.config)
        # Remote/unidentified endpoints can prove transport cessation only.
        succeeded = endpoint_terminated and (exited is True or self.process_identity is None)
        return EndpointShutdownResult(
            succeeded=succeeded,
            endpoint_terminated=endpoint_terminated,
            process_identity=self.process_identity,
            process_exited=exited,
            request_attempted=self.request_attempted,
            acknowledged=self.acknowledged,
        )

    def _process_exited(self) -> bool | None:
        if (
            self.process_identity is None
            or not self.target.transport_mode.declaration.endpoint_is_local(
                self.target.host, self.target.port
            )
        ):
            return None
        alive = self.process_identity.is_alive()
        return None if alive is None else not alive


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

    def close_owned_process(
        self,
        target: TransportEndpoint,
        config: ZMQConfig,
        process_identity: ProcessIdentity,
        *,
        operation_deadline: OperationDeadline,
    ) -> EndpointShutdownResult:
        """Admit this pair's exact owner before the one shutdown operation."""
        if not target.transport_mode.declaration.endpoint_is_local(target.host, target.port):
            raise ValueError("Owned process close requires a local endpoint")
        with target.startup_lock(config, operation_deadline=operation_deadline) as acquired:
            if not acquired:
                raise EndpointConnectionCancelledError("Close cancelled before dispatch")
            target.require_startup_owner(config, process_identity)
            operation_deadline.remaining_seconds()
            return _EndpointShutdownOperation.run(
                target, config, self, deadline=operation_deadline,
                expected_process_identity=process_identity,
            )


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

    def new_connection_attempt(
        self,
        *,
        cancellation: OperationCancellation | None = None,
    ) -> EndpointConnectionAttempt:
        """Create one connection attempt under its caller-selected authority."""

        return EndpointConnectionAttempt(self, cancellation=cancellation)

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

    def start_owned_process(
        self,
        *,
        operation_deadline: OperationDeadline,
    ) -> EndpointProcess:
        """Spawn once at an empty local pair, without attaching or warming.

        The returned platform handle remains authoritative even before readiness.
        Unlike connect(), this operation never replaces, cleans, or adopts an
        existing address. Readiness is a subsequent read-only observation.
        """
        declaration = self.transport_mode.declaration
        if not declaration.endpoint_is_local(self.host, self.port):
            raise ValueError("Explicit startup requires a local endpoint")
        with self._ensure_connection_attempt(), self._lock:
            if self._connection is not None:
                raise RuntimeError("Explicit startup cannot adopt an existing connection")
            with self.endpoint.startup_lock(
                self.config,
                operation_deadline=operation_deadline,
                cancellation=self._connection_cancellation.get(),
            ) as acquired:
                if not acquired:
                    raise EndpointConnectionCancelledError("Startup cancelled before spawn")
                operation_deadline.remaining_seconds()
                self.endpoint.require_available_startup(self.config)
                operation_deadline.remaining_seconds()
                self._emit_connection_status(
                    EndpointStartupPhase.STARTING_PROCESS,
                    f"Starting explicitly owned server on port {self.port}",
                )
                if self._connection_cancelled():
                    raise EndpointConnectionCancelledError("Startup cancelled before spawn")
                # Roll back only while no spawn has been attempted. Once the
                # side effect starts, claims preserve uncertainty, not replay.
                if not self.endpoint.reserve_startup_owner(
                    self.config, ProcessIdentity.current(),
                    operation_deadline=operation_deadline,
                    cancellation=self._connection_cancellation.get(),
                ):
                    raise EndpointConnectionCancelledError("Startup cancelled before spawn")
                process = endpoint_process(self._spawn_server_process())
                try:
                    self.endpoint.record_startup_owner(self.config, process.identity)
                except Exception as error:
                    raise EndpointStartupUncertainError(process) from error
                return process

    def close_owned_process(
        self,
        process_identity: ProcessIdentity,
        *,
        mode: EndpointShutdownMode,
        operation_deadline: OperationDeadline,
    ) -> EndpointShutdownResult:
        """Close only a child proven by this pair's existing startup reservations.

        No attach/start, port-owner killing, second process store, or RPC retry.
        Caller-supplied PID/creation time alone is not ownership admission.
        """
        return mode.close_owned_process(
            self.endpoint, self.config, process_identity,
            operation_deadline=operation_deadline,
        )

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
        with self.endpoint.startup_lock(
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
                if self.endpoint.has_live_startup_owner(self.config) or (
                    self.transport_mode.declaration.preserve_unresponsive_endpoint(
                        self.port,
                        self.config,
                    )
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
            if self.endpoint.has_live_startup_owner(self.config):
                return False
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
        """Apply a total deadline without changing the inactivity policy."""
        return self._wait_for_endpoint_ready_observed(
            process,
            timeout=timeout,
            operation_deadline=operation_deadline,
        )

    def _wait_for_endpoint_ready(
        self,
        process: EndpointProcess,
        timeout: float = 10.0,
    ) -> PongResponse | None:
        """Return the one authoritative startup handshake."""
        return self._wait_for_endpoint_ready_observed(process, timeout=timeout)

    def _wait_for_endpoint_ready_observed(
        self,
        process: EndpointProcess,
        *,
        timeout: float,
        operation_deadline: OperationDeadline | None = None,
    ) -> PongResponse | None:
        """Shared exact-child activity, cancellation and handshake algorithm."""
        endpoint = wait_for_endpoint_ready(
            self.port,
            self.transport_mode,
            host=self.host,
            config=self.config,
            timeout=timeout,
            poll_interval=self.config.server_poll_interval_seconds,
            startup_observer=self._connection_startup_observer(
                process.startup_observer(self._endpoint_startup_observer(process))
            ),
            operation_deadline=operation_deadline,
        )
        if endpoint is not None:
            self._endpoint_ready_observed(endpoint)
        return endpoint

    def _endpoint_startup_observer(self, process: EndpointProcess) -> EndpointStartupObserver:
        """Domain hook: generic endpoints have no child status journal."""
        return IDLE_ENDPOINT_STARTUP_OBSERVER

    def _endpoint_ready_observed(self, endpoint: PongResponse) -> None:
        """Domain hook for releasing startup evidence after readiness."""

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
        return TransportEndpoint.scan(
            tuple(ports), host=host, transport_mode=transport_mode,
            config=config, timeout_ms=timeout_ms,
        )

    @staticmethod
    def shutdown_endpoint_on_port(
        port: int,
        mode: EndpointShutdownMode,
        timeout: float = 5.0,
        transport_mode: TransportMode | None = None,
        host: str = "localhost",
        config: ZMQConfig | None = None,
        *,
        expected_process_identity: ProcessIdentity | None = None,
        operation_deadline: OperationDeadline | None = None,
    ) -> EndpointShutdownResult:
        config = config or ZMQConfig()
        transport_mode = resolve_transport_mode(transport_mode)
        if not isinstance(mode, EndpointShutdownMode):
            raise TypeError("Shutdown mode must be an EndpointShutdownMode instance.")
        deadline = operation_deadline or OperationDeadline.after_milliseconds(
            max(1, int(timeout * 1000)), operation="endpoint shutdown"
        )
        target = TransportEndpoint(
            host=host,
            port=port,
            transport_mode=transport_mode,
        )
        return _EndpointShutdownOperation.run(
            target, config, mode, deadline=deadline,
            expected_process_identity=expected_process_identity,
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
