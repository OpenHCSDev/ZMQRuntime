"""ZMQ Message Type System - generic runtime messages.

This library provides GENERIC ZMQ messaging primitives.
Application-specific logic (pipelines, compilation, etc.) should extend these types
at the application layer, not in this runtime library.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import signal
import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields, replace
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Dict, Optional, Tuple
from uuid import UUID

import psutil
from metaclass_registry import AutoRegisterMeta
from python_introspect import dataclass_from_mapping
from python_introspect.jsonable import JsonValue, to_jsonable
from typing_extensions import Self

from zmqruntime.timeouts import OperationDeadline

logger = logging.getLogger(__name__)


# =============================================================================
# Declared messages: the wire mapping of a message is its declared fields
# =============================================================================


class WireMessage:
    """A declared message whose wire mapping is exactly its declared fields.

    Encoding is ``to_jsonable`` and decoding is ``dataclass_from_mapping``:
    both read the dataclass declaration, so a message never restates its
    fields. Decoding is strict: an undeclared field fails loudly.
    """

    __slots__ = ()

    def to_dict(self) -> dict[str, JsonValue]:
        return dict(to_jsonable(self))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        return dataclass_from_mapping(cls, cls.declared_items(data))

    @classmethod
    def declared_items(cls, data: Mapping[str, Any]) -> Mapping[str, Any]:
        """The part of a wire mapping this message decodes."""

        return data


class WireView(WireMessage):
    """A declared message read out of a wider payload.

    Applications extend a generic envelope (a progress event, a status
    response) with their own fields; the view decodes only the fields it
    declares and leaves the rest to the application.
    """

    __slots__ = ()

    @classmethod
    def declared_items(cls, data: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            declared.name: data[declared.name]
            for declared in fields(cls)
            if declared.init and declared.name in data
        }


class TypedWireMessage(WireMessage):
    """A declared message that carries its kind under the wire ``type`` key."""

    __slots__ = ()

    wire_type: ClassVar[str]

    def to_dict(self) -> dict[str, JsonValue]:
        return {MessageFields.TYPE: self.wire_type, **super().to_dict()}

    @classmethod
    def declared_items(cls, data: Mapping[str, Any]) -> Mapping[str, Any]:
        if data.get(MessageFields.TYPE) != cls.wire_type:
            raise ValueError(f"{cls.__name__} requires type={cls.wire_type!r}.")
        return super().declared_items(
            {key: value for key, value in data.items() if key != MessageFields.TYPE}
        )


# =============================================================================
# Generic Progress Types - Application Agnostic
# =============================================================================


@dataclass(frozen=True)
class WorkerState(WireMessage):
    """Generic worker/process information."""

    pid: int
    status: str
    cpu_percent: float
    memory_mb: float
    create_time: float | None = None


@dataclass(frozen=True)
class TaskProgress(WireView):
    """The generic fields of one progress event.

    Applications publish progress events that carry these fields beside their
    own; the runtime reads and validates only these. Phase and status are the
    application's vocabulary: an application event narrows their type, and the
    runtime sees the spelling ``to_jsonable`` gives them on the wire.
    """

    execution_id: str
    phase: object
    status: object
    percent: float
    timestamp: float
    completed: int
    total: int
    subject_id: str = ""
    axis_id: str = ""
    error: Optional[str] = None
    traceback: Optional[str] = None

    def __post_init__(self):
        """Validate invariants."""
        if not (0.0 <= self.percent <= 100.0):
            raise ValueError(f"percent must be in [0, 100], got {self.percent}")
        if self.completed > self.total:
            raise ValueError(f"completed ({self.completed}) cannot exceed total ({self.total})")


# =============================================================================
# Message Field Constants (used by control messages)
# =============================================================================


class MessageFields:
    TYPE = "type"
    OBSERVATION_BUDGET_SECONDS = "observation_budget_seconds"
    OBSERVATION_OPERATION = "observation_operation"
    SUBJECT_ID = "subject_id"
    EXECUTION_SUBJECT_ID = "execution_subject_id"
    SELECTED_PIPELINE_PATH = "selected_pipeline_path"
    PIPELINE_CODE = "pipeline_code"
    CONFIG_PARAMS = "config_params"
    CONFIG_CODE = "config_code"
    PIPELINE_CONFIG_CODE = "pipeline_config_code"
    COMPILE_ONLY = "compile_only"
    COMPILE_ARTIFACT_ID = "compile_artifact_id"
    EXECUTION_ID = "execution_id"
    START_TIME = "start_time"
    END_TIME = "end_time"
    STATUS = "status"
    ERROR = "error"
    MESSAGE = "message"
    PORT = "port"
    CONTROL_PORT = "control_port"
    READY = "ready"
    SERVER = "server"
    SERVER_TYPE = "server_type"
    SERVER_ROLE = "server_role"
    RUNNING_EXECUTIONS = "running_executions"
    EXECUTION = "execution"
    PARTITION_COUNT = "partition_count"
    PARTITION_VALUES = "partition_values"
    RESULTS_SUMMARY = "results_summary"
    TIMESTAMP = "timestamp"
    AXIS_ID = "axis_id"
    TRACEBACK = "traceback"
    # Acknowledgment message fields
    IMAGE_ID = "image_id"
    # ROI message fields
    ROIS = "rois"
    SHAPES = "shapes"
    METADATA = "metadata"
    CLIENT_ID = "client_id"
    PROGRESS_SUBSCRIBERS = "progress_subscribers"
    PROGRESS_SEQUENCE = "progress_sequence"


@dataclass(frozen=True)
class ProcessIdentity(WireMessage):
    """PID-reuse-safe identity for a process visible on the local host."""

    pid: int
    create_time: float

    @classmethod
    def current(cls) -> "ProcessIdentity":
        process = psutil.Process()
        return cls(pid=process.pid, create_time=process.create_time())

    @classmethod
    def for_pid(cls, pid: int) -> "ProcessIdentity":
        """Capture the OS creation time at the process-handle boundary."""
        process = psutil.Process(pid)
        return cls(pid=process.pid, create_time=process.create_time())

    def is_current(self) -> bool:
        """Whether this exact incarnation is the calling process, not a child."""

        return self == self.current()

    def is_alive(self) -> bool | None:
        """Return exact local liveness, or unknown when access is denied."""

        try:
            process = psutil.Process(self.pid)
            if process.create_time() != self.create_time:
                return False
            return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            return None

    def descendants(self, *, recursive: bool = True) -> tuple[psutil.Process, ...]:
        """Discover this incarnation's current children without a host-wide scan.

        Linux exposes direct children per *thread*, including processes forked
        by a server's worker thread. Read every task, then traverse actual parent
        relationships. No ancestry or process handles survive between calls.
        Other platforms, or an unavailable task interface, use psutil's native
        discovery rather than publishing an incomplete resource observation.
        """
        process = psutil.Process(self.pid)
        if process.create_time() != self.create_time:
            raise psutil.NoSuchProcess(self.pid, msg="Process incarnation changed")
        if sys.platform != "linux":
            children = tuple(process.children(recursive=recursive))
        else:
            try:
                children = self._linux_descendants(process, recursive=recursive)
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                if ProcessIdentity.for_pid(self.pid) != self:
                    raise psutil.NoSuchProcess(
                        self.pid, msg="Process incarnation changed"
                    )
                children = tuple(process.children(recursive=recursive))
        if ProcessIdentity.for_pid(self.pid) != self:
            raise psutil.NoSuchProcess(self.pid, msg="Process incarnation changed")
        return children

    def _linux_descendants(
        self, process: psutil.Process, *, recursive: bool
    ) -> tuple[psutil.Process, ...]:
        children: list[psutil.Process] = []
        pending = [process]
        seen = {self.pid}
        while pending:
            parent = pending.pop()
            try:
                identity = ProcessIdentity(parent.pid, parent.create_time())
                if ProcessIdentity.for_pid(parent.pid) != identity:
                    continue
                task_root = f"{psutil.PROCFS_PATH}/{parent.pid}/task"
                child_pids: set[int] = set()
                for tid in os.listdir(task_root):
                    with open(f"{task_root}/{tid}/children", encoding="ascii") as stream:
                        child_pids.update(int(pid) for pid in stream.read().split())
                if ProcessIdentity.for_pid(parent.pid) != identity:
                    continue
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
            for pid in sorted(child_pids):
                if pid in seen:
                    continue
                try:
                    child = psutil.Process(pid)
                    if (
                        child.ppid() != parent.pid
                        or child.create_time() < identity.create_time
                    ):
                        continue
                    if not child.is_running():
                        continue
                except (psutil.NoSuchProcess, psutil.ZombieProcess):
                    continue
                seen.add(pid)
                children.append(child)
                if recursive:
                    pending.append(child)
        return tuple(children)

    def work_snapshot(self) -> dict[ProcessIdentity, float]:
        """Measure CPU work of this incarnation and its current descendants.

        An OS work counter is activity, not a readiness or ownership proof.
        Inaccessible/exited processes supply no new work. Each sample carries
        creation time, so a reused PID cannot refresh another child's wait.
        """
        samples: dict[ProcessIdentity, float] = {}
        try:
            process = psutil.Process(self.pid)
            if process.create_time() != self.create_time:
                return samples
            processes = (process, *self.descendants())
            for current in processes:
                try:
                    identity = ProcessIdentity(current.pid, current.create_time())
                    cpu = current.cpu_times()
                    if ProcessIdentity.for_pid(current.pid) == identity:
                        samples[identity] = cpu.user + cpu.system
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    return {}
            # The ancestry projection is valid only while its exact root remains.
            if ProcessIdentity.for_pid(self.pid) != self:
                return {}
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return {}
        return samples

    def terminate(self, timeout: float = 5.0) -> bool:
        """Terminate a different incarnation within one escalation budget.

        An in-process endpoint can identify this caller. Closing that endpoint
        does not authorize signalling its host, and cannot prove host exit.
        """

        if self.is_current():
            return False
        if timeout <= 0:
            return self.is_alive() is False
        deadline = OperationDeadline.after_milliseconds(
            max(1, int(timeout * 1000)), operation="exact process termination"
        )
        try:
            process = psutil.Process(self.pid)
            if process.create_time() != self.create_time:
                return True
            if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                return True
            if deadline.expired():
                return False
            process.terminate()
            try:
                # Leave budget for the existing force escalation and its wait.
                process.wait(timeout=deadline.remaining_seconds_or_zero() / 2)
            except psutil.TimeoutExpired:
                if not process.is_running():
                    return True
                process.kill()
                process.wait(timeout=deadline.remaining_seconds_or_zero())
            return True
        except psutil.NoSuchProcess:
            return True
        except (psutil.AccessDenied, psutil.TimeoutExpired):
            return False


@dataclass(frozen=True)
class ProcessExit:
    """Exact exit status retained by a process-owning client."""

    returncode: int

    def describe(self) -> str:
        """Render the platform process status without losing its numeric value."""

        if self.returncode >= 0:
            return f"exit code {self.returncode}"
        signal_number = -self.returncode
        try:
            signal_name = signal.Signals(signal_number).name
        except ValueError:
            signal_name = f"signal {signal_number}"
        return f"signal {signal_name} ({self.returncode})"


# =============================================================================
# Control Message Types
# =============================================================================


class ControlMessageType(Enum):
    PING = "ping"
    EXECUTE = "execute"
    STATUS = "status"
    CANCEL = "cancel"
    SHUTDOWN = "shutdown"
    FORCE_SHUTDOWN = "force_shutdown"
    REGISTER_PROGRESS = "register_progress"
    UNREGISTER_PROGRESS = "unregister_progress"


_CONTROL_HEADER_FIELDS = frozenset({"message_type", "operation_deadline"})


@dataclass(frozen=True, slots=True)
class ControlRequestHeader:
    """A control request: its kind, its observation budget and its declared body.

    The wire mapping carries the kind under ``type`` and the remaining
    observation budget as a duration (never an absolute host clock). Every
    other declared field is the request body, encoded and decoded by the
    declaration. Requests are read out of wider control payloads, so fields
    a request does not declare are left to the payload's other readers.
    """

    message_type: ControlMessageType
    operation_deadline: OperationDeadline | None = field(default=None, kw_only=True)

    @staticmethod
    def with_observation_budget(
        payload: Mapping[str, object], deadline: OperationDeadline,
    ) -> dict[str, object]:
        """Offer remaining duration at send, never an absolute host clock."""
        return {
            **payload,
            MessageFields.OBSERVATION_BUDGET_SECONDS: deadline.remaining_seconds(),
            MessageFields.OBSERVATION_OPERATION: deadline.operation,
        }

    @staticmethod
    def admit_observation(payload: Mapping[str, object]) -> OperationDeadline:
        """Decode once at receipt; retain the resulting local deadline."""
        return OperationDeadline.from_remaining_seconds(
            payload[MessageFields.OBSERVATION_BUDGET_SECONDS],
            operation=payload[MessageFields.OBSERVATION_OPERATION],
        )

    @classmethod
    def body_fields(cls) -> tuple[str, ...]:
        """Declared fields carried as the request body."""

        return tuple(
            declared.name
            for declared in fields(cls)
            if declared.name not in _CONTROL_HEADER_FIELDS
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw_message_type = payload[MessageFields.TYPE]
        if not isinstance(raw_message_type, str):
            raise TypeError("Control request type must be a string.")
        request = dataclass_from_mapping(
            cls,
            {
                "message_type": raw_message_type,
                **{name: payload[name] for name in cls.body_fields() if name in payload},
            },
        )
        if MessageFields.OBSERVATION_BUDGET_SECONDS not in payload:
            return request
        return replace(request, operation_deadline=cls.admit_observation(payload))

    @classmethod
    def from_wire_payload(cls, wire_payload: bytes) -> Self:
        payload = pickle.loads(wire_payload)
        if not isinstance(payload, Mapping):
            raise TypeError("Control request payload must be a mapping.")
        return cls.from_dict(payload)

    def to_dict(self) -> dict[str, object]:
        declared = to_jsonable(replace(self, operation_deadline=None))
        payload = {
            MessageFields.TYPE: declared["message_type"],
            **{name: declared[name] for name in self.body_fields()},
        }
        return (payload if self.operation_deadline is None else
                self.with_observation_budget(payload, self.operation_deadline))

    def to_wire_payload(self) -> bytes:
        return pickle.dumps(self.to_dict())


@dataclass(frozen=True, slots=True)
class EndpointShutdownRequest(ControlRequestHeader):
    """Bind shutdown to its observed native incarnation, before mutation."""

    process_identity: ProcessIdentity | None = None

    def validate(self) -> str | None:
        if self.process_identity is not None and self.process_identity != ProcessIdentity.current():
            return "Shutdown addressed a different native process incarnation; no mutation"
        return None


class ResponseType(Enum):
    PONG = "pong"
    ACCEPTED = "accepted"
    OK = "ok"
    ERROR = "error"
    SHUTDOWN_ACK = "shutdown_ack"


class ExecutionStatus(Enum):
    """Execution lifecycle declaration with behavior owned by each phase."""

    QUEUED = ("queued", False, False, False, False)
    RUNNING = ("running", False, True, True, True)
    COMPLETE = ("complete", True, False, False, False)
    COMPLETED = ("completed", True, False, False, False)
    FAILED = ("failed", True, False, False, False)
    CANCELLED = ("cancelled", True, False, False, False)
    ACCEPTED = ("accepted", False, False, False, False)

    def __new__(
        cls,
        value: str,
        is_terminal: bool,
        reports_running_transition: bool,
        records_start_time: bool,
        owns_active_work: bool,
    ) -> "ExecutionStatus":
        member = object.__new__(cls)
        member._value_ = value
        member._is_terminal = is_terminal
        member._reports_running_transition = reports_running_transition
        member._records_start_time = records_start_time
        member._owns_active_work = owns_active_work
        return member

    @property
    def is_terminal(self) -> bool:
        """Whether observing this phase completes status polling."""

        return self._is_terminal

    def reports_running_transition_from(self, previous: "ExecutionStatus") -> bool:
        """Whether entering this phase should emit the running transition."""

        return self._reports_running_transition and previous is type(self).QUEUED

    @property
    def owns_active_work(self) -> bool:
        """Whether this phase can have work that requires interruption."""

        return self._owns_active_work

    def apply_to_record(self, record: "ExecutionRecord", *, timestamp: float) -> None:
        """Apply phase-owned lifecycle fields to an execution record."""

        if self._records_start_time:
            record.start_time = timestamp
        if self.is_terminal:
            record.end_time = timestamp
        record.status = self.value

    @classmethod
    def from_wire(cls, value: object) -> Optional["ExecutionStatus"]:
        """Resolve a known wire value while preserving forward compatibility."""

        try:
            return cls(value)
        except (TypeError, ValueError):
            return None


class SocketType(Enum):
    PUB = "PUB"
    SUB = "SUB"
    REQ = "REQ"
    REP = "REP"

    @classmethod
    def from_zmq_constant(cls, zmq_const):
        import zmq

        try:
            return {
                zmq.PUB: cls.PUB,
                zmq.SUB: cls.SUB,
                zmq.REQ: cls.REQ,
                zmq.REP: cls.REP,
            }[zmq_const]
        except KeyError as error:
            raise ValueError(f"Unsupported ZMQ socket type: {zmq_const!r}") from error

    def get_display_name(self):
        return self.value


@dataclass(frozen=True, slots=True)
class ExecuteRequest(ControlRequestHeader):
    """Submit one subject's work to an execution server."""

    message_type: ControlMessageType = field(default=ControlMessageType.EXECUTE, init=False)
    subject_id: str
    pipeline_code: str
    execution_subject_id: Optional[str] = None
    selected_pipeline_path: Optional[str] = None
    config_params: Optional[Dict[str, Any]] = None
    config_code: Optional[str] = None
    pipeline_config_code: Optional[str] = None
    client_address: Optional[str] = None
    compile_only: bool = False
    compile_artifact_id: Optional[str] = None

    def validate(self):
        if not self.subject_id:
            return "Missing required field: subject_id"
        if not self.pipeline_code:
            return "Missing required field: pipeline_code"
        if self.compile_only and self.compile_artifact_id:
            return "compile_only and compile_artifact_id cannot both be set"
        if self.config_params is None and self.config_code is None:
            return "Missing config: provide either config_params or config_code"
        return None


@dataclass(frozen=True, slots=True)
class StatusRequest(ControlRequestHeader):
    message_type: ControlMessageType = field(default=ControlMessageType.STATUS, init=False)
    execution_id: Optional[str] = None

    def validate(self):
        return None


@dataclass(frozen=True, slots=True)
class CancelRequest(ControlRequestHeader):
    message_type: ControlMessageType = field(default=ControlMessageType.CANCEL, init=False)
    execution_id: str

    def validate(self):
        return "Missing execution_id" if not self.execution_id else None


@dataclass(frozen=True, slots=True)
class ProgressRegistrationRequest(ControlRequestHeader):
    message_type: ControlMessageType = field(
        default=ControlMessageType.REGISTER_PROGRESS, init=False
    )
    client_id: str

    def validate(self):
        return "Missing client_id" if not self.client_id else None


@dataclass(frozen=True, slots=True)
class ProgressUnregistrationRequest(ControlRequestHeader):
    message_type: ControlMessageType = field(
        default=ControlMessageType.UNREGISTER_PROGRESS, init=False
    )
    client_id: str

    def validate(self):
        return "Missing client_id" if not self.client_id else None


class ControlResponse(WireMessage):
    """Nominal response that owns its control-wire mapping."""

    __slots__ = ()


@dataclass(frozen=True)
class ControlErrorResponse(TypedWireMessage, ControlResponse):
    """A control request failed; ``message`` says why."""

    wire_type: ClassVar[str] = ResponseType.ERROR.value

    message: str
    status: ResponseType = ResponseType.ERROR

    @classmethod
    def from_exception(cls, error: Exception) -> "ControlErrorResponse":
        """Create a control failure from the exception owned by dispatch."""

        return cls(message=str(error))


@dataclass(frozen=True)
class ExecuteResponse(ControlResponse):
    status: ResponseType
    execution_id: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class CancelResponse(ControlResponse):
    """Outcome of one cancellation request."""

    status: ResponseType
    workers_killed: int
    message: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class ShutdownAck(TypedWireMessage, ControlResponse):
    """A server accepted a shutdown request."""

    wire_type: ClassVar[str] = ResponseType.SHUTDOWN_ACK.value

    message: str
    status: str = "success"


@dataclass(frozen=True)
class ProgressSubscriptionResponse(ControlResponse):
    """A progress subscriber was registered or released.

    A registration carries the latest observation of every execution the
    server still holds, keyed by execution id, so the subscriber starts from
    the server's baseline.
    """

    client_id: str
    progress_subscribers: int
    message: str
    progress_observations: Dict[str, Any] = field(default_factory=dict)
    status: ResponseType = ResponseType.OK


@dataclass
class ExecutionRecord(WireMessage):
    """One execution's lifecycle record; its declared fields are its wire record.

    ``extras`` and ``progress_event`` are server-local attachments (an
    application's orchestrator, the latest progress event). They are not
    fields, so they never travel.
    """

    execution_id: str
    subject_id: str
    status: str
    client_address: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    results_summary: Optional[Dict[str, Any]] = None
    compile_only: bool = False
    progress_sequence: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.progress_sequence, bool) or not isinstance(self.progress_sequence, int):
            raise TypeError("Execution progress sequence must be an integer")
        if self.progress_sequence < 0:
            raise ValueError("Execution progress sequence cannot be negative")
        self.extras: Dict[str, Any] = {}
        self.progress_event: dict | None = None

    def set_extra(self, key: str, value: Any) -> None:
        self.extras[key] = value

    def get_extra(self, key: str, default: Any = None) -> Any:
        return self.extras.get(key, default)

    def pop_extra(self, key: str, default: Any = None) -> Any:
        return self.extras.pop(key, default)


@dataclass(frozen=True)
class RunningExecutionInfo(WireMessage):
    execution_id: str
    subject_id: str
    start_time: float
    elapsed: float
    compile_only: bool = False


@dataclass(frozen=True)
class QueuedExecutionInfo(WireMessage):
    execution_id: str
    subject_id: str
    queue_position: int


@dataclass(frozen=True)
class ExecutionStatusSnapshot(WireView):
    """Status of one execution, or of the server's executions.

    Read as a view: status responses also carry diagnostics owned by the
    response envelope (``message``, ``error``).
    """

    status: ResponseType
    execution: Optional[ExecutionRecord] = None
    active_executions: Optional[int] = None
    uptime: Optional[float] = None
    executions: Optional[Tuple[str, ...]] = None
    running_executions: Optional[Tuple[RunningExecutionInfo, ...]] = None
    queued_executions: Optional[Tuple[QueuedExecutionInfo, ...]] = None


@dataclass(frozen=True)
class ProcessResourceUsage(WireMessage):
    """Process resource metrics carried by a server heartbeat."""

    memory_mb: float
    cpu_percent: float

    @classmethod
    def current(cls) -> Optional["ProcessResourceUsage"]:
        """Sample the current process when resource inspection is available."""

        try:
            process = psutil.Process()
            return cls(
                memory_mb=process.memory_info().rss / 1024 / 1024,
                cpu_percent=process.cpu_percent(interval=0),
            )
        except (psutil.Error, OSError):
            return None


class ServerRole(str, Enum):
    """Closed protocol-level role of a ZMQ server."""

    GENERIC = "generic"
    EXECUTION = "execution"
    VIEWER = "viewer"


class EndpointControlCapability(str, Enum):
    """Control operations explicitly advertised by a live endpoint."""

    PING = "ping"
    SHUTDOWN = "shutdown"
    FORCE_SHUTDOWN = "force_shutdown"


@dataclass(frozen=True)
class EndpointApplication(WireMessage):
    """Application identity declared by an endpoint owner."""

    identifier: str
    version: str

    def __post_init__(self) -> None:
        if not self.identifier:
            raise ValueError("Endpoint application identifier cannot be empty")
        if not self.version:
            raise ValueError("Endpoint application version cannot be empty")

    def compatibility_with(
        self,
        observed: "EndpointApplication | None",
    ) -> "EndpointApplicationCompatibility":
        """Compare one observed endpoint with this declared application."""

        return EndpointApplicationCompatibility(expected=self, observed=observed)


@dataclass(frozen=True)
class EndpointApplicationCompatibility:
    """Exact compatibility between a declared and observed application identity."""

    expected: EndpointApplication
    observed: EndpointApplication | None

    @property
    def matches(self) -> bool:
        return self.observed == self.expected

    @property
    def observed_version_label(self) -> str:
        if self.observed is None:
            return "not reported"
        return self.observed.version

    def require_match(self) -> None:
        """Raise a typed mismatch when the observed endpoint is incompatible."""

        if not self.matches:
            raise EndpointApplicationCompatibilityError(self)


@dataclass
class EndpointApplicationCompatibilityError(ValueError):
    """Raised when an endpoint does not match its required application identity."""

    compatibility: EndpointApplicationCompatibility

    def __str__(self) -> str:
        expected = self.compatibility.expected
        observed = self.compatibility.observed
        observed_label = (
            "not reported" if observed is None else f"{observed.identifier} {observed.version}"
        )
        return (
            "Endpoint application mismatch: expected "
            f"{expected.identifier} {expected.version}, observed {observed_label}."
        )


class EndpointAudience(ABC, metaclass=AutoRegisterMeta):
    """Which processes may use a server, and what it does once its owner is gone.

    The wire carries ``wire_name``; the registry is the only list of audiences.
    """

    __registry_key__ = "wire_name"
    __skip_if_no_key__ = True

    wire_name: ClassVar[Optional[str]] = None

    @classmethod
    def of_persistent(cls, persistent: bool) -> "type[EndpointAudience]":
        """A persistent server outlives its client, so other sessions may use it."""

        return AnySessionAudience if persistent else OwnerOnlyAudience

    @classmethod
    @abstractmethod
    def admits(cls, owner: "ProcessIdentity", client: "ProcessIdentity") -> bool:
        """Whether ``client`` may use a server that ``owner`` started."""

    @classmethod
    @abstractmethod
    def orphan_stops(cls, unfinished_work: bool) -> bool:
        """Whether a server whose owner is gone stops now."""


class OwnerOnlyAudience(EndpointAudience):
    """A server for the process that started it: its owner stops it on disconnect."""

    wire_name = "owner_only"

    @classmethod
    def admits(cls, owner: "ProcessIdentity", client: "ProcessIdentity") -> bool:
        return client == owner

    @classmethod
    def orphan_stops(cls, unfinished_work: bool) -> bool:
        return True


class AnySessionAudience(EndpointAudience):
    """A server other sessions running the same code may attach to."""

    wire_name = "any_session"

    @classmethod
    def admits(cls, owner: "ProcessIdentity", client: "ProcessIdentity") -> bool:
        return True

    @classmethod
    def orphan_stops(cls, unfinished_work: bool) -> bool:
        return not unfinished_work


def imported_code_root(declared: type) -> str:
    """Directory the top-level package that defines ``declared`` is imported from.

    Two checkouts of one application import the same class from different
    directories; this is what tells their servers apart.
    """

    top_level = sys.modules[declared.__module__.partition(".")[0]]
    package_paths = tuple(getattr(top_level, "__path__", ()))
    if package_paths:
        origin = Path(package_paths[0])
    else:
        module_file = getattr(top_level, "__file__", None)
        origin = Path(module_file) if module_file else Path.cwd() / top_level.__name__
    return str(origin.resolve().parent)


@dataclass(frozen=True)
class EndpointLaunch(WireMessage):
    """Where a server's code is, which process started it, and who may use it."""

    code_root: str
    owner: ProcessIdentity
    audience: str

    def __post_init__(self) -> None:
        if not self.code_root:
            raise ValueError("Endpoint launch requires a code root")
        if self.audience not in EndpointAudience.__registry__:
            raise ValueError(f"Unknown endpoint audience {self.audience!r}")

    @classmethod
    def by_this_process(
        cls,
        code_root: str,
        audience: "type[EndpointAudience]",
    ) -> Self:
        """The launch of an endpoint started by the calling process."""

        return cls(
            code_root=code_root,
            owner=ProcessIdentity.current(),
            audience=audience.wire_name,
        )

    @property
    def audience_type(self) -> "type[EndpointAudience]":
        return EndpointAudience.__registry__[self.audience]

    def refusal(self, client: "EndpointLaunch") -> Optional[str]:
        """Why a client launched as ``client`` may not use this server, if so."""

        if self.code_root != client.code_root:
            return (
                f"it runs code from {self.code_root}, "
                f"and this client runs code from {client.code_root}"
            )
        if not self.audience_type.admits(self.owner, client.owner):
            return (
                f"process {self.owner.pid} started it for its own use, "
                f"and this client is process {client.owner.pid}"
            )
        return None

    def owner_is_gone(self) -> bool:
        """Whether the starting process has exited (unknown counts as present)."""

        return self.owner.is_alive() is False

    def to_argument(self) -> str:
        """One command-line argument a spawned server decodes with ``from_argument``."""

        return json.dumps(self.to_dict())

    @classmethod
    def from_argument(cls, argument: str) -> Self:
        return cls.from_dict(json.loads(argument))


class EndpointLaunchMismatchError(RuntimeError):
    """A client refused a server it did not start and may not use."""

    def __init__(self, address: str, reason: str) -> None:
        self.address = address
        self.reason = reason
        super().__init__(
            f"Refusing the server at {address}: {reason}. "
            "Stop that server, or give this client an address of its own."
        )


@dataclass(frozen=True)
class PongResponse(TypedWireMessage, WireView, ControlResponse):
    """Complete typed server-heartbeat response.

    Read as a view: a server may extend its heartbeat with fields of its own
    (a subclass), and every client still reads the declared heartbeat.
    """

    wire_type: ClassVar[str] = "pong"

    port: int
    control_port: int
    ready: bool
    server: str
    server_role: ServerRole
    control_capabilities: Tuple[EndpointControlCapability, ...] = (
        EndpointControlCapability.PING,
    )
    server_type: Optional[str] = None
    log_file_path: Optional[str] = None
    active_executions: Optional[int] = None
    running_executions: Optional[Tuple[RunningExecutionInfo, ...]] = None
    queued_executions: Optional[Tuple[QueuedExecutionInfo, ...]] = None
    workers: Optional[Tuple[WorkerState, ...]] = None
    uptime: Optional[float] = None
    progress_subscribers: Optional[int] = None
    process_identity: Optional[ProcessIdentity] = None
    compile_status: Optional[str] = None
    compile_message: Optional[str] = None
    process_usage: Optional[ProcessResourceUsage] = None
    application: Optional[EndpointApplication] = None
    launch: Optional[EndpointLaunch] = None

    def __post_init__(self) -> None:
        """Hold advertised capabilities as one ordered set."""

        object.__setattr__(
            self,
            "control_capabilities",
            _capability_set(self.control_capabilities),
        )


def _capability_set(
    capabilities: Iterable[EndpointControlCapability],
) -> Tuple[EndpointControlCapability, ...]:
    return tuple(sorted(set(capabilities), key=lambda capability: capability.value))


# =============================================================================
# Streaming Message Types
# =============================================================================


@dataclass(frozen=True, slots=True)
class AckReturnRoute(WireMessage):
    """Bound producer destination and exact listener incarnation, not a viewer port."""

    url: str
    incarnation: str
    owner: ProcessIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.url, str) or not self.url:
            raise ValueError("ACK destination must be a nonempty transport URL")
        UUID(self.incarnation)
        if not isinstance(self.owner, ProcessIdentity):
            raise TypeError("ACK route requires a ProcessIdentity")


@dataclass(frozen=True, slots=True)
class ImageTransferIdentity(WireMessage):
    """Per-image return contract carried unchanged through deferred viewer work."""

    image_id: str
    return_route: AckReturnRoute
    producer: ProcessIdentity = field(default_factory=ProcessIdentity.current)

    def __post_init__(self) -> None:
        if not isinstance(self.image_id, str) or not self.image_id:
            raise ValueError("Image transfer requires a nonempty image ID")
        if not isinstance(self.return_route, AckReturnRoute):
            raise TypeError("Image transfer requires an AckReturnRoute")

    @classmethod
    def from_item(cls, data: Mapping[str, Any]) -> "ImageTransferIdentity | None":
        """Untracked control items have no image identity; tracked items require a route."""
        if data.get("image_id") is None:
            return None
        return cls.from_dict({declared.name: data[declared.name] for declared in fields(cls)})


@dataclass(frozen=True)
class ImageAck(TypedWireMessage):
    """Acknowledgment message sent by viewers after processing an image.

    Sent via PUSH socket to the original image's producer return route.
    Used to track real-time queue depth and show progress like '3/10 images processed'.
    """

    wire_type: ClassVar[str] = "image_ack"

    image_id: str  # UUID of the processed image
    viewer_port: int  # Port of the viewer that processed it (for routing)
    viewer_type: str  # 'napari' or 'fiji'
    return_route: AckReturnRoute = field(kw_only=True)
    producer: ProcessIdentity = field(kw_only=True)
    status: str = "success"  # 'success', 'error', etc.
    timestamp: Optional[float] = None  # When it was processed
    error: Optional[str] = None  # Error message if status='error'
