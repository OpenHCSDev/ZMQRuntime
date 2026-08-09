"""ZMQ Message Type System - generic runtime messages.

This library provides GENERIC ZMQ messaging primitives.
Application-specific logic (pipelines, compilation, etc.) should extend these types
at the application layer, not in this runtime library.
"""

import logging
import signal
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import psutil

logger = logging.getLogger(__name__)


# =============================================================================
# Generic Progress Types - Application Agnostic
# =============================================================================


class TaskPhase(Enum):
    """Generic task phases - base vocabulary for workflow states.

    Applications can extend with their own phase enums.
    TaskProgress accepts TaskPhase | <AppPhase> union types.
    """

    INIT = "init"
    QUEUED = "queued"
    RUNNING = "running"
    COMPILE = "compile"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(Enum):
    """Generic task status - base vocabulary for status values.

    Applications can extend with their own status enums.
    TaskProgress accepts TaskStatus | <AppStatus> union types.
    """

    PENDING = "pending"
    STARTED = "started"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class WorkerState:
    """Generic worker/process information."""

    pid: int
    status: str
    cpu_percent: float
    memory_mb: float
    create_time: float | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "pid": self.pid,
            "status": self.status,
            "cpu_percent": self.cpu_percent,
            "memory_mb": self.memory_mb,
        }
        if self.create_time is not None:
            result["create_time"] = self.create_time
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkerState":
        known_fields = {"pid", "status", "cpu_percent", "memory_mb", "create_time"}
        unexpected_fields = set(data).difference(known_fields)
        if unexpected_fields:
            raise ValueError(f"WorkerState received unknown fields: {sorted(unexpected_fields)!r}")

        return cls(
            pid=data["pid"],
            status=data["status"],
            cpu_percent=data["cpu_percent"],
            memory_mb=data["memory_mb"],
            create_time=data.get("create_time"),
        )


@dataclass(frozen=True)
class TaskProgress:
    """Generic task progress - supports both generic and app-specific enums.

    Phase and status accept EITHER strings (for transport) OR Enum types (for type safety).
    Applications can use TaskPhase/TaskStatus or extend with their own enums (e.g., AxisPhase/AxisStatus).

    The .value of the enum is stored/transmitted, allowing interop between
    generic and app-specific code.
    """

    # Required fields
    task_id: str
    phase: str | Enum  # String for transport, Enum for type safety
    status: str | Enum  # String for transport, Enum for type safety
    percent: float
    timestamp: float

    # Progress tracking
    completed: int
    total: int

    # Core tracking identifiers (moved from context)
    plate_id: str = ""
    axis_id: str = ""

    # Application-specific context (NOT generic runtime concerns)
    context: Dict[str, Any] = field(default_factory=dict)

    # Optional error info
    error: Optional[str] = None
    traceback: Optional[str] = None

    def __post_init__(self):
        """Validate invariants."""
        if not (0.0 <= self.percent <= 100.0):
            raise ValueError(f"percent must be in [0, 100], got {self.percent}")
        if self.completed > self.total:
            raise ValueError(f"completed ({self.completed}) cannot exceed total ({self.total})")
        # Allow both strings and enums for phase/status
        if not isinstance(self.phase, (str, Enum)):
            raise TypeError(f"phase must be a string or Enum, got {type(self.phase)}")
        if not isinstance(self.status, (str, Enum)):
            raise TypeError(f"status must be a string or Enum, got {type(self.status)}")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for transport."""
        # Handle both string and Enum types for phase/status
        phase_value = self.phase.value if isinstance(self.phase, Enum) else self.phase
        status_value = self.status.value if isinstance(self.status, Enum) else self.status

        result = {
            "type": "progress",
            "task_id": self.task_id,
            "execution_id": self.task_id,
            "phase": phase_value,
            "status": status_value,
            "percent": self.percent,
            "timestamp": self.timestamp,
            "completed": self.completed,
            "total": self.total,
            "plate_id": self.plate_id,
            "axis_id": self.axis_id,
            **self.context,
        }
        if self.error:
            result["error"] = self.error
        if self.traceback:
            result["traceback"] = self.traceback
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskProgress":
        """Parse from transport.

        Phase and status are passed through as strings from the dict.
        The application layer is responsible for converting them to appropriate Enum types.
        """

        # Separate generic fields from app-specific context
        generic_fields = {
            "type",
            "task_id",
            "execution_id",
            "phase",
            "status",
            "percent",
            "timestamp",
            "completed",
            "total",
            "plate_id",
            "axis_id",
            "error",
            "traceback",
            "step_name",
            "pid",
        }

        context = {k: v for k, v in data.items() if k not in generic_fields}

        task_id = data.get("execution_id")
        if task_id is None:
            task_id = data.get("task_id")
        if task_id is None:
            raise KeyError("Missing required field: execution_id")

        # Create TaskProgress with string phase/status (no enum conversion)
        return cls(
            task_id=task_id,
            phase=data["phase"],  # Pass through as string
            status=data["status"],  # Pass through as string
            percent=data["percent"],
            timestamp=data["timestamp"],
            completed=data["completed"],
            total=data["total"],
            plate_id=data.get("plate_id", ""),
            axis_id=data.get("axis_id", ""),
            context=context,
            error=data.get("error"),
            traceback=data.get("traceback"),
        )


def validate_progress_payload(payload: dict) -> dict:
    """Validate progress payload using generic TaskProgress."""
    TaskProgress.from_dict(payload)  # Will raise if invalid
    return payload


# =============================================================================
# Message Field Constants (used by control messages)
# =============================================================================


class MessageFields:
    TYPE = "type"
    PLATE_ID = "plate_id"
    EXECUTION_PLATE_ID = "execution_plate_id"
    SELECTED_PIPELINE_PATH = "selected_pipeline_path"
    PIPELINE_CODE = "pipeline_code"
    CONFIG_PARAMS = "config_params"
    CONFIG_CODE = "config_code"
    PIPELINE_CONFIG_CODE = "pipeline_config_code"
    CLIENT_ADDRESS = "client_address"
    COMPILE_ONLY = "compile_only"
    COMPILE_ARTIFACT_ID = "compile_artifact_id"
    COMPILE_STATUS = "compile_status"
    COMPILE_MESSAGE = "compile_message"
    MEMORY_MB = "memory_mb"
    CPU_PERCENT = "cpu_percent"
    EXECUTION_ID = "execution_id"
    START_TIME = "start_time"
    END_TIME = "end_time"
    ELAPSED = "elapsed"
    STATUS = "status"
    ERROR = "error"
    MESSAGE = "message"
    PORT = "port"
    CONTROL_PORT = "control_port"
    READY = "ready"
    SERVER = "server"
    SERVER_TYPE = "server_type"
    SERVER_ROLE = "server_role"
    LOG_FILE_PATH = "log_file_path"
    ACTIVE_EXECUTIONS = "active_executions"
    RUNNING_EXECUTIONS = "running_executions"
    WORKERS = "workers"
    WORKERS_KILLED = "workers_killed"
    UPTIME = "uptime"
    EXECUTIONS = "executions"
    EXECUTION = "execution"
    QUEUED_EXECUTIONS = "queued_executions"
    QUEUE_POSITION = "queue_position"
    WELL_COUNT = "well_count"
    WELLS = "wells"
    RESULTS_SUMMARY = "results_summary"
    WELL_ID = "well_id"
    STEP = "step"
    TIMESTAMP = "timestamp"
    AXIS_ID = "axis_id"
    STEP_NAME = "step_name"
    STEP_INDEX = "step_index"
    TOTAL_STEPS = "total_steps"
    PHASE = "phase"
    COMPLETED = "completed"
    TOTAL = "total"
    PERCENT = "percent"
    PATTERN = "pattern"
    COMPONENT = "component"
    TRACEBACK = "traceback"
    # Acknowledgment message fields
    IMAGE_ID = "image_id"
    VIEWER_PORT = "viewer_port"
    VIEWER_TYPE = "viewer_type"
    # ROI message fields
    ROIS = "rois"
    LAYER_NAME = "layer_name"
    SHAPES = "shapes"
    COORDINATES = "coordinates"
    METADATA = "metadata"
    CLIENT_ID = "client_id"
    PROGRESS_SUBSCRIBERS = "progress_subscribers"
    PROCESS_IDENTITY = "process_identity"
    CONTROL_CAPABILITIES = "control_capabilities"


@dataclass(frozen=True)
class ProcessIdentity:
    """PID-reuse-safe identity for a process visible on the local host."""

    pid: int
    create_time: float

    @classmethod
    def current(cls) -> "ProcessIdentity":
        process = psutil.Process()
        return cls(pid=process.pid, create_time=process.create_time())

    def to_dict(self) -> Dict[str, Any]:
        return {"pid": self.pid, "create_time": self.create_time}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProcessIdentity":
        return cls(pid=int(data["pid"]), create_time=float(data["create_time"]))

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

    def terminate(self, timeout: float = 5.0) -> bool:
        """Terminate this exact local process without crossing PID reuse."""

        try:
            process = psutil.Process(self.pid)
            if process.create_time() != self.create_time:
                return True
            process.terminate()
            try:
                process.wait(timeout=timeout)
            except psutil.TimeoutExpired:
                process.kill()
                process.wait(timeout=timeout)
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


class ResponseType(Enum):
    PONG = "pong"
    ACCEPTED = "accepted"
    OK = "ok"
    ERROR = "error"
    SHUTDOWN_ACK = "shutdown_ack"


class ExecutionStatus(Enum):
    """Execution lifecycle declaration with behavior owned by each phase."""

    QUEUED = ("queued", False, False)
    RUNNING = ("running", False, True)
    COMPLETE = ("complete", True, False)
    COMPLETED = ("completed", True, False)
    FAILED = ("failed", True, False)
    CANCELLED = ("cancelled", True, False)
    ACCEPTED = ("accepted", False, False)

    def __new__(
        cls,
        value: str,
        is_terminal: bool,
        reports_running_transition: bool,
    ) -> "ExecutionStatus":
        member = object.__new__(cls)
        member._value_ = value
        member._is_terminal = is_terminal
        member._reports_running_transition = reports_running_transition
        return member

    @property
    def is_terminal(self) -> bool:
        """Whether observing this phase completes status polling."""

        return self._is_terminal

    def reports_running_transition_from(self, previous: "ExecutionStatus") -> bool:
        """Whether entering this phase should emit the running transition."""

        return self._reports_running_transition and previous is type(self).QUEUED

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


@dataclass(frozen=True)
class ExecuteRequest:
    plate_id: str
    pipeline_code: str
    execution_plate_id: Optional[str] = None
    selected_pipeline_path: Optional[str] = None
    config_params: Optional[dict] = None
    config_code: Optional[str] = None
    pipeline_config_code: Optional[str] = None
    client_address: Optional[str] = None
    compile_only: bool = False
    compile_artifact_id: Optional[str] = None

    def validate(self):
        if not self.plate_id:
            return "Missing required field: plate_id"
        if not self.pipeline_code:
            return "Missing required field: pipeline_code"
        if self.compile_only and self.compile_artifact_id:
            return "compile_only and compile_artifact_id cannot both be set"
        if self.config_params is None and self.config_code is None:
            return "Missing config: provide either config_params or config_code"
        return None

    def to_dict(self):
        result: Dict[str, Any] = {}
        result[MessageFields.TYPE] = ControlMessageType.EXECUTE.value
        result[MessageFields.PLATE_ID] = self.plate_id
        if self.execution_plate_id is not None:
            result[MessageFields.EXECUTION_PLATE_ID] = self.execution_plate_id
        if self.selected_pipeline_path is not None:
            result[MessageFields.SELECTED_PIPELINE_PATH] = self.selected_pipeline_path
        result[MessageFields.PIPELINE_CODE] = self.pipeline_code
        if self.config_params is not None:
            result[MessageFields.CONFIG_PARAMS] = self.config_params
        if self.config_code is not None:
            result[MessageFields.CONFIG_CODE] = self.config_code
        if self.pipeline_config_code is not None:
            result[MessageFields.PIPELINE_CONFIG_CODE] = self.pipeline_config_code
        if self.client_address is not None:
            result[MessageFields.CLIENT_ADDRESS] = self.client_address
        if self.compile_only:
            result[MessageFields.COMPILE_ONLY] = True
        if self.compile_artifact_id is not None:
            result[MessageFields.COMPILE_ARTIFACT_ID] = self.compile_artifact_id
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(
            plate_id=data[MessageFields.PLATE_ID],
            pipeline_code=data[MessageFields.PIPELINE_CODE],
            execution_plate_id=data.get(MessageFields.EXECUTION_PLATE_ID),
            selected_pipeline_path=data.get(MessageFields.SELECTED_PIPELINE_PATH),
            config_params=data.get(MessageFields.CONFIG_PARAMS),
            config_code=data.get(MessageFields.CONFIG_CODE),
            pipeline_config_code=data.get(MessageFields.PIPELINE_CONFIG_CODE),
            client_address=data.get(MessageFields.CLIENT_ADDRESS),
            compile_only=bool(data.get(MessageFields.COMPILE_ONLY, False)),
            compile_artifact_id=data.get(MessageFields.COMPILE_ARTIFACT_ID),
        )


class ControlResponse(ABC):
    """Nominal response that owns its control-wire mapping."""

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Project this response to its control-wire representation."""


@dataclass(frozen=True)
class ControlErrorResponse(ControlResponse):
    """Canonical control error and its wire projection."""

    message: str
    response_type: ResponseType = ResponseType.ERROR

    @classmethod
    def from_exception(cls, error: Exception) -> "ControlErrorResponse":
        """Create a control failure from the exception owned by dispatch."""

        return cls(message=str(error))

    def to_dict(self) -> Dict[str, Any]:
        response_type = self.response_type.value
        return {
            MessageFields.STATUS: response_type,
            MessageFields.TYPE: response_type,
            MessageFields.MESSAGE: self.message,
        }


@dataclass(frozen=True)
class ExecuteResponse(ControlResponse):
    status: ResponseType
    execution_id: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self):
        result: Dict[str, Any] = {MessageFields.STATUS: self.status.value}
        if self.execution_id is not None:
            result[MessageFields.EXECUTION_ID] = self.execution_id
        if self.message is not None:
            result[MessageFields.MESSAGE] = self.message
        if self.error is not None:
            result[MessageFields.ERROR] = self.error
        return result


@dataclass
class ExecutionRecord:
    execution_id: str
    plate_id: str
    client_address: Optional[str]
    status: str
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    results_summary: Optional[Dict[str, Any]] = None
    compile_only: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def set_extra(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    def get_extra(self, key: str, default: Any = None) -> Any:
        if key in self.metadata:
            return self.metadata[key]
        return default

    def pop_extra(self, key: str, default: Any = None) -> Any:
        if key in self.metadata:
            value = self.metadata[key]
            del self.metadata[key]
            return value
        return default

    def to_dict(self) -> Dict[str, Any]:
        def _to_transport_value(value: Any) -> Any:
            if value is None or isinstance(value, (str, int, float, bool)):
                return value
            if isinstance(value, Enum):
                return value.value
            if isinstance(value, dict):
                return {
                    str(_to_transport_value(k)): _to_transport_value(v) for k, v in value.items()
                }
            if isinstance(value, (list, tuple, set)):
                return [_to_transport_value(v) for v in value]
            return str(value)

        result: Dict[str, Any] = {
            MessageFields.EXECUTION_ID: self.execution_id,
            MessageFields.PLATE_ID: _to_transport_value(self.plate_id),
            MessageFields.CLIENT_ADDRESS: _to_transport_value(self.client_address),
            MessageFields.STATUS: _to_transport_value(self.status),
            MessageFields.START_TIME: _to_transport_value(self.start_time),
            MessageFields.END_TIME: _to_transport_value(self.end_time),
            MessageFields.ERROR: _to_transport_value(self.error),
            MessageFields.COMPILE_ONLY: self.compile_only,
        }
        if self.traceback is not None:
            result[MessageFields.TRACEBACK] = _to_transport_value(self.traceback)
        if self.results_summary is not None:
            result[MessageFields.RESULTS_SUMMARY] = _to_transport_value(self.results_summary)
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionRecord":
        known = {
            MessageFields.EXECUTION_ID,
            MessageFields.PLATE_ID,
            MessageFields.CLIENT_ADDRESS,
            MessageFields.STATUS,
            MessageFields.START_TIME,
            MessageFields.END_TIME,
            MessageFields.ERROR,
            MessageFields.TRACEBACK,
            MessageFields.RESULTS_SUMMARY,
            MessageFields.COMPILE_ONLY,
        }
        metadata = {k: v for k, v in data.items() if k not in known}
        return cls(
            execution_id=data[MessageFields.EXECUTION_ID],
            plate_id=data[MessageFields.PLATE_ID],
            client_address=data.get(MessageFields.CLIENT_ADDRESS),
            status=data[MessageFields.STATUS],
            start_time=data.get(MessageFields.START_TIME),
            end_time=data.get(MessageFields.END_TIME),
            error=data.get(MessageFields.ERROR),
            traceback=data.get(MessageFields.TRACEBACK),
            results_summary=data.get(MessageFields.RESULTS_SUMMARY),
            compile_only=bool(data.get(MessageFields.COMPILE_ONLY, False)),
            metadata=metadata,
        )


@dataclass(frozen=True)
class RunningExecutionInfo:
    execution_id: str
    plate_id: str
    start_time: float
    elapsed: float
    compile_only: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            MessageFields.EXECUTION_ID: self.execution_id,
            MessageFields.PLATE_ID: self.plate_id,
            MessageFields.START_TIME: self.start_time,
            MessageFields.ELAPSED: self.elapsed,
            MessageFields.COMPILE_ONLY: self.compile_only,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunningExecutionInfo":
        return cls(
            execution_id=data[MessageFields.EXECUTION_ID],
            plate_id=data.get(MessageFields.PLATE_ID, "unknown"),
            start_time=float(data.get(MessageFields.START_TIME) or 0.0),
            elapsed=float(data.get(MessageFields.ELAPSED) or 0.0),
            compile_only=bool(data.get(MessageFields.COMPILE_ONLY, False)),
        )


@dataclass(frozen=True)
class QueuedExecutionInfo:
    execution_id: str
    plate_id: str
    queue_position: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            MessageFields.EXECUTION_ID: self.execution_id,
            MessageFields.PLATE_ID: self.plate_id,
            MessageFields.QUEUE_POSITION: self.queue_position,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QueuedExecutionInfo":
        return cls(
            execution_id=data[MessageFields.EXECUTION_ID],
            plate_id=data.get(MessageFields.PLATE_ID, "unknown"),
            queue_position=int(data.get(MessageFields.QUEUE_POSITION) or 0),
        )


@dataclass(frozen=True)
class ExecutionStatusSnapshot:
    status: ResponseType
    execution: Optional[ExecutionRecord] = None
    active_executions: Optional[int] = None
    uptime: Optional[float] = None
    executions: Optional[Tuple[str, ...]] = None
    running_executions: Optional[Tuple[RunningExecutionInfo, ...]] = None
    queued_executions: Optional[Tuple[QueuedExecutionInfo, ...]] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {MessageFields.STATUS: self.status.value}
        if self.execution is not None:
            result[MessageFields.EXECUTION] = self.execution.to_dict()
        if self.active_executions is not None:
            result[MessageFields.ACTIVE_EXECUTIONS] = self.active_executions
        if self.uptime is not None:
            result[MessageFields.UPTIME] = self.uptime
        if self.executions is not None:
            result[MessageFields.EXECUTIONS] = list(self.executions)
        if self.running_executions is not None:
            result[MessageFields.RUNNING_EXECUTIONS] = [
                info.to_dict() for info in self.running_executions
            ]
        if self.queued_executions is not None:
            result[MessageFields.QUEUED_EXECUTIONS] = [
                info.to_dict() for info in self.queued_executions
            ]
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionStatusSnapshot":
        execution = None
        execution_data = data.get(MessageFields.EXECUTION)
        if isinstance(execution_data, dict):
            execution = ExecutionRecord.from_dict(execution_data)

        running_executions = None
        running_data = data.get(MessageFields.RUNNING_EXECUTIONS)
        if isinstance(running_data, list):
            running_executions = tuple(
                RunningExecutionInfo.from_dict(entry) for entry in running_data
            )

        queued_executions = None
        queued_data = data.get(MessageFields.QUEUED_EXECUTIONS)
        if isinstance(queued_data, list):
            queued_executions = tuple(QueuedExecutionInfo.from_dict(entry) for entry in queued_data)

        executions = None
        execution_ids = data.get(MessageFields.EXECUTIONS)
        if isinstance(execution_ids, list):
            executions = tuple(str(eid) for eid in execution_ids)

        return cls(
            status=ResponseType(data[MessageFields.STATUS]),
            execution=execution,
            active_executions=data.get(MessageFields.ACTIVE_EXECUTIONS),
            uptime=data.get(MessageFields.UPTIME),
            executions=executions,
            running_executions=running_executions,
            queued_executions=queued_executions,
        )


@dataclass(frozen=True)
class StatusRequest:
    execution_id: Optional[str] = None

    def to_dict(self):
        result = {MessageFields.TYPE: ControlMessageType.STATUS.value}
        if self.execution_id is not None:
            result[MessageFields.EXECUTION_ID] = self.execution_id
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(execution_id=data.get(MessageFields.EXECUTION_ID))


@dataclass(frozen=True)
class CancelRequest:
    execution_id: str

    def validate(self):
        return "Missing execution_id" if not self.execution_id else None

    def to_dict(self):
        return {
            MessageFields.TYPE: ControlMessageType.CANCEL.value,
            MessageFields.EXECUTION_ID: self.execution_id,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(execution_id=data[MessageFields.EXECUTION_ID])


@dataclass(frozen=True)
class ProgressRegistrationRequest:
    client_id: str

    def validate(self):
        return "Missing client_id" if not self.client_id else None

    def to_dict(self):
        return {
            MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
            MessageFields.CLIENT_ID: self.client_id,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(client_id=data[MessageFields.CLIENT_ID])


@dataclass(frozen=True)
class ProgressUnregistrationRequest:
    client_id: str

    def validate(self):
        return "Missing client_id" if not self.client_id else None

    def to_dict(self):
        return {
            MessageFields.TYPE: ControlMessageType.UNREGISTER_PROGRESS.value,
            MessageFields.CLIENT_ID: self.client_id,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(client_id=data[MessageFields.CLIENT_ID])


@dataclass(frozen=True)
class ProcessResourceUsage:
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
class PongResponse(ControlResponse):
    """Complete typed server-heartbeat response."""

    port: int
    control_port: int
    ready: bool
    server: str
    server_role: ServerRole
    control_capabilities: frozenset[EndpointControlCapability] = frozenset(
        {EndpointControlCapability.PING}
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

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for transport."""
        result: Dict[str, Any] = {
            MessageFields.TYPE: ResponseType.PONG.value,
            MessageFields.PORT: self.port,
            MessageFields.CONTROL_PORT: self.control_port,
            MessageFields.READY: self.ready,
            MessageFields.SERVER: self.server,
        }
        if self.server_type is not None:
            result[MessageFields.SERVER_TYPE] = self.server_type
        result[MessageFields.SERVER_ROLE] = self.server_role.value
        result[MessageFields.CONTROL_CAPABILITIES] = sorted(
            capability.value for capability in self.control_capabilities
        )
        if self.log_file_path is not None:
            result[MessageFields.LOG_FILE_PATH] = self.log_file_path
        if self.active_executions is not None:
            result[MessageFields.ACTIVE_EXECUTIONS] = self.active_executions
        if self.running_executions is not None:
            result[MessageFields.RUNNING_EXECUTIONS] = [
                info.to_dict() for info in self.running_executions
            ]
        if self.queued_executions is not None:
            result[MessageFields.QUEUED_EXECUTIONS] = [
                info.to_dict() for info in self.queued_executions
            ]
        if self.workers is not None:
            result[MessageFields.WORKERS] = [w.to_dict() for w in self.workers]
        if self.uptime is not None:
            result[MessageFields.UPTIME] = self.uptime
        if self.progress_subscribers is not None:
            result[MessageFields.PROGRESS_SUBSCRIBERS] = self.progress_subscribers
        if self.process_identity is not None:
            result[MessageFields.PROCESS_IDENTITY] = self.process_identity.to_dict()
        if self.compile_status is not None:
            result[MessageFields.COMPILE_STATUS] = self.compile_status
        if self.compile_message is not None:
            result[MessageFields.COMPILE_MESSAGE] = self.compile_message
        if self.process_usage is not None:
            result[MessageFields.MEMORY_MB] = self.process_usage.memory_mb
            result[MessageFields.CPU_PERCENT] = self.process_usage.cpu_percent
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PongResponse":
        """Parse from transport."""
        if data[MessageFields.TYPE] != ResponseType.PONG.value:
            raise ValueError(
                f"PongResponse requires {MessageFields.TYPE}={ResponseType.PONG.value!r}."
            )

        workers_data = data.get(MessageFields.WORKERS)
        workers = None
        if workers_data is not None:
            if not isinstance(workers_data, list) or not all(
                isinstance(entry, dict) for entry in workers_data
            ):
                raise TypeError("PongResponse.workers must be a list of dict entries")
            workers = tuple(WorkerState.from_dict(w) for w in workers_data)

        running_executions_data = data.get(MessageFields.RUNNING_EXECUTIONS)
        running_executions = None
        if running_executions_data is not None:
            if not isinstance(running_executions_data, list):
                raise TypeError("PongResponse.running_executions must be a list of dict entries")
            if not all(isinstance(entry, dict) for entry in running_executions_data):
                raise TypeError("PongResponse.running_executions must be a list of dict entries")
            running_executions = tuple(
                RunningExecutionInfo.from_dict(entry) for entry in running_executions_data
            )

        queued_executions_data = data.get(MessageFields.QUEUED_EXECUTIONS)
        queued_executions = None
        if queued_executions_data is not None:
            if not isinstance(queued_executions_data, list):
                raise TypeError("PongResponse.queued_executions must be a list of dict entries")
            if not all(isinstance(entry, dict) for entry in queued_executions_data):
                raise TypeError("PongResponse.queued_executions must be a list of dict entries")
            queued_executions = tuple(
                QueuedExecutionInfo.from_dict(entry) for entry in queued_executions_data
            )

        process_identity_data = data.get(MessageFields.PROCESS_IDENTITY)
        process_identity = None
        if process_identity_data is not None:
            if not isinstance(process_identity_data, dict):
                raise TypeError("PongResponse.process_identity must be a dict")
            process_identity = ProcessIdentity.from_dict(process_identity_data)

        control_capabilities_data = data.get(
            MessageFields.CONTROL_CAPABILITIES,
            [EndpointControlCapability.PING.value],
        )
        if not isinstance(control_capabilities_data, list) or not all(
            isinstance(capability, str) for capability in control_capabilities_data
        ):
            raise TypeError("PongResponse.control_capabilities must be a list of strings")
        control_capabilities = frozenset(
            EndpointControlCapability(capability) for capability in control_capabilities_data
        )

        memory_mb = data.get(MessageFields.MEMORY_MB)
        cpu_percent = data.get(MessageFields.CPU_PERCENT)
        process_usage = None
        if memory_mb is not None or cpu_percent is not None:
            if memory_mb is None or cpu_percent is None:
                raise TypeError(
                    "PongResponse process usage requires both memory_mb and cpu_percent"
                )
            process_usage = ProcessResourceUsage(
                memory_mb=float(memory_mb),
                cpu_percent=float(cpu_percent),
            )

        return cls(
            port=data[MessageFields.PORT],
            control_port=data[MessageFields.CONTROL_PORT],
            ready=data[MessageFields.READY],
            server=data[MessageFields.SERVER],
            server_type=data.get(MessageFields.SERVER_TYPE),
            server_role=ServerRole(data[MessageFields.SERVER_ROLE]),
            control_capabilities=control_capabilities,
            log_file_path=data.get(MessageFields.LOG_FILE_PATH),
            active_executions=data.get(MessageFields.ACTIVE_EXECUTIONS),
            running_executions=running_executions,
            queued_executions=queued_executions,
            workers=workers,
            uptime=data.get(MessageFields.UPTIME),
            progress_subscribers=data.get(MessageFields.PROGRESS_SUBSCRIBERS),
            process_identity=process_identity,
            compile_status=data.get(MessageFields.COMPILE_STATUS),
            compile_message=data.get(MessageFields.COMPILE_MESSAGE),
            process_usage=process_usage,
        )


# =============================================================================
# Streaming Message Types
# =============================================================================


@dataclass(frozen=True)
class ImageAck:
    """Acknowledgment message sent by viewers after processing an image.

    Sent via PUSH socket from viewer to shared ack port (7555).
    Used to track real-time queue depth and show progress like '3/10 images processed'.
    """

    image_id: str  # UUID of the processed image
    viewer_port: int  # Port of the viewer that processed it (for routing)
    viewer_type: str  # 'napari' or 'fiji'
    status: str = "success"  # 'success', 'error', etc.
    timestamp: Optional[float] = None  # When it was processed
    error: Optional[str] = None  # Error message if status='error'

    def to_dict(self):
        result = {
            MessageFields.TYPE: "image_ack",
            MessageFields.IMAGE_ID: self.image_id,
            MessageFields.VIEWER_PORT: self.viewer_port,
            MessageFields.VIEWER_TYPE: self.viewer_type,
            MessageFields.STATUS: self.status,
        }
        if self.timestamp is not None:
            result[MessageFields.TIMESTAMP] = self.timestamp
        if self.error is not None:
            result[MessageFields.ERROR] = self.error
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(
            image_id=data[MessageFields.IMAGE_ID],
            viewer_port=data[MessageFields.VIEWER_PORT],
            viewer_type=data[MessageFields.VIEWER_TYPE],
            status=data.get(MessageFields.STATUS, "success"),
            timestamp=data.get(MessageFields.TIMESTAMP),
            error=data.get(MessageFields.ERROR),
        )


@dataclass(frozen=True)
class ROIMessage:
    """Message for streaming ROIs to viewers (Napari/Fiji).

    Sent via ZMQ to viewer servers to display ROIs in real-time.
    """

    rois: list  # List of ROI dictionaries with shapes and metadata
    layer_name: str = "ROIs"  # Name of the layer/overlay

    def to_dict(self):
        return {
            MessageFields.TYPE: "rois",
            MessageFields.ROIS: self.rois,
            MessageFields.LAYER_NAME: self.layer_name,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            rois=data[MessageFields.ROIS], layer_name=data.get(MessageFields.LAYER_NAME, "ROIs")
        )


@dataclass(frozen=True)
class ShapesMessage:
    """Message for Napari shapes layer.

    Napari-specific format for displaying polygon/ellipse shapes.
    """

    shapes: list  # List of shape dictionaries with type, coordinates, metadata
    layer_name: str = "ROIs"

    def to_dict(self):
        return {
            MessageFields.TYPE: "shapes",
            MessageFields.SHAPES: self.shapes,
            MessageFields.LAYER_NAME: self.layer_name,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            shapes=data[MessageFields.SHAPES], layer_name=data.get(MessageFields.LAYER_NAME, "ROIs")
        )
