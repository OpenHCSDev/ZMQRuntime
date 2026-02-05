"""ZMQ Message Type System - generic runtime messages.

This library provides GENERIC ZMQ messaging primitives.
Application-specific logic (pipelines, compilation, etc.) should extend these types
at the application layer, not in this runtime library.
"""

import logging
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


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
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pid": self.pid,
            "status": self.status,
            "cpu_percent": self.cpu_percent,
            "memory_mb": self.memory_mb,
            **self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkerState":
        # Extract known fields, rest goes to metadata
        known_fields = {"pid", "status", "cpu_percent", "memory_mb"}
        metadata = {k: v for k, v in data.items() if k not in known_fields}

        return cls(
            pid=data["pid"],
            status=data["status"],
            cpu_percent=data["cpu_percent"],
            memory_mb=data["memory_mb"],
            metadata=metadata,
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

        # DEBUG: Log early if total_wells is present
        if 'total_wells' in data:
            logger.info(f"TaskProgress.from_dict ENTRY: total_wells={data.get('total_wells')}, data.keys()={list(data.keys())}")

        # Separate generic fields from app-specific context
        generic_fields = {
            "type", "task_id", "phase", "status", "percent",
            "timestamp", "completed", "total", "plate_id", "axis_id",
            "error", "traceback"
        }

        # DEBUG: Log all keys in data before separation
        if 'total_wells' in data or any(k not in generic_fields for k in data.keys()):
            logger.info(f"TaskProgress.from_dict BEFORE: data.keys()={list(data.keys())}, total_wells in data={'total_wells' in data}")

        context = {k: v for k, v in data.items() if k not in generic_fields}

        # DEBUG: Log what ended up in context
        if 'total_wells' in data or context:
            logger.info(f"TaskProgress.from_dict AFTER: context.keys()={list(context.keys())}, total_wells in context={'total_wells' in context}, total_wells value={context.get('total_wells')}")

        # Create TaskProgress with string phase/status (no enum conversion)
        return cls(
            task_id=data["task_id"],
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
# Generic Server Info
# =============================================================================

class ServerCapability(Enum):
    """Generic server capabilities - extend in applications."""
    TASK_EXECUTION = "task_execution"  # Can execute tasks/jobs
    WORKER_POOL = "worker_pool"        # Has worker processes
    PROGRESS_STREAMING = "progress_streaming"  # Streams progress updates
    VIEWER = "viewer"                 # Displays results


@dataclass(frozen=True)
class ServerInfo:
    """Generic server info returned from ping."""

    port: int
    control_port: int
    server_class: str  # Actual Python class name
    capabilities: Tuple[ServerCapability, ...]
    ready: bool

    # Generic worker info (if applicable)
    workers: Optional[Tuple[WorkerState, ...]] = None

    # Task execution info (if applicable)
    active_tasks: Optional[int] = None
    running_tasks: Optional[Tuple[str, ...]] = None

    # Server metadata
    uptime: Optional[float] = None
    log_file: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for transport."""
        result = {
            "type": "pong",
            "port": self.port,
            "control_port": self.control_port,
            "server": self.server_class,
            "ready": self.ready,
            "capabilities": [c.value for c in self.capabilities],
        }
        if self.workers:
            result["workers"] = [w.to_dict() for w in self.workers]
        if self.active_tasks is not None:
            result["active_tasks"] = self.active_tasks
        if self.running_tasks is not None:
            result["running_tasks"] = list(self.running_tasks)
        if self.uptime is not None:
            result["uptime"] = self.uptime
        if self.log_file is not None:
            result["log_file_path"] = self.log_file
        result.update(self.metadata)
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServerInfo":
        """Parse from transport."""
        cap_data = data.get("capabilities", [])
        capabilities = []
        for cap_str in cap_data:
            try:
                capabilities.append(ServerCapability(cap_str))
            except ValueError:
                pass  # Skip unknown capabilities

        workers_data = data.get("workers")
        workers = None
        if workers_data:
            workers = tuple(WorkerState.from_dict(w) for w in workers_data)

        running_tasks_data = data.get("running_tasks")
        running_tasks = None
        if running_tasks_data:
            running_tasks = tuple(running_tasks_data)

        # Separate generic fields from metadata
        generic_fields = {
            "type", "port", "control_port", "server", "ready",
            "capabilities", "workers", "active_tasks", "running_tasks",
            "uptime", "log_file_path"
        }
        metadata = {k: v for k, v in data.items() if k not in generic_fields}

        return cls(
            port=data["port"],
            control_port=data["control_port"],
            server_class=data["server"],
            capabilities=tuple(capabilities),
            ready=data["ready"],
            workers=workers,
            active_tasks=data.get("active_tasks"),
            running_tasks=running_tasks,
            uptime=data.get("uptime"),
            log_file=data.get("log_file_path"),
            metadata=metadata,
        )


# =============================================================================
# Message Field Constants (used by control messages)
# =============================================================================

class MessageFields:
    TYPE = "type"
    PLATE_ID = "plate_id"
    PIPELINE_CODE = "pipeline_code"
    CONFIG_PARAMS = "config_params"
    CONFIG_CODE = "config_code"
    PIPELINE_CONFIG_CODE = "pipeline_config_code"
    CLIENT_ADDRESS = "client_address"
    COMPILE_ONLY = "compile_only"
    COMPILE_STATUS = "compile_status"
    COMPILE_MESSAGE = "compile_message"
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
    LOG_FILE_PATH = "log_file_path"
    ACTIVE_EXECUTIONS = "active_executions"
    RUNNING_EXECUTIONS = "running_executions"
    WORKERS = "workers"
    WORKERS_KILLED = "workers_killed"
    UPTIME = "uptime"
    EXECUTIONS = "executions"
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

    def get_handler_method(self):
        return {
            ControlMessageType.EXECUTE: "_handle_execute",
            ControlMessageType.STATUS: "_handle_status",
            ControlMessageType.CANCEL: "_handle_cancel",
            ControlMessageType.SHUTDOWN: "_handle_shutdown",
            ControlMessageType.FORCE_SHUTDOWN: "_handle_force_shutdown",
        }[self]

    def dispatch(self, server, message):
        return getattr(server, self.get_handler_method())(message)


class ResponseType(Enum):
    PONG = "pong"
    ACCEPTED = "accepted"
    OK = "ok"
    ERROR = "error"
    SHUTDOWN_ACK = "shutdown_ack"


class ExecutionStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ACCEPTED = "accepted"


class SocketType(Enum):
    PUB = "PUB"
    SUB = "SUB"
    REQ = "REQ"
    REP = "REP"

    @classmethod
    def from_zmq_constant(cls, zmq_const):
        import zmq
        return {zmq.PUB: cls.PUB, zmq.SUB: cls.SUB, zmq.REQ: cls.REQ, zmq.REP: cls.REP}.get(zmq_const, cls.PUB)

    def get_display_name(self):
        return self.value


@dataclass(frozen=True)
class ExecuteRequest:
    plate_id: str
    pipeline_code: str
    config_params: Optional[dict] = None
    config_code: Optional[str] = None
    pipeline_config_code: Optional[str] = None
    client_address: Optional[str] = None
    compile_only: bool = False

    def validate(self):
        if not self.plate_id:
            return "Missing required field: plate_id"
        if not self.pipeline_code:
            return "Missing required field: pipeline_code"
        if self.config_params is None and self.config_code is None:
            return "Missing config: provide either config_params or config_code"
        return None

    def to_dict(self):
        result: Dict[str, Any] = {}
        result[MessageFields.TYPE] = ControlMessageType.EXECUTE.value
        result[MessageFields.PLATE_ID] = self.plate_id
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
        return result

    @classmethod
    def from_dict(cls, data):
        return cls(
            plate_id=data[MessageFields.PLATE_ID],
            pipeline_code=data[MessageFields.PIPELINE_CODE],
            config_params=data.get(MessageFields.CONFIG_PARAMS),
            config_code=data.get(MessageFields.CONFIG_CODE),
            pipeline_config_code=data.get(MessageFields.PIPELINE_CONFIG_CODE),
            client_address=data.get(MessageFields.CLIENT_ADDRESS),
            compile_only=bool(data.get(MessageFields.COMPILE_ONLY, False))
        )


@dataclass(frozen=True)
class ExecuteResponse:
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
        return {MessageFields.TYPE: ControlMessageType.CANCEL.value, MessageFields.EXECUTION_ID: self.execution_id}

    @classmethod
    def from_dict(cls, data):
        return cls(execution_id=data[MessageFields.EXECUTION_ID])


@dataclass(frozen=True)
class PongResponse:
    """Pong response with typed worker info."""
    port: int
    control_port: int
    ready: bool
    server: str
    log_file_path: Optional[str] = None
    active_executions: Optional[int] = None
    running_executions: Optional[Tuple[str, ...]] = None
    workers: Optional[Tuple[WorkerState, ...]] = None
    uptime: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for transport."""
        result: Dict[str, Any] = {
            MessageFields.TYPE: ResponseType.PONG.value,
            MessageFields.PORT: self.port,
            MessageFields.CONTROL_PORT: self.control_port,
            MessageFields.READY: self.ready,
            MessageFields.SERVER: self.server,
        }
        if self.log_file_path is not None:
            result[MessageFields.LOG_FILE_PATH] = self.log_file_path
        if self.active_executions is not None:
            result[MessageFields.ACTIVE_EXECUTIONS] = self.active_executions
        if self.running_executions is not None:
            result[MessageFields.RUNNING_EXECUTIONS] = list(self.running_executions)
        if self.workers is not None:
            result[MessageFields.WORKERS] = [w.to_dict() for w in self.workers]
        if self.uptime is not None:
            result[MessageFields.UPTIME] = self.uptime
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PongResponse":
        """Parse from transport."""
        workers_data = data.get(MessageFields.WORKERS)
        workers = None
        if workers_data is not None:
            workers = tuple(WorkerState.from_dict(w) for w in workers_data)

        running_executions_data = data.get(MessageFields.RUNNING_EXECUTIONS)
        running_executions = None
        if running_executions_data is not None:
            running_executions = tuple(running_executions_data)

        return cls(
            port=data[MessageFields.PORT],
            control_port=data[MessageFields.CONTROL_PORT],
            ready=data[MessageFields.READY],
            server=data[MessageFields.SERVER],
            log_file_path=data.get(MessageFields.LOG_FILE_PATH),
            active_executions=data.get(MessageFields.ACTIVE_EXECUTIONS),
            running_executions=running_executions,
            workers=workers,
            uptime=data.get(MessageFields.UPTIME),
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
    image_id: str          # UUID of the processed image
    viewer_port: int       # Port of the viewer that processed it (for routing)
    viewer_type: str       # 'napari' or 'fiji'
    status: str = 'success'  # 'success', 'error', etc.
    timestamp: Optional[float] = None  # When it was processed
    error: Optional[str] = None      # Error message if status='error'

    def to_dict(self):
        result = {
            MessageFields.TYPE: "image_ack",
            MessageFields.IMAGE_ID: self.image_id,
            MessageFields.VIEWER_PORT: self.viewer_port,
            MessageFields.VIEWER_TYPE: self.viewer_type,
            MessageFields.STATUS: self.status
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
            status=data.get(MessageFields.STATUS, 'success'),
            timestamp=data.get(MessageFields.TIMESTAMP),
            error=data.get(MessageFields.ERROR)
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
            MessageFields.LAYER_NAME: self.layer_name
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            rois=data[MessageFields.ROIS],
            layer_name=data.get(MessageFields.LAYER_NAME, "ROIs")
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
            MessageFields.LAYER_NAME: self.layer_name
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            shapes=data[MessageFields.SHAPES],
            layer_name=data.get(MessageFields.LAYER_NAME, "ROIs")
        )
