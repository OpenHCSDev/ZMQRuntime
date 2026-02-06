"""Public API for zmqruntime."""

from __future__ import annotations

__version__ = "0.1.7"

from zmqruntime.ack_listener import GlobalAckListener
from zmqruntime.client import ZMQClient
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    CancelRequest,
    ControlMessageType,
    ExecuteRequest,
    ExecuteResponse,
    ExecutionRecord,
    ExecutionStatusSnapshot,
    RunningExecutionInfo,
    QueuedExecutionInfo,
    ExecutionStatus,
    ImageAck,
    MessageFields,
    PongResponse,
    ProgressRegistrationRequest,
    ProgressUnregistrationRequest,
    # Generic types
    TaskProgress,
    TaskPhase,
    TaskStatus,
    WorkerState,
    ServerInfo,
    ServerCapability,
    ResponseType,
    ROIMessage,
    ShapesMessage,
    SocketType,
    StatusRequest,
    validate_progress_payload,
)
from zmqruntime.execution import (
    ExecutionLifecycleEngineABC,
    InMemoryExecutionLifecycleEngine,
    ProgressStreamSubscriber,
    ExecutionWaiter,
    WaitPolicy,
)
from zmqruntime.progress import (
    EventRegistryABC,
    LatestEventRegistry,
    GenericAxisProjection,
    GenericPlateProjection,
    GenericExecutionProjection,
    ProgressProjectionAdapterABC,
    build_execution_projection,
)
from zmqruntime.queue_tracker import QueueTracker, GlobalQueueTrackerRegistry
from zmqruntime.runner import serve_forever
from zmqruntime.server import ZMQServer
from zmqruntime.transport import (
    coerce_transport_mode,
    get_control_port,
    get_control_url,
    get_default_transport_mode,
    get_ipc_socket_path,
    get_zmq_transport_url,
    is_port_in_use,
    ping_control_port,
    remove_ipc_socket,
    wait_for_server_ready,
)
from zmqruntime.viewer_state import (
    ViewerState,
    ViewerStateManager,
    ViewerInstance,
    get_or_create_viewer,
)
# from zmqruntime.streaming_config import (
#     DisplayConfig,
#     DictDisplayConfig,
#     ComponentMetadataProvider,
#     EmptyMetadataProvider,
#     ComponentMode,
#     COMPONENT_ABBREVIATIONS,
# )

__all__ = [
    "GlobalAckListener",
    "ZMQClient",
    "TransportMode",
    "ZMQConfig",
    "CancelRequest",
    "ControlMessageType",
    "ExecuteRequest",
    "ExecuteResponse",
    "ExecutionRecord",
    "ExecutionStatusSnapshot",
    "RunningExecutionInfo",
    "QueuedExecutionInfo",
    "ExecutionStatus",
    "ImageAck",
    "MessageFields",
    "PongResponse",
    "ProgressRegistrationRequest",
    "ProgressUnregistrationRequest",
    # Generic types
    "TaskProgress",
    "TaskPhase",
    "TaskStatus",
    "WorkerState",
    "ServerInfo",
    "ServerCapability",
    "ResponseType",
    "ROIMessage",
    "ShapesMessage",
    "SocketType",
    "StatusRequest",
    "validate_progress_payload",
    "QueueTracker",
    "GlobalQueueTrackerRegistry",
    "serve_forever",
    "ZMQServer",
    "coerce_transport_mode",
    "get_control_port",
    "get_control_url",
    "get_default_transport_mode",
    "get_ipc_socket_path",
    "get_zmq_transport_url",
    "is_port_in_use",
    "ping_control_port",
    "remove_ipc_socket",
    "wait_for_server_ready",
    "ViewerState",
    "ViewerStateManager",
    "ViewerInstance",
    "get_or_create_viewer",
    "ExecutionLifecycleEngineABC",
    "InMemoryExecutionLifecycleEngine",
    "ProgressStreamSubscriber",
    "ExecutionWaiter",
    "WaitPolicy",
    "EventRegistryABC",
    "LatestEventRegistry",
    "GenericAxisProjection",
    "GenericPlateProjection",
    "GenericExecutionProjection",
    "ProgressProjectionAdapterABC",
    "build_execution_projection",
]
