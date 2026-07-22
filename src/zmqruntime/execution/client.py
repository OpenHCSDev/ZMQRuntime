"""Execution client with submit/poll/wait and progress streaming."""
from __future__ import annotations

import pickle
import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import zmq

from zmqruntime.client import ZMQClient
from zmqruntime.execution.progress_stream import ProgressStreamSubscriber
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.execution.responses import (
    ExecutionSubmissionResponse,
    WireRequest,
    WireResponse,
)
from zmqruntime.messages import (
    ControlMessageType,
    MessageFields,
    PongResponse,
    ResponseType,
)
from zmqruntime.transport import get_zmq_transport_url

logger = logging.getLogger(__name__)
TaskT = TypeVar("TaskT")
ConfigT = TypeVar("ConfigT")


class ExecutionClient(ZMQClient, ABC, Generic[TaskT, ConfigT]):
    """Execution client with progress streaming."""

    def __init__(self, port: int, host: str = "localhost", persistent: bool = True,
                 progress_callback=None, transport_mode=None, config=None):
        super().__init__(port, host, persistent, transport_mode=transport_mode, config=config)
        self.progress_callback = progress_callback
        self._progress_stream: ProgressStreamSubscriber | None = None
        self._progress_client_id = str(uuid.uuid4())
        self._progress_registered = False
        self._progress_lock = threading.Lock()
        self._progress_sequence_by_execution_id: dict[str, int] = {}

    def _start_progress_listener(self):
        if self._progress_stream is None:
            self._progress_stream = ProgressStreamSubscriber(
                socket_provider=lambda: self.data_socket,
                callback=self._record_progress,
            )
        logger.info("Starting progress listener thread")
        self._progress_stream.start()

    def _stop_progress_listener(self):
        if self._progress_stream is None:
            return
        self._progress_stream.stop()

    def submit_execution(
        self,
        task: TaskT,
        config: ConfigT | None = None,
    ) -> WireResponse:
        if not self._connected and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        self._ensure_progress_subscription()
        request = self.serialize_task(task, config)
        if MessageFields.TYPE not in request:
            request[MessageFields.TYPE] = ControlMessageType.EXECUTE.value
        response = self._send_control_request(request)
        return response

    def poll_status(
        self,
        execution_id: str | None = None,
        *,
        timeout_ms: int = 5000,
    ):
        request = {MessageFields.TYPE: ControlMessageType.STATUS.value}
        if execution_id:
            request[MessageFields.EXECUTION_ID] = execution_id
        return self._send_control_request(request, timeout_ms=timeout_ms)

    def wait_for_completion(
        self,
        execution_id,
        poll_interval=0.5,
        max_consecutive_errors=5,
        status_timeout_ms: int = WaitPolicy.status_timeout_ms,
    ):
        logger.info("Waiting for execution %s to complete", execution_id)
        policy = WaitPolicy(
            poll_interval=poll_interval,
            max_consecutive_errors=max_consecutive_errors,
            status_timeout_ms=status_timeout_ms,
        )
        waiter = ExecutionWaiter(
            lambda current_execution_id: self.poll_status(
                current_execution_id,
                timeout_ms=policy.status_timeout_ms,
            ),
            progress_sequence=self._progress_sequence,
            known_server_process_is_alive=self.known_server_process_is_alive,
            owned_server_process_exit=self.owned_server_process_exit,
        )
        return waiter.wait(execution_id, policy)

    def execute(
        self,
        task: TaskT,
        config: ConfigT | None = None,
    ) -> WireResponse:
        response = self.submit_execution(task, config)
        submission = ExecutionSubmissionResponse.from_wire(response)
        if submission.accepted:
            execution_id = submission.require_execution_id("Execution submission")
            return self.wait_for_completion(execution_id)
        return response

    def cancel_execution(self, execution_id):
        return self._send_control_request(
            {MessageFields.TYPE: ControlMessageType.CANCEL.value, MessageFields.EXECUTION_ID: execution_id}
        )

    def ping(self):
        try:
            pong = self.get_server_info_snapshot()
            return bool(pong.ready)
        except Exception:
            return False

    def get_server_info_snapshot(self) -> PongResponse:
        """Request and validate typed server ping response.

        Returns:
            PongResponse: typed ping snapshot.

        Raises:
            RuntimeError: if client cannot connect or server returns non-pong response.
            TypeError: if payload type is invalid.
            KeyError/ValueError: if pong payload is malformed.
        """
        if not self._connected and not self.connect():
            raise RuntimeError("Not connected")
        response = self._send_control_request(
            {MessageFields.TYPE: ControlMessageType.PING.value},
            timeout_ms=1000,
        )
        if not isinstance(response, dict):
            raise TypeError(
                f"Expected ping response dict, got {type(response).__name__}"
            )
        if MessageFields.TYPE not in response:
            raise RuntimeError(f"Ping response missing type field: {response}")
        response_type = response[MessageFields.TYPE]
        if response_type != ResponseType.PONG.value:
            raise RuntimeError(
                f"Expected pong response, got type={response_type!r} payload={response}"
            )
        return PongResponse.from_dict(response)

    def get_server_info(self):
        """Backward-compatible dict view of typed ping snapshot."""
        return self.get_server_info_snapshot().to_dict()

    def _send_control_request(self, request, timeout_ms=5000):
        owns_context = self.zmq_context is None
        ctx = zmq.Context() if owns_context else self.zmq_context
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        control_url = get_zmq_transport_url(
            self.control_port,
            host=self.host,
            mode=self.transport_mode,
            config=self.config,
        )
        sock.connect(control_url)
        try:
            sock.send(pickle.dumps(request))
            response = sock.recv()
            return pickle.loads(response)
        except zmq.Again:
            raise TimeoutError(
                f"Server did not respond to {request[MessageFields.TYPE]} request within {timeout_ms}ms"
            )
        finally:
            sock.close(linger=0)
            if owns_context:
                ctx.term()

    def disconnect(self):
        if self._connected and self._progress_registered:
            try:
                self._send_control_request(
                    {
                        MessageFields.TYPE: ControlMessageType.UNREGISTER_PROGRESS.value,
                        MessageFields.CLIENT_ID: self._progress_client_id,
                    }
                )
            except Exception as error:
                logger.debug("Progress unregistration failed during disconnect: %s", error)
            self._progress_registered = False
        self._stop_progress_listener()
        super().disconnect()

    def _ensure_progress_subscription(self) -> None:
        self._start_progress_listener()
        if self._progress_registered:
            return
        response = self._send_control_request(
            {
                MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
                MessageFields.CLIENT_ID: self._progress_client_id,
            }
        )
        if MessageFields.STATUS not in response:
            raise RuntimeError(f"Progress registration response missing status: {response}")
        if response[MessageFields.STATUS] != ResponseType.OK.value:
            raise RuntimeError(f"Progress registration failed: {response}")
        self._progress_registered = True

    def enable_progress_stream(self) -> None:
        """Explicitly register and start progress streaming for this client."""
        if not self._connected and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        self._ensure_progress_subscription()

    def _record_progress(self, data: dict) -> None:
        execution_id = data[MessageFields.EXECUTION_ID]
        with self._progress_lock:
            self._progress_sequence_by_execution_id[execution_id] = (
                self._progress_sequence_by_execution_id.get(execution_id, 0) + 1
            )
        if self.progress_callback is not None:
            self.progress_callback(data)

    def _progress_sequence(self, execution_id: str) -> int | None:
        with self._progress_lock:
            return self._progress_sequence_by_execution_id.get(execution_id)

    @abstractmethod
    def serialize_task(
        self,
        task: TaskT,
        config: ConfigT | None,
    ) -> WireRequest:
        """Serialize task for transmission. Subclass provides serialization logic."""
        raise NotImplementedError
