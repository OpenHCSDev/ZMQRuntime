"""Execution client with submit/poll/wait and progress streaming."""
from __future__ import annotations

import pickle
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any

import zmq

from zmqruntime.client import ZMQClient
from zmqruntime.execution.progress_stream import ProgressStreamSubscriber
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.messages import (
    ControlMessageType,
    MessageFields,
    PongResponse,
    ResponseType,
)
from zmqruntime.transport import get_zmq_transport_url

logger = logging.getLogger(__name__)


class ExecutionClient(ZMQClient, ABC):
    """Execution client with progress streaming."""

    def __init__(self, port: int, host: str = "localhost", persistent: bool = True,
                 progress_callback=None, transport_mode=None, config=None):
        super().__init__(port, host, persistent, transport_mode=transport_mode, config=config)
        self.progress_callback = progress_callback
        self._progress_stream: ProgressStreamSubscriber | None = None
        self._execution_waiter = ExecutionWaiter(self.poll_status)
        self._progress_client_id = str(uuid.uuid4())
        self._progress_registered = False

    def _start_progress_listener(self):
        if not self.progress_callback:
            logger.info("No progress callback, skipping listener")
            return
        if self._progress_stream is None:
            self._progress_stream = ProgressStreamSubscriber(
                socket_provider=lambda: self.data_socket,
                callback=self.progress_callback,
            )
        logger.info("Starting progress listener thread")
        self._progress_stream.start()

    def _stop_progress_listener(self):
        if self._progress_stream is None:
            return
        self._progress_stream.stop()

    def submit_execution(self, task: Any, config: Any = None):
        if not self._connected and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        self._ensure_progress_subscription()
        request = self.serialize_task(task, config)
        if MessageFields.TYPE not in request:
            request[MessageFields.TYPE] = ControlMessageType.EXECUTE.value
        response = self._send_control_request(request)
        return response

    def poll_status(self, execution_id: str | None = None):
        request = {MessageFields.TYPE: ControlMessageType.STATUS.value}
        if execution_id:
            request[MessageFields.EXECUTION_ID] = execution_id
        return self._send_control_request(request)

    def wait_for_completion(self, execution_id, poll_interval=0.5, max_consecutive_errors=5):
        logger.info("Waiting for execution %s to complete", execution_id)
        policy = WaitPolicy(
            poll_interval=poll_interval,
            max_consecutive_errors=max_consecutive_errors,
        )
        return self._execution_waiter.wait(execution_id, policy)

    def execute(self, task: Any, config: Any = None):
        response = self.submit_execution(task, config)
        if response.get(MessageFields.STATUS) == "accepted":
            execution_id = response.get(MessageFields.EXECUTION_ID)
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
        response_type = response.get(MessageFields.TYPE)
        if response_type != ResponseType.PONG.value:
            raise RuntimeError(
                f"Expected pong response, got type={response_type!r} payload={response}"
            )
        return PongResponse.from_dict(response)

    def get_server_info(self):
        """Backward-compatible dict view of typed ping snapshot."""
        return self.get_server_info_snapshot().to_dict()

    def _send_control_request(self, request, timeout_ms=5000):
        ctx = zmq.Context()
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
                f"Server did not respond to {request.get(MessageFields.TYPE)} request within {timeout_ms}ms"
            )
        finally:
            sock.close()
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
        if not self.progress_callback:
            return
        self._start_progress_listener()
        if self._progress_registered:
            return
        response = self._send_control_request(
            {
                MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
                MessageFields.CLIENT_ID: self._progress_client_id,
            }
        )
        if response.get(MessageFields.STATUS) != ResponseType.OK.value:
            raise RuntimeError(f"Progress registration failed: {response}")
        self._progress_registered = True

    def enable_progress_stream(self) -> None:
        """Explicitly register and start progress streaming for this client."""
        if not self._connected and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        self._ensure_progress_subscription()

    @abstractmethod
    def serialize_task(self, task: Any, config: Any) -> dict:
        """Serialize task for transmission. Subclass provides serialization logic."""
        raise NotImplementedError
