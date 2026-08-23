"""Execution client with submit/poll/wait and progress streaming."""

from __future__ import annotations

import logging
import pickle
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import zmq

from zmqruntime.client import ZMQClient
from zmqruntime.execution.progress_stream import (
    ExecutionProgressObservation,
    ProgressStreamSubscriber,
)
from zmqruntime.execution.responses import (
    ExecutionSubmissionResponse,
    WireRequest,
    WireResponse,
)
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.messages import (
    ControlMessageType,
    ControlRequestHeader,
    MessageFields,
    PongResponse,
    ResponseType,
)
from zmqruntime.subscription import CallbackSubscription, SubscriptionABC
from zmqruntime.timeouts import OperationDeadline
from zmqruntime.transport import get_zmq_transport_url

logger = logging.getLogger(__name__)
TaskT = TypeVar("TaskT")
ConfigT = TypeVar("ConfigT")


class ExecutionClient(ZMQClient, ABC, Generic[TaskT, ConfigT]):
    """Execution client with progress streaming."""

    def __init__(
        self,
        port: int,
        host: str = "localhost",
        persistent: bool = True,
        progress_callback=None,
        transport_mode=None,
        config=None,
        connection_status_callback=None,
    ):
        super().__init__(
            port,
            host,
            persistent,
            transport_mode=transport_mode,
            config=config,
            connection_status_callback=connection_status_callback,
        )
        self.progress_callback = progress_callback
        self._progress_stream: ProgressStreamSubscriber | None = None
        self._progress_client_id = str(uuid.uuid4())
        self._progress_registration: SubscriptionABC | None = None
        self._progress_lock = threading.Lock()
        self._progress_by_execution_id: dict[
            str,
            ExecutionProgressObservation,
        ] = {}

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
        *,
        timeout_ms: int = 5000,
    ) -> WireResponse:
        if not self.is_connected() and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        self._ensure_progress_subscription(timeout_ms=timeout_ms)
        request = self.serialize_task(task, config)
        if MessageFields.TYPE not in request:
            request[MessageFields.TYPE] = ControlMessageType.EXECUTE.value
        response = self._send_control_request(request, timeout_ms=timeout_ms)
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
            {
                MessageFields.TYPE: ControlMessageType.CANCEL.value,
                MessageFields.EXECUTION_ID: execution_id,
            }
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
        if not self.is_connected() and not self.connect():
            raise RuntimeError("Not connected")
        response = self._send_control_request(
            ControlRequestHeader(ControlMessageType.PING).to_dict(),
            timeout_ms=1000,
        )
        if not isinstance(response, dict):
            raise TypeError(f"Expected ping response dict, got {type(response).__name__}")
        return PongResponse.from_dict(response)

    def _send_control_request(self, request, timeout_ms=5000):
        request_type = request.get(MessageFields.TYPE, "control")
        deadline = OperationDeadline.after_milliseconds(
            timeout_ms,
            operation=f"{request_type} control request",
        )
        owns_context = self.zmq_context is None
        ctx = zmq.Context() if owns_context else self.zmq_context
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.IMMEDIATE, 1)
        sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        control_url = get_zmq_transport_url(
            self.control_port,
            host=self.host,
            mode=self.transport_mode,
            config=self.config,
        )
        sock.connect(control_url)
        poller = zmq.Poller()
        try:
            poller.register(sock, zmq.POLLOUT)
            writable = dict(poller.poll(deadline.remaining_milliseconds()))
            if not writable.get(sock):
                raise TimeoutError(
                    f"Server was not writable for {request_type} request within {timeout_ms}ms"
                )
            sock.send(pickle.dumps(request), flags=zmq.NOBLOCK)
            poller.unregister(sock)
            poller.register(sock, zmq.POLLIN)
            readable = dict(poller.poll(deadline.remaining_milliseconds()))
            if not readable.get(sock):
                raise TimeoutError(
                    f"Server did not respond to {request_type} request within {timeout_ms}ms"
                )
            return pickle.loads(sock.recv(flags=zmq.NOBLOCK))
        except zmq.Again as exc:
            raise TimeoutError(
                f"Server did not complete {request_type} request within {timeout_ms}ms"
            ) from exc
        finally:
            try:
                poller.unregister(sock)
            except (KeyError, zmq.ZMQError):
                pass
            sock.close(linger=0)
            if owns_context:
                ctx.term()

    def disconnect(self):
        registration = self._progress_registration
        self._progress_registration = None
        if registration is not None:
            try:
                registration.release()
            except Exception as error:
                logger.debug("Progress unregistration failed during disconnect: %s", error)
        listener_error = None
        try:
            self._stop_progress_listener()
        except Exception as error:
            listener_error = error
        try:
            super().disconnect()
        except Exception as base_error:
            if listener_error is not None:
                raise base_error from listener_error
            raise
        if listener_error is not None:
            raise listener_error

    def _ensure_progress_subscription(self, *, timeout_ms: int = 5000) -> None:
        self._start_progress_listener()
        if self._progress_registration is not None:
            return
        response = self._send_control_request(
            {
                MessageFields.TYPE: ControlMessageType.REGISTER_PROGRESS.value,
                MessageFields.CLIENT_ID: self._progress_client_id,
            },
            timeout_ms=timeout_ms,
        )
        if MessageFields.STATUS not in response:
            raise RuntimeError(f"Progress registration response missing status: {response}")
        if response[MessageFields.STATUS] != ResponseType.OK.value:
            raise RuntimeError(f"Progress registration failed: {response}")
        self._progress_registration = CallbackSubscription(
            self._unregister_progress,
        )

    def _unregister_progress(self) -> bool:
        if not self.is_connected():
            return True
        self._send_control_request(
            {
                MessageFields.TYPE: ControlMessageType.UNREGISTER_PROGRESS.value,
                MessageFields.CLIENT_ID: self._progress_client_id,
            }
        )
        return True

    def enable_progress_stream(self) -> None:
        """Explicitly register and start progress streaming for this client."""
        if not self.is_connected() and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        self._ensure_progress_subscription()

    def _record_progress(self, data: dict) -> None:
        execution_id = data[MessageFields.EXECUTION_ID]
        with self._progress_lock:
            current = self._progress_by_execution_id.get(execution_id)
            observation = (
                ExecutionProgressObservation.first(data)
                if current is None
                else current.followed_by(data)
            )
            self._progress_by_execution_id[execution_id] = observation
        if self.progress_callback is not None:
            self.progress_callback(data)

    def progress_observation(
        self,
        execution_id: str,
    ) -> ExecutionProgressObservation | None:
        """Return the immutable latest progress observation for one execution."""

        with self._progress_lock:
            return self._progress_by_execution_id.get(execution_id)

    def _progress_sequence(self, execution_id: str) -> int | None:
        observation = self.progress_observation(execution_id)
        return None if observation is None else observation.sequence

    @abstractmethod
    def serialize_task(
        self,
        task: TaskT,
        config: ConfigT | None,
    ) -> WireRequest:
        """Serialize task for transmission. Subclass provides serialization logic."""
        raise NotImplementedError
