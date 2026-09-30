"""Global acknowledgment listener for ZMQ visualizers."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Callable, Optional

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ImageAck
from zmqruntime.queue_tracker import GlobalQueueTrackerRegistry
from zmqruntime.startup import EndpointStartupPhase, EndpointStartupStatus
from zmqruntime.timeouts import OperationCancellation, OperationDeadline
from zmqruntime.transport import TransportEndpoint, resolve_transport_mode
from zmqruntime.viewer_state import ViewerStateManager

logger = logging.getLogger(__name__)


class GlobalAckListener:
    """Singleton listener for acknowledgment messages from visualizers."""

    _instance: Optional["GlobalAckListener"] = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        with self._lock:
            if self._initialized:
                return
            self._condition = threading.Condition()
            self._callbacks: list[Callable[[ImageAck], None]] = []
            self._thread: Optional[threading.Thread] = None
            self._endpoint: TransportEndpoint | None = None
            self._config = ZMQConfig()
            self._cancellation = OperationCancellation()
            self._startup: Future[None] = Future()
            self._status = EndpointStartupStatus(
                EndpointStartupPhase.DISCONNECTED, "Ack listener stopped"
            )
            self._register_default_callback()
            self._initialized = True

    @property
    def startup_status(self) -> EndpointStartupStatus:
        """Return the listener-owned lifecycle, including terminal failure."""
        with self._condition:
            return self._status

    @property
    def _running(self) -> bool:
        """Derive readiness from the existing lifecycle declaration."""
        return self.startup_status.phase.accepts_requests

    def _set_status(self, phase: EndpointStartupPhase, message: str) -> None:
        with self._condition:
            self._status = EndpointStartupStatus(
                phase, message, self._status.sequence + 1, time.time()
            )

    def _register_default_callback(self) -> None:
        def _mark_processed(ack: ImageAck) -> None:
            tracker = GlobalQueueTrackerRegistry().get_tracker(ack.viewer_port)
            if tracker:
                tracker.mark_processed(ack.image_id)
                return

            # If no tracker exists, still notify ViewerStateManager
            ViewerStateManager.get_instance().increment_processed(ack.viewer_type, ack.viewer_port)
            ViewerStateManager.get_instance().update_queued_images(
                ack.viewer_type, ack.viewer_port, 0
            )

        self._callbacks.append(_mark_processed)

    def register_callback(self, callback: Callable[[ImageAck], None]) -> None:
        """Register callback for ack messages."""
        with self._condition:
            self._callbacks.append(callback)

    def start(
        self,
        port: int,
        transport_mode: TransportMode | None = None,
        host: str = "*",
        config: ZMQConfig | None = None,
        *,
        timeout_ms: int = 5000,
        operation_deadline: OperationDeadline | None = None,
    ) -> None:
        """Return only after bind succeeds, or propagate the actual failure.

        Concurrent callers for the same address share its startup outcome. An
        already-owned different address is rejected rather than silently reused.
        The finite wait uses the existing operation deadline/cancellation owners.
        """
        endpoint = TransportEndpoint(host, port, resolve_transport_mode(transport_mode))
        config = config or ZMQConfig()
        deadline = operation_deadline or OperationDeadline.after_milliseconds(
            timeout_ms, operation="ACK listener startup"
        )
        deadline.remaining_seconds()
        launch = None
        with self._condition:
            if self._thread is not None:
                assert self._endpoint is not None
                if self._endpoint.data_url(self._config) != endpoint.data_url(config):
                    raise ValueError("Ack listener already owns a different endpoint")
                if self._cancellation.requested():
                    raise RuntimeError("Ack listener is still stopping")
            else:
                self._endpoint = endpoint
                self._config = config
                self._cancellation = OperationCancellation()
                self._startup = Future()
                self._set_status(EndpointStartupPhase.BINDING_ENDPOINT, endpoint.data_url(config))
                try:
                    self._thread = threading.Thread(
                        target=self._listener_loop,
                        args=(endpoint, config, self._cancellation, self._startup),
                        daemon=True,
                        name="AckListener",
                    )
                except Exception as error:
                    self._set_status(EndpointStartupPhase.FAILED, str(error))
                    self._startup.set_exception(error)
                launch = self._thread
            startup = self._startup
            cancellation = self._cancellation
        # Neither thread launch nor outcome/cleanup waits hold the lifecycle lock.
        if launch is not None:
            try:
                launch.start()
            except Exception as error:
                with self._condition:
                    self._thread = None
                    self._set_status(EndpointStartupPhase.FAILED, str(error))
                    startup.set_exception(error)
        try:
            startup.result(timeout=deadline.remaining_seconds_or_zero())
        except FutureTimeoutError:
            if startup.done():
                raise
            if launch is not None:
                with self._condition:
                    if self._startup is startup:
                        cancellation.cancel()
                        self._set_status(
                            EndpointStartupPhase.FAILED, str(deadline.timeout_error())
                        )
            raise deadline.timeout_error() from None
        with self._condition:
            if self._startup is not startup or not self._running:
                raise RuntimeError(
                    f"Ack listener stopped during startup: {self._status.message}"
                )

    def stop(
        self,
        *,
        timeout_ms: int = 5000,
        operation_deadline: OperationDeadline | None = None,
    ) -> None:
        """Cancel and join only this listener; its thread closes its resources."""
        deadline = operation_deadline or OperationDeadline.after_milliseconds(
            timeout_ms, operation="ACK listener shutdown"
        )
        with self._condition:
            thread = self._thread
            if thread is None:
                return
            startup = self._startup
            self._cancellation.cancel()
            self._set_status(EndpointStartupPhase.DISCONNECTED, "Ack listener stopping")
        if thread is threading.current_thread():
            return
        # A concurrent stop may arrive before start() launches the stored thread.
        # Its startup outcome certifies launch or failure before join is legal.
        try:
            startup.exception(timeout=deadline.remaining_seconds_or_zero())
        except FutureTimeoutError:
            raise deadline.timeout_error() from None
        if thread.ident is None:
            return
        thread.join(timeout=deadline.remaining_seconds_or_zero())
        if thread.is_alive():
            raise deadline.timeout_error()

    def _listener_loop(
        self,
        endpoint: TransportEndpoint,
        config: ZMQConfig,
        cancellation: OperationCancellation,
        startup: Future[None],
    ) -> None:
        context = None
        socket = None
        failure = None
        try:
            if cancellation.requested():
                raise RuntimeError("Ack listener cancelled before startup")
            context = zmq.Context()
            socket = context.socket(zmq.PULL)
            socket.setsockopt(zmq.LINGER, 0)
            ack_url = endpoint.data_url(config)
            socket.bind(ack_url)
            with self._condition:
                if cancellation.requested():
                    raise RuntimeError("Ack listener cancelled before readiness")
                self._set_status(EndpointStartupPhase.CONNECTED, ack_url)
                startup.set_result(None)
            logger.info("Ack listener bound to %s", ack_url)

            while not cancellation.requested():
                try:
                    if socket.poll(timeout=1000):
                        ack_dict = socket.recv_json()
                        try:
                            ack = ImageAck.from_dict(ack_dict)
                        except Exception as e:
                            logger.error("Failed to parse ack message: %s", e, exc_info=True)
                            continue
                        with self._condition:
                            callbacks = tuple(self._callbacks)
                        for callback in callbacks:
                            try:
                                callback(ack)
                            except Exception as e:
                                logger.error("Ack callback error: %s", e, exc_info=True)
                except zmq.ZMQError:
                    if not cancellation.requested():
                        raise
        except Exception as e:
            failure = e
            self._set_status(EndpointStartupPhase.FAILED, str(e))
            logger.error("Fatal error in ack listener: %s", e, exc_info=True)
        finally:
            if socket is not None:
                try:
                    socket.close(linger=0)
                except Exception as error:
                    failure = failure or error
                    logger.error("Ack socket cleanup failed: %s", error, exc_info=True)
            if context is not None:
                try:
                    context.term()
                except Exception as error:
                    failure = failure or error
                    logger.error("Ack context cleanup failed: %s", error, exc_info=True)
            with self._condition:
                self._thread = None
                if failure is not None:
                    self._set_status(EndpointStartupPhase.FAILED, str(failure))
                else:
                    self._set_status(EndpointStartupPhase.DISCONNECTED, "Ack listener stopped")
                if not startup.done():
                    startup.set_exception(
                        failure or RuntimeError("Ack listener stopped before readiness")
                    )
            logger.info("Ack listener stopped")
