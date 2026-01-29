"""Global acknowledgment listener for ZMQ visualizers."""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ImageAck
from zmqruntime.queue_tracker import GlobalQueueTrackerRegistry

from zmqruntime.viewer_state import ViewerStateManager
from zmqruntime.transport import get_default_transport_mode, get_zmq_transport_url

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
        if self._initialized:
            return
        self._callbacks: list[Callable[[ImageAck], None]] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._transport_mode: TransportMode | None = None
        self._config: ZMQConfig | None = None
        self._port: int | None = None
        self._host: str = "*"
        self._initialized = True
        self._register_default_callback()

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
        self._callbacks.append(callback)

    def start(
        self,
        port: int,
        transport_mode: TransportMode | None = None,
        host: str = "*",
        config: ZMQConfig | None = None,
    ) -> None:
        """Start listening on given port."""
        with self._lock:
            if self._running:
                logger.debug("Ack listener already running")
                return
            self._transport_mode = transport_mode or get_default_transport_mode()
            self._config = config or ZMQConfig()
            self._port = port
            self._host = host
            self._running = True
            self._thread = threading.Thread(
                target=self._listener_loop,
                daemon=True,
                name="AckListener",
            )
            self._thread.start()

    def stop(self) -> None:
        """Stop the listener."""
        with self._lock:
            if not self._running:
                return
            self._running = False

    def _listener_loop(self) -> None:
        context = zmq.Context()
        socket = None
        try:
            socket = context.socket(zmq.PULL)
            if self._port is None:
                raise RuntimeError("Ack listener port not set")
            ack_url = get_zmq_transport_url(
                self._port,
                host=self._host,
                mode=self._transport_mode,
                config=self._config,
            )
            socket.bind(ack_url)
            logger.info("Ack listener bound to %s", ack_url)

            while self._running:
                try:
                    if socket.poll(timeout=1000):
                        ack_dict = socket.recv_json()
                        try:
                            ack = ImageAck.from_dict(ack_dict)
                        except Exception as e:
                            logger.error("Failed to parse ack message: %s", e, exc_info=True)
                            continue
                        for callback in list(self._callbacks):
                            try:
                                callback(ack)
                            except Exception as e:
                                logger.error("Ack callback error: %s", e, exc_info=True)
                except zmq.ZMQError as e:
                    if self._running:
                        logger.error("ZMQ error in ack listener: %s", e)
                        time.sleep(0.1)
        except Exception as e:
            logger.error("Fatal error in ack listener: %s", e, exc_info=True)
        finally:
            if socket:
                try:
                    socket.close()
                except Exception:
                    pass
            try:
                context.term()
            except Exception:
                pass
            logger.info("Ack listener stopped")
