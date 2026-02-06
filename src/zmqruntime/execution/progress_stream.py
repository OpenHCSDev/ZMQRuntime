"""Background subscriber for execution progress stream."""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Callable

import zmq

from zmqruntime.messages import validate_progress_payload

logger = logging.getLogger(__name__)


class ProgressStreamSubscriber:
    """Owns progress listener thread lifecycle and callback dispatch."""

    def __init__(
        self,
        socket_provider: Callable[[], Any],
        callback: Callable[[dict], None],
    ) -> None:
        self._socket_provider = socket_provider
        self._callback = callback
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None

    def _listen_loop(self) -> None:
        logger.info("Progress listener loop started")
        message_count = 0
        while not self._stop_event.is_set():
            socket = self._socket_provider()
            if socket is None:
                time.sleep(0.1)
                continue
            try:
                message = socket.recv_string(zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.05)
                continue

            data = json.loads(message)
            validate_progress_payload(data)
            message_count += 1
            try:
                self._callback(data)
            except Exception as error:
                logger.exception("Progress callback raised exception: %s", error)
                raise

        logger.info(
            "Progress listener loop exited (received %s messages total)",
            message_count,
        )
