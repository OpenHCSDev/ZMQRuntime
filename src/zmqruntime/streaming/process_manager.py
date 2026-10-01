"""Process manager base class for visualizer subprocesses."""
from __future__ import annotations

import threading
from abc import ABC, abstractmethod

from zmqruntime.client import EndpointProcess


class VisualizerProcessManager(ABC):
    """Manages visualizer subprocess lifecycle."""

    def __init__(self, port: int | None = None):
        super().__init__()
        self.port = port
        self.process: EndpointProcess | None = None
        self._lock = threading.Lock()

    @abstractmethod
    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        """Wait until viewer is ready to receive streamed payloads."""
        raise NotImplementedError

    @abstractmethod
    def start(self, detached: bool = True) -> EndpointProcess:
        """Capture and retain the exact child through its launch authority."""
        raise NotImplementedError

    def stop(self, timeout: float = 5.0):
        """Stop the exact child without discarding its identity or exit evidence."""
        with self._lock:
            if self.process is not None:
                self.process.stop(timeout=timeout)

    def force_stop(self, timeout: float = 5.0):
        """Stop the visualizer subprocess regardless of viewer persistence policy."""
        self.stop(timeout=timeout)

    @property
    def is_running(self) -> bool:
        return self.process is not None and self.process.is_alive()
