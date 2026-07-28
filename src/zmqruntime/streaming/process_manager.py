"""Process manager base class for visualizer subprocesses."""
from __future__ import annotations

import subprocess
import threading
from abc import ABC, abstractmethod


class VisualizerProcessManager(ABC):
    """Manages visualizer subprocess lifecycle."""

    def __init__(self, port: int | None = None):
        self.port = port
        self.process: subprocess.Popen | None = None
        self._lock = threading.Lock()

    @abstractmethod
    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        """Wait until viewer is ready to receive streamed payloads."""
        raise NotImplementedError

    @abstractmethod
    def start(self, detached: bool = True) -> subprocess.Popen:
        """Start the concrete visualizer through its launch authority."""
        raise NotImplementedError

    def stop(self, timeout: float = 5.0):
        """Stop the visualizer subprocess."""
        with self._lock:
            if not self.process:
                return
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.process.kill()
            self.process = None

    def force_stop(self, timeout: float = 5.0):
        """Stop the visualizer subprocess regardless of viewer persistence policy."""
        self.stop(timeout=timeout)

    @property
    def is_running(self) -> bool:
        if not self.process:
            return False
        return self.process.poll() is None
