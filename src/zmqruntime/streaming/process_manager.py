"""Process manager base class for visualizer subprocesses."""
from __future__ import annotations

import subprocess
import threading
from abc import ABC, abstractmethod
from typing import List


class VisualizerProcessManager(ABC):
    """Manages visualizer subprocess lifecycle."""

    def __init__(self, port: int | None = None):
        self.port = port
        self.process: subprocess.Popen | None = None
        self._lock = threading.Lock()

    @abstractmethod
    def get_launch_command(self) -> List[str]:
        """Get command to launch visualizer. Implementation provides command."""
        raise NotImplementedError

    @abstractmethod
    def get_launch_env(self) -> dict:
        """Get environment variables for subprocess."""
        raise NotImplementedError

    @abstractmethod
    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        """Wait until viewer is ready to receive streamed payloads."""
        raise NotImplementedError

    def start(self, detached: bool = True):
        """Start the visualizer subprocess."""
        with self._lock:
            if self.process and self.is_running:
                return self.process
            cmd = self.get_launch_command()
            env = self.get_launch_env()
            self.process = subprocess.Popen(
                cmd,
                env=env or None,
                start_new_session=detached,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            return self.process

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

    @property
    def is_running(self) -> bool:
        if not self.process:
            return False
        return self.process.poll() is None
