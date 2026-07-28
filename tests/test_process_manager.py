from __future__ import annotations

import subprocess

import pytest

from zmqruntime.streaming.process_manager import VisualizerProcessManager


class ConcreteVisualizerProcessManager(VisualizerProcessManager):
    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        del timeout
        return True

    def start(self, detached: bool = True) -> subprocess.Popen:
        del detached
        raise RuntimeError("test launch authority")


class MissingStartVisualizerProcessManager(VisualizerProcessManager):
    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        del timeout
        return True


def test_visualizer_process_manager_requires_concrete_launch_authority() -> None:
    manager = ConcreteVisualizerProcessManager()

    with pytest.raises(RuntimeError, match="test launch authority"):
        manager.start()

    with pytest.raises(TypeError, match="abstract method 'start'"):
        MissingStartVisualizerProcessManager()


def test_visualizer_process_manager_has_no_parallel_command_or_environment_hooks() -> None:
    assert not hasattr(VisualizerProcessManager, "get_launch_command")
    assert not hasattr(VisualizerProcessManager, "get_launch_env")
