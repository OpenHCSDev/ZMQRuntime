"""Viewer lifecycles retain the original exact child, not a raw-handle mirror."""

import pytest

from zmqruntime.client import EndpointProcess
from zmqruntime.messages import ProcessExit, ProcessIdentity
from zmqruntime.streaming.process_manager import VisualizerProcessManager
from zmqruntime.viewer_state import ViewerStateManager


class DeclaredChild(EndpointProcess):
    identity = ProcessIdentity(123, 456.0)

    def __init__(self):
        self.alive = True
        self.terminal = None
        self.stop_calls = []

    def is_alive(self):
        return self.alive

    def exit(self):
        return self.terminal

    def wait_for_exit(self, timeout):
        return self.terminal

    def stop(self, timeout=5.0, kill_timeout=2.0):
        self.stop_calls.append((timeout, kill_timeout))
        if self.alive:
            self.alive = False
            self.terminal = ProcessExit(-15)
        return False


class UnfinishedChild(DeclaredChild):
    def stop(self, timeout=5.0, kill_timeout=2.0):
        self.stop_calls.append((timeout, kill_timeout))
        raise TimeoutError("controlled exact-child stop timeout")


class DeclaredViewer(VisualizerProcessManager):
    def __init__(self, child):
        super().__init__(port=5999)
        self.child_to_launch = child
        self.ready_observations = []

    def start(self, detached=True):
        self.process = self.child_to_launch
        return self.process

    def wait_for_ready(self, timeout=10.0):
        self.ready_observations.append(timeout)
        return False


class StopAudit(VisualizerProcessManager):
    """Independent observation capability; never owns another stop algorithm."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_events = []

    def stop(self, timeout=5.0):
        self.stop_events.append("before")
        try:
            return super().stop(timeout=timeout)
        finally:
            self.stop_events.append("after")


class AuditBeforeViewer(StopAudit, DeclaredViewer):
    pass


class AuditAfterViewer(DeclaredViewer, StopAudit):
    pass


VIEWERS = (AuditBeforeViewer, AuditAfterViewer)


@pytest.mark.parametrize("viewer_type", VIEWERS)
def test_stop_delegates_once_and_retains_exact_exit(viewer_type):
    child = DeclaredChild()
    viewer = viewer_type(child)
    assert viewer.start() is child
    assert viewer.is_running
    viewer.force_stop(timeout=1.25)
    assert child.stop_calls == [(1.25, 2.0)]
    assert viewer.stop_events == ["before", "after"]
    assert viewer.process is child
    assert viewer.process.identity == ProcessIdentity(123, 456.0)
    assert viewer.process.exit() == ProcessExit(-15)
    assert not viewer.is_running


@pytest.mark.parametrize("viewer_type", VIEWERS)
def test_readiness_observation_timeout_preserves_active_handle(viewer_type):
    child = DeclaredChild()
    viewer = viewer_type(child)
    viewer.start()
    assert not viewer.wait_for_ready(timeout=30)
    assert viewer.ready_observations == [30]
    assert viewer.process is child and viewer.is_running
    assert child.wait_for_exit(timeout=10) is None
    assert child.stop_calls == viewer.stop_events == []


@pytest.mark.parametrize("viewer_type", VIEWERS)
def test_failed_acquisition_cleanup_retains_child_after_manager_removal(viewer_type):
    ViewerStateManager._instance = None
    manager = ViewerStateManager.get_instance()
    child = DeclaredChild()
    viewer = viewer_type(child)
    try:
        with pytest.raises(TimeoutError, match="after 30s"):
            manager.get_or_create_viewer("declaration-only", 5999, lambda: viewer,
                                         ready_timeout=30)
        assert manager.get_viewer_state("declaration-only", 5999) is None
        assert viewer.process is child
        assert child.identity == ProcessIdentity(123, 456.0)
        assert child.exit() == ProcessExit(-15)
        assert child.stop_calls == [(5.0, 2.0)]
        assert viewer.stop_events == ["before", "after"]
    finally:
        manager.stop_all_viewers()
        ViewerStateManager._instance = None


@pytest.mark.parametrize("viewer_type", VIEWERS)
def test_unfinished_stop_does_not_discard_or_replace_child(viewer_type):
    child = UnfinishedChild()
    viewer = viewer_type(child)
    viewer.start()
    with pytest.raises(TimeoutError, match="controlled exact-child stop timeout"):
        viewer.force_stop(timeout=0.25)
    assert viewer.process is child and viewer.is_running
    assert child.stop_calls == [(0.25, 2.0)]
    assert child.exit() is None
    assert viewer.stop_events == ["before", "after"]


@pytest.mark.parametrize("viewer_type", VIEWERS)
def test_preexisting_terminal_status_survives_cleanup(viewer_type):
    child = DeclaredChild()
    child.alive = False
    child.terminal = ProcessExit(7)
    viewer = viewer_type(child)
    viewer.start()
    viewer.stop()
    assert viewer.process is child
    assert viewer.process.exit() == ProcessExit(7)
    assert not viewer.is_running


@pytest.mark.parametrize("viewer_type", VIEWERS)
def test_unstarted_viewer_never_fabricates_an_incarnation(viewer_type):
    child = DeclaredChild()
    viewer = viewer_type(child)
    viewer.force_stop()
    assert viewer.process is None
    assert not viewer.is_running
    assert child.stop_calls == []


def test_shared_initialization_reaches_independent_downstream_capability():
    class InitializedCapability:
        def __init__(self):
            super().__init__()
            self.capability_initialized = True

    class AnotherDeclaredViewer(DeclaredViewer, InitializedCapability):
        pass

    viewer = AnotherDeclaredViewer(DeclaredChild())
    assert viewer.capability_initialized
    assert viewer.process is None
    assert viewer.start() is viewer.child_to_launch
