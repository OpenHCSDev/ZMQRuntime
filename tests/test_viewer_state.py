import pytest

from zmqruntime.viewer_state import ViewerStateManager


class RecordingVisualizer:
    def __init__(self, *, fail_start: bool = False) -> None:
        self.fail_start = fail_start
        self.stop_calls = 0
        self.force_stop_calls = 0
        self.ready_timeouts = []
        self.running = True

    def start(self):
        if self.fail_start:
            raise RuntimeError("start failed")
        return None

    def wait_for_ready(self, timeout: float) -> bool:
        self.ready_timeouts.append(timeout)
        return True

    def stop(self):
        self.stop_calls += 1

    def force_stop(self):
        self.force_stop_calls += 1
        self.running = False

    @property
    def is_running(self) -> bool:
        return self.running


@pytest.fixture
def viewer_manager():
    ViewerStateManager._instance = None
    manager = ViewerStateManager.get_instance()
    yield manager
    manager.stop_all_viewers()
    ViewerStateManager._instance = None


def test_failed_viewer_start_uses_force_stop(viewer_manager):
    visualizer = RecordingVisualizer(fail_start=True)

    with pytest.raises(RuntimeError, match="start failed"):
        viewer_manager.get_or_create_viewer(
            viewer_type="napari",
            port=5700,
            factory=lambda: visualizer,
        )

    assert visualizer.force_stop_calls == 1
    assert visualizer.stop_calls == 0


def test_stop_all_viewers_uses_force_stop(viewer_manager):
    visualizer = RecordingVisualizer()
    viewer_manager.get_or_create_viewer(
        viewer_type="napari",
        port=5700,
        factory=lambda: visualizer,
    )

    viewer_manager.stop_all_viewers()

    assert visualizer.force_stop_calls == 1
    assert visualizer.stop_calls == 0


def test_viewer_manager_delegates_the_full_readiness_timeout(viewer_manager):
    visualizer = RecordingVisualizer()

    viewer_manager.get_or_create_viewer(
        viewer_type="napari",
        port=5700,
        factory=lambda: visualizer,
        ready_timeout=12.5,
    )

    assert visualizer.ready_timeouts == [12.5]
