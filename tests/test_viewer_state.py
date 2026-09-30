import pytest

from zmqruntime.viewer_state import ViewerReuseAdmissionABC, ViewerStateManager


class RecordingVisualizer:
    def __init__(self, *, fail_start: bool = False) -> None:
        self.fail_start = fail_start
        self.stop_calls = 0
        self.force_stop_calls = 0
        self.ready_timeouts = []
        self.running = False
        self.start_calls = 0

    def start(self):
        self.start_calls += 1
        if self.fail_start:
            raise RuntimeError("start failed")
        self.running = True
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


def test_viewer_state_subscription_owns_idempotent_release(viewer_manager):
    observed = []
    subscription = viewer_manager.subscribe_state(observed.append)

    viewer_manager.get_or_create_viewer(
        viewer_type="napari",
        port=5700,
        factory=RecordingVisualizer,
    )
    assert subscription.release() is True
    assert subscription.release() is False
    viewer_manager.release_viewer("napari", 5700)

    assert len(observed) == 2


def test_reuse_admission_runs_atomically_and_preserves_rejected_viewer(viewer_manager):
    import threading

    existing = RecordingVisualizer()
    viewer_manager.get_or_create_viewer("napari", 5700, lambda: existing)
    observations = []

    class RejectReuse(ViewerReuseAdmissionABC):
        def require_reusable(self, visualizer):
            # A second thread cannot acquire the owner's lock during admission.
            def probe():
                acquired = viewer_manager._lock.acquire(blocking=False)
                observations.append(acquired)
                if acquired:
                    viewer_manager._lock.release()

            thread = threading.Thread(target=probe)
            thread.start()
            thread.join(timeout=1)
            assert not thread.is_alive()
            assert visualizer is existing
            raise ValueError("incompatible launch request")

    def forbidden_factory():
        raise AssertionError("rejected reuse must not construct or restart")

    with pytest.raises(ValueError, match="incompatible launch request"):
        viewer_manager.get_or_create_viewer(
            "napari", 5700, forbidden_factory, reuse_admission=RejectReuse()
        )
    assert observations == [False]
    assert viewer_manager.get_viewer("napari", 5700) is existing
    assert existing.force_stop_calls == 0
    assert existing.start_calls == 1


def test_live_external_factory_is_registered_without_start(viewer_manager):
    existing = RecordingVisualizer(fail_start=True)
    existing.running = True
    acquired, created = viewer_manager.get_or_create_viewer(
        "fiji", 5701, lambda: existing
    )
    assert acquired is existing and created is True
    assert viewer_manager.get_viewer("fiji", 5701) is existing
    assert existing.start_calls == 0
