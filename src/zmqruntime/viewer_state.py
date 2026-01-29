"""Centralized viewer state management for ZMQ-based visualizers.

Provides a unified, thread-safe interface for managing viewer lifecycle across
the entire application. Replaces fragmented tracking in:
- _launching_viewers (zmq_server_manager.py)
- _visualizers (orchestrator.py)

This is the SINGLE SOURCE OF TRUTH for viewer state.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol, Tuple

if TYPE_CHECKING:
    from zmqruntime.streaming.process_manager import VisualizerProcessManager

logger = logging.getLogger(__name__)


class ViewerState(Enum):
    """Lifecycle states for a viewer."""

    LAUNCHING = auto()  # Process spawning, not yet responding to pings
    READY = auto()  # Responding to control port, ready for data
    PROCESSING = auto()  # Currently receiving/processing images
    ERROR = auto()  # Failed to start or crashed
    STOPPED = auto()  # Gracefully stopped


@dataclass
class ViewerInstance:
    """Complete state for a managed viewer instance."""

    viewer_type: str
    port: int
    visualizer: "VisualizerProcessManager"
    state: ViewerState = ViewerState.LAUNCHING
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    queued_images: int = 0
    processed_images: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_healthy(self) -> bool:
        """Check if viewer is in a usable state."""
        if self.state in (ViewerState.ERROR, ViewerState.STOPPED):
            return False

        # Check if process is still running
        if hasattr(self.visualizer, "is_running"):
            return self.visualizer.is_running

        return True


class VisualizerFactory(Protocol):
    """Protocol for factory functions that create visualizers."""

    def __call__(self) -> "VisualizerProcessManager": ...


class ViewerStateManager:
    """
    Singleton manager for viewer lifecycle state.

    Thread-safe, atomic get-or-create operations prevent duplicate spawning.
    Provides both synchronous and observer-based APIs for UI integration.

    This replaces:
    - _launching_viewers global dict in zmq_server_manager.py
    - _visualizers dict in orchestrator.py
    """

    _instance: Optional["ViewerStateManager"] = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "ViewerStateManager":
        """Prevent direct instantiation - use get_instance()."""
        raise RuntimeError(
            "ViewerStateManager is a singleton. Use ViewerStateManager.get_instance()"
        )

    @classmethod
    def get_instance(cls) -> "ViewerStateManager":
        """Get or create the singleton instance."""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    # Bypass __new__ check for singleton creation
                    instance = object.__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
                    # Ensure __init__ runs to initialize internal structures
                    # (get_instance may be called from many places; calling
                    # __init__ here guarantees the singleton is ready.)
                    try:
                        instance.__init__()
                    except Exception:
                        # If initialization fails, clear the instance so future
                        # calls can retry and propagate the original error.
                        cls._instance = None
                        raise
        return cls._instance

    def __init__(self) -> None:
        """Initialize the manager (called once via get_instance)."""
        if getattr(self, "_initialized", False):
            return

        self._viewers: Dict[Tuple[str, int], ViewerInstance] = {}
        self._lock = threading.RLock()
        self._state_callbacks: List[Callable[[ViewerInstance], None]] = []
        self._initialized = True

        logger.info("ViewerStateManager initialized")

    def get_or_create_viewer(
        self,
        viewer_type: str,
        port: int,
        factory: VisualizerFactory,
        wait_for_ready: bool = True,
        ready_timeout: float = 30.0,
    ) -> Tuple["VisualizerProcessManager", bool]:
        """
        Atomically get existing viewer or create a new one.

        This is THE method to use for viewer acquisition. It prevents the
        race condition where multiple threads spawn duplicate viewers.

        Args:
            viewer_type: Type of viewer ('napari', 'fiji', etc.)
            port: Port number for the viewer
            factory: Callable that creates a VisualizerProcessManager
            wait_for_ready: If True, block until viewer is ready (or timeout)
            ready_timeout: Maximum seconds to wait for ready state

        Returns:
            Tuple of (visualizer_instance, was_created)
            - was_created=True: This call created the viewer
            - was_created=False: Returned existing viewer

        Raises:
            RuntimeError: If viewer creation or startup fails
        """
        key = (viewer_type, port)

        with self._lock:
            existing = self._viewers.get(key)

            if existing is not None:
                if existing.is_healthy:
                    existing.last_used = time.time()
                    logger.debug(
                        "ViewerStateManager: Reusing existing %s viewer on port %d (state=%s)",
                        viewer_type,
                        port,
                        existing.state.name,
                    )
                    return existing.visualizer, False
                else:
                    # Remove dead/stale entry
                    logger.warning(
                        "ViewerStateManager: Removing stale %s viewer on port %d (state=%s)",
                        viewer_type,
                        port,
                        existing.state.name,
                    )
                    self._remove_viewer_locked(key)

            # Create new viewer (still holding lock to prevent races)
            logger.info("ViewerStateManager: Creating new %s viewer on port %d", viewer_type, port)

            try:
                visualizer = factory()
            except Exception as e:
                logger.exception(
                    "ViewerStateManager: Factory failed for %s viewer on port %d",
                    viewer_type,
                    port,
                )
                raise RuntimeError(
                    f"Failed to create {viewer_type} viewer on port {port}: {e}"
                ) from e

            # Register immediately (before starting) so other threads see it
            instance = ViewerInstance(
                viewer_type=viewer_type,
                port=port,
                visualizer=visualizer,
                state=ViewerState.LAUNCHING,
            )
            self._viewers[key] = instance
            self._notify_state_change(instance)

        # Release lock before potentially blocking operations
        try:
            self._start_and_wait(instance, wait_for_ready, ready_timeout)
        except Exception:
            # Clean up on failure
            with self._lock:
                self._remove_viewer_locked(key)
            raise

        return visualizer, True

    def _start_and_wait(
        self,
        instance: ViewerInstance,
        wait_for_ready: bool,
        timeout: float,
    ) -> None:
        """Start the viewer process and optionally wait for ready state."""
        try:
            # Start the process
            instance.visualizer.start()
            logger.info(
                "ViewerStateManager: Started %s viewer on port %d",
                instance.viewer_type,
                instance.port,
            )

            if wait_for_ready and hasattr(instance.visualizer, "wait_for_ready"):
                self._wait_for_viewer_ready(instance, timeout)
            else:
                # Assume ready immediately if no wait_for_ready method
                instance.state = ViewerState.READY
                self._notify_state_change(instance)

        except Exception as e:
            instance.state = ViewerState.ERROR
            instance.error_message = str(e)
            self._notify_state_change(instance)
            raise

    def _wait_for_viewer_ready(self, instance: ViewerInstance, timeout: float) -> None:
        """Wait for viewer to become ready, updating state along the way."""
        start_time = time.time()
        check_interval = 0.5

        while time.time() - start_time < timeout:
            if instance.visualizer.wait_for_ready(timeout=check_interval):
                instance.state = ViewerState.READY
                self._notify_state_change(instance)
                logger.info(
                    "ViewerStateManager: %s viewer on port %d is ready",
                    instance.viewer_type,
                    instance.port,
                )
                return

            # Check if process died
            if hasattr(instance.visualizer, "is_running") and not instance.visualizer.is_running:
                raise RuntimeError(
                    f"{instance.viewer_type} viewer on port {instance.port} "
                    "process terminated unexpectedly during startup"
                )

        raise TimeoutError(
            f"Timeout waiting for {instance.viewer_type} viewer on port {instance.port} "
            f"after {timeout}s"
        )

    def _remove_viewer_locked(self, key: Tuple[str, int]) -> None:
        """Remove a viewer from tracking (must hold lock)."""
        instance = self._viewers.pop(key, None)
        if instance:
            instance.state = ViewerState.STOPPED
            self._notify_state_change(instance)

            # Try to stop the process
            if hasattr(instance.visualizer, "stop"):
                try:
                    instance.visualizer.stop()
                except Exception as e:
                    logger.warning("Error stopping viewer during removal: %s", e)

    def get_viewer(self, viewer_type: str, port: int) -> Optional["VisualizerProcessManager"]:
        """Get existing viewer if healthy."""
        key = (viewer_type, port)
        with self._lock:
            instance = self._viewers.get(key)
            if instance and instance.is_healthy:
                instance.last_used = time.time()
                return instance.visualizer
            return None

    def get_viewer_state(self, viewer_type: str, port: int) -> Optional[ViewerState]:
        """Get current state of a viewer."""
        key = (viewer_type, port)
        with self._lock:
            instance = self._viewers.get(key)
            return instance.state if instance else None

    def update_queued_images(self, viewer_type: str, port: int, count: int) -> None:
        """Update the queued image count for a viewer."""
        key = (viewer_type, port)
        with self._lock:
            instance = self._viewers.get(key)
            if instance:
                instance.queued_images = count
                if count > 0 and instance.state == ViewerState.READY:
                    instance.state = ViewerState.PROCESSING
                elif count == 0 and instance.state == ViewerState.PROCESSING:
                    instance.state = ViewerState.READY
                self._notify_state_change(instance)

    def increment_processed(self, viewer_type: str, port: int) -> None:
        """Increment processed image count for a viewer."""
        key = (viewer_type, port)
        with self._lock:
            instance = self._viewers.get(key)
            if instance:
                instance.processed_images += 1
                self._notify_state_change(instance)

    def release_viewer(self, viewer_type: str, port: int, stop: bool = False) -> bool:
        """
        Release a viewer from management.

        Args:
            viewer_type: Type of viewer
            port: Port number
            stop: If True, stop the process. If False, just untrack it.

        Returns:
            True if viewer was found and released
        """
        key = (viewer_type, port)
        with self._lock:
            instance = self._viewers.pop(key, None)
            if not instance:
                return False

            instance.state = ViewerState.STOPPED if stop else ViewerState.READY
            self._notify_state_change(instance)

            if stop:
                if hasattr(instance.visualizer, "stop"):
                    try:
                        instance.visualizer.stop()
                        logger.info(
                            "ViewerStateManager: Stopped %s viewer on port %d",
                            viewer_type,
                            port,
                        )
                    except Exception as e:
                        logger.warning("Error stopping viewer: %s", e)
            else:
                logger.info(
                    "ViewerStateManager: Released %s viewer on port %d (still running)",
                    viewer_type,
                    port,
                )

            return True

    def stop_all_viewers(self) -> None:
        """Stop all managed viewers and clear the pool."""
        with self._lock:
            viewers_to_stop = list(self._viewers.items())
            self._viewers.clear()

        for key, instance in viewers_to_stop:
            try:
                if hasattr(instance.visualizer, "stop"):
                    instance.visualizer.stop()
                logger.info(
                    "ViewerStateManager: Stopped %s viewer on port %d",
                    instance.viewer_type,
                    instance.port,
                )
            except Exception as e:
                logger.warning(
                    "ViewerStateManager: Error stopping %s viewer on port %d: %s",
                    instance.viewer_type,
                    instance.port,
                    e,
                )

    def list_viewers(self) -> List[ViewerInstance]:
        """Get list of all managed viewers (copy)."""
        with self._lock:
            return [
                ViewerInstance(
                    viewer_type=v.viewer_type,
                    port=v.port,
                    visualizer=v.visualizer,
                    state=v.state,
                    created_at=v.created_at,
                    last_used=v.last_used,
                    queued_images=v.queued_images,
                    processed_images=v.processed_images,
                    error_message=v.error_message,
                    metadata=dict(v.metadata),
                )
                for v in self._viewers.values()
            ]

    def register_state_callback(self, callback: Callable[[ViewerInstance], None]) -> None:
        """Register a callback for state change notifications."""
        self._state_callbacks.append(callback)

    def unregister_state_callback(self, callback: Callable[[ViewerInstance], None]) -> None:
        """Unregister a state change callback."""
        if callback in self._state_callbacks:
            self._state_callbacks.remove(callback)

    def _notify_state_change(self, instance: ViewerInstance) -> None:
        """Notify all registered callbacks of a state change."""
        for callback in self._state_callbacks:
            try:
                callback(instance)
            except Exception as e:
                logger.warning("State callback failed: %s", e)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about managed viewers."""
        with self._lock:
            instances = list(self._viewers.values())

        state_counts = {}
        for instance in instances:
            state_counts[instance.state.name] = state_counts.get(instance.state.name, 0) + 1

        return {
            "total_viewers": len(instances),
            "by_state": state_counts,
            "viewers": [
                {
                    "type": i.viewer_type,
                    "port": i.port,
                    "state": i.state.name,
                    "queued_images": i.queued_images,
                    "processed_images": i.processed_images,
                    "created_at": i.created_at,
                    "last_used": i.last_used,
                    "is_healthy": i.is_healthy,
                }
                for i in instances
            ],
        }

    def cleanup_stale_viewers(self, max_idle_seconds: float = 3600) -> int:
        """Remove viewers that haven't been used recently."""
        now = time.time()
        to_remove = []

        with self._lock:
            for key, instance in list(self._viewers.items()):
                if now - instance.last_used > max_idle_seconds:
                    to_remove.append(key)

            for key in to_remove:
                self._remove_viewer_locked(key)

        return len(to_remove)


# Convenience function for common use case
def get_or_create_viewer(
    viewer_type: str,
    port: int,
    factory: VisualizerFactory,
    wait_for_ready: bool = True,
    ready_timeout: float = 30.0,
) -> Tuple["VisualizerProcessManager", bool]:
    """
    Convenience function for atomic viewer get-or-create.

    Example:
        viewer, created = get_or_create_viewer(
            viewer_type="napari",
            port=5555,
            factory=lambda: NapariStreamVisualizer(...),
        )
    """
    manager = ViewerStateManager.get_instance()
    return manager.get_or_create_viewer(viewer_type, port, factory, wait_for_ready, ready_timeout)
