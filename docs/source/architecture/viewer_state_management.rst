Viewer State Management
=======================

Overview
--------

``ViewerStateManager`` provides a unified, thread-safe interface for managing
viewer lifecycle across the entire application. It replaces fragmented
viewer tracking in:

- ``_launching_viewers`` global dict in zmq_server_manager.py
- ``_visualizers`` dict in orchestrator.py

This is the **SINGLE SOURCE OF TRUTH** for viewer state.

Core Concepts
~~~~~~~~~~~~

**ViewerState Enum**
   Describes the lifecycle state of a viewer:

   - ``LAUNCHING``: Process spawning, not yet responding to pings
   - ``READY``: Responding to control port, ready for data
   - ``PROCESSING``: Currently receiving/processing images
   - ``ERROR``: Failed to start or crashed
   - ``STOPPED``: Gracefully stopped

**ViewerInstance Dataclass**
   Complete state snapshot for a managed viewer:

   .. code-block:: python

       @dataclass
       class ViewerInstance:
           viewer_type: str  # 'napari', 'fiji', etc.
           port: int  # Port number
           visualizer: VisualizerProcessManager
           state: ViewerState
           created_at: float  # Unix timestamp
           last_used: float  # Unix timestamp
           queued_images: int  # Number of queued images
           processed_images: int  # Number of processed images
           error_message: Optional[str]  # Error details if state is ERROR
           metadata: Dict[str, Any]  # Additional metadata

**ViewerStateManager Singleton**
   Thread-safe singleton that:

   - Provides atomic ``get_or_create_viewer()`` to prevent race conditions
   - Maintains internal registry of all active viewers
   - Supports observer pattern via state change callbacks
   - Updates queued/processed counts for UI integration

Basic Usage
~~~~~~~~~~~~

**Atomic Viewer Acquisition**
   Use ``get_or_create_viewer()`` to get or create a viewer:

   .. code-block:: python

       from zmqruntime import get_or_create_viewer

       viewer, was_created = get_or_create_viewer(
           viewer_type="napari",
           port=5555,
           factory=lambda: NapariStreamVisualizer(...),
           wait_for_ready=True,
           ready_timeout=30.0,
       )

   This is **thread-safe** and prevents multiple threads from spawning
   duplicate viewers for the same port.

**Using the Manager Directly**
   For advanced use cases, access the manager directly:

   .. code-block:: python

       from zmqruntime import ViewerStateManager

       manager = ViewerStateManager.get_instance()

       # Get a viewer
       visualizer = manager.get_viewer("napari", 5555)

       # Create a new viewer (atomic)
       visualizer, created = manager.get_or_create_viewer(
           "napari", 5555, factory, wait_for_ready=True
       )

       # Update queued count
       manager.update_queued_images("napari", 5555, count=10)

       # Increment processed count
       manager.increment_processed("napari", 5555)

       # Release a viewer
       manager.release_viewer("napari", 5555, stop=True)

**State Change Callbacks**
   Register callbacks to be notified of viewer state changes:

   .. code-block:: python

       from zmqruntime import ViewerStateManager

       manager = ViewerStateManager.get_instance()

       def on_viewer_state_change(instance: ViewerInstance):
           print(f"Viewer {instance.viewer_type}:{instance.port} state changed to {instance.state}")

       manager.register_state_callback(on_viewer_state_change)

Thread Safety
~~~~~~~~~~~~

All ``ViewerStateManager`` operations are thread-safe:

- ``get_or_create_viewer()``: Uses lock to ensure atomic get-or-create
- ``list_viewers()``: Returns a snapshot (copy) of current viewers
- ``update_queued_images()`` / ``increment_processed()``: Lock-protected updates
- Singleton initialization: Double-checked locking pattern

Integration with Queue Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ViewerStateManager`` integrates with ``QueueTracker`` and ``GlobalAckListener``:

- ``QueueTracker`` calls ``update_queued_images()`` on ``register_sent()``
- ``QueueTracker`` calls ``increment_processed()`` on ``mark_processed()``
- ``GlobalAckListener`` calls ``increment_processed()`` when no tracker exists
- This ensures queued/processed counts are always accurate in the UI

Migration from Legacy Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Before:**
   - ``_launching_viewers`` global dict in zmq_server_manager.py
   - ``_visualizers`` dict in orchestrator.py
   - Manual registration/unregistration functions
   - No centralized state or observer pattern

**After:**
   - ``ViewerStateManager`` singleton as single source of truth
   - Atomic get-or-create prevents race conditions
   - Observer pattern for UI integration
   - Consistent queued/processed tracking

See Also
~~~~~~~~

- :ref:`viewer_streaming_architecture`: Overview of streaming servers
- :ref:`image_acknowledgment_system`: Queue tracking and acknowledgments
- API Reference: ``ViewerStateManager``, ``ViewerState``, ``ViewerInstance``
