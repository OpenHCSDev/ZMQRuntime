Viewer state management
=======================

``ViewerStateManager`` is the process-wide authority for viewer instances keyed
by ``(viewer_type, port)``. Its atomic ``get_or_create_viewer`` operation avoids
duplicate launches from concurrent callers.

Lifecycle
---------

``ViewerInstance`` records the visualizer, lifecycle state, timestamps, queue
counters, error details, and application metadata. ``ViewerState`` distinguishes
launching, ready, processing, error, and stopped instances.

The manager:

- constructs a visualizer through an application-supplied factory;
- starts it and optionally waits for readiness;
- removes unhealthy entries before replacement;
- exposes snapshots and state-change callbacks;
- coordinates queue and processed-image counters;
- stops a visualizer when its registration is released.

Usage
-----

.. code-block:: python

   from zmqruntime import get_or_create_viewer

   visualizer, created = get_or_create_viewer(
       viewer_type="example",
       port=5555,
       factory=create_visualizer,
       wait_for_ready=True,
       ready_timeout=30.0,
   )

The factory must return a ``VisualizerProcessManager`` implementation. Viewer
types, executable entry points, payload semantics, and presentation policy
belong to the host application.

Thread safety
-------------

Registry access and state updates are protected by the manager's reentrant
lock. Potentially blocking process startup/readiness work happens outside the
registry lock after the launching instance has been registered.

See :doc:`viewer_streaming_architecture` and
:doc:`image_acknowledgment_system`.
