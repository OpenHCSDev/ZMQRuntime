API Reference
=============

zmqruntime exposes a small, focused surface area for ZMQ-based execution
and streaming. Key classes live in the public package namespace.

Core Types
----------

**Configuration**

.. code-block:: python

    from zmqruntime import ZMQConfig, TransportMode

**Execution**

.. code-block:: python

    from zmqruntime.execution import ExecutionClient, ExecutionServer

**Streaming**

.. code-block:: python

    from zmqruntime.streaming import StreamingVisualizerServer, VisualizerProcessManager

**Viewer State Management**

.. code-block:: python

    from zmqruntime import (
        ViewerState,
        ViewerStateManager,
        ViewerInstance,
        get_or_create_viewer,
    )

**Low-level sockets**

.. code-block:: python

    from zmqruntime import ZMQClient, ZMQServer

Refer to the architecture docs for usage patterns and lifecycle details.
