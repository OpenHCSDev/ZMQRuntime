ZMQ Runtime
===========

zmqruntime is a generic ZMQ-based distributed execution and streaming framework.
It provides reusable server/client patterns, transport helpers, and optional
acknowledgment tracking for real-time visualization workflows.

``ZMQServer`` and ``ZMQClient`` are abstract integration points. Host packages
provide control/data handlers, process spawning, and domain request payloads;
the base package owns transport, lifecycle, progress, cancellation, and
projection mechanics.

.. toctree::
   :maxdepth: 2

   installation
   api/index
   architecture/index
