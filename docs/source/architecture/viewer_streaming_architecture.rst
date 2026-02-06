Viewer Streaming Architecture
=============================

Overview
--------

zmqruntime provides a reusable streaming server pattern for real-time
visualization. Viewer implementations receive image payloads over ZMQ and
optionally emit acknowledgments back to the shared ack port.

Boundary
--------

``zmqruntime`` owns transport and server/process lifecycle mechanics for
streaming viewers. Application-level payload semantics (component projection,
window/layer grouping, viewer-specific batch assembly) are intentionally out of
scope and belong in higher-level libraries such as ``polystore``.

Core Components
~~~~~~~~~~~~~~~

**StreamingVisualizerServer** (``zmqruntime/streaming/server.py``)
  ZMQ server base class that receives image batches, dispatches them to
  ``display_image`` implementations, and can emit ``ImageAck`` messages.

**VisualizerProcessManager** (``zmqruntime/streaming/process_manager.py``)
  Lifecycle manager for launching and stopping viewer subprocesses.

Transport Pattern
~~~~~~~~~~~~~~~~~

- Data channel uses the standard ZMQ transport URL from
  ``zmqruntime.transport.get_zmq_transport_url``.
- Control channel handles ping/pong readiness checks via ``ZMQServer``.
- Transport mode (IPC/TCP) is configured via ``TransportMode`` and ``ZMQConfig``.

Acknowledgments
~~~~~~~~~~~~~~~

Streaming servers can send ``ImageAck`` messages back to the shared ack port.
``GlobalAckListener`` routes these acknowledgments to queue trackers for
progress reporting.
