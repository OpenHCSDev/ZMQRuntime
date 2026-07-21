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

Wire Protocol Authority
-----------------------

``zmqruntime.viewer_protocol`` owns the shared viewer wire vocabulary.
``ViewerWireField`` is the single declaration of the canonical JSON keys.
The batch, batch-item, display-config, and batch-context field names are type
aliases of that enum, not separately maintained field tables.  Producers and
consumers should use the enum or those aliases instead of spelling a second
field inventory.

Three fields let an application carry producer-declared axis information:

``source_channel_axis``
  An optional locator for the source channel axis.

``plane_axis``
  An optional locator for the axis whose positions identify streamed planes.

``plane_component_values``
  Optional component values associated with those planes.

These values are deliberately transport-level declarations.  zmqruntime does
not infer them from an array, reorder an array, or define an application's axis
model.  The producing application owns their meaning and the receiving
application decides how to project them into viewer state.

Payload Construction
~~~~~~~~~~~~~~~~~~~~

``ViewerWirePayload`` recursively normalizes declared wire values.  Strings,
booleans, integers, floats, ``None``, enums, mappings, and non-string sequences
become JSON-compatible values.  Unsupported objects and non-string mapping
keys other than enums raise ``TypeError``; values are never silently converted
with ``str``.

Use the nominal payload builders for a complete batch:

- ``ViewerBatchItemPayload`` owns one image item's payload plus its data type,
  metadata, producer identity, and image ID.
- ``ViewerBatchDisplayPayload`` owns component modes, component order, and
  display-specific extra fields.
- ``ViewerComponentMetadataPayload`` owns component names and value domains.
- ``ViewerBatchMessagePayload.from_parts`` combines those sections with the
  ``batch`` message type, timestamp, and normalized extra fields.

The transport preserves extra fields, including the three axis fields above,
but their semantic contract remains with the application that declares them.

Control Replies and Acknowledgments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ViewerControlReplyHeader`` always emits ``status`` and optionally emits
``type`` and ``message``.  ``ViewerControlReplyPayload`` then adds normalized
``fields``.  Its separate ``payload`` member is intentionally opaque and is
placed under the ``payload`` key unchanged; the host application must ensure
that object is accepted by the serializer used on its control socket.

``ViewerAckResponsePayload`` is stricter: an acknowledgment must be a mapping
and is normalized before it is returned.  ``ViewerAckPolicy`` additionally
requires a known ``success`` or ``error`` status and applies the configured
REQ/REP timeout.

See :doc:`../api/viewer_protocol` for the current protocol types and methods.

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

Readiness Contract
~~~~~~~~~~~~~~~~~~

Endpoint presence is necessary for discovery but is not readiness. A server is
ready only when its control endpoint returns the canonical PONG response and,
when requested, that response declares ``ready``. Applications must not treat an
IPC socket path or bound TCP port as equivalent evidence.

``request_control_ping`` keeps one REQ socket for one complete PING/PONG
exchange. Sending and receiving share one absolute deadline; the receive phase
gets only the time left after the socket became writable and the request was
sent. ``wait_for_server_ready`` delegates the remaining overall readiness
interval to that exchange rather than repeatedly abandoning short-lived REQ
sockets while a live server is preparing its reply. A prompt PONG whose
``ready`` value is false may be probed again until the overall deadline.

This transport contract does not decide when an application is capable of
servicing control traffic. A host process must publish its data and control
endpoints only after its own dispatch loop is live. Conversely, a slow or
blocked application loop is not repaired by weakening the PONG requirement or
by interpreting endpoint files as success.

Acknowledgments
~~~~~~~~~~~~~~~

Streaming servers can send ``ImageAck`` messages back to the shared ack port.
``GlobalAckListener`` routes these acknowledgments to queue trackers for
progress reporting.
