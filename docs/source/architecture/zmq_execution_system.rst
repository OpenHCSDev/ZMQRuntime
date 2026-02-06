ZMQ Execution System
====================

Overview
--------

``zmqruntime`` provides a dual-channel execution architecture:

- control channel (REQ/REP): submit, status, cancel, ping
- data channel (PUB/SUB): progress stream

This keeps command handling and progress streaming decoupled.

Core Components
---------------

- ``ZMQServer`` / ``ZMQClient`` transport base
- ``ExecutionServer`` (queue + lifecycle ownership)
- ``ExecutionClient`` (submit/poll/wait + progress callback)
- ``ProgressStreamSubscriber`` (client-side background progress loop)
- typed messages in ``zmqruntime.messages``

Progress Registration
---------------------

Progress streaming now uses an explicit control-plane registration handshake:

- client sends ``register_progress`` with a stable ``client_id``
- server acknowledges registration on REQ/REP
- client then submits ``execute`` requests
- client sends ``unregister_progress`` on disconnect

This makes progress subscription intent explicit and avoids hidden lifecycle coupling.

Boundary
--------

``zmqruntime`` owns transport, execution lifecycle, status polling, and typed
message contracts. Application-specific task payload semantics, domain progress
phases, and UI topology are intentionally out of scope.

Receiver-side streaming projection semantics (component grouping, window/layer
assembly, viewer-specific batch processing) belong to higher-level libraries
such as ``polystore``, not ``zmqruntime``.

ExecutionServer Responsibilities
--------------------------------

- enqueue typed ``ExecutionRecord`` instances
- maintain queue/running/completion state via lifecycle engine
- execute one queued record at a time
- emit typed status snapshots and ping payloads
- expose running and queued execution metadata in ping responses

ExecutionClient Responsibilities
--------------------------------

- serialize app task payloads into ``ExecuteRequest``
- register progress subscription before first execution submit when progress callback is enabled
- submit and retrieve ``execution_id``
- wait for completion using polling waiter/policy
- provide optional progress callback lifecycle

Typed Status Model
------------------

``zmqruntime.messages`` now includes:

- ``ExecutionRecord``
- ``RunningExecutionInfo``
- ``QueuedExecutionInfo``
- ``ExecutionStatusSnapshot``

These types normalize server status/ping payloads and reduce ad-hoc dict logic.
Ping responses also include ``progress_subscribers`` for observability.

Execution Flow
--------------

.. code-block:: text

   Client                          Server
     |                               |
     |-- PING (control) ------------>|
     |<-- PONG + queue/running ------|
     |                               |
     |-- REGISTER_PROGRESS ---------->|
     |<-- OK ------------------------|
     |                               |
     |-- EXECUTE (control) --------->|
     |<-- ACCEPTED + execution_id ---|
     |                               |
     |<-- PROGRESS (data/pubsub) ----|
     |<-- PROGRESS (data/pubsub) ----|
     |                               |
     |-- STATUS (control) ---------->|
     |<-- SNAPSHOT ------------------|
     |                               |
     |-- UNREGISTER_PROGRESS ------->|
     |<-- OK ------------------------|

See Also
--------

- :doc:`execution_lifecycle_engine`
- :doc:`execution_batch_submit_wait`
- :doc:`execution_status_poller`
- :doc:`execution_progress_stream`
- :doc:`execution_wait_policy`
