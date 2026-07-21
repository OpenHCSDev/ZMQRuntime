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

Endpoint startup and ownership lifecycle
----------------------------------------

``ZMQClient.connect()`` performs discovery and optional auto-spawn inside
``endpoint_startup_lock()``. For IPC transport, that context manager takes an
exclusive file lock derived from the endpoint path, so concurrent clients cannot
both observe an absent endpoint and spawn competing servers. TCP transport does
not use the filesystem lock.

While holding the startup boundary, the client follows one ordered decision:

1. If the endpoint path/port exists, require a ready control PONG before treating
   it as an existing server.
2. For IPC, preserve an unresponsive path when the kernel still owns a Unix
   socket. ``ipc_socket_is_stale()`` reports stale only when the path exists and
   the Unix-socket inventory proves no matching kernel-owned socket. If process
   inspection is unavailable or fails, it returns false and cleanup remains
   conservative.
3. Remove only a proven stale IPC path, or apply the platform's explicit TCP
   port cleanup policy, before spawning.
4. Retain the returned process handle as ``server_process`` and require the
   normal readiness handshake before creating client data sockets.

Startup failure is ownership-aware. If readiness times out or client-socket
setup raises, the client stops the child it just spawned and clears the handle.
It never assigns an existing server's process to ``server_process``.

Disconnect has the same distinction. Client sockets always close, but a server
process is stopped only when this client spawned it, the client did not connect
to an existing endpoint, and ``persistent`` is false. Both
``multiprocessing.Process`` and ``subprocess.Popen`` children receive graceful
termination plus a bounded wait, then forced kill if necessary. IPC data and
control paths are removed only after the owned child is confirmed stopped.
``ZMQServer.stop()`` independently removes its own IPC endpoints after closing
the sockets and context.

This lifecycle keeps endpoint existence, control readiness, process ownership,
and cleanup as separate facts. A socket path is not readiness evidence; an
unresponsive live IPC socket is not stale; and knowing a port does not grant a
client ownership of an existing process.

Viewer readiness adds a stricter application-level handshake on top of this
transport lifecycle. See :doc:`viewer_streaming_architecture`; it does not
replace startup serialization or owned-process cleanup.

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
