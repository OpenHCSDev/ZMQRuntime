Execution Progress Stream
=========================

Module
------

``zmqruntime.execution.progress_stream``

Purpose
-------

Encapsulate progress-listener thread lifecycle and exact per-execution activity
observations for execution clients.

Core Type
---------

- ``ProgressStreamSubscriber`` owns background receive and callback dispatch.
- ``ExecutionProgressObservation`` owns an immutable latest event plus the
  monotonic sequence assigned by its submitting client.

Responsibilities
----------------

- start/stop listener thread safely
- consume PUB/SUB messages in non-blocking loop
- validate payload shape via ``validate_progress_payload``
- dispatch payloads to callback
- isolate malformed-message or callback failures without terminating the stream
- retain one immutable latest observation per execution on ``ExecutionClient``
- project and parse observations through their declared wire fields

Why Separate This
-----------------

Progress subscription is orthogonal to submit/poll control messaging. The
submitting client nevertheless owns both its subscription and the activity
observations received through it. Status consumers must retain and query that
same client instead of opening unrelated polling clients and copying progress
state elsewhere.

See Also
--------

- :doc:`zmq_execution_system`
- :doc:`execution_status_poller`
