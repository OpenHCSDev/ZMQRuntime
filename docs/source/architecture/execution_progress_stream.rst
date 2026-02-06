Execution Progress Stream
=========================

Module
------

``zmqruntime.execution.progress_stream``

Purpose
-------

Encapsulate progress-listener thread lifecycle for execution clients.

Core Type
---------

- ``ProgressStreamSubscriber``

Responsibilities
----------------

- start/stop listener thread safely
- consume PUB/SUB messages in non-blocking loop
- validate payload shape via ``validate_progress_payload``
- dispatch payloads to callback
- fail loudly if callback raises

Why Separate This
-----------------

Progress subscription is orthogonal to submit/poll control messaging. Extracting
it keeps client control flow clean and testable.

See Also
--------

- :doc:`zmq_execution_system`
- :doc:`execution_status_poller`
