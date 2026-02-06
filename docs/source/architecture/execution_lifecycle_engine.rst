Execution Lifecycle Engine
==========================

Module
------

``zmqruntime.execution.lifecycle``

Purpose
-------

Provide a typed lifecycle boundary for queued/running/terminal execution
records independent of transport details.

Contracts
---------

- ``ExecutionLifecycleEngineABC``
- ``InMemoryExecutionLifecycleEngine``

Lifecycle Operations
--------------------

- ``enqueue(record)`` -> queue position
- ``mark_running(execution_id)``
- ``mark_complete(execution_id)``
- ``mark_failed(execution_id, error)``
- ``mark_cancelled(execution_id)``
- ``cancel_all_active()``
- ``snapshot(...)`` -> ``ExecutionStatusSnapshot``

State Data
----------

The in-memory implementation keeps:

- mutable record map (``execution_id`` -> ``ExecutionRecord``)
- explicit queue order list for deterministic ``queue_position``

Typed Snapshot Output
---------------------

``snapshot(...)`` emits ``ExecutionStatusSnapshot`` with:

- single execution view, or
- global view including running/queued execution entries

This is the canonical source for server status responses.

See Also
--------

- :doc:`zmq_execution_system`
- :doc:`execution_status_poller`
