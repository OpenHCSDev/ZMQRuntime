Execution Wait Policy
=====================

Module
------

``zmqruntime.execution.wait_policy``

Purpose
-------

Provide a reusable blocking wait loop with explicit retry/timeout policy for
execution completion.

Core Types
----------

- ``WaitPolicy``
- ``ExecutionWaiter``

WaitPolicy Fields
-----------------

- ``poll_interval``
- ``max_consecutive_errors``
- ``retry_backoff_seconds``

ExecutionWaiter Behavior
------------------------

- polls server status repeatedly
- parses responses into ``ExecutionStatusSnapshot``
- resets the consecutive-error budget only after that decode succeeds;
  a received but malformed response remains a failed status observation
- returns normalized terminal dict for complete/failed/cancelled/error
- treats repeated polling failures as lost-connection cancellation
- retains retries while known server liveness or advancing progress is observed;
  the error budget is not a deadline for a healthy long-running execution

Design Outcome
--------------

One policy-driven waiting mechanism prevents duplicated ad-hoc polling loops
across clients.

See Also
--------

- :doc:`execution_status_poller`
- :doc:`zmq_execution_system`
