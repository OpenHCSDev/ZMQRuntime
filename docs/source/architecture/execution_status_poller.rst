Execution Status Poller
=======================

Module
------

``zmqruntime.execution.status_poller``

Purpose
-------

Offer a generic status polling loop for queued/running/terminal transitions
with callback policy injection.

Core Types
----------

- ``ExecutionStatusPollPolicyABC``
- ``CallbackExecutionStatusPollPolicy``
- ``ExecutionStatusPoller``

Terminal Statuses
-----------------

The poller treats these as terminal:

- ``complete``
- ``failed``
- ``cancelled``

Policy Hooks
------------

- ``poll_status(execution_id)``
- ``on_running(...)``
- ``on_terminal(...)``
- ``on_status_error(...)``
- ``on_poll_exception(...)`` -> continue or stop

Design Outcome
--------------

Domain services can reuse one tested polling loop and only inject behavior via
policy callbacks.

See Also
--------

- :doc:`execution_batch_submit_wait`
- :doc:`execution_wait_policy`
