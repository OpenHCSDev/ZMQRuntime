Execution Batch Submit/Wait
===========================

Module
------

``zmqruntime.execution.batch_submit_wait``

Purpose
-------

Provide a reusable orchestration primitive for:

1. submitting a sequence of jobs
2. waiting submitted jobs
3. emitting lifecycle callbacks at each stage

Core Types
----------

- ``SubmittedBatchJob``
- ``BatchSubmitWaitPolicyABC``
- ``CallbackBatchSubmitWaitPolicy``
- ``BatchSubmitWaitEngine``

Policy Model
------------

The policy abstraction defines:

- ``submit(job)`` -> ``submission_id``
- ``wait(submission_id, job)``
- ``job_key(job)`` for result mapping
- fail-fast behavior for submit and wait phases
- callback hooks for start/success/error/finally events

Engine Behavior
---------------

``BatchSubmitWaitEngine.run(...)``:

- submits all jobs in order
- collects accepted submissions
- waits accepted submissions in order
- returns artifact/result mapping by policy job key

This eliminates duplicated orchestration loops in higher-level apps.

See Also
--------

- :doc:`execution_status_poller`
- :doc:`execution_wait_policy`
