Progress Registry and Projection
================================

Modules
-------

- ``zmqruntime.progress.registry``
- ``zmqruntime.progress.projection``

Purpose
-------

Provide generic progress state primitives that are independent of any specific
application phase vocabulary.

Abstraction Boundary
--------------------

``zmqruntime.progress`` owns:

- latest-event retention by injected semantic key
- generic projection traversal/aggregation mechanics
- adapter contracts that let applications inject domain semantics

Applications own:

- domain event schema and enums
- channel/topology semantics
- domain-specific status rendering and UI hierarchy

Registry Primitive
------------------

``LatestEventRegistry`` stores one latest event per semantic key:

- key strategy is injected via ``key_builder``
- terminal detection is injected via ``is_terminal``
- timestamp extraction is injected via ``timestamp_of``

Registration is monotonic: an event whose timestamp is not newer than the
retained event for the same key is rejected and does not notify consumers.
Accepted registrations and clear operations emit typed
``EventRegistryMutation`` values after the registry lock is released. Listener
failures are isolated from both registry mutation and progress-stream delivery.
This makes retention, de-duplication, and projection invalidation reusable
across domains.

Registry Contract
-----------------

``EventRegistryABC`` defines a minimal nominal interface:

- ``register_event(execution_id, event)``
- ``get_events(execution_id)``
- ``get_execution_ids()``
- ``clear_execution(execution_id)``
- ``clear_all()``

``LatestEventRegistry`` is the reference in-memory implementation for this
contract.

Projection Primitive
--------------------

``build_execution_projection`` computes per-unit, per-group, and cross-group
aggregate progress from execution event snapshots.

All domain semantics are adapter-driven through
``ProgressProjectionAdapterABC``:

- channel classification
- known unit universe
- failure/success terminal rules
- state token mapping
- application refinement of an otherwise-idle state from the event snapshot

Projection Contract
-------------------

The projection kernel expects an adapter that can answer:

- which channel an event belongs to
- which unit IDs exist for a group
- whether an event is failure terminal or success terminal
- how to map domain status into stable projection state tokens

This preserves one projection algorithm while keeping domain semantics explicit.

Pipeline Shape
--------------

Generic projection computes:

1. per-unit latest-state summaries
2. per-group aggregation from unit summaries
3. cross-group totals and average percent

No domain-specific entity names are required by the kernel.

Design Outcome
--------------

Applications can keep domain-specific progress enums and topology rules while
sharing one projection kernel and one latest-event registry implementation.

See Also
--------

- :doc:`zmq_execution_system`
- :doc:`execution_status_poller`
