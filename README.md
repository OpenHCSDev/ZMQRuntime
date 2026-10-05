# ZMQRuntime

Generic ZeroMQ transport, lifecycle, progress, cancellation, viewer-control,
and execution-projection primitives.

## Important boundary

``ZMQServer`` and ``ZMQClient`` are abstract application integration points.
They cannot be instantiated directly:

- a server implements ``handle_control_message`` and ``handle_data_message``;
- a client implements ``_spawn_server_process`` and ``send_data``.

The package does not provide a generic ``send_request`` API or define an
application's execution payload. Applications subclass the bases and map their
typed domain requests onto the transport.

## Transport configuration

```python
from zmqruntime import TransportMode, ZMQConfig
from zmqruntime.transport import get_zmq_transport_url

config = ZMQConfig(control_port_offset=1000)
data_url = get_zmq_transport_url(
    7777,
    host="localhost",
    mode=TransportMode.TCP,
    config=config,
)
```

Application subclasses can use ``serve_forever``, readiness probes, control
port helpers, progress records, cancellation messages, lifecycle engines, and
projection adapters without duplicating socket or process machinery.

## Execution completion

Execution clients with a registered progress subscription wake on the server's
terminal `ExecutionStatusSnapshot`. The existing callback sequence barrier still
waits for every admitted progress callback. Registration replays the latest
progress and terminal record; a missing terminal notification recovers through
STATUS using the configured wait interval. Clients without progress registration
continue to use STATUS.

Applications that enrich successful results override
`ExecutionServer.finalize_execution_record(record)`. This record-only hook runs
under the lifecycle lock after task completion and before STATUS or terminal
publication can observe the record. It must not publish progress or acquire
transport resources. The task's recorded end time retains its existing boundary.
Already admitted progress is published before the terminal record; new progress
admissions after a terminal transition are discarded.

## Installation

```bash
python -m pip install zmqruntime
```

Documentation: [zmqruntime.readthedocs.io](https://zmqruntime.readthedocs.io/).

For local development, install the documentation dependencies and run a clean,
warning-fatal build:

```bash
python -m pip install -e ".[dev,docs]"
python -m sphinx -E -W --keep-going -b html docs/source docs/_build/html
```

Repository: [OpenHCSDev/ZMQRuntime](https://github.com/OpenHCSDev/ZMQRuntime).
