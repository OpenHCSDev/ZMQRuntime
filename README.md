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
