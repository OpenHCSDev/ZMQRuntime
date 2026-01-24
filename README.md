# zmqruntime

Generic ZMQ-based distributed execution framework extracted from OpenHCS.

## Features

- **Client-Server Architecture**: Simple ZMQ-based client and server for distributed task execution
- **Multiple Transport Modes**: Support for TCP and IPC transports
- **Message Protocol**: Typed message classes for requests, responses, progress updates, and control messages
- **Queue Tracking**: Built-in queue tracking for monitoring execution status
- **Acknowledgment System**: Global acknowledgment listener for reliable message delivery

## Installation

```bash
pip install zmqruntime
```

## Quick Start

### Server

```python
from zmqruntime import ZMQServer, serve_forever

# Create and start a server
server = ZMQServer(port=7777)
serve_forever(server)
```

### Client

```python
from zmqruntime import ZMQClient, ExecuteRequest

# Connect to the server
client = ZMQClient(port=7777)

# Send an execution request
request = ExecuteRequest(
    execution_id="task-001",
    payload={"task": "process_data"}
)
response = client.send_request(request)
```

## Core Components

- `ZMQServer`: Server that listens for and processes execution requests
- `ZMQClient`: Client for sending requests to the server
- `ExecuteRequest` / `ExecuteResponse`: Request/response message types
- `ProgressUpdate`: Progress reporting during execution
- `QueueTracker`: Track pending and active executions
- `GlobalAckListener`: Acknowledgment handling for reliable delivery

## Transport Configuration

```python
from zmqruntime import ZMQConfig, TransportMode

# TCP transport (default)
config = ZMQConfig(port=7777, transport_mode=TransportMode.TCP)

# IPC transport (Unix sockets)
config = ZMQConfig(port=7777, transport_mode=TransportMode.IPC)
```

## License

MIT License - see LICENSE file for details.
