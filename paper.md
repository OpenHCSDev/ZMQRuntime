---
title: 'zmqruntime: Distributed Execution Framework with Dual-Channel ZMQ Transport'
tags:
  - Python
  - distributed computing
  - ZMQ
  - microscopy
  - scientific computing
authors:
  - name: Tristan Simas
    orcid: 0000-0002-6526-3149
    affiliation: 1
affiliations:
  - name: McGill University
    index: 1
date: 15 January 2026
bibliography: paper.bib
---

# Summary

`zmqruntime` runs tasks in a separate server process over ZeroMQ [@Hintjens2013]. Each server binds two sockets, the **dual-channel pattern**: a control channel (REQ/REP) for commands and a data channel (PUB/SUB by default). An application subclasses `ExecutionServer` and `ExecutionClient` and gets:

- **Non-blocking submission**: `submit_execution` returns an execution ID to poll or wait on
- **Progress streaming**: numbered progress events arrive on the data channel
- **Cancellation**: cancelling terminates the server's worker processes, not the server
- **Server start on connect**: `connect` attaches to a usable server or starts one

```python
import subprocess, sys
from zmqruntime.execution import ExecutionClient, ExecutionSubmissionResponse
from zmqruntime.messages import ExecuteRequest

class PipelineClient(ExecutionClient):
    def serialize_task(self, task, config):
        return ExecuteRequest(subject_id="plate1", pipeline_code=task, config_code=config)
    def _spawn_server_process(self):
        return subprocess.Popen([sys.executable, "serve.py", self.launch().to_argument()])
    def send_data(self, data): ...

client = PipelineClient(port=7777)
reply = client.submit_execution(pipeline_code, config_code)
exec_id = ExecutionSubmissionResponse.from_wire(reply).require_execution_id("submit")
result = client.wait_for_completion(exec_id)
```

# Statement of Need

Scientific pipelines often run in a process separate from the interface that submits them. Task frameworks such as Celery and Dask need a broker or scheduler service; hand-written socket code leaves server startup and shutdown to each application.

`zmqruntime` sits between the two: typed messages over ZeroMQ and one owner for the server lifecycle. That owner also decides which server a client may use: by port number alone, two checkouts of an application on one machine could attach to each other's servers.

# Software Design

**Dual-Channel Transport**: The server answers control requests in its main loop and runs executions one at a time on a worker thread, so status and cancel requests are answered while a task runs.

**Transports**: `TransportDeclaration` is an abstract base class with one subclass each for TCP and IPC. IPC (Unix sockets) is the default; on Windows only TCP is supported. An IPC endpoint is a file `<prefix>-<port>.sock` below `ZMQConfig.socket_root()` (default `~/.<app_name>`), which an application overrides to keep installations apart. For IPC, `private_endpoint` yields a new socket directory under that root and removes it afterwards.

**Server Isolation**: Every pong carries an `EndpointLaunch`: the directory the server's code is imported from, the process that started it and its audience. A client attaches to a local server only if the code roots match and the audience admits it; a server started by a non-persistent client admits only that process. Otherwise `connect` raises `EndpointLaunchMismatchError` and leaves that server running. A server stops once its starter has exited; a persistent one first finishes its executions.

**Message Types**: Requests and responses are dataclasses; `ExecutionServer` maps each `ControlMessageType` member to a handler.

# Research Application

`zmqruntime` runs pipeline execution in OpenHCS: the interface submits pipelines to an execution server and follows their progress.

# References
