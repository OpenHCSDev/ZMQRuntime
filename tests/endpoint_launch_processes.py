"""Processes the endpoint-launch tests start: a server, and an owner that starts one."""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import replace

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.execution import ExecutionClient, ExecutionServer
from zmqruntime.messages import EndpointLaunch, ExecuteRequest, ProcessIdentity
from zmqruntime.runner import serve_forever

TRANSPORT_MODE = TransportMode.default()


def address_config(socket_directory: str, port: int) -> ZMQConfig:
    """One address in the namespace a test shares between its clients."""

    return ZMQConfig(ipc_socket_dir=socket_directory, default_port=port)


class LaunchedServer(ExecutionServer):
    def execute_task(self, execution_id: str, request: ExecuteRequest):
        del execution_id, request
        return {}


class LaunchingClient(ExecutionClient):
    """A client whose code root and owner a test may replace to stand for another."""

    def __init__(
        self,
        config: ZMQConfig,
        *,
        persistent: bool = False,
        code_root: str | None = None,
        owner: ProcessIdentity | None = None,
    ) -> None:
        super().__init__(
            port=config.default_port,
            host="127.0.0.1",
            persistent=persistent,
            transport_mode=TRANSPORT_MODE,
            config=config,
        )
        self._code_root = code_root
        self._owner = owner

    def launch(self) -> EndpointLaunch:
        launch = super().launch()
        return replace(
            launch,
            code_root=self._code_root or launch.code_root,
            owner=self._owner or launch.owner,
        )

    def _spawn_server_process(self) -> subprocess.Popen:
        return subprocess.Popen(
            [
                sys.executable,
                __file__,
                "serve",
                self.config.ipc_socket_dir,
                str(self.port),
                self.launch().to_argument(),
            ]
        )

    def send_data(self, data) -> None:
        del data

    def serialize_task(self, task, config) -> ExecuteRequest:
        del config
        return ExecuteRequest(subject_id="subject", pipeline_code=str(task))


def serve(socket_directory: str, port: str, launch: str) -> None:
    config = address_config(socket_directory, int(port))
    serve_forever(
        LaunchedServer(
            port=config.default_port,
            host="127.0.0.1",
            transport_mode=TRANSPORT_MODE,
            config=config,
            launch=EndpointLaunch.from_argument(launch),
        )
    )


def own(socket_directory: str, port: str, persistence: str) -> None:
    """Start a server, name its process on stdout, then wait to be killed."""

    client = LaunchingClient(
        address_config(socket_directory, int(port)),
        persistent=persistence == "persistent",
    )
    if not client.connect(timeout=30):
        raise SystemExit("owner could not start its server")
    print(client.connected_endpoint.process_identity.pid, flush=True)
    time.sleep(600)


if __name__ == "__main__":
    {"serve": serve, "own": own}[sys.argv[1]](*sys.argv[2:])
