"""A client reaches only a server it may use, and an orphaned server exits."""

from __future__ import annotations

import subprocess
import sys
import time
from contextlib import ExitStack
from dataclasses import replace

import psutil
import pytest
from endpoint_launch_processes import (
    TRANSPORT_MODE,
    LaunchedServer,
    LaunchingClient,
    address_config,
)

import endpoint_launch_processes
from zmqruntime import DataControlPortPairAuthority, EndpointShutdownMode, ZMQConfig
from zmqruntime.messages import (
    AnySessionAudience,
    EndpointAudience,
    EndpointLaunch,
    EndpointLaunchMismatchError,
    OwnerOnlyAudience,
    ProcessIdentity,
)

ANOTHER_PROCESS = ProcessIdentity(pid=1, create_time=0.0)


def _stop_server(owner: LaunchingClient) -> None:
    """Stop the server a client started, persistent or not."""

    owner.disconnect()
    owner.shutdown_endpoint_on_port(
        owner.port,
        mode=EndpointShutdownMode.FORCE,
        transport_mode=TRANSPORT_MODE,
        host=owner.host,
        config=owner.config,
    )


@pytest.fixture
def two_addresses():
    """Two free addresses in one namespace private to the test."""

    declaration = TRANSPORT_MODE.declaration
    with declaration.private_endpoint(ZMQConfig(default_port=41000)) as first:
        second_port = DataControlPortPairAuthority.acquire(
            first,
            transport_mode=TRANSPORT_MODE,
            excluded=frozenset(
                (first.default_port, first.default_port + first.control_port_offset)
            ),
        ).data_port
        yield first, address_config(first.ipc_socket_dir, second_port)


def test_each_client_reaches_only_the_server_from_its_own_code_root(two_addresses):
    first_address, second_address = two_addresses
    with ExitStack() as clients:
        first = LaunchingClient(first_address, code_root="/checkout-a")
        second = LaunchingClient(second_address, code_root="/checkout-b")
        for client in (first, second):
            clients.callback(client.disconnect)
            assert client.connect(timeout=30)

        assert first.connected_endpoint.launch.code_root == "/checkout-a"
        assert second.connected_endpoint.launch.code_root == "/checkout-b"
        assert (
            first.connected_endpoint.process_identity
            != second.connected_endpoint.process_identity
        )

        stray = LaunchingClient(first_address, code_root="/checkout-b")
        clients.callback(stray.disconnect)
        with pytest.raises(EndpointLaunchMismatchError) as refused:
            stray.connect(timeout=5)

        assert "/checkout-a" in str(refused.value)
        assert "/checkout-b" in str(refused.value)
        assert not stray.is_connected()
        assert first.owned_server_process_is_alive() is True
        assert first.endpoint.ping(first_address) is not None


@pytest.mark.parametrize(
    ("persistent", "admitted"),
    [(False, False), (True, True)],
    ids=["owner_only", "any_session"],
)
def test_only_a_persistent_server_admits_another_process(
    two_addresses, persistent, admitted
):
    address, _ = two_addresses
    owner = LaunchingClient(address, persistent=persistent)
    try:
        assert owner.connect(timeout=30)
        server = owner.connected_endpoint.process_identity

        same_process = LaunchingClient(address, persistent=persistent)
        assert same_process.connect_existing(timeout=5)
        same_process.disconnect()

        other_process = LaunchingClient(
            address, persistent=persistent, owner=ANOTHER_PROCESS
        )
        if admitted:
            assert other_process.connect_existing(timeout=5)
            assert other_process.connected_endpoint.process_identity == server
            other_process.disconnect()
        else:
            with pytest.raises(EndpointLaunchMismatchError, match="for its own use"):
                other_process.connect_existing(timeout=5)
    finally:
        _stop_server(owner)


def test_a_server_that_reports_no_launch_is_refused(two_addresses, monkeypatch):
    address, _ = two_addresses
    owner = LaunchingClient(address, persistent=True)
    try:
        assert owner.connect(timeout=30)
        unreported = LaunchingClient(address, persistent=True)
        observed = unreported._try_connect_to_existing
        monkeypatch.setattr(
            unreported,
            "_try_connect_to_existing",
            lambda *args, **kwargs: replace(observed(*args, **kwargs), launch=None),
        )
        with pytest.raises(EndpointLaunchMismatchError, match="does not report"):
            unreported.connect_existing(timeout=5)
    finally:
        _stop_server(owner)


@pytest.mark.parametrize("persistence", ["owner_only", "persistent"])
def test_a_server_exits_after_its_owner_is_killed(two_addresses, persistence):
    address, _ = two_addresses
    owner = subprocess.Popen(
        [
            sys.executable,
            endpoint_launch_processes.__file__,
            "own",
            address.ipc_socket_dir,
            str(address.default_port),
            persistence,
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    server = None
    try:
        server = psutil.Process(int(owner.stdout.readline()))
        assert server.is_running()

        owner.kill()
        owner.wait(timeout=10)

        server.wait(timeout=15)
    finally:
        owner.kill()
        if server is not None and server.is_running():
            server.kill()
            pytest.fail("the server outlived its owner")


def _orphaned_server(audience: type[EndpointAudience]) -> LaunchedServer:
    server = LaunchedServer(
        port=41999,
        launch=EndpointLaunch(
            code_root="/checkout-a",
            owner=ProcessIdentity(pid=ProcessIdentity.current().pid, create_time=0.0),
            audience=audience.wire_name,
        ),
    )
    server._running = True
    return server


def _check_owner_now(server: LaunchedServer) -> None:
    server._owner_checked_at = time.monotonic() - server.orphan_check_interval_seconds
    server._stop_if_orphaned()


def test_an_orphaned_owner_only_server_stops_with_work_unfinished(monkeypatch):
    server = _orphaned_server(OwnerOnlyAudience)
    monkeypatch.setattr(server, "has_unfinished_work", lambda: True)

    _check_owner_now(server)

    assert not server.is_running()


def test_an_orphaned_persistent_server_finishes_its_work_first(monkeypatch):
    server = _orphaned_server(AnySessionAudience)
    monkeypatch.setattr(server, "has_unfinished_work", lambda: True)
    _check_owner_now(server)
    assert server.is_running()

    monkeypatch.setattr(server, "has_unfinished_work", lambda: False)
    _check_owner_now(server)
    assert not server.is_running()


def test_a_server_whose_owner_is_alive_keeps_running():
    server = LaunchedServer(port=41998)
    server._running = True

    _check_owner_now(server)

    assert server.launch.owner == ProcessIdentity.current()
    assert server.is_running()


def test_every_audience_decodes_from_its_wire_name():
    for wire_name, audience in EndpointAudience.__registry__.items():
        launch = EndpointLaunch.by_this_process(LaunchedServer.code_root(), audience)
        assert launch.audience == wire_name
        assert EndpointLaunch.from_argument(launch.to_argument()) == launch
        assert launch.audience_type is audience
