"""Source-only tests of the canonical exclusive-spawn boundary."""

from unittest.mock import Mock
from dataclasses import replace

import pytest

from zmqruntime.client import EndpointProcess, ZMQClient
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ProcessIdentity
from zmqruntime.timeouts import OperationDeadline


class Process(EndpointProcess):
    identity = ProcessIdentity.current()

    def is_alive(self):
        return True

    def exit(self):
        return None

    def wait_for_exit(self, timeout):
        return None

    def stop(self, timeout=5, kill_timeout=2):
        raise AssertionError("Exclusive startup must never stop a process")


class Client(ZMQClient):
    def _spawn_server_process(self):
        return self.spawn()

    def send_data(self, data):
        raise AssertionError("No source dispatch in bootstrap")


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    result = Client(
        port=5913, host="127.0.0.1", transport_mode=TransportMode.TCP, config=ZMQConfig()
    )
    result.spawn = Mock(return_value=Process())
    return result


def start(client):
    return client.start_owned_process(
        operation_deadline=OperationDeadline.after_milliseconds(500, operation="test bootstrap")
    )


@pytest.mark.parametrize("occupied", [(5913,), (5914,), (5913, 5914)])
def test_occupied_pair_never_attaches_kills_or_spawns(client, monkeypatch, occupied):
    monkeypatch.setattr(type(client.endpoint), "occupied_ports", lambda *_: frozenset(occupied))
    client._kill_processes_on_port = Mock(side_effect=AssertionError("foreign kill"))
    client._attach_existing_endpoint = Mock(side_effect=AssertionError("foreign attach"))
    with pytest.raises(RuntimeError, match="empty data/control"):
        start(client)
    client.spawn.assert_not_called()


def test_local_empty_pair_spawns_once_and_reserves_prebind_child(client, monkeypatch):
    declaration = client.transport_mode.declaration
    monkeypatch.setattr(type(client.endpoint), "occupied_ports", lambda *_: frozenset())
    monkeypatch.setattr(declaration, "data_control_pair_is_available", lambda *_: True)
    process = start(client)
    assert process.identity == ProcessIdentity.current()
    assert declaration.startup_owner(client.port, client.config) == process.identity
    with pytest.raises(RuntimeError, match="startup owner"):
        start(client)
    client.spawn.assert_called_once()


def test_foreign_host_rejected_before_lock_or_spawn(client):
    client.endpoint = replace(client.endpoint, host="203.0.113.7")
    with pytest.raises(ValueError, match="local endpoint"):
        start(client)
    client.spawn.assert_not_called()


def test_malformed_startup_reservation_fails_closed(client):
    path = client.transport_mode.declaration.startup_lock_path(client.port, client.config)
    path.parent.mkdir(parents=True)
    path.write_text("unknown partial reservation", encoding="utf-8")
    with pytest.raises(ValueError):
        start(client)
    client.spawn.assert_not_called()


def test_unavailable_pair_never_spawns(client, monkeypatch):
    monkeypatch.setattr(type(client.endpoint), "occupied_ports", lambda *_: frozenset())
    monkeypatch.setattr(
        client.transport_mode.declaration, "data_control_pair_is_available", lambda *_: False
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        start(client)
    client.spawn.assert_not_called()
