"""Source-only tests of the canonical exclusive-spawn boundary."""

from unittest.mock import Mock
from dataclasses import replace

import pytest
import portalocker

from zmqruntime.client import (
    EndpointConnectionCancelledError,
    EndpointProcess,
    EndpointStartupUncertainError,
    ZMQClient,
)
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ProcessIdentity
from zmqruntime.timeouts import OperationCancellation, OperationDeadline


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
    result = Client(
        port=5913, host="127.0.0.1", transport_mode=TransportMode.TCP, config=ZMQConfig()
    )
    monkeypatch.setattr(
        result.transport_mode.declaration,
        "startup_lock_path",
        lambda port, config: tmp_path / "locks" / f"{port}.startup.lock",
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


def test_cross_pair_prebind_reservation_rejects_second_child(client, monkeypatch):
    declaration = client.transport_mode.declaration
    monkeypatch.setattr(type(client.endpoint), "occupied_ports", lambda *_: frozenset())
    monkeypatch.setattr(declaration, "data_control_pair_is_available", lambda *_: True)
    start(client)
    overlapping = Client(
        port=client.control_port,
        host="127.0.0.1",
        transport_mode=client.transport_mode,
        config=client.config,
    )
    overlapping.spawn = Mock(side_effect=AssertionError("Duplicate prebind child"))
    with pytest.raises(RuntimeError, match="startup owner"):
        start(overlapping)
    overlapping.spawn.assert_not_called()
    client.spawn.assert_called_once()


def test_expired_budget_never_spawns(client):
    deadline = OperationDeadline(operation="expired bootstrap", timeout_ms=1, expires_at=0)
    with pytest.raises(TimeoutError):
        client.start_owned_process(operation_deadline=deadline)
    client.spawn.assert_not_called()


@pytest.mark.parametrize("control", [False, True])
def test_ordinary_connect_preserves_both_pending_address_owners(client, monkeypatch, control):
    declaration = client.transport_mode.declaration
    reserved_port = client.control_port if control else client.port
    path = declaration.startup_lock_path(reserved_port, client.config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    owner = ProcessIdentity.current()
    declaration.record_startup_owner(reserved_port, client.config, owner)
    inode = path.stat().st_ino
    monkeypatch.setattr(client, "_is_port_in_use", lambda *_: False)
    kill = Mock(side_effect=AssertionError("A pending child is not replaceable"))
    monkeypatch.setattr(client, "_kill_processes_on_port", kill)
    assert client.connect(timeout=0.01) is False
    assert declaration.startup_owner(reserved_port, client.config) == owner
    assert path.stat().st_ino == inode
    client.spawn.assert_not_called()
    kill.assert_not_called()


@pytest.fixture
def empty_pair(client, monkeypatch):
    monkeypatch.setattr(type(client.endpoint), "occupied_ports", lambda *_: frozenset())
    monkeypatch.setattr(
        client.transport_mode.declaration, "data_control_pair_is_available", lambda *_: True
    )
    return client


def assert_released_under_pair_locks(client, monkeypatch):
    """Observe the real release, retaining inode and lock exclusion evidence."""
    declaration = client.transport_mode.declaration
    original_release = declaration.release_startup_owner
    released = []
    ports = sorted(client.endpoint.port_pair(client.config).ports)

    def release(port, config, owner):
        inodes = {p: declaration.startup_lock_path(p, config).stat().st_ino for p in ports}
        for p in ports:
            with declaration.startup_lock_path(p, config).open("r+b") as contender:
                with pytest.raises(portalocker.exceptions.AlreadyLocked):
                    portalocker.lock(contender, portalocker.LOCK_EX | portalocker.LOCK_NB)
        result = original_release(port, config, owner)
        assert {p: declaration.startup_lock_path(p, config).stat().st_ino for p in ports} == inodes
        released.append((port, result))
        return result

    monkeypatch.setattr(declaration, "release_startup_owner", release)
    return released


def assert_independent_start_admitted(client):
    independent = Client(
        port=client.port,
        host=client.host,
        transport_mode=client.transport_mode,
        config=client.config,
    )
    independent.spawn = Mock(return_value=Process())
    start(independent)
    independent.spawn.assert_called_once()


def test_expiry_after_both_provisional_records_rolls_back_before_spawn(
    empty_pair, monkeypatch
):
    client = empty_pair
    declaration = client.transport_mode.declaration
    ports = sorted(client.endpoint.port_pair(client.config).ports)
    original_record = declaration.record_startup_owner
    deadline = OperationDeadline.after_milliseconds(500, operation="pre-spawn expiry")
    released = assert_released_under_pair_locks(client, monkeypatch)

    def record(port, config, owner):
        original_record(port, config, owner)
        if port == ports[-1]:
            object.__setattr__(deadline, "expires_at", 0)

    with monkeypatch.context() as fault:
        fault.setattr(declaration, "record_startup_owner", record)
        with pytest.raises(TimeoutError):
            client.start_owned_process(operation_deadline=deadline)
    client.spawn.assert_not_called()
    assert released == [(port, True) for port in ports]
    assert all(declaration.startup_owner(port, client.config) is None for port in ports)
    assert_independent_start_admitted(client)


@pytest.mark.parametrize("published_before_error", [False, True])
def test_second_provisional_publication_failure_rolls_back_before_spawn(
    empty_pair, monkeypatch, published_before_error
):
    client = empty_pair
    declaration = client.transport_mode.declaration
    ports = sorted(client.endpoint.port_pair(client.config).ports)
    original_record = declaration.record_startup_owner
    failure = OSError("second provisional publication failed")
    released = assert_released_under_pair_locks(client, monkeypatch)

    def record(port, config, owner):
        if port != ports[-1] or published_before_error:
            original_record(port, config, owner)
        if port == ports[-1]:
            raise failure

    with monkeypatch.context() as fault:
        fault.setattr(declaration, "record_startup_owner", record)
        with pytest.raises(OSError) as raised:
            start(client)
    assert raised.value is failure
    client.spawn.assert_not_called()
    assert released == [(ports[0], True), (ports[1], published_before_error)]
    assert all(declaration.startup_owner(port, client.config) is None for port in ports)
    assert_independent_start_admitted(client)


def test_cancellation_after_reservation_rolls_back_without_spawn(empty_pair, monkeypatch):
    client = empty_pair
    declaration = client.transport_mode.declaration
    original_record = declaration.record_startup_owner
    ports = sorted(client.endpoint.port_pair(client.config).ports)
    released = assert_released_under_pair_locks(client, monkeypatch)

    def record(port, config, owner):
        original_record(port, config, owner)
        if port == ports[-1]:
            client._connection_cancellation.get().cancel()

    monkeypatch.setattr(declaration, "record_startup_owner", record)
    with pytest.raises(EndpointConnectionCancelledError):
        start(client)
    client.spawn.assert_not_called()
    assert released == [(port, True) for port in ports]


def test_partial_second_record_remains_unknown_after_pre_spawn_failure(empty_pair, monkeypatch):
    client = empty_pair
    declaration = client.transport_mode.declaration
    ports = sorted(client.endpoint.port_pair(client.config).ports)
    original_record = declaration.record_startup_owner
    failure = OSError("partial provisional publication")
    released = assert_released_under_pair_locks(client, monkeypatch)

    def record(port, config, owner):
        if port == ports[-1]:
            declaration.startup_lock_path(port, config).write_text("partial", encoding="utf-8")
            raise failure
        original_record(port, config, owner)

    with monkeypatch.context() as fault:
        fault.setattr(declaration, "record_startup_owner", record)
        with pytest.raises(OSError) as raised:
            start(client)
    assert raised.value is failure
    client.spawn.assert_not_called()
    assert released == [(ports[0], True), (ports[1], False)]
    assert declaration.startup_owner(ports[0], client.config) is None
    assert declaration.startup_lock_path(ports[1], client.config).read_text() == "partial"
    with pytest.raises(ValueError):
        assert_independent_start_admitted(client)
    client.spawn.assert_not_called()


@pytest.mark.parametrize("spawn_returns_child", [False, True])
def test_spawn_boundary_failure_never_rolls_back_or_replays(
    empty_pair, monkeypatch, spawn_returns_child
):
    client = empty_pair
    declaration = client.transport_mode.declaration
    ports = sorted(client.endpoint.port_pair(client.config).ports)
    release = Mock(side_effect=AssertionError("Post-spawn rollback is forbidden"))
    monkeypatch.setattr(declaration, "release_startup_owner", release)
    failure = OSError("spawn or child publication uncertain")
    child = Process()
    child.identity = replace(ProcessIdentity.current(), create_time=1.0)

    if spawn_returns_child:
        client.spawn.return_value = child
        original_record = declaration.record_startup_owner

        def record(port, config, owner):
            if owner == child.identity:
                if port == ports[-1]:
                    raise failure
            original_record(port, config, owner)

        monkeypatch.setattr(declaration, "record_startup_owner", record)
        with pytest.raises(EndpointStartupUncertainError) as raised:
            start(client)
        assert raised.value.process is child
        assert raised.value.__cause__ is failure
        assert declaration.startup_owner(ports[0], client.config) == child.identity
    else:
        client.spawn.side_effect = failure
        with pytest.raises(OSError) as raised:
            start(client)
        assert raised.value is failure
        assert declaration.startup_owner(ports[0], client.config) == ProcessIdentity.current()
    assert declaration.startup_owner(ports[-1], client.config) == ProcessIdentity.current()
    client.spawn.assert_called_once()
    release.assert_not_called()


@pytest.mark.parametrize("mode", [TransportMode.TCP, TransportMode.IPC])
@pytest.mark.parametrize("record", ["unknown", "different_incarnation", "child"])
def test_provisional_release_preserves_unproved_or_changed_owner(
    client, monkeypatch, mode, record
):
    declaration = mode.declaration
    paths = client.transport_mode.declaration.startup_lock_path
    monkeypatch.setattr(declaration, "startup_lock_path", paths)
    with declaration.startup_lock(client.port, client.config, None, OperationCancellation()):
        owner = ProcessIdentity.current()
        path = declaration.startup_lock_path(client.port, client.config)
        if record == "unknown":
            path.write_text("unknown partial record", encoding="utf-8")
        else:
            recorded = replace(owner, create_time=1.0)
            if record == "child":
                recorded = replace(recorded, pid=owner.pid + 1)
            declaration.record_startup_owner(client.port, client.config, recorded)
        before = path.read_bytes()
        inode = path.stat().st_ino
        assert declaration.release_startup_owner(client.port, client.config, owner) is False
        assert path.read_bytes() == before
        assert path.stat().st_ino == inode
        if record == "unknown":
            with pytest.raises(ValueError):
                declaration.startup_owner(client.port, client.config)
