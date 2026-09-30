"""Endpoint-owner controls without sockets, native processes or executor threads."""

from concurrent.futures import Future
from dataclasses import replace
from unittest.mock import Mock

import portalocker
import pytest

from zmqruntime import transport as module
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import PongResponse, ProcessIdentity, ServerRole
from zmqruntime.timeouts import OperationCancellation, OperationDeadline
from zmqruntime.transport import TransportEndpoint


@pytest.mark.parametrize("mode", tuple(TransportMode))
@pytest.mark.parametrize("offset", [1, 17])
def test_pair_transaction_consumes_selected_transport_and_incarnation(
    tmp_path, monkeypatch, mode, offset,
):
    config = ZMQConfig(control_port_offset=offset, app_name="owner-control")
    endpoint = TransportEndpoint("127.0.0.1", 5980, mode)
    declaration = mode.declaration
    monkeypatch.setattr(
        declaration, "startup_lock_path", lambda port, _config: tmp_path / f"{port}.lock",
    )
    monkeypatch.setattr(TransportEndpoint, "occupied_ports", lambda *_: frozenset())
    availability = Mock(return_value=True)
    monkeypatch.setattr(declaration, "data_control_pair_is_available", availability)
    original_lock = declaration.startup_lock
    acquisitions = []

    def lock(port, selected_config, deadline, cancellation):
        acquisitions.append((port, selected_config, deadline, cancellation))
        return original_lock(port, selected_config, deadline, cancellation)

    monkeypatch.setattr(declaration, "startup_lock", lock)
    cancellation = OperationCancellation()
    deadline = OperationDeadline.after_milliseconds(500, operation="pair source control")
    owner = ProcessIdentity.current()
    child = replace(owner, create_time=owner.create_time - 1)
    ports = sorted(endpoint.port_pair(config).ports)
    with endpoint.startup_lock(
        config, operation_deadline=deadline, cancellation=cancellation,
    ) as acquired:
        assert acquired
        assert acquisitions == [(port, config, deadline, cancellation) for port in ports]
        for port in ports:
            with declaration.startup_lock_path(port, config).open("r+b") as contender:
                with pytest.raises(portalocker.exceptions.AlreadyLocked):
                    portalocker.lock(contender, portalocker.LOCK_EX | portalocker.LOCK_NB)
        endpoint.require_available_startup(config)
        availability.assert_called_once_with(5980, 5980 + offset, endpoint.host, config)
        assert endpoint.reserve_startup_owner(
            config, owner, operation_deadline=deadline, cancellation=cancellation,
        )
        endpoint.require_startup_owner(config, owner)
        with pytest.raises(RuntimeError, match="startup owner"):
            endpoint.require_available_startup(config)
        inodes = tuple(declaration.startup_lock_path(port, config).stat().st_ino for port in ports)
        endpoint.record_startup_owner(config, child)
        endpoint.require_startup_owner(config, child)
        with pytest.raises(RuntimeError, match="not reserved"):
            endpoint.require_startup_owner(config, owner)
        assert tuple(declaration.startup_lock_path(port, config).stat().st_ino for port in ports) == inodes
    assert all(declaration.startup_owner(port, config) == child for port in ports)


@pytest.mark.parametrize("mode", tuple(TransportMode))
@pytest.mark.parametrize("ports", [(), (5990, 5988, 5988, 5989), tuple(range(5900, 5933))])
def test_discovery_inherits_endpoint_behavior_and_preserves_bounds_order_and_absence(
    monkeypatch, mode, ports,
):
    config = ZMQConfig(control_port_offset=17)
    probes = []
    pools = []
    pending_port = 5990
    absent_port = 5989

    class Endpoint(TransportEndpoint):
        def ping(self, selected_config, *, timeout_ms):
            assert selected_config is config
            assert timeout_ms == 1
            probes.append(self)
            if self.port == absent_port:
                return None
            return PongResponse(
                port=42, control_port=43, ready=True, server="endpoint-source-control",
                server_role=ServerRole.EXECUTION,
            )

    class Pool:
        def __init__(self, *, max_workers):
            self.max_workers = max_workers
            self.futures = []
            self.shutdown_calls = []
            pools.append(self)

        def submit(self, operation, selected_config, *, timeout_ms):
            future = Future()
            if operation.__self__.port != pending_port:
                future.set_result(operation(selected_config, timeout_ms=timeout_ms))
            self.futures.append(future)
            return future

        def shutdown(self, *, wait, cancel_futures):
            self.shutdown_calls.append((wait, cancel_futures))
            for future in self.futures:
                if cancel_futures and not future.done():
                    future.cancel()

    monkeypatch.setattr(module, "ThreadPoolExecutor", Pool)
    responses = Endpoint.scan(
        ports, host="selected-host", transport_mode=mode, config=config, timeout_ms=1,
    )
    expected = [port for port in ports if port not in (pending_port, absent_port)]
    assert [pong.port for pong in responses] == expected
    assert [pong.control_port for pong in responses] == [port + 17 for port in expected]
    assert [endpoint.port for endpoint in probes] == [port for port in ports if port != pending_port]
    assert all(endpoint.host == "selected-host" and endpoint.transport_mode is mode for endpoint in probes)
    if ports:
        assert len(pools) == 1
        assert pools[0].max_workers == min(len(ports), 32)
        assert pools[0].shutdown_calls == [(False, True)]
        assert all(future.done() for future in pools[0].futures)
    else:
        assert pools == [] and probes == []
