"""Same-process endpoints must close without signalling their calling host."""

from dataclasses import replace
from unittest.mock import Mock

import pytest

from zmqruntime.client import (
    EndpointShutdownMode,
    _EndpointShutdownOperation,
    _OwnedProcessShutdownOperation,
)
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ProcessIdentity
from zmqruntime.timeouts import OperationDeadline
from zmqruntime.transport import TransportEndpoint


@pytest.mark.parametrize("timeout", [0.0, 0.5])
def test_exact_process_owner_refuses_to_signal_its_own_incarnation(monkeypatch, timeout):
    identity = ProcessIdentity.current()
    process = Mock(pid=identity.pid)
    process.create_time.return_value = identity.create_time
    process.is_running.return_value = True
    monkeypatch.setattr("zmqruntime.messages.psutil.Process", lambda *_: process)

    assert identity.terminate(timeout=timeout) is False
    process.terminate.assert_not_called()
    process.kill.assert_not_called()
    process.wait.assert_not_called()


@pytest.mark.parametrize("occupied", [False, True])
def test_in_process_completion_keeps_host_exit_and_endpoint_disposition_distinct(
    monkeypatch, occupied
):
    identity = ProcessIdentity.current()
    target = TransportEndpoint("127.0.0.1", 47777, TransportMode.TCP)
    monkeypatch.setattr(TransportEndpoint, "ping", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        TransportEndpoint, "occupied_ports",
        lambda *_: frozenset((47777,)) if occupied else frozenset(),
    )
    operation = _EndpointShutdownOperation(
        target=target,
        deadline=OperationDeadline.after_milliseconds(100, operation="source close"),
        config=ZMQConfig(),
        process_identity=identity,
        acknowledged=True,
        request_attempted=True,
    )
    # Safety boundary for the red experiment; this must never send a real signal.
    terminate = Mock(return_value=False)
    monkeypatch.setattr(ProcessIdentity, "terminate", terminate)
    result = operation.termination_result()

    assert result.succeeded is not occupied
    assert result.endpoint_terminated is not occupied
    assert result.process_exited is False
    assert result.process_identity == identity
    assert result.request_attempted and result.acknowledged


def test_current_identity_query_distinguishes_reused_pid():
    identity = ProcessIdentity.current()
    assert identity.is_current()
    assert not replace(identity, create_time=identity.create_time - 1).is_current()


@pytest.mark.parametrize("alive", [False, True])
def test_new_audit_capability_cooperates_with_owned_exit_hook(monkeypatch, alive):
    observations = []

    class ExitAudit:
        def _process_exit_satisfied(self, exited):
            satisfied = super()._process_exit_satisfied(exited)
            observations.append((exited, satisfied))
            return satisfied

    class AuditedOwnedClose(ExitAudit, _OwnedProcessShutdownOperation):
        pass

    identity = ProcessIdentity.current()
    target = TransportEndpoint("127.0.0.1", 47777, TransportMode.TCP)
    monkeypatch.setattr(ProcessIdentity, "is_alive", lambda _self: alive)
    monkeypatch.setattr(ProcessIdentity, "terminate", Mock(return_value=False))
    monkeypatch.setattr(TransportEndpoint, "ping", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(TransportEndpoint, "occupied_ports", lambda *_: frozenset())
    monkeypatch.setattr(TransportEndpoint, "cleanup_stale_addresses", Mock())
    operation = AuditedOwnedClose(
        target=target,
        deadline=OperationDeadline.after_milliseconds(100, operation="new capability"),
        config=ZMQConfig(),
        process_identity=identity,
        acknowledged=True,
        request_attempted=True,
    )

    # The original member-owned completion uses the shared traversal and the
    # new capability's real C3 hook without edits to that generic consumer.
    result = EndpointShutdownMode.FORCE.complete(operation)
    assert result.succeeded is not alive
    assert result.endpoint_terminated
    assert result.process_exited is not alive
    assert observations == [(not alive, not alive)]
