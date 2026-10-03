"""Provider-free startup clocks: silence is not inactivity or readiness."""

import asyncio
import threading
from dataclasses import dataclass

import pytest

from zmqruntime.client import (
    EndpointConnectionAttempt,
    EndpointConnectionCancelledError,
    EndpointConnectionPolicy,
    EndpointProcess,
)
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import PongResponse, ProcessIdentity, ServerRole
from zmqruntime.startup import (
    IDLE_ENDPOINT_STARTUP_OBSERVER,
    EndpointStartupCancellationObserver,
    EndpointStartupPhase,
    EndpointStartupStatusMonitor,
    EndpointStartupStatusWriter,
)
from zmqruntime.timeouts import OperationCancellation, OperationDeadline
from zmqruntime.transport import TransportEndpoint


class DeclaredChild(EndpointProcess):
    identity = ProcessIdentity(123, 456.0)

    def is_alive(self):
        return True

    def exit(self):
        return None

    def wait_for_exit(self, timeout):
        return None

    def stop(self, timeout=5.0, kill_timeout=2.0):
        pytest.fail("Read-only readiness observations cannot stop a child")


@dataclass(frozen=True)
class StartupCase:
    work_until: float
    ready_at: float = 109.0
    cancel_at: float = float("inf")
    exit_at: float = float("inf")
    fail_at: float = float("inf")
    deadline_at: float = 1000.0
    descendant: bool = False
    expected_ready: bool = False
    expected_stop: float = 15.0
    observes_work: bool = True


@pytest.mark.parametrize("case", [
    StartupCase(120, expected_ready=True, expected_stop=110),
    StartupCase(120, descendant=True, expected_ready=True, expected_stop=110),
    StartupCase(120, observes_work=False),  # Exact original journal-only predecessor.
    StartupCase(0),
    StartupCase(30, expected_stop=45),
    StartupCase(120, cancel_at=25, expected_stop=25),
    StartupCase(120, exit_at=25, expected_stop=25),
    StartupCase(120, fail_at=25, expected_stop=25),
    StartupCase(120, deadline_at=25, expected_stop=25),
])
def test_same_readiness_owner_survives_cold_work_and_bounds_terminal_controls(
    monkeypatch, tmp_path, case
):
    import zmqruntime.transport as transport

    now = [0.0]
    cancellation = OperationCancellation()
    path = tmp_path / "startup.jsonl"
    writer = EndpointStartupStatusWriter(path)
    writer.emit(EndpointStartupPhase.PREPARING_CAPABILITIES,
                "Discovering registered callables")
    child = DeclaredChild()
    worker = ProcessIdentity(124, 457.0) if case.descendant else child.identity
    monkeypatch.setattr(transport.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(ProcessIdentity, "is_alive", lambda self: now[0] < case.exit_at)
    monkeypatch.setattr(ProcessIdentity, "work_snapshot", lambda self: {
        worker: min(now[0], case.work_until)
    })

    def advance(duration):
        now[0] += duration
        if now[0] >= case.cancel_at:
            cancellation.cancel()
        if now[0] >= case.fail_at:
            writer.emit(EndpointStartupPhase.FAILED, "declared startup failure")

    monkeypatch.setattr(transport.time, "sleep", advance)
    monkeypatch.setattr(TransportMode.TCP.declaration, "endpoint_in_use",
                        staticmethod(lambda *args: True))
    ready = PongResponse(port=12345, control_port=22345, ready=True,
                         server="synthetic", server_role=ServerRole.GENERIC)
    monkeypatch.setattr(TransportEndpoint, "ping", lambda *args, **kwargs:
                        ready if now[0] >= case.ready_at else None)
    monitor = EndpointStartupStatusMonitor(
        path, status_emitter=lambda *args: None,
        process_has_exited=lambda: now[0] >= case.exit_at,
    )
    observer = EndpointStartupCancellationObserver(cancellation,
        child.startup_observer(monitor) if case.observes_work else monitor)
    result = TransportEndpoint("localhost", 12345, TransportMode.TCP).wait_for_ready_response(
        ZMQConfig(), timeout=15.0, require_ready=True, poll_interval=5.0,
        startup_observer=observer,
        operation_deadline=OperationDeadline.after_milliseconds(
            int(case.deadline_at * 1000), operation="test startup"
        ),
    )
    assert (result is ready) is case.expected_ready
    assert now[0] == case.expected_stop
    assert len(path.read_text().splitlines()) == (2 if case.fail_at == 25 else 1)


def test_work_samples_reject_pid_reuse_and_keep_exact_descendant_identity(monkeypatch):
    import zmqruntime.messages as messages

    class CPU:
        user = 2.0
        system = 3.0

    class NativeProcess:
        pid = 123

        def create_time(self):
            return 456.0

        def children(self, *, recursive):
            assert recursive
            return ()

        def cpu_times(self):
            return CPU()

    native = NativeProcess()
    monkeypatch.setattr(messages.psutil, "Process", lambda pid: native)
    monkeypatch.setattr(ProcessIdentity, "descendants", lambda self: ())
    assert ProcessIdentity(123, 455.0).work_snapshot() == {}
    assert ProcessIdentity(123, 456.0).work_snapshot() == {
        ProcessIdentity(123, 456.0): 5.0
    }


def test_async_attempt_cancellation_reaps_same_worker_before_returning():
    class CancellableAttempt(EndpointConnectionAttempt):
        def __init__(self):
            self.started = threading.Event()
            self.cancelled = threading.Event()
            self.reaped = threading.Event()

        def cancel(self):
            self.cancelled.set()

        def connect(self, policy, timeout):
            self.started.set()
            assert self.cancelled.wait(timeout=1)
            self.reaped.set()
            raise EndpointConnectionCancelledError("exact attempt cancelled")

    async def exercise():
        attempt = CancellableAttempt()
        task = asyncio.create_task(attempt.connect_async(
            EndpointConnectionPolicy.ATTACH_OR_START, 15.0
        ))
        while not attempt.started.is_set():
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert attempt.reaped.is_set()

    asyncio.run(exercise())


def test_unavailable_work_does_not_manufacture_activity_on_access_return(monkeypatch):
    child = DeclaredChild()
    samples = iter(({child.identity: 3.0}, {}, {child.identity: 3.0}))
    monkeypatch.setattr(ProcessIdentity, "work_snapshot", lambda self: next(samples))
    observer = child.startup_observer(IDLE_ENDPOINT_STARTUP_OBSERVER)
    assert observer.poll_activity() is False
    assert observer.poll_activity() is False
