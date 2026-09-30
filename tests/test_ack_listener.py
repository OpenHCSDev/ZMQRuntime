"""Real-thread ACK lifecycle checks with controlled sockets, never endpoints."""

from __future__ import annotations

import errno
import queue
import threading
from unittest.mock import Mock

import pytest

from zmqruntime import ack_listener
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ImageAck
from zmqruntime.startup import EndpointStartupPhase
from zmqruntime.timeouts import OperationDeadline, OperationTimeoutError


class ControlledAckSocket:
    def __init__(self, owner):
        self.owner = owner
        self.worker = threading.get_ident()
        self.bound = threading.Event()
        self.closed = threading.Event()
        self.url = None
        self.bind_error = owner.bind_error
        self.poll_error = threading.Event()
        self.calls = []
        self.incoming = queue.Queue()
        self.received = None

    def setsockopt(self, option, value):
        self.calls.append(("setsockopt", threading.get_ident(), option, value))

    def bind(self, url):
        self.calls.append(("bind", threading.get_ident()))
        self.url = url
        self.bound.set()
        if not self.owner.release_bind.wait(1):
            raise TimeoutError("Test did not release controlled bind")
        if self.bind_error is not None:
            raise self.bind_error

    def poll(self, timeout):
        if self.poll_error.wait(min(timeout / 1000, 0.01)):
            raise ack_listener.zmq.ZMQError(errno.ETERM)
        try:
            self.received = self.incoming.get_nowait()
        except queue.Empty:
            return False
        return True

    def recv_json(self):
        self.calls.append(("recv_json", threading.get_ident()))
        return self.received

    def close(self, *, linger):
        self.calls.append(("close", threading.get_ident(), linger))
        self.closed.set()


class ControlledAckContext:
    def __init__(self, owner):
        self.controlled_socket = ControlledAckSocket(owner)
        self.terminated = threading.Event()
        self.terminating_thread = None

    def socket(self, kind):
        assert kind == ack_listener.zmq.PULL
        return self.controlled_socket

    def term(self):
        self.terminating_thread = threading.get_ident()
        self.terminated.set()


class AckLifecycleHarness:
    def __init__(self):
        self.contexts = []
        self.bind_error = None
        self.release_bind = threading.Event()
        self.release_bind.set()
        self.context_created = threading.Event()
        self.listener = None
        self.callers = []

    def make_context(self):
        context = ControlledAckContext(self)
        self.contexts.append(context)
        self.context_created.set()
        return context

    def start_in_caller_thread(self, **kwargs):
        errors = []
        returned = threading.Event()

        def start():
            try:
                self.listener.start(
                    ZMQConfig().shared_ack_port,
                    transport_mode=TransportMode.TCP,
                    timeout_ms=750,
                    **kwargs,
                )
            except Exception as error:
                errors.append(error)
            finally:
                returned.set()

        caller = threading.Thread(target=start, daemon=True)
        self.callers.append(caller)
        caller.start()
        return caller, returned, errors


@pytest.fixture
def harness(monkeypatch):
    controlled = AckLifecycleHarness()
    monkeypatch.setattr(ack_listener.GlobalAckListener, "_instance", None)
    monkeypatch.setattr(ack_listener.zmq, "Context", controlled.make_context)
    monkeypatch.setattr(ack_listener, "logger", Mock())
    controlled.listener = ack_listener.GlobalAckListener()
    yield controlled
    controlled.release_bind.set()
    controlled.listener.stop(timeout_ms=1000)
    for caller in controlled.callers:
        caller.join(timeout=1)
        assert not caller.is_alive(), "Controlled caller leaked"
    for context in controlled.contexts:
        assert context.terminated.wait(1), "Controlled context leaked"
        socket = context.controlled_socket
        assert socket.closed.is_set()
        assert context.terminating_thread == socket.worker
        assert all(call[1] == socket.worker for call in socket.calls)


def start(listener, **kwargs):
    config = ZMQConfig()
    listener.start(
        config.shared_ack_port,
        transport_mode=TransportMode.TCP,
        config=config,
        timeout_ms=750,
        **kwargs,
    )


def test_bind_failure_is_original_error_after_worker_cleanup_and_explicit_retry(harness):
    listener = harness.listener
    collision = ack_listener.zmq.ZMQError(errno.EADDRINUSE)
    harness.bind_error = collision
    with pytest.raises(ack_listener.zmq.ZMQError) as caught:
        start(listener)
    assert caught.value is collision
    assert not listener._running
    assert listener.startup_status.phase is EndpointStartupPhase.FAILED
    assert harness.contexts[0].terminated.is_set()
    assert listener._thread is None

    harness.bind_error = None
    start(listener)
    assert listener._running
    assert listener.startup_status.phase is EndpointStartupPhase.CONNECTED
    assert len(harness.contexts) == 2


def test_bind_readiness_wait_releases_lock_and_stop_joins_worker(harness):
    harness.release_bind.clear()
    caller, returned, errors = harness.start_in_caller_thread()
    assert harness.context_created.wait(1)
    assert harness.contexts[0].controlled_socket.bound.wait(1)
    assert harness.listener.startup_status.phase is EndpointStartupPhase.BINDING_ENDPOINT
    assert not harness.listener._running
    assert not returned.is_set()
    harness.release_bind.set()
    assert returned.wait(1), "Startup/loop lock inversion"
    caller.join(timeout=1)
    assert not errors
    assert harness.listener._running
    harness.listener.stop(timeout_ms=750)
    assert not harness.listener._running
    assert harness.contexts[0].terminated.is_set()
    assert harness.listener._thread is None


def test_same_endpoint_reuse_and_different_endpoint_rejection(harness):
    start(harness.listener)
    start(harness.listener)
    assert len(harness.contexts) == 1
    with pytest.raises(ValueError, match="different endpoint"):
        harness.listener.start(
            ZMQConfig().shared_ack_port + 1,
            transport_mode=TransportMode.TCP,
            timeout_ms=750,
        )
    assert harness.listener._running
    assert len(harness.contexts) == 1


def test_fatal_receive_clears_readiness_and_releases_resources(harness):
    start(harness.listener)
    context = harness.contexts[0]
    context.controlled_socket.poll_error.set()
    assert context.terminated.wait(1)
    # Join through the public owner to synchronize final lifecycle publication.
    harness.listener.stop(timeout_ms=750)
    assert not harness.listener._running
    assert harness.listener.startup_status.phase is EndpointStartupPhase.FAILED


def test_stop_during_bind_is_finite_and_never_publishes_readiness(harness):
    harness.release_bind.clear()
    caller, returned, errors = harness.start_in_caller_thread()
    assert harness.context_created.wait(1)
    assert harness.contexts[0].controlled_socket.bound.wait(1)
    with pytest.raises(OperationTimeoutError):
        harness.listener.stop(timeout_ms=20)
    assert not harness.listener._running
    harness.release_bind.set()
    assert returned.wait(1)
    caller.join(timeout=1)
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "cancelled before readiness" in str(errors[0])
    harness.listener.stop(timeout_ms=750)
    assert not harness.listener._running
    assert harness.listener._thread is None


def test_joining_caller_timeout_does_not_cancel_existing_start_owner(harness):
    harness.release_bind.clear()
    caller, returned, errors = harness.start_in_caller_thread()
    assert harness.context_created.wait(1)
    assert harness.contexts[0].controlled_socket.bound.wait(1)
    with pytest.raises(OperationTimeoutError):
        harness.listener.start(
            ZMQConfig().shared_ack_port,
            transport_mode=TransportMode.TCP,
            timeout_ms=20,
        )
    assert harness.listener.startup_status.phase is EndpointStartupPhase.BINDING_ENDPOINT
    harness.release_bind.set()
    assert returned.wait(1)
    caller.join(timeout=1)
    assert not errors
    assert harness.listener._running


def test_expired_deadline_has_no_context_or_thread_side_effect(harness):
    with pytest.raises(OperationTimeoutError):
        start(
            harness.listener,
            operation_deadline=OperationDeadline("already expired", 1, 0),
        )
    assert not harness.contexts
    assert harness.listener._thread is None


def test_context_creation_failure_is_reported_without_stale_running_state(harness, monkeypatch):
    failure = RuntimeError("controlled context creation failure")
    monkeypatch.setattr(ack_listener.zmq, "Context", Mock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        start(harness.listener)
    assert caught.value is failure
    assert harness.listener._thread is None
    assert not harness.listener._running


def test_thread_launch_failure_is_reported_without_context_allocation(harness, monkeypatch):
    failure = RuntimeError("controlled thread launch failure")
    thread = Mock()
    thread.start.side_effect = failure
    monkeypatch.setattr(ack_listener.threading, "Thread", Mock(return_value=thread))
    with pytest.raises(RuntimeError) as caught:
        start(harness.listener)
    assert caught.value is failure
    assert not harness.contexts
    assert harness.listener._thread is None
    assert not harness.listener._running


def test_thread_construction_failure_has_terminal_status(harness, monkeypatch):
    failure = RuntimeError("controlled thread construction failure")
    monkeypatch.setattr(ack_listener.threading, "Thread", Mock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        start(harness.listener)
    assert caught.value is failure
    assert harness.listener.startup_status.phase is EndpointStartupPhase.FAILED
    assert harness.listener._thread is None
    assert not harness.contexts


def test_start_owner_timeout_cancels_only_its_attempt_then_allows_explicit_retry(harness):
    harness.release_bind.clear()
    with pytest.raises(OperationTimeoutError):
        harness.listener.start(
            ZMQConfig().shared_ack_port,
            transport_mode=TransportMode.TCP,
            timeout_ms=20,
        )
    assert not harness.listener._running
    assert harness.listener.startup_status.phase is EndpointStartupPhase.FAILED
    with pytest.raises(RuntimeError, match="still stopping"):
        start(harness.listener)
    harness.release_bind.set()
    harness.listener.stop(timeout_ms=750)
    start(harness.listener)
    assert harness.listener._running
    assert len(harness.contexts) == 2


def test_typed_ack_callback_can_stop_its_own_worker_without_self_join(harness):
    received = []

    def stop_from_callback(ack):
        received.append(ack)
        harness.listener.stop(timeout_ms=750)

    harness.listener.register_callback(stop_from_callback)
    start(harness.listener)
    context = harness.contexts[0]
    ack = ImageAck("controlled-image", ZMQConfig().shared_ack_port, "controlled-viewer")
    # A rejected malformed frame must not kill the healthy listener or introduce
    # another decoder. The subsequent valid record uses the original wire owner.
    context.controlled_socket.incoming.put({})
    context.controlled_socket.incoming.put(ack.to_dict())
    assert context.terminated.wait(1)
    harness.listener.stop(timeout_ms=750)
    assert received == [ack]
    assert not harness.listener._running
    assert harness.listener.startup_status.phase is EndpointStartupPhase.DISCONNECTED


def test_stop_before_listener_thread_launch_is_bounded_without_joining_unstarted_thread(
    harness, monkeypatch
):
    before_launch = threading.Event()
    release_launch = threading.Event()
    original_start = threading.Thread.start

    def gated_start(thread):
        if thread.name == "AckListener":
            before_launch.set()
            if not release_launch.wait(1):
                raise TimeoutError("Test did not release listener launch")
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", gated_start)
    caller, returned, errors = harness.start_in_caller_thread()
    try:
        assert before_launch.wait(1)
        with pytest.raises(OperationTimeoutError):
            harness.listener.stop(timeout_ms=20)
        assert not harness.listener._running
    finally:
        release_launch.set()
    assert returned.wait(1)
    caller.join(timeout=1)
    assert len(errors) == 1
    assert "cancelled before startup" in str(errors[0])
    harness.listener.stop(timeout_ms=750)
    assert harness.listener._thread is None
    assert not harness.contexts
