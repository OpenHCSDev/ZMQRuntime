"""Request-local observers use the original client status emission mechanism."""

import asyncio

import pytest

from zmqruntime.client import ZMQClient
from zmqruntime.startup import EndpointStartupPhase, EndpointStartupStatus


class DeclaredClient(ZMQClient):
    def _spawn_server_process(self):
        raise AssertionError("Source feedback experiment must not launch a child")

    def send_data(self, data):
        raise AssertionError("Source feedback experiment must not send data")


class StatusAudit(ZMQClient):
    """Independent same-hook capability; C3 places the shared emitter last."""

    def _emit_connection_status(self, phase, message):
        self.audit.append((phase, message))
        return super()._emit_connection_status(phase, message)


class AuditBefore(StatusAudit, DeclaredClient):
    pass


class AuditAfter(DeclaredClient, StatusAudit):
    pass


@pytest.mark.parametrize("client_type", (AuditBefore, AuditAfter))
def test_same_hook_capability_and_explicit_ui_observer_receive_one_original_status(client_type):
    ui, outer, inner = [], [], []
    client = client_type(5555, connection_status_callback=ui.append)
    client.audit = []
    # Constructed before binding, like a reused function-catalog client.
    with EndpointStartupStatus.callback_scope(outer.append):
        client._emit_connection_status(EndpointStartupPhase.IMPORTING_RUNTIME, "Importing")
        with EndpointStartupStatus.callback_scope(inner.append):
            client._emit_connection_status(EndpointStartupPhase.PREPARING_CAPABILITIES, "Preparing")
        client._emit_connection_status(EndpointStartupPhase.CONNECTED, "Typed ready")
    client._emit_connection_status(EndpointStartupPhase.DISCONNECTED, "Closed")
    assert [status.sequence for status in ui] == [1, 2, 3, 4]
    assert outer == [ui[0], ui[2]]
    assert inner == [ui[1]]
    assert len(client.audit) == 4
    assert all(status is original for status, original in zip(outer, (ui[0], ui[2])))


def test_scope_resets_on_exception_and_does_not_duplicate_identical_callback():
    events = []
    callback = events.append
    client = DeclaredClient(5555, connection_status_callback=callback)
    with pytest.raises(ValueError, match="terminal"):
        with EndpointStartupStatus.callback_scope(callback):
            client._emit_connection_status(EndpointStartupPhase.FAILED, "Terminal")
            raise ValueError("terminal")
    client._emit_connection_status(EndpointStartupPhase.DISCONNECTED, "No scoped observer")
    assert [status.sequence for status in events] == [1, 2]


def test_concurrent_requests_and_to_thread_delivery_are_isolated():
    client = DeclaredClient(5555)

    async def request(label):
        events = []
        with EndpointStartupStatus.callback_scope(events.append):
            await asyncio.sleep(0)
            await asyncio.to_thread(
                client._emit_connection_status,
                EndpointStartupPhase.PREPARING_CAPABILITIES,
                label,
            )
        return events

    async def exercise():
        return await asyncio.gather(request("first"), request("second"))

    first, second = asyncio.run(exercise())
    assert [status.message for status in first] == ["first"]
    assert [status.message for status in second] == ["second"]
