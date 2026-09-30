"""Owned real-process/socket acceptance of the producer return contract."""

from dataclasses import replace
from multiprocessing import get_context
from threading import Event
from uuid import uuid4

import pytest
import zmq

from zmqruntime.ack_listener import GlobalAckListener
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ImageAck, ImageTransferIdentity
from zmqruntime.queue_tracker import GlobalQueueTrackerRegistry
from zmqruntime.streaming.server import StreamingVisualizerServer


class InfrastructureViewer(StreamingVisualizerServer):
    def display_image(self, image_data, metadata):
        pass

    def handle_control_message(self, message):
        raise NotImplementedError


def producer(connection, mode, config, image_id):
    listener = GlobalAckListener()
    tracker = GlobalQueueTrackerRegistry().get_or_create_tracker(12345, "fixture")
    tracker.register_sent(image_id)
    received = Event()
    listener.register_callback(lambda ack: received.set())
    try:
        route = listener.start(transport_mode=mode, host="127.0.0.1", config=config)
        connection.send(ImageTransferIdentity(image_id, route).to_dict())
        while connection.poll(5):
            command = connection.recv()
            if command == "close":
                break
            if command == "progress":
                connection.send((tracker.get_progress(), tracker.get_pending_count()))
            if command == "wait":
                connection.send((received.wait(2), tracker.get_progress()))
                received.clear()
    finally:
        listener.stop(timeout_ms=2000)
        connection.close()


def receive(connection):
    assert connection.poll(4), "Owned producer did not respond within its bound"
    return connection.recv()


@pytest.mark.parametrize("mode", [TransportMode.TCP, TransportMode.IPC])
def test_two_producers_return_exact_ids_and_close_independently(mode, tmp_path_factory):
    if not mode.declaration.is_supported():
        pytest.skip("Transport unavailable on this platform")
    tmp_path = tmp_path_factory.mktemp("ipc")
    config = ZMQConfig(ipc_socket_dir=str(tmp_path))
    context = get_context("spawn")
    owners = []
    viewer = InfrastructureViewer(12345, "fixture", transport_mode=mode, config=config)
    try:
        for image_id in ("producer-a", "producer-b"):
            parent, child = context.Pipe()
            process = context.Process(target=producer, args=(child, mode, config, image_id))
            process.start()
            child.close()
            owners.append((process, parent))
        transfers = [ImageTransferIdentity.from_dict(receive(pipe)) for _, pipe in owners]
        a, b = transfers
        assert a.return_route.url != b.return_route.url
        assert a.return_route.owner != b.return_route.owner
        # A wrong-ID ACK on the correct route cannot complete a pending image.
        assert viewer.send_ack(replace(b, return_route=a.return_route))
        owners[0][1].send("wait")
        assert receive(owners[0][1]) == (True, (0, 1))
        assert viewer.send_ack(a)
        owners[0][1].send("wait")
        assert receive(owners[0][1]) == (True, (1, 1))
        assert viewer.send_ack(a)  # duplicate
        owners[0][1].send("progress")
        assert receive(owners[0][1]) == ((1, 1), 0)
        owners[1][1].send("progress")
        assert receive(owners[1][1]) == ((0, 1), 1)
        owners[0][1].send("close")
        owners[0][0].join(3)
        assert owners[0][0].exitcode == 0
        assert viewer.send_ack(b)
        owners[1][1].send("wait")
        assert receive(owners[1][1]) == (True, (1, 1))
        owners[1][1].send("close")
        owners[1][0].join(3)
        assert owners[1][0].exitcode == 0
        if mode is TransportMode.IPC:
            assert not any(tmp_path.glob("*.sock"))
    finally:
        for process, pipe in owners:
            if process.is_alive():
                try:
                    pipe.send("close")
                except BrokenPipeError:
                    pass
                process.join(3)
            if process.is_alive():
                process.terminate()
                process.join(2)
            pipe.close()
            assert not process.is_alive()


def test_stale_incarnation_unknown_late_and_explicit_worker_accounting(tmp_path):
    listener = GlobalAckListener()
    config = ZMQConfig(ipc_socket_dir=str(tmp_path))
    tracker = GlobalQueueTrackerRegistry().get_or_create_tracker(23456, "fixture")
    tracker.clear()
    tracker.register_sent("pending")
    received = Event()
    listener.register_callback(lambda ack: received.set())
    viewer = InfrastructureViewer(23456, "fixture", transport_mode=TransportMode.TCP)
    try:
        route = listener.start(transport_mode=TransportMode.TCP, config=config)
        stale = replace(route, incarnation=str(uuid4()))
        assert viewer.send_ack(ImageTransferIdentity("pending", stale))
        assert not received.wait(0.1)
        assert tracker.get_progress() == (0, 1)
        assert viewer.send_ack(ImageTransferIdentity("unknown", route))
        assert received.wait(2)
        assert tracker.get_progress() == (0, 1)
        tracker.reset_for_new_batch()
        received.clear()
        assert viewer.send_ack(ImageTransferIdentity("pending", route))
        assert received.wait(2)
        assert tracker.get_progress() == (0, 0)
        # Deliberate cross-worker accounting has an explicit admitted entrypoint.
        worker = replace(route.owner, pid=route.owner.pid + 1)
        tracker.register_worker_processed("worker-result", worker)
        assert tracker.get_progress() == (0, 0)
        tracker.register_worker(worker)
        tracker.register_worker_processed("worker-result", worker)
        tracker.register_worker_processed("worker-result", worker)
        assert tracker.get_progress() == (1, 1)
    finally:
        listener.stop(timeout_ms=2000)
        GlobalQueueTrackerRegistry().remove_tracker(23456)


def test_required_return_route_has_no_legacy_reader():
    with pytest.raises(KeyError, match="return_route"):
        ImageTransferIdentity.from_dict({"image_id": "missing-route"})
    with pytest.raises(ValueError, match="return_route"):
        ImageAck.from_dict(dict(type="image_ack", image_id="missing", viewer_port=1,
                               viewer_type="fixture"))


def test_untracked_item_does_not_open_an_ack_socket(monkeypatch):
    def forbidden_context():
        pytest.fail("Untracked item opened a transport context")

    monkeypatch.setattr(zmq.Context, "instance", forbidden_context)
    assert not InfrastructureViewer(12345, "fixture").send_ack(None)


@pytest.mark.parametrize("mode", [TransportMode.TCP, TransportMode.IPC])
def test_occupied_explicit_destination_fails_without_stealing_owner(mode, tmp_path_factory):
    if not mode.declaration.is_supported():
        pytest.skip("Transport unavailable on this platform")
    config = ZMQConfig(ipc_socket_dir=str(tmp_path_factory.mktemp("ack")))
    context = zmq.Context()
    owner = context.socket(zmq.PULL)
    sender = context.socket(zmq.PUSH)
    listener = GlobalAckListener()
    port = 44556 if mode is TransportMode.IPC else 0
    try:
        port = mode.declaration.bind_socket(owner, "127.0.0.1", port, config)
        url = mode.declaration.endpoint_url(port, "127.0.0.1", config)
        with pytest.raises(zmq.ZMQError):
            listener.start(port=port, transport_mode=mode, host="127.0.0.1",
                           config=config, timeout_ms=2000)
        assert not listener.startup_status.phase.accepts_requests
        with pytest.raises(RuntimeError, match="no ready return route"):
            _ = listener.return_route
        sender.setsockopt(zmq.LINGER, 0)
        sender.setsockopt(zmq.SNDTIMEO, 1000)
        sender.connect(url)
        sender.send_json({"owned": "still-here"})
        assert owner.poll(1000)
        assert owner.recv_json() == {"owned": "still-here"}
    finally:
        listener.stop(timeout_ms=2000)
        sender.close(0)
        owner.close(0)
        context.term()
        mode.declaration.cleanup_endpoint(port, config)


def delegated_worker(pipe, route):
    transfer = ImageTransferIdentity("delegated-image", route)
    pipe.send(transfer.to_dict())
    assert pipe.poll(3)
    assert pipe.recv() == "ack"
    viewer = InfrastructureViewer(23457, "fixture", transport_mode=TransportMode.TCP)
    assert viewer.send_ack(transfer)
    assert viewer.send_ack(transfer)  # The same worker's duplicate stays idempotent.
    pipe.close()


def test_admitted_worker_receipt_is_accounted_through_real_listener():
    listener = GlobalAckListener()
    registry = GlobalQueueTrackerRegistry()
    tracker = registry.get_or_create_tracker(23457, "fixture")
    received = Event()
    listener.register_callback(lambda ack: received.set())
    context = get_context("spawn")
    parent, child = context.Pipe()
    process = None
    try:
        route = listener.start(transport_mode=TransportMode.TCP)
        process = context.Process(target=delegated_worker, args=(child, route))
        process.start()
        child.close()
        transfer = ImageTransferIdentity.from_dict(receive(parent))
        assert transfer.producer != route.owner
        tracker.register_worker(transfer.producer)
        parent.send("ack")
        assert received.wait(2)
        process.join(3)
        assert process.exitcode == 0
        assert tracker.get_progress() == (1, 1)
        tracker.reset_for_new_batch()
        tracker.register_worker_processed(transfer.image_id, transfer.producer)
        assert tracker.get_progress() == (0, 0)  # Old delegation cannot reopen a batch.
    finally:
        if process is not None and process.is_alive():
            process.terminate()
            process.join(2)
        parent.close()
        listener.stop(timeout_ms=2000)
        registry.remove_tracker(23457)
