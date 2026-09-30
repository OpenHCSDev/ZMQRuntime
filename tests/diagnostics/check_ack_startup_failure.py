"""Provider-free issue 10 readiness diagnostic; exits nonzero until fixed.

Run with the existing interpreter. This is deliberately not pytest-discovered:
it is a visible failing acceptance witness, not an xfail or a passing fix claim.
The real ACK startup/loop runs against a controlled failing socket and inline
test thread. No real endpoint, listener thread, native process or viewer opens.
"""

from __future__ import annotations

import errno
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch


SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(SOURCE_ROOT))

from zmqruntime import ack_listener
from zmqruntime.config import TransportMode, ZMQConfig


if not Path(ack_listener.__file__).resolve().is_relative_to(SOURCE_ROOT):
    raise RuntimeError("Diagnostic must exercise this worktree's ACK listener")


class AckStartupFailureDiagnostic(unittest.TestCase):
    def setUp(self) -> None:
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(ack_listener.GlobalAckListener, "_instance", None))
        stack.enter_context(patch.object(ack_listener, "logger", Mock()))
        self.socket = Mock()
        self.socket.bind.side_effect = ack_listener.zmq.ZMQError(errno.EADDRINUSE)
        self.context = Mock()
        self.context.socket.return_value = self.socket
        self.context_factory = stack.enter_context(
            patch.object(ack_listener.zmq, "Context", return_value=self.context)
        )
        self.thread_factory = stack.enter_context(
            patch.object(ack_listener.threading, "Thread", side_effect=self._inline_thread)
        )
        self.listener = ack_listener.GlobalAckListener()
        self.config = ZMQConfig()

    def _inline_thread(self, *, target, **options):
        thread = Mock()
        thread.start.side_effect = target
        return thread

    def _start(self) -> None:
        self.listener.start(
            self.config.shared_ack_port,
            transport_mode=TransportMode.TCP,
            config=self.config,
        )

    def tearDown(self) -> None:
        self.listener.stop()

    def test_failed_bind_is_reported_to_start_caller(self) -> None:
        with self.assertRaises(ack_listener.zmq.ZMQError):
            self._start()

    def test_failed_listener_does_not_claim_running(self) -> None:
        try:
            self._start()
        except ack_listener.zmq.ZMQError:
            pass
        self.assertFalse(self.listener._running)

    def test_controlled_reproducer_releases_its_socket_and_context(self) -> None:
        try:
            self._start()
        except ack_listener.zmq.ZMQError:
            pass
        self.context_factory.assert_called_once_with()
        self.context.socket.assert_called_once_with(ack_listener.zmq.PULL)
        self.thread_factory.assert_called_once()
        self.socket.bind.assert_called_once_with(
            f"tcp://*:{self.config.shared_ack_port}"
        )
        self.socket.close.assert_called_once_with()
        self.context.term.assert_called_once_with()


if __name__ == "__main__":
    unittest.main(verbosity=2)
