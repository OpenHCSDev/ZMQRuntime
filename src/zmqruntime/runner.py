"""Generic server run loop utilities."""
from __future__ import annotations

import signal
import time
from typing import Callable, Optional


def serve_forever(server, poll_interval: float = 0.01, handle_signals: bool = True,
                  on_shutdown: Optional[Callable[[], None]] = None) -> None:
    """Start a ZMQ server and process messages until shutdown.

    Args:
        server: Any object with start(), stop(), is_running(), process_messages()
        poll_interval: Sleep interval between message polls (seconds)
        handle_signals: If True, register SIGINT/SIGTERM to stop the server
        on_shutdown: Optional callback invoked after server stops
    """
    def _signal_handler(sig, frame):
        server.stop()

    if handle_signals:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    server.start()

    try:
        while server.is_running():
            server.process_messages()
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        server.stop()
    finally:
        if on_shutdown:
            on_shutdown()
