from dataclasses import dataclass

from zmqruntime.progress import LatestEventRegistry


@dataclass(frozen=True)
class _Event:
    plate_id: str
    axis_id: str
    channel: str
    timestamp: float
    terminal: bool = False


def test_latest_event_registry_keeps_channels_separate():
    registry = LatestEventRegistry[
        _Event,
        tuple[str, str, str],
    ](
        key_builder=lambda event: (event.plate_id, event.axis_id, event.channel),
        is_terminal=lambda event: event.terminal,
        timestamp_of=lambda event: event.timestamp,
    )

    registry.register_event(
        "exec-1", _Event("plate-1", "A01", "pipeline", timestamp=1.0)
    )
    registry.register_event(
        "exec-1", _Event("plate-1", "A01", "step", timestamp=2.0)
    )

    events = registry.get_events("exec-1")
    assert len(events) == 2
    assert {event.channel for event in events} == {"pipeline", "step"}


def test_latest_event_registry_cleanup_removes_old_terminal_executions():
    registry = LatestEventRegistry[
        _Event,
        tuple[str, str, str],
    ](
        key_builder=lambda event: (event.plate_id, event.axis_id, event.channel),
        is_terminal=lambda event: event.terminal,
        timestamp_of=lambda event: event.timestamp,
    )

    registry.register_event(
        "exec-1",
        _Event("plate-1", "A01", "pipeline", timestamp=1.0, terminal=True),
    )
    registry.register_event(
        "exec-2",
        _Event("plate-1", "A02", "pipeline", timestamp=100.0, terminal=False),
    )

    removed = registry.cleanup_old_executions(retention_seconds=10.0)

    assert removed == 1
    assert registry.get_execution_ids() == ["exec-2"]
