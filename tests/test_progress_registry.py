from dataclasses import dataclass

from zmqruntime.progress import (
    EventRegistryMutationKind,
    LatestEventRegistry,
)


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

    registry.register_event("exec-1", _Event("plate-1", "A01", "pipeline", timestamp=1.0))
    registry.register_event("exec-1", _Event("plate-1", "A01", "step", timestamp=2.0))

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


def test_latest_event_registry_rejects_stale_or_duplicate_channel_events():
    registry = LatestEventRegistry[
        _Event,
        tuple[str, str, str],
    ](
        key_builder=lambda event: (event.plate_id, event.axis_id, event.channel),
        is_terminal=lambda event: event.terminal,
        timestamp_of=lambda event: event.timestamp,
    )
    listener_events = []
    registry.add_listener(lambda execution_id, event: listener_events.append((execution_id, event)))

    newest = _Event("plate-1", "A01", "pipeline", timestamp=2.0)
    stale = _Event("plate-1", "A01", "pipeline", timestamp=1.0)

    assert registry.register_event("exec-1", newest) is True
    assert registry.register_event("exec-1", stale) is False
    assert registry.register_event("exec-1", newest) is False
    assert registry.get_events("exec-1") == [newest]
    assert listener_events == [("exec-1", newest)]


def test_registry_mutation_listener_observes_register_and_all_clear_routes():
    registry = LatestEventRegistry[
        _Event,
        tuple[str, str, str],
    ](
        key_builder=lambda event: (event.plate_id, event.axis_id, event.channel),
        is_terminal=lambda event: event.terminal,
        timestamp_of=lambda event: event.timestamp,
    )
    mutations = []
    registry.add_mutation_listener(mutations.append)

    event_one = _Event("plate-1", "A01", "pipeline", timestamp=1.0)
    event_two = _Event("plate-2", "A01", "pipeline", timestamp=1.0)
    registry.register_event("exec-1", event_one)
    registry.register_event("exec-2", event_two)
    registry.clear_execution("exec-1")
    registry.clear_all()
    event_three = _Event("plate-3", "A01", "pipeline", timestamp=2.0)
    registry.register_event("exec-3", event_three)

    assert [mutation.kind for mutation in mutations] == [
        EventRegistryMutationKind.REGISTERED,
        EventRegistryMutationKind.REGISTERED,
        EventRegistryMutationKind.CLEARED,
        EventRegistryMutationKind.CLEARED,
        EventRegistryMutationKind.REGISTERED,
    ]
    assert [mutation.execution_id for mutation in mutations] == [
        "exec-1",
        "exec-2",
        "exec-1",
        None,
        "exec-3",
    ]


def test_cleanup_notifies_mutation_listener_for_each_removed_execution():
    registry = LatestEventRegistry[
        _Event,
        tuple[str, str, str],
    ](
        key_builder=lambda event: (event.plate_id, event.axis_id, event.channel),
        is_terminal=lambda event: event.terminal,
        timestamp_of=lambda event: event.timestamp,
    )
    mutations = []
    registry.add_mutation_listener(mutations.append)
    registry.register_event(
        "exec-1",
        _Event("plate-1", "A01", "pipeline", timestamp=1.0, terminal=True),
    )
    mutations.clear()

    assert registry.cleanup_old_executions(retention_seconds=10.0) == 1
    assert len(mutations) == 1
    assert mutations[0].kind is EventRegistryMutationKind.CLEARED
    assert mutations[0].execution_id == "exec-1"
