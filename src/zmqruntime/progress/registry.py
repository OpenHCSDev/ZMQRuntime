"""Generic latest-event registry keyed by semantic channels."""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Generic, List, Optional, TypeVar

from zmqruntime.subscription import CallbackSubscription, SubscriptionABC

TEvent = TypeVar("TEvent")
TKey = TypeVar("TKey")
logger = logging.getLogger(__name__)


class EventRegistryMutationKind(Enum):
    """Closed mutation vocabulary for latest-event registries."""

    REGISTERED = "registered"
    CLEARED = "cleared"


@dataclass(frozen=True, slots=True)
class EventRegistryMutation(Generic[TEvent]):
    """One accepted registry mutation delivered after the registry lock releases."""

    kind: EventRegistryMutationKind
    execution_id: str | None
    event: TEvent | None = None


class EventRegistryABC(ABC, Generic[TEvent]):
    """Nominal contract for latest-event registries."""

    @abstractmethod
    def register_event(self, execution_id: str, event: TEvent) -> bool:
        """Register one event when it is newer than the retained event."""

    @abstractmethod
    def get_events(self, execution_id: str) -> List[TEvent]:
        """Return latest event set for one execution id."""

    @abstractmethod
    def get_execution_ids(self) -> List[str]:
        """Return execution ids currently tracked."""

    @abstractmethod
    def clear_execution(self, execution_id: str) -> None:
        """Remove one execution id from the registry."""

    @abstractmethod
    def clear_all(self) -> None:
        """Drop all events while preserving registered listeners."""

    @abstractmethod
    def subscribe_mutations(
        self,
        listener: Callable[[EventRegistryMutation[TEvent]], None],
    ) -> SubscriptionABC:
        """Return the owned registration for one mutation listener."""


class LatestEventRegistry(EventRegistryABC[TEvent], Generic[TEvent, TKey]):
    """Thread-safe registry that keeps one latest event per semantic key."""

    def __init__(
        self,
        *,
        key_builder: Callable[[TEvent], TKey],
        is_terminal: Callable[[TEvent], bool],
        timestamp_of: Callable[[TEvent], float],
        retention_seconds: float = 60.0,
    ) -> None:
        self._key_builder = key_builder
        self._is_terminal = is_terminal
        self._timestamp_of = timestamp_of
        self._retention_seconds = retention_seconds
        self._events: Dict[str, Dict[TKey, TEvent]] = {}
        self._listeners: List[Callable[[str, TEvent], None]] = []
        self._mutation_listeners: List[Callable[[EventRegistryMutation[TEvent]], None]] = []
        self._lock = threading.Lock()

    def register_event(self, execution_id: str, event: TEvent) -> bool:
        listeners: List[Callable[[str, TEvent], None]]
        mutation_listeners: List[Callable[[EventRegistryMutation[TEvent]], None]]
        with self._lock:
            event_dict = self._events.setdefault(execution_id, {})
            event_key = self._key_builder(event)
            current = event_dict.get(event_key)
            if current is not None and self._timestamp_of(event) <= self._timestamp_of(current):
                return False
            event_dict[event_key] = event
            listeners = list(self._listeners)
            mutation_listeners = list(self._mutation_listeners)

        for event_listener in listeners:
            self._notify_listener(event_listener, execution_id, event)
        mutation: EventRegistryMutation[TEvent] = EventRegistryMutation(
            kind=EventRegistryMutationKind.REGISTERED,
            execution_id=execution_id,
            event=event,
        )
        for mutation_listener in mutation_listeners:
            self._notify_mutation_listener(mutation_listener, mutation)
        return True

    def get_events(self, execution_id: str) -> List[TEvent]:
        with self._lock:
            events = list(self._events.get(execution_id, {}).values())
        return sorted(events, key=self._timestamp_of)

    def get_latest_event(self, execution_id: str) -> Optional[TEvent]:
        events = self.get_events(execution_id)
        return max(events, key=self._timestamp_of) if events else None

    def add_listener(self, listener: Callable[[str, TEvent], None]) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_listener(self, listener: Callable[[str, TEvent], None]) -> bool:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)
                return True
            return False

    def clear_listeners(self) -> None:
        with self._lock:
            self._listeners.clear()

    def add_mutation_listener(
        self,
        listener: Callable[[EventRegistryMutation[TEvent]], None],
    ) -> None:
        """Subscribe to accepted registrations and event-clearing mutations."""

        with self._lock:
            if listener not in self._mutation_listeners:
                self._mutation_listeners.append(listener)

    def subscribe_mutations(
        self,
        listener: Callable[[EventRegistryMutation[TEvent]], None],
    ) -> SubscriptionABC:
        """Return the release capability for one mutation-listener registration."""

        self.add_mutation_listener(listener)
        return CallbackSubscription(lambda: self.remove_mutation_listener(listener))

    def remove_mutation_listener(
        self,
        listener: Callable[[EventRegistryMutation[TEvent]], None],
    ) -> bool:
        with self._lock:
            if listener not in self._mutation_listeners:
                return False
            self._mutation_listeners.remove(listener)
            return True

    def clear_mutation_listeners(self) -> None:
        with self._lock:
            self._mutation_listeners.clear()

    def clear_execution(self, execution_id: str) -> None:
        mutation_listeners: List[Callable[[EventRegistryMutation[TEvent]], None]]
        with self._lock:
            removed = self._events.pop(execution_id, None)
            mutation_listeners = list(self._mutation_listeners)
        if removed is None:
            return
        mutation: EventRegistryMutation[TEvent] = EventRegistryMutation(
            kind=EventRegistryMutationKind.CLEARED,
            execution_id=execution_id,
        )
        for listener in mutation_listeners:
            self._notify_mutation_listener(listener, mutation)

    def clear_all(self) -> None:
        mutation_listeners: List[Callable[[EventRegistryMutation[TEvent]], None]]
        with self._lock:
            had_events = bool(self._events)
            self._events.clear()
            mutation_listeners = list(self._mutation_listeners)
        if not had_events:
            return
        mutation: EventRegistryMutation[TEvent] = EventRegistryMutation(
            kind=EventRegistryMutationKind.CLEARED,
            execution_id=None,
        )
        for listener in mutation_listeners:
            self._notify_mutation_listener(listener, mutation)

    def cleanup_old_executions(self, retention_seconds: Optional[float] = None) -> int:
        max_age = self._retention_seconds if retention_seconds is None else retention_seconds
        now = time.time()
        removed_execution_ids: List[str] = []
        mutation_listeners: List[Callable[[EventRegistryMutation[TEvent]], None]]
        with self._lock:
            for execution_id in list(self._events.keys()):
                event_dict = self._events.get(execution_id)
                if not event_dict:
                    continue
                latest = max(event_dict.values(), key=self._timestamp_of)
                if not self._is_terminal(latest):
                    continue
                age = now - self._timestamp_of(latest)
                if age <= max_age:
                    continue
                del self._events[execution_id]
                removed_execution_ids.append(execution_id)
            mutation_listeners = list(self._mutation_listeners)

        for execution_id in removed_execution_ids:
            mutation: EventRegistryMutation[TEvent] = EventRegistryMutation(
                kind=EventRegistryMutationKind.CLEARED,
                execution_id=execution_id,
            )
            for listener in mutation_listeners:
                self._notify_mutation_listener(listener, mutation)
        return len(removed_execution_ids)

    def get_execution_ids(self) -> List[str]:
        with self._lock:
            return list(self._events.keys())

    def get_event_count(self, execution_id: str) -> int:
        with self._lock:
            return len(self._events.get(execution_id, {}))

    @staticmethod
    def _notify_listener(
        listener: Callable[[str, TEvent], None],
        execution_id: str,
        event: TEvent,
    ) -> None:
        try:
            listener(execution_id, event)
        except Exception:
            logger.exception("Progress listener failed")

    @staticmethod
    def _notify_mutation_listener(
        listener: Callable[[EventRegistryMutation[TEvent]], None],
        mutation: EventRegistryMutation[TEvent],
    ) -> None:
        try:
            listener(mutation)
        except Exception:
            logger.exception("Progress registry mutation listener failed")
