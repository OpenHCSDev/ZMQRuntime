"""Generic latest-event registry keyed by semantic channels."""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, Dict, Generic, List, Optional, TypeVar
import logging


TEvent = TypeVar("TEvent")
TKey = TypeVar("TKey")
logger = logging.getLogger(__name__)


class EventRegistryABC(ABC, Generic[TEvent]):
    """Nominal contract for latest-event registries."""

    @abstractmethod
    def register_event(self, execution_id: str, event: TEvent) -> None:
        """Register one event for an execution id."""

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
        """Drop all events and listeners."""


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
        self._lock = threading.Lock()

    def register_event(self, execution_id: str, event: TEvent) -> None:
        listeners: List[Callable[[str, TEvent], None]]
        with self._lock:
            event_dict = self._events.setdefault(execution_id, {})
            event_dict[self._key_builder(event)] = event
            listeners = list(self._listeners)

        for listener in listeners:
            try:
                listener(execution_id, event)
            except Exception:
                logger.exception("Progress listener failed")

    def get_events(self, execution_id: str) -> List[TEvent]:
        with self._lock:
            return list(self._events.get(execution_id, {}).values())

    def get_latest_event(self, execution_id: str) -> Optional[TEvent]:
        events = self.get_events(execution_id)
        return max(events, key=self._timestamp_of) if events else None

    def add_listener(self, listener: Callable[[str, TEvent], None]) -> None:
        with self._lock:
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

    def clear_execution(self, execution_id: str) -> None:
        with self._lock:
            self._events.pop(execution_id, None)

    def clear_all(self) -> None:
        with self._lock:
            self._events.clear()
            self._listeners.clear()

    def cleanup_old_executions(self, retention_seconds: Optional[float] = None) -> int:
        max_age = self._retention_seconds if retention_seconds is None else retention_seconds
        now = time.time()
        removed = 0
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
                removed += 1
        return removed

    def get_execution_ids(self) -> List[str]:
        with self._lock:
            return list(self._events.keys())

    def get_event_count(self, execution_id: str) -> int:
        with self._lock:
            return len(self._events.get(execution_id, {}))
