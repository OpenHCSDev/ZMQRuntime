"""Typed execution lifecycle state management."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import RLock

from zmqruntime.messages import (
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStatusSnapshot,
    MessageFields,
    QueuedExecutionInfo,
    ResponseType,
    RunningExecutionInfo,
)


@dataclass(frozen=True, slots=True)
class ExecutionLifecycleTransition:
    """Observed result of one lifecycle transition request."""

    execution_id: str
    previous: ExecutionStatus
    requested: ExecutionStatus
    current: ExecutionStatus
    applied: bool


@dataclass(frozen=True, slots=True)
class ExecutionCancellationResult:
    """Cancellation outcome with generic interruption and wire semantics."""

    transition: ExecutionLifecycleTransition

    @property
    def should_interrupt_active_work(self) -> bool:
        """Whether the cancelled record owned work that is currently running."""

        return self.transition.applied and self.transition.previous.owns_active_work

    def to_response(self, *, workers_killed: int) -> dict:
        """Project this outcome onto the execution control response contract."""

        if self.transition.applied:
            return {
                MessageFields.STATUS: ResponseType.OK.value,
                MessageFields.MESSAGE: f"Cancelled - killed {workers_killed} workers",
                MessageFields.WORKERS_KILLED: workers_killed,
            }
        return {
            MessageFields.STATUS: ResponseType.ERROR.value,
            MessageFields.ERROR: (
                f"Execution {self.transition.execution_id} is already "
                f"{self.transition.current.value}; cancellation was not applied"
            ),
            MessageFields.WORKERS_KILLED: workers_killed,
        }


class ExecutionLifecycleEngineABC(ABC):
    """Abstract execution lifecycle manager."""

    @abstractmethod
    def enqueue(self, record: ExecutionRecord) -> int:
        """Store queued record and return 1-based queue position."""

    @abstractmethod
    def get(self, execution_id: str) -> ExecutionRecord | None:
        """Get record by id."""

    @abstractmethod
    def records(self) -> dict[str, ExecutionRecord]:
        """Mutable record store."""

    @abstractmethod
    def mark_running(
        self, execution_id: str, start_time: float | None = None
    ) -> ExecutionLifecycleTransition:
        """Transition to running."""

    @abstractmethod
    def mark_complete(
        self, execution_id: str, end_time: float | None = None
    ) -> ExecutionLifecycleTransition:
        """Transition to complete."""

    @abstractmethod
    def mark_failed(
        self,
        execution_id: str,
        error: str,
        end_time: float | None = None,
    ) -> ExecutionLifecycleTransition:
        """Transition to failed."""

    @abstractmethod
    def mark_cancelled(
        self, execution_id: str, end_time: float | None = None
    ) -> ExecutionLifecycleTransition:
        """Transition to cancelled."""

    @abstractmethod
    def cancel(
        self, execution_id: str, end_time: float | None = None
    ) -> ExecutionCancellationResult:
        """Cancel one execution and describe whether active work must stop."""

    @abstractmethod
    def queue_position(self, execution_id: str) -> int:
        """Return 1-based queue position, 0 when not queued."""

    @abstractmethod
    def cancel_all_active(
        self, end_time: float | None = None
    ) -> tuple[ExecutionCancellationResult, ...]:
        """Cancel all running/queued records."""

    @abstractmethod
    def snapshot(
        self,
        *,
        uptime: float,
        execution_id: str | None = None,
    ) -> ExecutionStatusSnapshot:
        """Build typed status snapshot for one/all executions."""


class InMemoryExecutionLifecycleEngine(ExecutionLifecycleEngineABC):
    """Thread-compatible in-memory lifecycle implementation."""

    def __init__(self) -> None:
        self._records: dict[str, ExecutionRecord] = {}
        self._queue_order: list[str] = []
        self._lock = RLock()

    def enqueue(self, record: ExecutionRecord) -> int:
        with self._lock:
            if record.execution_id in self._records:
                raise ValueError(f"Execution already exists: {record.execution_id}")
            self._records[record.execution_id] = record
            self._queue_order.append(record.execution_id)
            return len(self._queue_order)

    def get(self, execution_id: str) -> ExecutionRecord | None:
        with self._lock:
            return self._records.get(execution_id)

    def records(self) -> dict[str, ExecutionRecord]:
        return self._records

    def mark_running(
        self, execution_id: str, start_time: float | None = None
    ) -> ExecutionLifecycleTransition:
        return self._transition(
            execution_id,
            ExecutionStatus.RUNNING,
            timestamp=time.time() if start_time is None else start_time,
        )

    def mark_complete(
        self, execution_id: str, end_time: float | None = None
    ) -> ExecutionLifecycleTransition:
        return self._transition(
            execution_id,
            ExecutionStatus.COMPLETE,
            timestamp=time.time() if end_time is None else end_time,
        )

    def mark_failed(
        self,
        execution_id: str,
        error: str,
        end_time: float | None = None,
    ) -> ExecutionLifecycleTransition:
        transition = self._transition(
            execution_id,
            ExecutionStatus.FAILED,
            timestamp=time.time() if end_time is None else end_time,
        )
        if transition.applied:
            with self._lock:
                self._require(execution_id).error = error
        return transition

    def mark_cancelled(
        self, execution_id: str, end_time: float | None = None
    ) -> ExecutionLifecycleTransition:
        return self._transition(
            execution_id,
            ExecutionStatus.CANCELLED,
            timestamp=time.time() if end_time is None else end_time,
        )

    def cancel(
        self, execution_id: str, end_time: float | None = None
    ) -> ExecutionCancellationResult:
        return ExecutionCancellationResult(self.mark_cancelled(execution_id, end_time=end_time))

    def queue_position(self, execution_id: str) -> int:
        with self._lock:
            try:
                return self._queue_order.index(execution_id) + 1
            except ValueError:
                return 0

    def cancel_all_active(
        self, end_time: float | None = None
    ) -> tuple[ExecutionCancellationResult, ...]:
        timestamp = time.time() if end_time is None else end_time
        with self._lock:
            active_ids = tuple(
                execution_id
                for execution_id, record in self._records.items()
                if not ExecutionStatus(record.status).is_terminal
            )
        return tuple(self.cancel(execution_id, end_time=timestamp) for execution_id in active_ids)

    def snapshot(
        self,
        *,
        uptime: float,
        execution_id: str | None = None,
    ) -> ExecutionStatusSnapshot:
        with self._lock:
            if execution_id is not None:
                record = self._records.get(execution_id)
                if record is None:
                    raise KeyError(execution_id)
                return ExecutionStatusSnapshot(
                    status=ResponseType.OK,
                    execution=record,
                )

            running = self._running_executions()
            queued = self._queued_executions()
            return ExecutionStatusSnapshot(
                status=ResponseType.OK,
                active_executions=len(running) + len(queued),
                uptime=uptime,
                executions=tuple(self._records.keys()),
                running_executions=running,
                queued_executions=queued,
            )

    def _transition(
        self,
        execution_id: str,
        requested: ExecutionStatus,
        *,
        timestamp: float,
    ) -> ExecutionLifecycleTransition:
        with self._lock:
            record = self._require(execution_id)
            previous = ExecutionStatus(record.status)
            if previous.is_terminal:
                return ExecutionLifecycleTransition(
                    execution_id=execution_id,
                    previous=previous,
                    requested=requested,
                    current=previous,
                    applied=False,
                )
            requested.apply_to_record(record, timestamp=timestamp)
            self._remove_from_queue(execution_id)
            return ExecutionLifecycleTransition(
                execution_id=execution_id,
                previous=previous,
                requested=requested,
                current=requested,
                applied=True,
            )

    def _running_executions(self) -> tuple[RunningExecutionInfo, ...]:
        running: list[RunningExecutionInfo] = []
        now = time.time()
        for execution_id, record in self._records.items():
            if record.status != ExecutionStatus.RUNNING.value:
                continue
            start_time = record.start_time or 0.0
            running.append(
                RunningExecutionInfo(
                    execution_id=execution_id,
                    plate_id=str(record.plate_id),
                    start_time=start_time,
                    elapsed=(now - start_time) if start_time > 0 else 0.0,
                    compile_only=bool(record.compile_only),
                )
            )
        return tuple(running)

    def _queued_executions(self) -> tuple[QueuedExecutionInfo, ...]:
        queued: list[QueuedExecutionInfo] = []
        for idx, execution_id in enumerate(self._queue_order, start=1):
            record = self._records.get(execution_id)
            if record is None:
                continue
            queued.append(
                QueuedExecutionInfo(
                    execution_id=execution_id,
                    plate_id=str(record.plate_id),
                    queue_position=idx,
                )
            )
        return tuple(queued)

    def _remove_from_queue(self, execution_id: str) -> None:
        self._queue_order = [eid for eid in self._queue_order if eid != execution_id]

    def _require(self, execution_id: str) -> ExecutionRecord:
        record = self._records.get(execution_id)
        if record is None:
            raise KeyError(execution_id)
        return record
