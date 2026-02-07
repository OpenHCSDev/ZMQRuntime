"""Typed execution lifecycle state management."""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

from zmqruntime.messages import (
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStatusSnapshot,
    QueuedExecutionInfo,
    ResponseType,
    RunningExecutionInfo,
)


class ExecutionLifecycleEngineABC(ABC):
    """Abstract execution lifecycle manager."""

    @abstractmethod
    def enqueue(self, record: ExecutionRecord) -> int:
        """Store queued record and return 1-based queue position."""

    @abstractmethod
    def get(self, execution_id: str) -> Optional[ExecutionRecord]:
        """Get record by id."""

    @abstractmethod
    def records(self) -> Dict[str, ExecutionRecord]:
        """Mutable record store."""

    @abstractmethod
    def mark_running(self, execution_id: str, start_time: Optional[float] = None) -> None:
        """Transition to running."""

    @abstractmethod
    def mark_complete(self, execution_id: str, end_time: Optional[float] = None) -> None:
        """Transition to complete."""

    @abstractmethod
    def mark_failed(self, execution_id: str, error: str, end_time: Optional[float] = None) -> None:
        """Transition to failed."""

    @abstractmethod
    def mark_cancelled(self, execution_id: str, end_time: Optional[float] = None) -> None:
        """Transition to cancelled."""

    @abstractmethod
    def queue_position(self, execution_id: str) -> int:
        """Return 1-based queue position, 0 when not queued."""

    @abstractmethod
    def cancel_all_active(self, end_time: Optional[float] = None) -> None:
        """Cancel all running/queued records."""

    @abstractmethod
    def snapshot(self, *, uptime: float, execution_id: str | None = None) -> ExecutionStatusSnapshot:
        """Build typed status snapshot for one/all executions."""


class InMemoryExecutionLifecycleEngine(ExecutionLifecycleEngineABC):
    """Thread-compatible in-memory lifecycle implementation."""

    def __init__(self) -> None:
        self._records: Dict[str, ExecutionRecord] = {}
        self._queue_order: list[str] = []

    def enqueue(self, record: ExecutionRecord) -> int:
        if record.execution_id in self._records:
            raise ValueError(f"Execution already exists: {record.execution_id}")
        self._records[record.execution_id] = record
        self._queue_order.append(record.execution_id)
        return len(self._queue_order)

    def get(self, execution_id: str) -> Optional[ExecutionRecord]:
        return self._records.get(execution_id)

    def records(self) -> Dict[str, ExecutionRecord]:
        return self._records

    def mark_running(self, execution_id: str, start_time: Optional[float] = None) -> None:
        record = self._require(execution_id)
        record.status = ExecutionStatus.RUNNING.value
        record.start_time = time.time() if start_time is None else start_time
        self._remove_from_queue(execution_id)

    def mark_complete(self, execution_id: str, end_time: Optional[float] = None) -> None:
        record = self._require(execution_id)
        record.status = ExecutionStatus.COMPLETE.value
        record.end_time = time.time() if end_time is None else end_time
        self._remove_from_queue(execution_id)

    def mark_failed(self, execution_id: str, error: str, end_time: Optional[float] = None) -> None:
        record = self._require(execution_id)
        record.status = ExecutionStatus.FAILED.value
        record.error = error
        record.end_time = time.time() if end_time is None else end_time
        self._remove_from_queue(execution_id)

    def mark_cancelled(self, execution_id: str, end_time: Optional[float] = None) -> None:
        record = self._require(execution_id)
        record.status = ExecutionStatus.CANCELLED.value
        record.end_time = time.time() if end_time is None else end_time
        self._remove_from_queue(execution_id)

    def queue_position(self, execution_id: str) -> int:
        try:
            return self._queue_order.index(execution_id) + 1
        except ValueError:
            return 0

    def cancel_all_active(self, end_time: Optional[float] = None) -> None:
        ts = time.time() if end_time is None else end_time
        for execution_id, record in self._records.items():
            if record.status not in {
                ExecutionStatus.RUNNING.value,
                ExecutionStatus.QUEUED.value,
            }:
                continue
            record.status = ExecutionStatus.CANCELLED.value
            record.end_time = ts
        self._queue_order.clear()

    def snapshot(self, *, uptime: float, execution_id: str | None = None) -> ExecutionStatusSnapshot:
        if execution_id is not None:
            record = self.get(execution_id)
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

    def _running_executions(self) -> Tuple[RunningExecutionInfo, ...]:
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

    def _queued_executions(self) -> Tuple[QueuedExecutionInfo, ...]:
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
