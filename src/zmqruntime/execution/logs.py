"""Execution-worker log path identities shared by runtimes and observers."""

from __future__ import annotations

import re
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from zmqruntime.messages import ProcessIdentity, RunningExecutionInfo, WorkerState


@dataclass(frozen=True, slots=True)
class ExecutionWorkerLogIdentity:
    """Authoritative execution-worker log filename identity."""

    execution_id: str
    worker_pid: int

    _PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"^zmq_worker_exec_(?P<execution_id>.+)_worker_(?P<worker_pid>\d+)\.log$"
    )

    @classmethod
    def base_path(cls, log_directory: Path, execution_id: str) -> Path:
        """Return the log base passed to worker-process launch infrastructure."""

        return log_directory / f"zmq_worker_exec_{execution_id}"

    @classmethod
    def from_path(cls, path: Path) -> ExecutionWorkerLogIdentity | None:
        """Parse one complete worker-log path through the owned grammar."""

        match = cls._PATTERN.fullmatch(path.name)
        if match is None:
            return None
        return cls(
            execution_id=match.group("execution_id"),
            worker_pid=int(match.group("worker_pid")),
        )

    def path(self, log_directory: Path) -> Path:
        """Materialize this worker log under a directory."""

        return log_directory / (
            f"zmq_worker_exec_{self.execution_id}_worker_{self.worker_pid}.log"
        )


@dataclass(frozen=True, slots=True)
class ExecutionWorkerLogObservation:
    """Existing worker log correlated with an endpoint heartbeat identity."""

    identity: ExecutionWorkerLogIdentity
    path: Path
    process_identity: ProcessIdentity

    @classmethod
    def discover(
        cls,
        log_directory: Path,
        *,
        running_executions: Collection[RunningExecutionInfo] | None,
        workers: Collection[WorkerState] | None,
    ) -> tuple[ExecutionWorkerLogObservation, ...]:
        """Correlate owned filenames with currently reported endpoint workers."""

        if running_executions is None or workers is None:
            return ()
        execution_ids = {
            execution.execution_id
            for execution in running_executions
        }
        process_identities = {
            worker.pid: ProcessIdentity(
                pid=worker.pid,
                create_time=worker.create_time,
            )
            for worker in workers
            if worker.create_time is not None
        }
        observations = []
        for path in log_directory.glob("zmq_worker_exec_*_worker_*.log"):
            identity = ExecutionWorkerLogIdentity.from_path(path)
            if (
                identity is None
                or identity.execution_id not in execution_ids
                or identity.worker_pid not in process_identities
            ):
                continue
            observations.append(
                cls(
                    identity=identity,
                    path=path,
                    process_identity=process_identities[identity.worker_pid],
                )
            )
        return tuple(sorted(observations, key=lambda observation: observation.path))
