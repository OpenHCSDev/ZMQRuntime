"""Execution-worker log identity contracts."""

from zmqruntime.execution.logs import (
    ExecutionWorkerLogIdentity,
    ExecutionWorkerLogObservation,
)
from zmqruntime.messages import RunningExecutionInfo, WorkerState


def test_execution_worker_log_identity_owns_path_roundtrip(tmp_path) -> None:
    identity = ExecutionWorkerLogIdentity(
        execution_id="run_with_worker_token",
        worker_pid=4321,
    )

    path = identity.path(tmp_path)

    assert path == (
        tmp_path / "zmq_worker_exec_run_with_worker_token_worker_4321.log"
    )
    assert ExecutionWorkerLogIdentity.from_path(path) == identity
    assert ExecutionWorkerLogIdentity.base_path(
        tmp_path,
        identity.execution_id,
    ) == tmp_path / "zmq_worker_exec_run_with_worker_token"


def test_execution_worker_log_identity_rejects_non_owned_filenames(tmp_path) -> None:
    assert ExecutionWorkerLogIdentity.from_path(tmp_path / "worker_4321.log") is None
    assert (
        ExecutionWorkerLogIdentity.from_path(
            tmp_path / "zmq_worker_exec_run_worker_not-a-pid.log"
        )
        is None
    )


def test_worker_log_observation_correlates_endpoint_heartbeat(tmp_path) -> None:
    active = ExecutionWorkerLogIdentity("execution-1", 4321).path(tmp_path)
    inactive_execution = ExecutionWorkerLogIdentity("execution-2", 4321).path(
        tmp_path
    )
    inactive_worker = ExecutionWorkerLogIdentity("execution-1", 9876).path(tmp_path)
    for log_path in (active, inactive_execution, inactive_worker):
        log_path.touch()

    observations = ExecutionWorkerLogObservation.discover(
        tmp_path,
        running_executions=(
            RunningExecutionInfo(
                execution_id="execution-1",
                plate_id="plate-1",
                start_time=1.0,
                elapsed=2.0,
            ),
        ),
        workers=(
            WorkerState(
                pid=4321,
                status="running",
                cpu_percent=1.0,
                memory_mb=2.0,
                create_time=3.0,
            ),
            WorkerState(
                pid=9876,
                status="running",
                cpu_percent=1.0,
                memory_mb=2.0,
                create_time=None,
            ),
        ),
    )

    assert len(observations) == 1
    assert observations[0].identity == ExecutionWorkerLogIdentity(
        "execution-1",
        4321,
    )
    assert observations[0].path == active
    assert observations[0].process_identity.pid == 4321
    assert observations[0].process_identity.create_time == 3.0
