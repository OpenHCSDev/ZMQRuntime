"""Generic batch submit-then-wait orchestration."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Generic, Sequence, TypeVar


TJob = TypeVar("TJob")


@dataclass(frozen=True)
class SubmittedBatchJob(Generic[TJob]):
    """Job accepted by remote system."""

    job: TJob
    submission_id: str


class BatchSubmitWaitPolicyABC(ABC, Generic[TJob]):
    """Policy boundary for batch orchestration behavior."""

    @abstractmethod
    async def submit(self, job: TJob) -> str:
        """Submit one job and return submission id."""

    @abstractmethod
    async def wait(self, submission_id: str, job: TJob) -> None:
        """Wait for completion of one submitted job."""

    @abstractmethod
    def job_key(self, job: TJob) -> str:
        """Stable key for artifact/result mapping."""

    @property
    def fail_fast_submit(self) -> bool:
        return True

    @property
    def fail_fast_wait(self) -> bool:
        return True

    def on_submit_start(self, job: TJob, index: int, total: int) -> None:
        pass

    def on_submit_success(
        self, job: TJob, submission_id: str, index: int, total: int
    ) -> None:
        pass

    def on_submit_error(
        self, job: TJob, error: Exception, index: int, total: int
    ) -> None:
        pass

    def on_wait_start(self, job: TJob, index: int, total: int) -> None:
        pass

    def on_wait_success(
        self, job: TJob, submission_id: str, index: int, total: int
    ) -> None:
        pass

    def on_wait_error(
        self, job: TJob, error: Exception, index: int, total: int
    ) -> None:
        pass

    def on_wait_finally(self, job: TJob, index: int, total: int) -> None:
        pass


@dataclass(frozen=True)
class CallbackBatchSubmitWaitPolicy(BatchSubmitWaitPolicyABC[TJob], Generic[TJob]):
    """Callback-backed policy implementation."""

    submit_fn: Callable[[TJob], Awaitable[str]]
    wait_fn: Callable[[str, TJob], Awaitable[None]]
    job_key_fn: Callable[[TJob], str]
    fail_fast_submit_value: bool = True
    fail_fast_wait_value: bool = True
    on_submit_start_fn: Callable[[TJob, int, int], None] | None = None
    on_submit_success_fn: Callable[[TJob, str, int, int], None] | None = None
    on_submit_error_fn: Callable[[TJob, Exception, int, int], None] | None = None
    on_wait_start_fn: Callable[[TJob, int, int], None] | None = None
    on_wait_success_fn: Callable[[TJob, str, int, int], None] | None = None
    on_wait_error_fn: Callable[[TJob, Exception, int, int], None] | None = None
    on_wait_finally_fn: Callable[[TJob, int, int], None] | None = None

    async def submit(self, job: TJob) -> str:
        return await self.submit_fn(job)

    async def wait(self, submission_id: str, job: TJob) -> None:
        await self.wait_fn(submission_id, job)

    def job_key(self, job: TJob) -> str:
        return self.job_key_fn(job)

    @property
    def fail_fast_submit(self) -> bool:
        return self.fail_fast_submit_value

    @property
    def fail_fast_wait(self) -> bool:
        return self.fail_fast_wait_value

    def on_submit_start(self, job: TJob, index: int, total: int) -> None:
        if self.on_submit_start_fn is not None:
            self.on_submit_start_fn(job, index, total)

    def on_submit_success(
        self, job: TJob, submission_id: str, index: int, total: int
    ) -> None:
        if self.on_submit_success_fn is not None:
            self.on_submit_success_fn(job, submission_id, index, total)

    def on_submit_error(
        self, job: TJob, error: Exception, index: int, total: int
    ) -> None:
        if self.on_submit_error_fn is not None:
            self.on_submit_error_fn(job, error, index, total)

    def on_wait_start(self, job: TJob, index: int, total: int) -> None:
        if self.on_wait_start_fn is not None:
            self.on_wait_start_fn(job, index, total)

    def on_wait_success(
        self, job: TJob, submission_id: str, index: int, total: int
    ) -> None:
        if self.on_wait_success_fn is not None:
            self.on_wait_success_fn(job, submission_id, index, total)

    def on_wait_error(
        self, job: TJob, error: Exception, index: int, total: int
    ) -> None:
        if self.on_wait_error_fn is not None:
            self.on_wait_error_fn(job, error, index, total)

    def on_wait_finally(self, job: TJob, index: int, total: int) -> None:
        if self.on_wait_finally_fn is not None:
            self.on_wait_finally_fn(job, index, total)


class BatchSubmitWaitEngine(Generic[TJob]):
    """Submit all jobs first, then wait all submitted jobs."""

    async def run(
        self,
        jobs: Sequence[TJob],
        policy: BatchSubmitWaitPolicyABC[TJob],
    ) -> Dict[str, str]:
        submitted_jobs: list[SubmittedBatchJob[TJob]] = []
        submit_total = len(jobs)
        for submit_index, job in enumerate(jobs, start=1):
            policy.on_submit_start(job, submit_index, submit_total)
            try:
                submission_id = await policy.submit(job)
                submitted_jobs.append(
                    SubmittedBatchJob(job=job, submission_id=submission_id)
                )
                policy.on_submit_success(job, submission_id, submit_index, submit_total)
            except Exception as error:
                policy.on_submit_error(job, error, submit_index, submit_total)
                if policy.fail_fast_submit:
                    raise

        artifacts: Dict[str, str] = {}
        wait_total = len(submitted_jobs)
        for wait_index, submitted in enumerate(submitted_jobs, start=1):
            job = submitted.job
            policy.on_wait_start(job, wait_index, wait_total)
            try:
                await policy.wait(submitted.submission_id, job)
                artifacts[policy.job_key(job)] = submitted.submission_id
                policy.on_wait_success(
                    job, submitted.submission_id, wait_index, wait_total
                )
            except Exception as error:
                policy.on_wait_error(job, error, wait_index, wait_total)
                if policy.fail_fast_wait:
                    raise
            finally:
                policy.on_wait_finally(job, wait_index, wait_total)
        return artifacts
