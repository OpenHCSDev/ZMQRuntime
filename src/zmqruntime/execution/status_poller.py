"""Generic execution status polling loop with callback policy."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict


class ExecutionStatusPollPolicyABC(ABC):
    """Policy boundary for execution status polling behavior."""

    @property
    def poll_interval_seconds(self) -> float:
        return 0.5

    @abstractmethod
    def poll_status(self, execution_id: str) -> Dict[str, Any]:
        """Fetch current status payload."""

    def on_running(self, execution_id: str, execution_payload: Dict[str, Any]) -> None:
        pass

    def on_terminal(
        self,
        execution_id: str,
        terminal_status: str,
        execution_payload: Dict[str, Any],
    ) -> None:
        pass

    def on_status_error(self, execution_id: str, message: str) -> None:
        pass

    def on_poll_exception(self, execution_id: str, error: Exception) -> bool:
        return True


@dataclass(frozen=True)
class CallbackExecutionStatusPollPolicy(ExecutionStatusPollPolicyABC):
    """Callback-backed poll policy."""

    poll_status_fn: Callable[[str], Dict[str, Any]]
    poll_interval_seconds_value: float = 0.5
    on_running_fn: Callable[[str, Dict[str, Any]], None] | None = None
    on_terminal_fn: Callable[[str, str, Dict[str, Any]], None] | None = None
    on_status_error_fn: Callable[[str, str], None] | None = None
    on_poll_exception_fn: Callable[[str, Exception], bool] | None = None

    @property
    def poll_interval_seconds(self) -> float:
        return self.poll_interval_seconds_value

    def poll_status(self, execution_id: str) -> Dict[str, Any]:
        return self.poll_status_fn(execution_id)

    def on_running(self, execution_id: str, execution_payload: Dict[str, Any]) -> None:
        if self.on_running_fn is not None:
            self.on_running_fn(execution_id, execution_payload)

    def on_terminal(
        self,
        execution_id: str,
        terminal_status: str,
        execution_payload: Dict[str, Any],
    ) -> None:
        if self.on_terminal_fn is not None:
            self.on_terminal_fn(execution_id, terminal_status, execution_payload)

    def on_status_error(self, execution_id: str, message: str) -> None:
        if self.on_status_error_fn is not None:
            self.on_status_error_fn(execution_id, message)

    def on_poll_exception(self, execution_id: str, error: Exception) -> bool:
        if self.on_poll_exception_fn is not None:
            return self.on_poll_exception_fn(execution_id, error)
        return True


class ExecutionStatusPoller:
    """Polling engine for queued/running/terminal execution statuses."""

    TERMINAL_STATUSES = {"complete", "failed", "cancelled"}

    def run(self, execution_id: str, policy: ExecutionStatusPollPolicyABC) -> None:
        previous_status = "queued"
        while True:
            time.sleep(policy.poll_interval_seconds)
            try:
                status_response = policy.poll_status(execution_id)
            except Exception as error:
                should_continue = policy.on_poll_exception(execution_id, error)
                if not should_continue:
                    return
                continue

            if status_response.get("status") != "ok":
                message = str(
                    status_response.get("error", "Execution status unavailable")
                )
                policy.on_status_error(execution_id, message)
                return

            execution_payload = status_response["execution"]
            status = execution_payload["status"]

            if status == "running" and previous_status == "queued":
                policy.on_running(execution_id, execution_payload)
                previous_status = "running"

            if status in self.TERMINAL_STATUSES:
                policy.on_terminal(execution_id, status, execution_payload)
                return
