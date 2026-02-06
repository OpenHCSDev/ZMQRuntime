"""Polling wait policy for execution completion."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict

from zmqruntime.messages import (
    ExecutionStatus,
    ExecutionStatusSnapshot,
    MessageFields,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaitPolicy:
    poll_interval: float = 0.5
    max_consecutive_errors: int = 5
    retry_backoff_seconds: float = 1.0


class ExecutionWaiter:
    """Reusable waiter for execution status polling loops."""

    def __init__(self, poll_status: Callable[[str], Dict[str, Any]]) -> None:
        self._poll_status = poll_status

    def wait(self, execution_id: str, policy: WaitPolicy) -> Dict[str, Any]:
        consecutive_errors = 0
        while True:
            time.sleep(policy.poll_interval)
            try:
                status_response = self._poll_status(execution_id)
                consecutive_errors = 0
                snapshot = ExecutionStatusSnapshot.from_dict(status_response)

                if snapshot.status.value == "ok" and snapshot.execution is not None:
                    record = snapshot.execution
                    if record.status == ExecutionStatus.COMPLETE.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            "results": record.results_summary or {},
                        }
                    if record.status == ExecutionStatus.FAILED.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.FAILED.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            MessageFields.MESSAGE: record.error,
                        }
                    if record.status == ExecutionStatus.CANCELLED.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            MessageFields.MESSAGE: "Execution was cancelled",
                        }

                if snapshot.status.value == "error":
                    return {
                        MessageFields.STATUS: "error",
                        MessageFields.EXECUTION_ID: execution_id,
                        MessageFields.MESSAGE: status_response.get(
                            MessageFields.MESSAGE, "Unknown error"
                        ),
                    }

            except Exception as error:
                consecutive_errors += 1
                logger.warning(
                    "Error checking execution status (attempt %s/%s): %s",
                    consecutive_errors,
                    policy.max_consecutive_errors,
                    error,
                )
                if consecutive_errors >= policy.max_consecutive_errors:
                    return {
                        MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
                        MessageFields.EXECUTION_ID: execution_id,
                        MessageFields.MESSAGE: "Lost connection to server",
                    }
                time.sleep(policy.retry_backoff_seconds)
