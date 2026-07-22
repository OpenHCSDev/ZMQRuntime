"""Polling wait policy for execution completion."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable

from zmqruntime.messages import (
    ExecutionStatus,
    ExecutionStatusSnapshot,
    MessageFields,
    ProcessExit,
    ResponseType,
)
from zmqruntime.execution.responses import (
    ExecutionResponseDiagnostic,
    WireResponse,
    WireValue,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaitPolicy:
    poll_interval: float = 0.5
    max_consecutive_errors: int = 5
    retry_backoff_seconds: float = 1.0
    status_timeout_ms: int = 1000


class ExecutionWaiter:
    """Reusable waiter for execution status polling loops."""

    def __init__(
        self,
        poll_status: Callable[[str], WireResponse],
        progress_sequence: Callable[[str], int | None] | None = None,
        known_server_process_is_alive: Callable[[], bool | None] | None = None,
        owned_server_process_exit: Callable[[], ProcessExit | None] | None = None,
    ) -> None:
        self._poll_status = poll_status
        self._progress_sequence = progress_sequence
        self._known_server_process_is_alive = known_server_process_is_alive
        self._owned_server_process_exit = owned_server_process_exit

    def wait(self, execution_id: str, policy: WaitPolicy) -> dict[str, WireValue]:
        consecutive_errors = 0
        observed_progress_sequence = self._observed_progress_sequence(execution_id)
        while True:
            time.sleep(policy.poll_interval)
            try:
                status_response = self._poll_status(execution_id)
                consecutive_errors = 0
                observed_progress_sequence = self._observed_progress_sequence(
                    execution_id
                )
                snapshot = ExecutionStatusSnapshot.from_dict(status_response)

                if snapshot.status is ResponseType.OK and snapshot.execution is not None:
                    record = snapshot.execution
                    if record.status == ExecutionStatus.COMPLETE.value:
                        result = {
                            MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
                            MessageFields.EXECUTION_ID: execution_id,
                        }
                        if record.results_summary is None:
                            result["results"] = {}
                        else:
                            result["results"] = dict(record.results_summary)
                        return result
                    if record.status == ExecutionStatus.FAILED.value:
                        result = {
                            MessageFields.STATUS: ExecutionStatus.FAILED.value,
                            MessageFields.EXECUTION_ID: execution_id,
                        }
                        if record.error is not None:
                            result[MessageFields.MESSAGE] = record.error
                        return result
                    if record.status == ExecutionStatus.CANCELLED.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            MessageFields.MESSAGE: "Execution was cancelled",
                        }

                if snapshot.status is ResponseType.ERROR:
                    result = {
                        MessageFields.STATUS: ResponseType.ERROR.value,
                        MessageFields.EXECUTION_ID: execution_id,
                    }
                    result.update(
                        ExecutionResponseDiagnostic.from_wire(
                            status_response
                        ).as_message_items()
                    )
                    return result

            except Exception as error:
                known_server_alive = self._known_server_alive()
                if known_server_alive is True:
                    consecutive_errors = 0
                    logger.debug(
                        "Status poll failed for %s, but its known server "
                        "process remains alive: %s",
                        execution_id,
                        error,
                    )
                    continue
                if known_server_alive is False:
                    return self._lost_connection(execution_id, error)
                progress_sequence = self._observed_progress_sequence(execution_id)
                if (
                    progress_sequence is not None
                    and progress_sequence != observed_progress_sequence
                ):
                    observed_progress_sequence = progress_sequence
                    consecutive_errors = 0
                    logger.debug(
                        "Status poll failed for %s, but progress advanced: %s",
                        execution_id,
                        error,
                    )
                    continue
                consecutive_errors += 1
                logger.warning(
                    "Error checking execution status (attempt %s/%s): %s",
                    consecutive_errors,
                    policy.max_consecutive_errors,
                    error,
                )
                if consecutive_errors >= policy.max_consecutive_errors:
                    return self._lost_connection(execution_id, error)
                time.sleep(policy.retry_backoff_seconds)

    def _observed_progress_sequence(self, execution_id: str) -> int | None:
        if self._progress_sequence is None:
            return None
        return self._progress_sequence(execution_id)

    def _known_server_alive(self) -> bool | None:
        if self._known_server_process_is_alive is None:
            return None
        return self._known_server_process_is_alive()

    def _lost_connection(
        self,
        execution_id: str,
        last_error: Exception,
    ) -> dict[str, WireValue]:
        details = []
        process_exit = (
            None
            if self._owned_server_process_exit is None
            else self._owned_server_process_exit()
        )
        if process_exit is not None:
            details.append(f"server process exited with {process_exit.describe()}")
        details.append(
            f"last status error: {type(last_error).__name__}: {last_error}"
        )
        message = f"Lost connection to server ({'; '.join(details)})"
        return {
            MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
            MessageFields.EXECUTION_ID: execution_id,
            MessageFields.MESSAGE: message,
        }
