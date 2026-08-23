import time
from threading import Timer

import pytest

from zmqruntime import OperationCancellation
from zmqruntime.timeouts import (
    OperationDeadline,
    OperationTimeoutError,
)


def test_operation_cancellation_owns_interruptible_waiting() -> None:
    cancellation = OperationCancellation()
    timer = Timer(0.01, cancellation.cancel)
    timer.start()
    try:
        assert cancellation.wait(1.0) is True
    finally:
        timer.join()

    assert cancellation.requested() is True
    assert OperationCancellation().wait(0.001) is False


def test_operation_deadline_owns_one_monotonic_budget() -> None:
    deadline = OperationDeadline.after_milliseconds(
        20,
        operation="test operation",
    )

    assert 0 < deadline.remaining_milliseconds() <= 20
    assert 0 < deadline.cap_seconds(1.0) <= 0.02

    time.sleep(0.03)

    assert deadline.expired() is True
    with pytest.raises(OperationTimeoutError, match="test operation.*20ms"):
        deadline.remaining_milliseconds()


@pytest.mark.parametrize("timeout_ms", (0, -1))
def test_operation_deadline_rejects_non_positive_budgets(timeout_ms: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        OperationDeadline.after_milliseconds(
            timeout_ms,
            operation="test operation",
        )
