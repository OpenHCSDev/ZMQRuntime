"""Bounded, provider-free waiter probes using the real status decoder."""

from collections.abc import Callable

import pytest

from zmqruntime.execution.responses import WireResponse
from zmqruntime.execution.wait_policy import ExecutionWaiter, WaitPolicy
from zmqruntime.messages import (
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStatusSnapshot,
    MessageFields,
    ProcessExit,
    ResponseType,
)

EXECUTION_ID = "waiter-job"
POLICY = WaitPolicy(poll_interval=0, max_consecutive_errors=3, retry_backoff_seconds=0)


@pytest.fixture(autouse=True)
def no_waiter_sleep(monkeypatch):
    monkeypatch.setattr("zmqruntime.execution.wait_policy.time.sleep", lambda _delay: None)


def execution_response(status: ExecutionStatus) -> WireResponse:
    return ExecutionStatusSnapshot(
        status=ResponseType.OK,
        execution=ExecutionRecord(
            execution_id=EXECUTION_ID,
            plate_id="plate",
            client_address=None,
            status=status.value,
            error="worker failed" if status is ExecutionStatus.FAILED else None,
            results_summary={"objects": 7} if status is ExecutionStatus.COMPLETE else None,
        ),
    ).to_dict()


def bounded_poller(
    observations: list[WireResponse | Exception],
) -> tuple[Callable[[str], WireResponse], list[str]]:
    calls: list[str] = []

    def poll(execution_id: str) -> WireResponse:
        index = len(calls)
        calls.append(execution_id)
        if index >= len(observations):
            # pytest.fail raises outside Exception, so the production retry loop
            # cannot swallow the guard and make a regression test hang.
            pytest.fail("waiter exceeded the declared observation budget")
        observation = observations[index]
        if isinstance(observation, Exception):
            raise observation
        return observation

    return poll, calls


@pytest.mark.parametrize(
    "malformed",
    [
        {},
        {MessageFields.STATUS: "not-a-response-type"},
        {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.EXECUTION: {MessageFields.STATUS: ExecutionStatus.RUNNING.value},
        },
    ],
    ids=["missing-status", "unknown-response-type", "incomplete-execution"],
)
def test_malformed_status_without_liveness_exhausts_error_budget(malformed, caplog):
    poll, calls = bounded_poller([malformed] * POLICY.max_consecutive_errors)

    result = ExecutionWaiter(poll).wait(EXECUTION_ID, POLICY)

    assert calls == [EXECUTION_ID] * 3
    assert result[MessageFields.STATUS] == ExecutionStatus.CANCELLED.value
    assert "Lost connection to server" in result[MessageFields.MESSAGE]
    assert "last status error:" in result[MessageFields.MESSAGE]
    assert [record.args[0] for record in caplog.records] == [1, 2, 3]


def test_transport_and_decode_errors_share_the_consecutive_budget():
    poll, calls = bounded_poller(
        [TimeoutError("transport busy"), {MessageFields.STATUS: "invalid"}, {}]
    )

    result = ExecutionWaiter(poll).wait(EXECUTION_ID, POLICY)

    assert len(calls) == 3
    assert result[MessageFields.STATUS] == ExecutionStatus.CANCELLED.value
    assert "KeyError" in result[MessageFields.MESSAGE]


def test_valid_running_observation_resets_the_error_budget():
    malformed = {MessageFields.STATUS: "invalid"}
    poll, calls = bounded_poller(
        [
            malformed,
            malformed,
            execution_response(ExecutionStatus.RUNNING),
            malformed,
            malformed,
            execution_response(ExecutionStatus.COMPLETE),
        ]
    )

    result = ExecutionWaiter(poll).wait(EXECUTION_ID, POLICY)

    assert len(calls) == 6
    assert result == {
        MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
        MessageFields.EXECUTION_ID: EXECUTION_ID,
        MessageFields.EXECUTION: execution_response(ExecutionStatus.COMPLETE)[MessageFields.EXECUTION],
        "results": {"objects": 7},
    }


def test_healthy_long_job_with_unchanged_progress_has_no_poll_count_deadline():
    poll, calls = bounded_poller(
        [execution_response(ExecutionStatus.RUNNING)] * 100
        + [execution_response(ExecutionStatus.COMPLETE)]
    )

    result = ExecutionWaiter(poll, progress_sequence=lambda _execution_id: 0).wait(
        EXECUTION_ID, POLICY
    )

    assert len(calls) == 101
    assert result[MessageFields.STATUS] == ExecutionStatus.COMPLETE.value


def test_known_live_process_retains_transport_timeout_tolerance():
    poll, calls = bounded_poller(
        [TimeoutError("interpreter busy")] * 5 + [execution_response(ExecutionStatus.COMPLETE)]
    )

    result = ExecutionWaiter(poll, known_server_process_is_alive=lambda: True).wait(
        EXECUTION_ID, POLICY
    )

    assert len(calls) == 6
    assert result[MessageFields.STATUS] == ExecutionStatus.COMPLETE.value


@pytest.mark.parametrize(
    "observation", [TimeoutError("status busy"), {MessageFields.STATUS: "invalid"}]
)
def test_advancing_progress_still_admits_retry_after_failed_status_observation(observation):
    poll, calls = bounded_poller([observation] * 5 + [execution_response(ExecutionStatus.COMPLETE)])

    result = ExecutionWaiter(poll, progress_sequence=lambda _execution_id: len(calls)).wait(
        EXECUTION_ID, POLICY
    )

    assert len(calls) == 6
    assert result[MessageFields.STATUS] == ExecutionStatus.COMPLETE.value


@pytest.mark.parametrize("status", [ExecutionStatus.FAILED, ExecutionStatus.CANCELLED])
def test_terminal_failure_and_cancellation_keep_their_diagnostics(status):
    poll, calls = bounded_poller([execution_response(status)])

    result = ExecutionWaiter(poll).wait(EXECUTION_ID, POLICY)

    assert calls == [EXECUTION_ID]
    assert result == {
        MessageFields.STATUS: status.value,
        MessageFields.EXECUTION_ID: EXECUTION_ID,
        MessageFields.EXECUTION: execution_response(status)[MessageFields.EXECUTION],
        MessageFields.MESSAGE: (
            "worker failed" if status is ExecutionStatus.FAILED else "Execution was cancelled"
        ),
    }


def test_lost_known_process_stops_on_first_poll_with_exit_diagnostic():
    poll, calls = bounded_poller([TimeoutError("no response")])

    result = ExecutionWaiter(
        poll,
        known_server_process_is_alive=lambda: False,
        owned_server_process_exit=lambda: ProcessExit(-9),
    ).wait(EXECUTION_ID, POLICY)

    assert calls == [EXECUTION_ID]
    assert result == {
        MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
        MessageFields.EXECUTION_ID: EXECUTION_ID,
        MessageFields.MESSAGE: (
            "Lost connection to server (server process exited with signal "
            "SIGKILL (-9); last status error: TimeoutError: no response)"
        ),
    }


def test_unknown_process_transport_errors_remain_bounded():
    poll, calls = bounded_poller([TimeoutError("unreachable")] * 3)

    result = ExecutionWaiter(poll).wait(EXECUTION_ID, POLICY)

    assert len(calls) == 3
    assert result[MessageFields.STATUS] == ExecutionStatus.CANCELLED.value
    assert "TimeoutError: unreachable" in result[MessageFields.MESSAGE]


def test_server_error_response_preserves_existing_error_field_diagnostic():
    poll, calls = bounded_poller(
        [{MessageFields.STATUS: ResponseType.ERROR.value, MessageFields.ERROR: "job missing"}]
    )

    result = ExecutionWaiter(poll).wait(EXECUTION_ID, POLICY)

    assert calls == [EXECUTION_ID]
    assert result[MessageFields.STATUS] == ResponseType.ERROR.value
    assert result[MessageFields.MESSAGE] == "job missing"
