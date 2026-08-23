"""Endpoint shutdown batching remains owned by the generic runtime."""

from zmqruntime.client import EndpointShutdownMode, EndpointShutdownResult
from zmqruntime.shutdown import EndpointShutdownService


def test_shutdown_batch_reports_exact_failures_and_terminated_endpoints() -> None:
    retired: list[int] = []

    def shutdown_endpoint(
        port: int,
        mode: EndpointShutdownMode,
    ) -> EndpointShutdownResult:
        assert mode is EndpointShutdownMode.GRACEFUL
        return EndpointShutdownResult(
            succeeded=port != 7778,
            endpoint_terminated=port != 7778,
        )

    result = EndpointShutdownService(
        shutdown_endpoint=shutdown_endpoint,
        retire_progress_tracker=retired.append,
    ).shutdown_ports(
        ports=[7777, 7778],
        mode=EndpointShutdownMode.GRACEFUL,
    )

    assert result.failed_ports == (7778,)
    assert not result.succeeded
    assert "7778" in result.failure_message
    assert result.message == result.failure_message
    assert result.terminated_ports == (7777,)
    assert retired == [7777]


def test_shutdown_batch_does_not_publish_failed_endpoint_termination() -> None:
    retired: list[int] = []

    result = EndpointShutdownService(
        shutdown_endpoint=lambda _port, _mode: EndpointShutdownResult(
            succeeded=False,
            endpoint_terminated=False,
        ),
        retire_progress_tracker=retired.append,
    ).shutdown_ports(
        ports=[8888, 9999],
        mode=EndpointShutdownMode.FORCE,
    )

    assert result.failed_ports == (8888, 9999)
    assert result.terminated_ports == ()
    assert retired == []


def test_shutdown_batch_separates_success_from_endpoint_termination() -> None:
    retired: list[int] = []

    result = EndpointShutdownService(
        shutdown_endpoint=lambda _port, _mode: EndpointShutdownResult(
            succeeded=True,
            endpoint_terminated=False,
        ),
        retire_progress_tracker=retired.append,
    ).shutdown_ports(
        ports=[7777],
        mode=EndpointShutdownMode.GRACEFUL,
    )

    assert result.succeeded
    assert result.message == "All endpoints shut down successfully"
    assert result.succeeded_ports == (7777,)
    assert result.terminated_ports == ()
    assert retired == [7777]


def test_shutdown_batch_contains_endpoint_exceptions() -> None:
    def shutdown_endpoint(
        port: int,
        _mode: EndpointShutdownMode,
    ) -> EndpointShutdownResult:
        if port == 7777:
            raise RuntimeError("unreachable")
        return EndpointShutdownResult(succeeded=True, endpoint_terminated=True)

    retired: list[int] = []
    result = EndpointShutdownService(
        shutdown_endpoint=shutdown_endpoint,
        retire_progress_tracker=retired.append,
    ).shutdown_ports(
        ports=[7777, 7778],
        mode=EndpointShutdownMode.FORCE,
    )

    assert result.failed_ports == (7777,)
    assert retired == [7778]
