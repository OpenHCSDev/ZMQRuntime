from dataclasses import dataclass

from zmqruntime.progress import (
    ProgressProjectionAdapterABC,
    build_execution_projection,
)


@dataclass(frozen=True)
class _Event:
    execution_id: str
    plate_id: str
    axis_id: str
    channel: str
    percent: float
    step_name: str
    status: str
    timestamp: float
    known_axes: tuple[str, ...] = ()


class _Adapter(ProgressProjectionAdapterABC[_Event, str]):
    def plate_id(self, event: _Event) -> str:
        return event.plate_id

    def axis_id(self, event: _Event) -> str:
        return event.axis_id

    def step_name(self, event: _Event) -> str:
        return event.step_name

    def percent(self, event: _Event) -> float:
        return event.percent

    def timestamp(self, event: _Event) -> float:
        return event.timestamp

    def channel(self, event: _Event) -> str:
        return event.channel

    def known_axes(self, events):
        axes: set[str] = set()
        for event in events:
            axes.update(event.known_axes)
        return sorted(axes)

    def is_failure_event(self, event: _Event) -> bool:
        return event.status == "failed"

    def is_success_terminal_event(self, event: _Event) -> bool:
        return event.status == "complete"

    def state_idle(self) -> str:
        return "idle"

    def state_compiling(self) -> str:
        return "compiling"

    def state_compiled(self) -> str:
        return "compiled"

    def state_executing(self) -> str:
        return "executing"

    def state_complete(self) -> str:
        return "complete"

    def state_failed(self) -> str:
        return "failed"


def test_build_execution_projection_marks_executing_from_pipeline_channel():
    adapter = _Adapter()
    events_by_execution = {
        "exec-1": [
            _Event(
                execution_id="exec-1",
                plate_id="plate-1",
                axis_id="",
                channel="compile",
                percent=0.0,
                step_name="pipeline",
                status="started",
                timestamp=1.0,
                known_axes=("A01", "B01"),
            ),
            _Event(
                execution_id="exec-1",
                plate_id="plate-1",
                axis_id="A01",
                channel="pipeline",
                percent=50.0,
                step_name="normalize",
                status="running",
                timestamp=2.0,
            ),
        ]
    }

    projection = build_execution_projection(events_by_execution, adapter=adapter)
    plate = projection.get_plate("plate-1", "exec-1")

    assert plate is not None
    assert plate.state == "executing"
    assert round(plate.percent, 1) == 25.0
    assert projection.count_state("executing") == 1


def test_build_execution_projection_marks_failed_when_axis_failed():
    adapter = _Adapter()
    events_by_execution = {
        "exec-1": [
            _Event(
                execution_id="exec-1",
                plate_id="plate-1",
                axis_id="",
                channel="compile",
                percent=0.0,
                step_name="pipeline",
                status="started",
                timestamp=1.0,
                known_axes=("A01", "B01"),
            ),
            _Event(
                execution_id="exec-1",
                plate_id="plate-1",
                axis_id="A01",
                channel="pipeline",
                percent=50.0,
                step_name="normalize",
                status="failed",
                timestamp=2.0,
            ),
            _Event(
                execution_id="exec-1",
                plate_id="plate-1",
                axis_id="B01",
                channel="pipeline",
                percent=100.0,
                step_name="pipeline",
                status="complete",
                timestamp=3.0,
            ),
        ]
    }

    projection = build_execution_projection(events_by_execution, adapter=adapter)
    plate = projection.get_plate("plate-1", "exec-1")

    assert plate is not None
    assert plate.state == "failed"
    assert round(plate.percent, 1) == 75.0
    assert projection.count_state("failed") == 1
