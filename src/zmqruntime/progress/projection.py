"""Generic runtime projection builder for hierarchical progress events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Generic, Hashable, Iterable, List, Mapping, Optional, Sequence, Tuple, TypeVar
from abc import ABC, abstractmethod


TEvent = TypeVar("TEvent")
TState = TypeVar("TState", bound=Hashable)


@dataclass(frozen=True)
class GenericAxisProjection:
    """Latest per-axis projection details."""

    axis_id: str
    percent: float
    step_name: str
    is_complete: bool
    is_failed: bool


@dataclass(frozen=True)
class GenericPlateProjection(Generic[TState]):
    """One plate projection inside one execution snapshot."""

    execution_id: str
    plate_id: str
    state: TState
    percent: float
    axis_progress: Tuple[GenericAxisProjection, ...]
    latest_timestamp: float

    @property
    def active_axes(self) -> Tuple[GenericAxisProjection, ...]:
        return tuple(
            axis
            for axis in self.axis_progress
            if not axis.is_complete and not axis.is_failed
        )


@dataclass
class GenericExecutionProjection(Generic[TState]):
    """Cross-execution projection with de-duplicated latest plate view."""

    plates: List[GenericPlateProjection[TState]] = field(default_factory=list)
    by_key: Dict[Tuple[str, str], GenericPlateProjection[TState]] = field(
        default_factory=dict
    )
    by_plate_latest: Dict[str, GenericPlateProjection[TState]] = field(
        default_factory=dict
    )
    state_counts: Dict[TState, int] = field(default_factory=dict)
    overall_percent: float = 0.0

    def get_plate(
        self, plate_id: str, execution_id: Optional[str] = None
    ) -> Optional[GenericPlateProjection[TState]]:
        if execution_id is not None:
            return self.by_key.get((execution_id, plate_id))
        return self.by_plate_latest.get(plate_id)

    def count_state(self, state: TState) -> int:
        return self.state_counts.get(state, 0)


class ProgressProjectionAdapterABC(ABC, Generic[TEvent, TState]):
    """Domain adapter for generic projection logic."""

    @property
    def compile_channel(self) -> str:
        return "compile"

    @property
    def pipeline_channel(self) -> str:
        return "pipeline"

    @abstractmethod
    def plate_id(self, event: TEvent) -> str:
        """Return plate id."""

    @abstractmethod
    def axis_id(self, event: TEvent) -> str:
        """Return axis id (empty string for non-axis events)."""

    @abstractmethod
    def step_name(self, event: TEvent) -> str:
        """Return step display name."""

    @abstractmethod
    def percent(self, event: TEvent) -> float:
        """Return event percent in [0, 100]."""

    @abstractmethod
    def timestamp(self, event: TEvent) -> float:
        """Return event timestamp."""

    @abstractmethod
    def channel(self, event: TEvent) -> str:
        """Return semantic channel identifier."""

    @abstractmethod
    def known_axes(self, events: Sequence[TEvent]) -> Sequence[str]:
        """Return declared axis universe for plate events when available."""

    @abstractmethod
    def is_failure_event(self, event: TEvent) -> bool:
        """Return True for failed terminal and failure-state events."""

    @abstractmethod
    def is_success_terminal_event(self, event: TEvent) -> bool:
        """Return True for successful terminal events."""

    @abstractmethod
    def state_idle(self) -> TState:
        """Return idle state token."""

    @abstractmethod
    def state_compiling(self) -> TState:
        """Return compiling state token."""

    @abstractmethod
    def state_compiled(self) -> TState:
        """Return compiled state token."""

    @abstractmethod
    def state_executing(self) -> TState:
        """Return executing state token."""

    @abstractmethod
    def state_complete(self) -> TState:
        """Return complete state token."""

    @abstractmethod
    def state_failed(self) -> TState:
        """Return failed state token."""


def _latest_by_axis(
    events: Iterable[TEvent],
    *,
    channel: str,
    adapter: ProgressProjectionAdapterABC[TEvent, TState],
) -> Dict[str, TEvent]:
    latest: Dict[str, TEvent] = {}
    for event in events:
        if adapter.channel(event) != channel:
            continue
        axis_id = adapter.axis_id(event)
        if not axis_id:
            continue
        latest[axis_id] = event
    return latest


def _build_plate_projection(
    execution_id: str,
    plate_id: str,
    events: List[TEvent],
    adapter: ProgressProjectionAdapterABC[TEvent, TState],
) -> GenericPlateProjection[TState]:
    latest_timestamp = max((adapter.timestamp(event) for event in events), default=0.0)
    known_axes = list(adapter.known_axes(events))
    pipeline_by_axis = _latest_by_axis(
        events,
        channel=adapter.pipeline_channel,
        adapter=adapter,
    )
    compile_by_axis = _latest_by_axis(
        events,
        channel=adapter.compile_channel,
        adapter=adapter,
    )
    has_compile_events = any(
        adapter.channel(event) == adapter.compile_channel for event in events
    )

    axis_universe = known_axes
    if not axis_universe:
        axis_universe = sorted(set(pipeline_by_axis.keys()) | set(compile_by_axis.keys()))

    if pipeline_by_axis:
        axis_progress: List[GenericAxisProjection] = []
        for axis_id in axis_universe:
            event = pipeline_by_axis.get(axis_id)
            if event is None:
                axis_progress.append(
                    GenericAxisProjection(
                        axis_id=axis_id,
                        percent=0.0,
                        step_name="queued",
                        is_complete=False,
                        is_failed=False,
                    )
                )
                continue

            axis_progress.append(
                GenericAxisProjection(
                    axis_id=axis_id,
                    percent=adapter.percent(event),
                    step_name=adapter.step_name(event),
                    is_complete=adapter.is_success_terminal_event(event),
                    is_failed=adapter.is_failure_event(event),
                )
            )

        percent = (
            sum(axis.percent for axis in axis_progress) / len(axis_progress)
            if axis_progress
            else 0.0
        )
        has_failure = any(axis.is_failed for axis in axis_progress)
        is_complete = (
            bool(axis_progress)
            and all(axis.is_complete for axis in axis_progress)
            and not has_failure
        )
        if has_failure:
            state = adapter.state_failed()
        elif is_complete:
            state = adapter.state_complete()
        else:
            state = adapter.state_executing()

        return GenericPlateProjection(
            execution_id=execution_id,
            plate_id=plate_id,
            state=state,
            percent=percent,
            axis_progress=tuple(axis_progress),
            latest_timestamp=latest_timestamp,
        )

    if has_compile_events:
        compile_failures = sum(
            1
            for event in compile_by_axis.values()
            if adapter.is_failure_event(event)
        )

        if axis_universe:
            compiled_success = sum(
                1
                for axis_id in axis_universe
                if axis_id in compile_by_axis
                and not adapter.is_failure_event(compile_by_axis[axis_id])
            )
            percent = (compiled_success / len(axis_universe)) * 100.0
            if compile_failures > 0:
                state = adapter.state_failed()
            elif compiled_success == len(axis_universe):
                state = adapter.state_compiled()
            else:
                state = adapter.state_compiling()
        else:
            compile_events = list(compile_by_axis.values())
            percent = (
                sum(adapter.percent(event) for event in compile_events) / len(compile_events)
                if compile_events
                else 0.0
            )
            if compile_failures > 0:
                state = adapter.state_failed()
            elif percent >= 100.0:
                state = adapter.state_compiled()
            else:
                state = adapter.state_compiling()

        axis_progress = tuple(
            GenericAxisProjection(
                axis_id=axis_id,
                percent=(
                    adapter.percent(compile_by_axis[axis_id])
                    if axis_id in compile_by_axis
                    else 0.0
                ),
                step_name="compilation",
                is_complete=(
                    axis_id in compile_by_axis
                    and not adapter.is_failure_event(compile_by_axis[axis_id])
                ),
                is_failed=(
                    axis_id in compile_by_axis
                    and adapter.is_failure_event(compile_by_axis[axis_id])
                ),
            )
            for axis_id in axis_universe
        )
        return GenericPlateProjection(
            execution_id=execution_id,
            plate_id=plate_id,
            state=state,
            percent=percent,
            axis_progress=axis_progress,
            latest_timestamp=latest_timestamp,
        )

    return GenericPlateProjection(
        execution_id=execution_id,
        plate_id=plate_id,
        state=adapter.state_idle(),
        percent=0.0,
        axis_progress=tuple(),
        latest_timestamp=latest_timestamp,
    )


def build_execution_projection(
    events_by_execution: Mapping[str, Sequence[TEvent]],
    *,
    adapter: ProgressProjectionAdapterABC[TEvent, TState],
) -> GenericExecutionProjection[TState]:
    grouped: Dict[Tuple[str, str], List[TEvent]] = {}
    for execution_id, events in events_by_execution.items():
        for event in events:
            grouped.setdefault((execution_id, adapter.plate_id(event)), []).append(event)

    projection: GenericExecutionProjection[TState] = GenericExecutionProjection()

    for (execution_id, plate_id), plate_events in grouped.items():
        plate_projection = _build_plate_projection(
            execution_id,
            plate_id,
            plate_events,
            adapter,
        )
        projection.plates.append(plate_projection)
        projection.by_key[(execution_id, plate_id)] = plate_projection

    for plate in projection.plates:
        existing = projection.by_plate_latest.get(plate.plate_id)
        if existing is None or plate.latest_timestamp > existing.latest_timestamp:
            projection.by_plate_latest[plate.plate_id] = plate

    for plate in projection.by_plate_latest.values():
        projection.state_counts[plate.state] = projection.state_counts.get(plate.state, 0) + 1

    if projection.by_plate_latest:
        projection.overall_percent = (
            sum(plate.percent for plate in projection.by_plate_latest.values())
            / len(projection.by_plate_latest)
        )

    return projection
