"""Progress declaration resolution and real downstream typed decoding (#243)."""

import inspect
import json
from dataclasses import FrozenInstanceError, replace
from typing import get_type_hints

import pytest
from python_introspect import dataclass_from_mapping

from zmqruntime.execution.progress_stream import ExecutionProgressObservation
from zmqruntime.messages import ExecutionStatus


@pytest.mark.parametrize(
    "declaration",
    (
        ExecutionProgressObservation,
        *(
            declaration
            for name, declaration in inspect.getmembers(ExecutionProgressObservation)
            if not name.startswith("_")
            and (inspect.isfunction(declaration) or inspect.ismethod(declaration))
        ),
    ),
)
def test_progress_annotations_resolve_without_caller_supplied_namespace(declaration):
    assert get_type_hints(declaration, include_extras=True)


def progress_event(status=ExecutionStatus.RUNNING):
    return {
        "type": "progress",
        "execution_id": "synthetic-execution",
        "phase": "compile",
        "status": status.value,
        "percent": 50.0,
        "timestamp": 1.0,
        "completed": 1,
        "total": 2,
        "context": {"axes": ["A01"], "details": [{"ready": True, "error": None}]},
    }


def assert_detached_immutable_progress(observation, expected):
    assert observation.as_wire() == expected
    with pytest.raises(FrozenInstanceError):
        observation.sequence = 99
    with pytest.raises(TypeError):
        observation.event["status"] = "changed"
    with pytest.raises(TypeError):
        observation.event["context"]["details"][0]["ready"] = False
    projection = observation.as_wire()
    projection["event"]["context"]["axes"].append("B01")
    assert observation.as_wire() == expected


def test_progress_mapping_decode_preserves_wire_and_immutable_nested_values():
    event = progress_event()
    original = ExecutionProgressObservation(1, event)
    payload = json.loads(json.dumps(original.as_wire()))
    decoded = dataclass_from_mapping(ExecutionProgressObservation, payload)
    assert decoded == original
    assert_detached_immutable_progress(decoded, original.as_wire())
    payload["event"]["context"]["axes"].append("C01")
    event["context"]["details"][0]["ready"] = False
    assert decoded == original
    assert ExecutionProgressObservation.from_wire(decoded.as_wire()) == decoded
    followed = replace(decoded, sequence=2, event=progress_event(ExecutionStatus.COMPLETE))
    assert followed.sequence == 2
    assert decoded.sequence == 1


@pytest.mark.parametrize("status", (ExecutionStatus.RUNNING, ExecutionStatus.COMPLETE))
def test_real_openhcs_job_status_decodes_nested_progress(status):
    # Optional downstream integration, not a dependency or copied DTO in ZMQRuntime.
    execution = pytest.importorskip("openhcs.agent.dto.execution")
    from openhcs.agent.dto.common import SCHEMA_VERSION
    from openhcs.serialization.json import to_jsonable

    original = execution.ExecutionJobStatus(
        schema_version=SCHEMA_VERSION,
        session_id="synthetic-session",
        job_id="synthetic-job",
        kind="compile",
        uri="synthetic-job-uri",
        server_execution_id="synthetic-execution",
        status=status.value,
        response={"status": status.value, "result": {"compile_only": True}},
        progress=ExecutionProgressObservation(1, progress_event(status)),
    )
    payload = json.loads(json.dumps(to_jsonable(original)))
    decoded = dataclass_from_mapping(execution.ExecutionJobStatus, payload)
    assert type(decoded) is execution.ExecutionJobStatus
    assert type(decoded.progress) is ExecutionProgressObservation
    assert decoded == original
    assert decoded.is_terminal is status.is_terminal
    assert to_jsonable(decoded) == payload
    assert_detached_immutable_progress(decoded.progress, original.progress.as_wire())

