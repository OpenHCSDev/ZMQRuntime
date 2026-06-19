"""Typed execution response views for ZMQ execution workflows."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, TypeAlias

from zmqruntime.messages import ExecutionStatus, MessageFields, ResponseType

WireValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | None
    | list["WireValue"]
    | tuple["WireValue", ...]
    | dict[str, "WireValue"]
)
WireRequest: TypeAlias = dict[str, WireValue]
WireResponse: TypeAlias = Mapping[str, WireValue]


class ExecutionDiagnosticField(str, Enum):
    """Wire fields that can carry explicit execution diagnostics."""

    MESSAGE = MessageFields.MESSAGE
    ERROR = MessageFields.ERROR


@dataclass(frozen=True, slots=True)
class ExecutionResponseDiagnostic:
    """Explicit message/error diagnostics parsed from a wire response."""

    message: str | None = None
    error: str | None = None

    @classmethod
    def from_wire(cls, response: WireResponse) -> "ExecutionResponseDiagnostic":
        return cls(
            message=optional_wire_text(response, ExecutionDiagnosticField.MESSAGE),
            error=optional_wire_text(response, ExecutionDiagnosticField.ERROR),
        )

    @property
    def has_text(self) -> bool:
        return self.message is not None or self.error is not None

    def require_text(self, context: str) -> str:
        if self.message is not None and self.error is not None:
            return f"{self.message} ({self.error})"
        if self.message is not None:
            return self.message
        if self.error is not None:
            return self.error
        raise RuntimeError(f"{context} did not include message or error")

    def as_message_items(self) -> dict[str, str]:
        if not self.has_text:
            return {}
        return {MessageFields.MESSAGE: self.require_text("Execution response")}


@dataclass(frozen=True, slots=True)
class ExecutionSubmissionResponse:
    """Typed view of an execution submission response."""

    status: ResponseType
    execution_id: str | None
    diagnostic: ExecutionResponseDiagnostic

    @classmethod
    def from_wire(cls, response: WireResponse) -> "ExecutionSubmissionResponse":
        fields = ExecutionResponseWireFields.from_wire(response)
        return cls(
            status=ResponseType(fields.status),
            execution_id=fields.execution_id,
            diagnostic=fields.diagnostic,
        )

    @property
    def accepted(self) -> bool:
        return self.status is ResponseType.ACCEPTED

    def require_execution_id(self, context: str) -> str:
        if not self.accepted:
            raise RuntimeError(
                f"{context} was not accepted; status={self.status.value!r}"
            )
        if self.execution_id is None:
            raise RuntimeError(f"{context} was accepted without execution_id")
        return self.execution_id

    def require_failure_text(self, context: str) -> str:
        if self.accepted:
            raise RuntimeError(f"{context} was accepted, not failed")
        return self.diagnostic.require_text(context)


class ExecutionSubmissionField(str, Enum):
    """Wire fields owned by execution submission responses."""

    EXECUTION_ID = MessageFields.EXECUTION_ID


@dataclass(frozen=True, slots=True)
class ExecutionResponseWireFields:
    """Shared parsed fields present on execution response envelopes."""

    status: str
    execution_id: str | None
    diagnostic: ExecutionResponseDiagnostic

    @classmethod
    def from_wire(cls, response: WireResponse) -> "ExecutionResponseWireFields":
        execution_id = None
        if MessageFields.EXECUTION_ID in response:
            execution_id = optional_wire_text(
                response,
                ExecutionSubmissionField.EXECUTION_ID,
            )

        return cls(
            status=str(response[MessageFields.STATUS]),
            execution_id=execution_id,
            diagnostic=ExecutionResponseDiagnostic.from_wire(response),
        )


@dataclass(frozen=True, slots=True)
class ExecutionWaitResult:
    """Typed view of an execution wait result."""

    fields: ExecutionResponseWireFields

    @classmethod
    def from_wire(cls, response: WireResponse) -> "ExecutionWaitResult":
        return cls(fields=ExecutionResponseWireFields.from_wire(response))

    @property
    def status(self) -> str:
        return self.fields.status

    @property
    def execution_id(self) -> str | None:
        return self.fields.execution_id

    @property
    def diagnostic(self) -> ExecutionResponseDiagnostic:
        return self.fields.diagnostic

    @property
    def complete(self) -> bool:
        return self.status == ExecutionStatus.COMPLETE.value

    def require_complete(self, context: str) -> None:
        if self.complete:
            return
        diagnostic_text = self.diagnostic.require_text(context)
        raise RuntimeError(f"{context}: {diagnostic_text}")


def optional_wire_text(
    response: WireResponse,
    field: ExecutionDiagnosticField | ExecutionSubmissionField,
) -> str | None:
    if field.value not in response:
        return None
    value = response[field.value]
    if value is None:
        return None
    return str(value)
