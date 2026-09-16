"""Shared viewer transport and batch-message protocol."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields
from enum import Enum
from math import isfinite
from numbers import Integral, Real
from typing import TYPE_CHECKING, Self, TypeAlias, Union

import zmq

from zmqruntime.messages import MessageFields
from zmqruntime.transport import TransportEndpoint

if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler
    from pydantic_core import CoreSchema

ViewerWireScalar: TypeAlias = str | int | float | bool | None
ViewerWireValue: TypeAlias = (
    ViewerWireScalar
    | tuple["ViewerWireValue", ...]
    | list["ViewerWireValue"]
    | dict[str, "ViewerWireValue"]
)
ViewerWireMapping: TypeAlias = Mapping[str, ViewerWireValue]
ViewerCleanup: TypeAlias = Callable[[], None]


class ViewerDeclaredWireValue:
    """Wire projection derived from a nominal value's dataclass declarations."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Preserve native constructor validation before optional wire coercion."""
        # Pydantic is optional: its integration imports only on this schema hook.
        from pydantic_core import core_schema

        def construct(value: object) -> object:
            if isinstance(value, Mapping):
                try:
                    return cls(**value)
                except TypeError as error:
                    raise ValueError(str(error)) from error
            return value

        return core_schema.no_info_before_validator_function(construct, handler(source_type))

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return {member.name: getattr(self, member.name) for member in fields(self)}

    @classmethod
    def from_wire_mapping(cls, payload: Mapping[str, object]) -> Self:
        members = fields(cls)
        expected = {member.name for member in members}
        if set(payload) != expected:
            raise ValueError(f"{cls.__name__} requires exactly {sorted(expected)!r}.")
        return cls(**{member.name: payload[member.name] for member in members})


@dataclass(frozen=True, slots=True)
class ViewerNativeLayerTransform(ViewerDeclaredWireValue):
    """Native layer coordinates, not camera fitting or inferred pixel calibration.

    The dataclass declarations own the wire members and their tuple projection.
    An empty snapshot represents a route without a mounted native layer.
    """

    scale: tuple[float, ...] = ()
    translate: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        for member in fields(self):
            values = getattr(self, member.name)
            if not isinstance(values, (tuple, list)) or any(
                isinstance(value, bool) or not isinstance(value, Real) for value in values
            ):
                raise TypeError(f"Native transform {member.name} must contain real numbers.")
            object.__setattr__(self, member.name, tuple(float(value) for value in values))
        if len(self.scale) != len(self.translate):
            raise ValueError("Native scale and translate must have the same dimensionality.")


@dataclass(frozen=True, slots=True)
class ViewerNativeImageIntensityPresentation(ViewerDeclaredWireValue):
    """Complete native display bounds and gamma, never image preprocessing."""

    contrast_limits: tuple[float, float]
    gamma: float

    def __post_init__(self) -> None:
        if not isinstance(self.contrast_limits, (tuple, list)) or len(self.contrast_limits) != 2:
            raise TypeError("contrast_limits must contain exactly two real bounds.")
        values = (*self.contrast_limits, self.gamma)
        if any(isinstance(value, bool) or not isinstance(value, Real) for value in values):
            raise TypeError("Image intensity presentation requires real numbers, not bools.")
        if not all(isfinite(value) for value in values):
            raise ValueError("Image intensity presentation requires finite values.")
        lower, upper = (float(value) for value in self.contrast_limits)
        if lower >= upper:
            raise ValueError("contrast_limits lower bound must be smaller than upper bound.")
        if self.gamma <= 0:
            raise ValueError("gamma must be positive.")
        object.__setattr__(self, "contrast_limits", (lower, upper))
        object.__setattr__(self, "gamma", float(self.gamma))


@dataclass(frozen=True, slots=True)
class ViewerNativeViewportPresentation(ViewerDeclaredWireValue):
    """Native 2D camera center in world coordinates and canvas/world zoom."""

    center: tuple[float, float, float]
    zoom: float

    def __post_init__(self) -> None:
        if not isinstance(self.center, (tuple, list)) or len(self.center) != 3:
            raise TypeError("Viewport center must contain three native coordinates.")
        values = (*self.center, self.zoom)
        if any(isinstance(value, bool) or not isinstance(value, Real) for value in values):
            raise TypeError("Viewport presentation requires real numbers, not bools.")
        if not all(isfinite(value) for value in values):
            raise ValueError("Viewport presentation requires finite values.")
        if self.zoom <= 0:
            raise ValueError("Viewport zoom must be positive.")
        object.__setattr__(self, "center", tuple(float(value) for value in self.center))
        object.__setattr__(self, "zoom", float(self.zoom))


@dataclass(frozen=True, slots=True)
class ViewerImageIntensityControlOptions:
    """Generic exact-route native image-presentation command contract."""

    route_key: str
    presentation: ViewerNativeImageIntensityPresentation

    def __post_init__(self) -> None:
        if not isinstance(self.route_key, str) or not self.route_key.strip():
            raise ValueError("Image intensity route_key must be a nonblank string.")
        if not isinstance(self.presentation, ViewerNativeImageIntensityPresentation):
            raise TypeError("Image intensity control requires a typed native presentation.")


class ViewerControlMessageType(Enum):
    """Shared control-message identities consumed by viewer servers."""

    SCREENSHOT = "screenshot"
    CLEAR_STATE = "clear_state"
    SETTLE = "settle"
    STATE = "state"
    PAYLOADS = "payloads"
    NAVIGATE = "navigate"
    ISOLATE_LAYERS = "isolate_layers"
    IMAGE_INTENSITY = "image_intensity"
    VIEWPORT = "viewport"

    @property
    def acknowledgement_type(self) -> str:
        return f"{self.value}_ack"


class ViewerProtocolStatus(Enum):
    """Control and stream-reply status values shared by viewer servers."""

    SUCCESS = "success"
    ERROR = "error"


class ViewerControlResponseField(str, Enum):
    """Wire fields shared by viewer control and stream replies."""

    STATUS = MessageFields.STATUS
    TYPE = MessageFields.TYPE
    MESSAGE = MessageFields.MESSAGE
    PAYLOAD = "payload"


class ViewerBatchMessageType(str, Enum):
    """Viewer stream message kinds accepted by blocking stream servers."""

    BATCH = "batch"


class ViewerWireField(str, Enum):
    """Authoritative wire fields shared by viewer stream messages."""

    TYPE = MessageFields.TYPE
    IMAGES = "images"
    DISPLAY_CONFIG = "display_config"
    COMPONENT_NAMES_METADATA = "component_names_metadata"
    COMPONENT_VALUE_DOMAIN = "component_value_domain"
    TIMESTAMP = MessageFields.TIMESTAMP
    PATH = "path"
    SHAPE = "shape"
    DTYPE = "dtype"
    SHM_NAME = "shm_name"
    DATA = "data"
    DATA_TYPE = "data_type"
    METADATA = MessageFields.METADATA
    PRODUCER_IDENTITY = "producer_identity"
    IMAGE_ID = MessageFields.IMAGE_ID
    SOURCE_CHANNEL_AXIS = "source_channel_axis"
    IMAGE_METADATA = "image_metadata"
    PLANE_AXIS = "plane_axis"
    PLANE_COMPONENT_VALUES = "plane_component_values"
    SHAPES = MessageFields.SHAPES
    ROIS = MessageFields.ROIS
    COMPONENT_MODES = "component_modes"
    COMPONENT_ORDER = "component_order"
    IMAGES_DIR = "images_dir"


ViewerWireRawValue: TypeAlias = (
    ViewerWireScalar
    | Enum
    | Mapping[str | Enum, "ViewerWireRawValue"]
    | Sequence["ViewerWireRawValue"]
)
ViewerWireRawMapping: TypeAlias = Mapping[str | Enum, ViewerWireRawValue]


class ViewerWirePayload:
    """Normalize declared viewer payloads into JSON-compatible wire values."""

    @classmethod
    def mapping(
        cls,
        values: ViewerWireRawMapping,
        *,
        context: str,
    ) -> dict[str, ViewerWireValue]:
        payload: dict[str, ViewerWireValue] = {}
        for field_name, value in values.items():
            key = cls.key(field_name, context=context)
            payload[key] = cls.value(value, context=f"{context}.{key}")
        return payload

    @classmethod
    def value(
        cls,
        value: ViewerWireRawValue,
        *,
        context: str,
    ) -> ViewerWireValue:
        if value is None or isinstance(value, (str, bool, int, float)):
            return value
        if isinstance(value, Integral):
            return int(value)
        if isinstance(value, Real):
            return float(value)
        if isinstance(value, Enum):
            return cls.value(value.value, context=f"{context}.value")
        if isinstance(value, Mapping):
            return cls.mapping(value, context=context)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return [
                cls.value(item, context=f"{context}[{index}]") for index, item in enumerate(value)
            ]
        raise TypeError(
            f"Viewer wire payload field {context!r} cannot serialize {type(value).__name__}."
        )

    @staticmethod
    def key(field: str | Enum, *, context: str) -> str:
        if isinstance(field, (str, Enum)):
            return viewer_wire_key(field)
        raise TypeError(
            f"Viewer wire payload mapping {context!r} has non-string key "
            f"{field!r} ({type(field).__name__})."
        )


class ViewerSourceSpatialWireField(str, Enum):
    """Wire fields for source-image XY placement metadata."""

    SPATIAL_ORIGIN_YX = "spatial_origin_yx"
    SOURCE_SPATIAL_SHAPE_YX = "source_spatial_shape_yx"


class ViewerComponentMode(str, Enum):
    """Viewer component placement modes shared by stream receivers."""

    STACK = "stack"
    LAYER = "layer"
    SLICE = "slice"
    WINDOW = "window"
    CHANNEL = "channel"
    FRAME = "frame"


@dataclass(frozen=True)
class ViewerComponentModeGroups:
    """Components grouped by viewer placement mode for a display payload."""

    components_by_mode: Mapping[str, tuple[str, ...]]
    unsupported_component_modes: Mapping[str, str]

    def components_for_mode(
        self,
        mode: ViewerComponentMode | str | Enum,
    ) -> tuple[str, ...]:
        mode_value = viewer_component_mode_value(mode)
        if mode_value not in self.components_by_mode:
            raise ValueError(f"Viewer component mode groups missing mode {mode_value!r}.")
        return self.components_by_mode[mode_value]

    def require_all_supported(self, context: str) -> None:
        if self.unsupported_component_modes:
            raise ValueError(
                f"Unsupported viewer component modes for {context}: "
                f"{dict(self.unsupported_component_modes)!r}."
            )


ViewerBatchWireField: TypeAlias = ViewerWireField
ViewerBatchItemWireField: TypeAlias = ViewerWireField
ViewerDisplayConfigWireField: TypeAlias = ViewerWireField
ViewerBatchContextWireField: TypeAlias = ViewerWireField
ViewerBatchItemWireMapping: TypeAlias = Mapping[
    str | ViewerBatchItemWireField,
    ViewerWireRawValue,
]
ViewerBatchMessageImages: TypeAlias = Sequence[
    Union["ViewerBatchItemPayload", ViewerWireRawMapping]
]
ViewerBatchMessageExtraInput: TypeAlias = (
    Mapping[
        str | ViewerBatchWireField | ViewerBatchContextWireField,
        ViewerWireRawValue,
    ]
    | None
)


class ViewerBatchMessageExtraPayload(dict[str, ViewerWireValue]):
    """Normalized extra fields for a viewer batch message."""

    @classmethod
    def from_mapping(
        cls,
        values: ViewerBatchMessageExtraInput,
    ) -> "ViewerBatchMessageExtraPayload":
        if values is None:
            return cls()
        return cls(
            ViewerWirePayload.mapping(
                values,
                context="viewer batch message extra",
            )
        )


@dataclass(frozen=True)
class ViewerSourceSpatialDomainPayload:
    """Viewer-wire source-image XY placement metadata."""

    origin_yx: tuple[int, int] | None = None
    source_shape_yx: tuple[int, int] | None = None

    @classmethod
    def from_wire_mapping(
        cls,
        payload: ViewerWireMapping,
        *,
        source_label: str,
    ) -> "ViewerSourceSpatialDomainPayload":
        return cls(
            origin_yx=cls._optional_pair(
                payload,
                ViewerSourceSpatialWireField.SPATIAL_ORIGIN_YX,
                source_label,
            ),
            source_shape_yx=cls._optional_pair(
                payload,
                ViewerSourceSpatialWireField.SOURCE_SPATIAL_SHAPE_YX,
                source_label,
            ),
        )

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        payload: dict[str, ViewerWireValue] = {}
        if self.origin_yx is not None:
            payload[ViewerSourceSpatialWireField.SPATIAL_ORIGIN_YX.value] = (
                int(self.origin_yx[0]),
                int(self.origin_yx[1]),
            )
        if self.source_shape_yx is not None:
            payload[ViewerSourceSpatialWireField.SOURCE_SPATIAL_SHAPE_YX.value] = (
                int(self.source_shape_yx[0]),
                int(self.source_shape_yx[1]),
            )
        return payload

    def required_source_shape_yx(self, *, source_label: str) -> tuple[int, int]:
        if self.source_shape_yx is None:
            raise ValueError(
                f"{source_label} requires "
                f"{ViewerSourceSpatialWireField.SOURCE_SPATIAL_SHAPE_YX.value!r}."
            )
        return self.source_shape_yx

    @staticmethod
    def _optional_pair(
        payload: ViewerWireMapping,
        field: ViewerSourceSpatialWireField,
        source_label: str,
    ) -> tuple[int, int] | None:
        field_name = field.value
        if field_name not in payload or payload[field_name] is None:
            return None
        value = payload[field_name]
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise TypeError(
                f"{source_label} field {field_name!r} must be a two-value sequence, "
                f"got {type(value).__name__}."
            )
        if len(value) != 2:
            raise ValueError(
                f"{source_label} field {field_name!r} must have exactly two values, got {value!r}."
            )
        return int(value[0]), int(value[1])


class ViewerBatchMessageWirePayload(dict[str, ViewerWireValue]):
    """Normalized wire envelope for a complete viewer batch message."""


def viewer_wire_key(field: str | Enum) -> str:
    """Return the concrete JSON key for a named viewer wire field."""

    if isinstance(field, Enum):
        return str(field.value)
    return field


@dataclass(frozen=True)
class ViewerBatchDisplayPayload:
    """Shared display-config section of a viewer batch message."""

    component_modes: Mapping[str, str]
    component_order: Sequence[str]
    extra: Mapping[str | ViewerDisplayConfigWireField, ViewerWireValue] = field(
        default_factory=dict
    )

    def components_for_mode(
        self,
        mode: ViewerComponentMode | str | Enum,
    ) -> tuple[str, ...]:
        mode_value = viewer_component_mode_value(mode)
        components: list[str] = []
        for component in self.component_order:
            if component not in self.component_modes:
                raise ValueError(
                    f"Viewer display payload missing mode for component {component!r}."
                )
            if viewer_component_mode_value(self.component_modes[component]) == mode_value:
                components.append(component)
        return tuple(components)

    def component_mode_groups(
        self,
        supported_modes: Sequence[ViewerComponentMode | str | Enum],
    ) -> ViewerComponentModeGroups:
        supported_mode_values = tuple(viewer_component_mode_value(mode) for mode in supported_modes)
        components_by_mode: dict[str, list[str]] = {mode: [] for mode in supported_mode_values}
        unsupported_component_modes: dict[str, str] = {}
        for component in self.component_order:
            if component not in self.component_modes:
                raise ValueError(
                    f"Viewer display payload missing mode for component {component!r}."
                )
            mode_value = viewer_component_mode_value(self.component_modes[component])
            if mode_value in components_by_mode:
                components_by_mode[mode_value].append(component)
            else:
                unsupported_component_modes[component] = mode_value
        return ViewerComponentModeGroups(
            components_by_mode={
                mode: tuple(components) for mode, components in components_by_mode.items()
            },
            unsupported_component_modes=unsupported_component_modes,
        )

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        payload: dict[str, ViewerWireValue] = {
            ViewerDisplayConfigWireField.COMPONENT_MODES.value: ViewerWirePayload.mapping(
                self.component_modes,
                context="viewer display component modes",
            ),
            ViewerDisplayConfigWireField.COMPONENT_ORDER.value: ViewerWirePayload.value(
                list(self.component_order),
                context="viewer display component order",
            ),
        }
        payload.update(
            ViewerWirePayload.mapping(
                self.extra,
                context="viewer display extra",
            )
        )
        return payload


def viewer_component_mode_value(mode: ViewerComponentMode | str | Enum) -> str:
    """Return the canonical wire value for a viewer component mode."""

    if isinstance(mode, Enum):
        return str(mode.value)
    return str(mode)


@dataclass(frozen=True)
class ViewerBatchItemPayload:
    """One item inside a shared viewer batch message."""

    fields: ViewerBatchItemWireMapping

    @classmethod
    def from_parts(
        cls,
        *,
        item_payload: ViewerBatchItemWireMapping,
        data_type: str,
        metadata: ViewerWireMapping,
        producer_identity: ViewerWireMapping,
        image_id: str,
    ) -> "ViewerBatchItemPayload":
        fields = ViewerWirePayload.mapping(
            item_payload,
            context="viewer batch item payload",
        )
        fields.update(
            {
                ViewerBatchItemWireField.DATA_TYPE.value: data_type,
                ViewerBatchItemWireField.METADATA.value: ViewerWirePayload.mapping(
                    metadata,
                    context="viewer batch item metadata",
                ),
                ViewerBatchItemWireField.PRODUCER_IDENTITY.value: ViewerWirePayload.mapping(
                    producer_identity,
                    context="viewer batch item producer identity",
                ),
                ViewerBatchItemWireField.IMAGE_ID.value: image_id,
            }
        )
        return cls(fields)

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return ViewerWirePayload.mapping(
            self.fields,
            context="viewer batch item",
        )


@dataclass(frozen=True)
class ViewerComponentMetadataPayload:
    """Component value domains and display names shared by viewer payloads."""

    component_names_metadata: ViewerWireMapping
    component_value_domain: ViewerWireMapping

    def scoped_to(
        self,
        component_order: Iterable[str],
    ) -> ViewerComponentMetadataPayload:
        """Restrict both component projections to the declared display axes."""

        components = frozenset(component_order)
        return type(self)(
            component_names_metadata={
                component: values
                for component, values in self.component_names_metadata.items()
                if component in components
            },
            component_value_domain={
                component: values
                for component, values in self.component_value_domain.items()
                if component in components
            },
        )

    @classmethod
    def from_wire_mapping(
        cls,
        payload: ViewerWireMapping,
    ) -> "ViewerComponentMetadataPayload":
        return cls(
            component_names_metadata=cls._required_mapping(
                payload,
                ViewerBatchWireField.COMPONENT_NAMES_METADATA,
            ),
            component_value_domain=cls._required_mapping(
                payload,
                ViewerBatchWireField.COMPONENT_VALUE_DOMAIN,
            ),
        )

    @classmethod
    def from_optional_wire_mapping(
        cls,
        payload: ViewerWireMapping,
    ) -> "ViewerComponentMetadataPayload | None":
        names_key = ViewerBatchWireField.COMPONENT_NAMES_METADATA.value
        domain_key = ViewerBatchWireField.COMPONENT_VALUE_DOMAIN.value
        if names_key not in payload and domain_key not in payload:
            return None
        return cls.from_wire_mapping(payload)

    @classmethod
    def strip_component_metadata(
        cls,
        payload: Mapping[str | ViewerBatchWireField, ViewerWireValue],
    ) -> dict[str, ViewerWireValue]:
        metadata_keys = {
            ViewerBatchWireField.COMPONENT_NAMES_METADATA.value,
            ViewerBatchWireField.COMPONENT_VALUE_DOMAIN.value,
        }
        stripped: dict[str | ViewerBatchWireField, ViewerWireValue] = {}
        for field_name, value in payload.items():
            key = viewer_wire_key(field_name)
            if key not in metadata_keys:
                stripped[field_name] = value
        return ViewerWirePayload.mapping(
            stripped,
            context="viewer batch message extra without component metadata",
        )

    def component_metadata_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return {
            ViewerBatchWireField.COMPONENT_NAMES_METADATA.value: ViewerWirePayload.mapping(
                self.component_names_metadata,
                context="viewer component names metadata",
            ),
            ViewerBatchWireField.COMPONENT_VALUE_DOMAIN.value: ViewerWirePayload.mapping(
                self.component_value_domain,
                context="viewer component value domain",
            ),
        }

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return self.component_metadata_wire_mapping()

    @staticmethod
    def _required_mapping(
        payload: ViewerWireMapping,
        field: ViewerBatchWireField,
    ) -> ViewerWireMapping:
        field_name = field.value
        if field_name not in payload:
            raise ValueError(f"Viewer component metadata payload missing {field_name!r}.")
        value = payload[field_name]
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Viewer component metadata field {field_name!r} must be a mapping, "
                f"got {type(value).__name__}."
            )
        return ViewerWirePayload.mapping(
            value,
            context=f"viewer component metadata {field_name}",
        )


class ViewerBatchMessagePayload(ViewerBatchMessageWirePayload):
    """Shared wire payload sent to blocking viewer stream servers."""

    @classmethod
    def from_parts(
        cls,
        *,
        images: ViewerBatchMessageImages,
        display_payload: ViewerBatchDisplayPayload | ViewerWireMapping,
        component_metadata: ViewerComponentMetadataPayload,
        timestamp: float,
        extra: ViewerBatchMessageExtraInput = None,
    ) -> "ViewerBatchMessagePayload":
        payload = cls(
            {
                ViewerBatchWireField.TYPE.value: ViewerBatchMessageType.BATCH.value,
                ViewerBatchWireField.IMAGES.value: [
                    (
                        image.to_wire_mapping()
                        if isinstance(image, ViewerBatchItemPayload)
                        else ViewerWirePayload.mapping(
                            image,
                            context=f"viewer batch message image[{index}]",
                        )
                    )
                    for index, image in enumerate(images)
                ],
                ViewerBatchWireField.DISPLAY_CONFIG.value: (
                    display_payload.to_wire_mapping()
                    if isinstance(display_payload, ViewerBatchDisplayPayload)
                    else ViewerWirePayload.mapping(
                        display_payload,
                        context="viewer batch display payload",
                    )
                ),
                ViewerBatchWireField.TIMESTAMP.value: ViewerWirePayload.value(
                    timestamp,
                    context="viewer batch timestamp",
                ),
            }
        )
        payload.update(component_metadata.component_metadata_wire_mapping())
        payload.update(ViewerBatchMessageExtraPayload.from_mapping(extra))
        return cls(
            ViewerWirePayload.mapping(
                payload,
                context="viewer batch message",
            )
        )

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return ViewerWirePayload.mapping(
            self,
            context="viewer batch message",
        )


ViewerTransportEndpoint = TransportEndpoint


@dataclass(frozen=True)
class ViewerControlReplyHeader:
    """Shared status/type/message header for viewer control replies."""

    status: ViewerProtocolStatus
    response_type: str | None = None
    message: str | None = None

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        payload: dict[str, ViewerWireValue] = {
            ViewerControlResponseField.STATUS.value: self.status.value,
        }
        if self.response_type is not None:
            payload[ViewerControlResponseField.TYPE.value] = self.response_type
        if self.message is not None:
            payload[ViewerControlResponseField.MESSAGE.value] = self.message
        return payload


@dataclass(frozen=True)
class ViewerControlReplyPayload:
    """Wire payload for a viewer control-message reply."""

    header: ViewerControlReplyHeader
    fields: Mapping[str, ViewerWireValue] = field(default_factory=dict)
    payload: object | None = None

    def to_wire_mapping(self) -> dict[str, object]:
        wire_payload: dict[str, object] = dict(self.header.to_wire_mapping())
        wire_payload.update(
            ViewerWirePayload.mapping(
                self.fields,
                context="viewer control reply fields",
            )
        )
        if self.payload is not None:
            wire_payload[ViewerControlResponseField.PAYLOAD.value] = self.payload
        return wire_payload


@dataclass(frozen=True)
class ViewerAckResponsePayload:
    """Validated mapping returned by a blocking viewer stream REP socket."""

    payload: Mapping[str, ViewerWireValue]

    @classmethod
    def from_wire(cls, payload) -> "ViewerAckResponsePayload":
        if not isinstance(payload, Mapping):
            raise TypeError(f"Viewer ack response must be a mapping, got {type(payload).__name__}.")
        return cls(payload)

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return ViewerWirePayload.mapping(
            self.payload,
            context="viewer ack response",
        )


@dataclass(frozen=True)
class ViewerAckPolicy:
    """REQ/REP ack contract for a blocking streaming viewer."""

    viewer_name: str
    timeout_ms: int

    def apply_socket_options(self, socket: zmq.Socket) -> None:
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)

    def receive(
        self,
        socket: zmq.Socket,
        cleanup: ViewerCleanup,
        *,
        port: int,
    ) -> dict[str, ViewerWireValue]:
        try:
            ack_response = ViewerAckResponsePayload.from_wire(socket.recv_json())
        except zmq.Again as exc:
            cleanup()
            raise TimeoutError(
                f"Timed out waiting {self.timeout_ms}ms for {self.viewer_name} ack on port {port}"
            ) from exc
        try:
            self.require_success(ack_response.payload)
        except Exception:
            cleanup()
            raise
        return ack_response.to_wire_mapping()

    def status(self, ack_response: Mapping[str, ViewerWireValue]) -> str:
        if ViewerControlResponseField.STATUS.value not in ack_response:
            raise ValueError(
                f"{self.viewer_name} ack response missing required "
                f"{ViewerControlResponseField.STATUS.value!r}: {ack_response}"
            )
        return str(ack_response[ViewerControlResponseField.STATUS.value])

    def require_success(self, ack_response: Mapping[str, ViewerWireValue]) -> None:
        status = self.status(ack_response)
        if status == ViewerProtocolStatus.SUCCESS.value:
            return
        if status != ViewerProtocolStatus.ERROR.value:
            raise ValueError(
                f"{self.viewer_name} ack response has unknown status {status!r}: {ack_response}"
            )

        if ViewerControlResponseField.MESSAGE.value in ack_response:
            detail = ack_response[ViewerControlResponseField.MESSAGE.value]
        else:
            detail = ack_response
        raise RuntimeError(f"{self.viewer_name} rejected stream batch: {detail}")
