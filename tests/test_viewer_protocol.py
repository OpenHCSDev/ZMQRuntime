import json
from types import MappingProxyType

import pytest

from zmqruntime import TransportEndpoint, TransportMode, ZMQConfig
from zmqruntime.messages import ProcessResourceUsage, ServerRole
from zmqruntime.streaming import StreamingVisualizerServer
from zmqruntime.viewer_protocol import (
    ViewerBatchDisplayPayload,
    ViewerBatchItemPayload,
    ViewerBatchItemWireField,
    ViewerBatchMessagePayload,
    ViewerBatchWireField,
    ViewerComponentMetadataPayload,
    ViewerComponentMode,
    ViewerDisplayConfigWireField,
    ViewerImageIntensityControlOptions,
    ViewerNativeImageIntensityPresentation,
    ViewerNativeLayerTransform,
    ViewerNativeViewportPresentation,
    ViewerWirePayload,
)


def test_native_viewport_declaration_owns_json_round_trip():
    from dataclasses import fields

    presentation = ViewerNativeViewportPresentation(center=(0, -2.5, 4.25), zoom=3)
    payload = presentation.to_wire_mapping()
    assert set(payload) == {member.name for member in fields(presentation)}
    assert (
        ViewerNativeViewportPresentation.from_wire_mapping(json.loads(json.dumps(payload)))
        == presentation
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"center": (0, 1, 2)},
        {"center": (0, 1, 2), "zoom": 1, "camera_mirror": 1},
        {"center": (0, True, 2), "zoom": 1},
        {"center": (0, 1, float("nan")), "zoom": 1},
        {"center": (0, 1), "zoom": 1},
        {"center": (0, 1, 2), "zoom": False},
        {"center": (0, 1, 2), "zoom": 0},
    ],
)
def test_native_viewport_rejects_malformed_transport(payload):
    with pytest.raises((TypeError, ValueError)):
        ViewerNativeViewportPresentation.from_wire_mapping(payload)


def test_optional_pydantic_preserves_native_defaults_instances_and_schema():
    pydantic = pytest.importorskip("pydantic")
    adapter = pydantic.TypeAdapter(ViewerNativeLayerTransform)
    assert adapter.validate_python({}) == ViewerNativeLayerTransform()
    native = ViewerNativeLayerTransform((1, 2), (3, 4))
    assert adapter.validate_python(native) is native
    with pytest.raises(pydantic.ValidationError, match="unexpected keyword"):
        adapter.validate_python({"unknown": 1})
    with pytest.raises(ValueError):
        ViewerNativeLayerTransform.from_wire_mapping({})  # External wire is strict.
    viewport = pydantic.TypeAdapter(ViewerNativeViewportPresentation)
    assert viewport.validate_python(
        {"center": [0, 1, 2], "zoom": 3}
    ) == ViewerNativeViewportPresentation((0, 1, 2), 3)
    assert set(viewport.json_schema()["properties"]) == {"center", "zoom"}


@pytest.mark.parametrize(
    "owner,payload",
    [
        (ViewerNativeViewportPresentation, {"center": [0, True, 2], "zoom": 1}),
        (ViewerNativeViewportPresentation, {"center": [0, 1, 2], "zoom": True}),
        (ViewerNativeImageIntensityPresentation, {"contrast_limits": [0, True], "gamma": 1}),
        (ViewerNativeImageIntensityPresentation, {"contrast_limits": [0, 1], "gamma": True}),
        (ViewerNativeLayerTransform, {"scale": [True], "translate": [0]}),
    ],
)
def test_optional_pydantic_cannot_coerce_bool_before_native_validation(owner, payload):
    pydantic = pytest.importorskip("pydantic")
    with pytest.raises(pydantic.ValidationError, match="bool|numbers"):
        pydantic.TypeAdapter(owner).validate_python(payload)


def test_native_layer_transform_declarations_own_wire_round_trip():
    from dataclasses import fields

    snapshot = ViewerNativeLayerTransform(scale=(1, 0.65, 0.65), translate=(3, 0, 0))
    payload = snapshot.to_wire_mapping()
    assert set(payload) == {member.name for member in fields(snapshot)}
    assert ViewerNativeLayerTransform.from_wire_mapping(payload) == snapshot
    assert ViewerNativeLayerTransform().to_wire_mapping() == {
        "scale": (),
        "translate": (),
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"scale": (1,), "translate": ()},
        {"scale": (True,), "translate": (0,)},
        {"scale": "1", "translate": (0,)},
        {"scale": (1,), "translate": (0,), "inferred_calibration": 1},
    ],
)
def test_native_layer_transform_rejects_invalid_transport(payload):
    with pytest.raises((TypeError, ValueError)):
        ViewerNativeLayerTransform.from_wire_mapping(payload)


def test_native_image_intensity_round_trip_and_typed_route():
    presentation = ViewerNativeImageIntensityPresentation((-10, 100), 1.25)
    assert (
        ViewerNativeImageIntensityPresentation.from_wire_mapping(presentation.to_wire_mapping())
        == presentation
    )
    assert ViewerImageIntensityControlOptions("image", presentation).presentation is presentation
    with pytest.raises(ValueError):
        ViewerImageIntensityControlOptions(" ", presentation)
    with pytest.raises(TypeError):
        ViewerImageIntensityControlOptions("image", presentation.to_wire_mapping())


@pytest.mark.parametrize(
    "bounds,gamma",
    [
        ((0, 0), 1),
        ((2, 1), 1),
        ((0, float("inf")), 1),
        ((0, 1), float("nan")),
        ((0, 1), 0),
        ((0, 1), -1),
        ((False, 1), 1),
        ((0, 1), True),
        ((0,), 1),
    ],
)
def test_native_image_intensity_rejects_invalid_values(bounds, gamma):
    with pytest.raises((TypeError, ValueError)):
        ViewerNativeImageIntensityPresentation(bounds, gamma)


class _TestStreamingVisualizerServer(StreamingVisualizerServer):
    _server_type = "test-viewer"

    def handle_control_message(self, message):
        del message
        return {}

    def display_image(self, image_data, metadata) -> None:
        del image_data, metadata


def test_viewer_component_modes_distinguish_napari_layers_from_plane_slices():
    assert ViewerComponentMode.LAYER.value == "layer"
    assert ViewerComponentMode.SLICE.value == "slice"


def test_streaming_server_inheritance_owns_viewer_process_usage(monkeypatch):
    usage = ProcessResourceUsage(memory_mb=12.5, cpu_percent=3.0)
    monkeypatch.setattr(
        ProcessResourceUsage,
        "current",
        classmethod(lambda cls: usage),
    )
    server = object.__new__(_TestStreamingVisualizerServer)
    server.config = ZMQConfig(default_port=5555, control_port_offset=1000)
    server.transport_mode = TransportMode.TCP
    server.endpoint = TransportEndpoint(
        host="127.0.0.1",
        port=5555,
        transport_mode=server.transport_mode,
    )
    server._ready = True
    server.log_file_path = None
    server.application = None

    pong = server._create_pong_response()

    assert pong.server_role is ServerRole.VIEWER
    assert pong.process_usage is usage


def test_viewer_batch_message_normalizes_nested_mapping_proxy_to_json_wire():
    item = ViewerBatchItemPayload.from_parts(
        item_payload={
            ViewerBatchItemWireField.PATH: "TrackObjects/labels.tiff",
            ViewerBatchItemWireField.SHAPE: (16, 16),
            ViewerBatchItemWireField.DTYPE: "uint16",
            ViewerBatchItemWireField.SHM_NAME: "napari_test",
        },
        data_type="image",
        metadata={
            "channel": "1",
            "OpenHCSOriginalSourceMetadata": MappingProxyType({"FrameNumber": "0011"}),
        },
        producer_identity={"producer": "TrackObjects"},
        image_id="image-1",
    )

    message = ViewerBatchMessagePayload.from_parts(
        images=[item],
        display_payload=ViewerBatchDisplayPayload(
            component_modes={"channel": ViewerComponentMode.CHANNEL},
            component_order=("channel",),
            extra={
                ViewerDisplayConfigWireField.IMAGES_DIR: "/tmp/openhcs",
            },
        ),
        component_metadata=ViewerComponentMetadataPayload(
            component_names_metadata={
                "channel": MappingProxyType({"1": "DNA"}),
            },
            component_value_domain={
                "channel": ("1",),
            },
        ),
        timestamp=1.0,
        extra={
            "extra": MappingProxyType({"nested": "value"}),
        },
    ).to_wire_mapping()

    json.dumps(message)

    image = message[ViewerBatchWireField.IMAGES.value][0]
    metadata = image[ViewerBatchItemWireField.METADATA.value]
    assert metadata["OpenHCSOriginalSourceMetadata"] == {"FrameNumber": "0011"}
    assert message[ViewerBatchWireField.COMPONENT_NAMES_METADATA.value] == {
        "channel": {"1": "DNA"},
    }
    assert message["extra"] == {"nested": "value"}


def test_viewer_wire_payload_rejects_unsupported_objects_with_context():
    class Unsupported:
        pass

    with pytest.raises(
        TypeError,
        match="viewer batch item metadata.bad",
    ):
        ViewerWirePayload.mapping(
            {"bad": Unsupported()},
            context="viewer batch item metadata",
        )
