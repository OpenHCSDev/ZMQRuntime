import json
from types import MappingProxyType

import pytest

from zmqruntime.viewer_protocol import (
    ViewerBatchDisplayPayload,
    ViewerBatchItemPayload,
    ViewerBatchItemWireField,
    ViewerBatchMessagePayload,
    ViewerBatchWireField,
    ViewerComponentMetadataPayload,
    ViewerComponentMode,
    ViewerDisplayConfigWireField,
    ViewerWirePayload,
)


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
            "OpenHCSOriginalSourceMetadata": MappingProxyType(
                {"FrameNumber": "0011"}
            ),
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
