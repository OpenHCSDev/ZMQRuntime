"""Streaming visualizer server base class."""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import replace
from multiprocessing import shared_memory
from typing import Any

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ImageAck,
    ImageTransferIdentity,
    PongResponse,
    ProcessResourceUsage,
    ServerRole,
)
from zmqruntime.server import ZMQServer

logger = logging.getLogger(__name__)


class StreamingVisualizerServer(ZMQServer, ABC):
    """Streaming server that receives and displays images."""

    _server_role = ServerRole.VIEWER

    @staticmethod
    def load_images_from_shared_memory(images, error_callback=None):
        """Decode viewer image payloads from shared memory and clean up."""

        import numpy as np

        image_data_list = []
        for image_info in images:
            shm_name = image_info.get("shm_name")
            shape = tuple(image_info.get("shape"))
            dtype = np.dtype(image_info.get("dtype"))
            metadata = image_info.get("metadata", {})
            transfer = ImageTransferIdentity.from_item(image_info)

            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                np_data = np.ndarray(shape, dtype=dtype, buffer=shm.buf).copy()
                shm.close()
                shm.unlink()
                copied = dict(image_info)
                copied.update(data=np_data, metadata=metadata)
                image_data_list.append(copied)
            except Exception as error:
                logger.error("Failed to read shared memory %s: %s", shm_name, error)
                if error_callback and transfer is not None:
                    error_callback(
                        transfer,
                        "error",
                        f"Failed to read shared memory: {error}",
                    )

        return image_data_list

    def __init__(
        self,
        port: int,
        viewer_type: str,
        host: str = "*",
        log_file_path: str | None = None,
        data_socket_type=None,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
    ):
        super().__init__(
            port,
            host=host,
            log_file_path=log_file_path,
            data_socket_type=data_socket_type,
            transport_mode=transport_mode,
            config=config,
        )
        self.viewer_type = viewer_type

    def send_ack(
        self, transfer: ImageTransferIdentity, status: str = "success", error: str | None = None,
    ) -> bool:
        """Return a bounded per-image ACK to its original producer incarnation.

        Sockets belong to the calling thread, including deferred GUI callbacks.
        No global destination, cross-thread socket or unbounded route cache exists.
        """
        socket = None
        try:
            socket = zmq.Context.instance().socket(zmq.PUSH)
            socket.setsockopt(zmq.LINGER, 1000)
            socket.setsockopt(zmq.SNDTIMEO, 1000)
            socket.setsockopt(zmq.IMMEDIATE, 1)
            socket.connect(transfer.return_route.url)
            ack = ImageAck(
                image_id=transfer.image_id,
                viewer_port=self.port,
                viewer_type=self.viewer_type,
                status=status,
                timestamp=time.time(),
                error=error,
                return_route=transfer.return_route,
                producer=transfer.producer,
            )
            socket.send_json(ack.to_dict())
            return True
        except Exception as e:
            logger.warning("Failed to send ack for %s: %s", transfer.image_id, e)
            return False
        finally:
            if socket is not None:
                socket.close()

    def _create_pong_response(self) -> PongResponse:
        """Extend the shared heartbeat with current viewer-process usage."""

        return replace(
            super()._create_pong_response(),
            process_usage=ProcessResourceUsage.current(),
        )

    def deserialize_message(self, message: bytes) -> dict:
        """Deserialize a raw message payload into a dict."""
        return json.loads(message.decode("utf-8"))

    def handle_data_message(self, message):
        """Handle incoming image data messages by calling display_image."""
        payload = message
        if isinstance(message, (bytes, bytearray)):
            payload = self.deserialize_message(message)
        if not isinstance(payload, dict):
            return

        if "images" in payload and isinstance(payload["images"], list):
            for item in payload["images"]:
                if not isinstance(item, dict):
                    continue
                image_data = item.get("data")
                metadata = item.get("metadata", {})
                if image_data is not None:
                    self.display_image(image_data, metadata)
            return

        image_data = payload.get("data")
        metadata = payload.get("metadata", {})
        if image_data is not None:
            self.display_image(image_data, metadata)

    @abstractmethod
    def display_image(self, image_data: Any, metadata: dict) -> None:
        """Display received image. Implementation provides display logic."""
        raise NotImplementedError
