"""Streaming visualizer server base class."""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import replace
from multiprocessing import shared_memory
from typing import Any

import numpy as np
import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import (
    ImageAck,
    PongResponse,
    ProcessResourceUsage,
    ServerRole,
)
from zmqruntime.server import ZMQServer
from zmqruntime.transport import get_zmq_transport_url

logger = logging.getLogger(__name__)


class StreamingVisualizerServer(ZMQServer, ABC):
    """Streaming server that receives and displays images."""

    _server_role = ServerRole.VIEWER

    @staticmethod
    def load_images_from_shared_memory(images, error_callback=None):
        """Decode viewer image payloads from shared memory and clean up."""

        image_data_list = []
        for image_info in images:
            shm_name = image_info.get("shm_name")
            shape = tuple(image_info.get("shape"))
            dtype = np.dtype(image_info.get("dtype"))
            metadata = image_info.get("metadata", {})
            image_id = image_info.get("image_id")

            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                np_data = np.ndarray(shape, dtype=dtype, buffer=shm.buf).copy()
                shm.close()
                shm.unlink()
                image_data_list.append(
                    {"data": np_data, "metadata": metadata, "image_id": image_id}
                )
            except Exception as error:
                logger.error("Failed to read shared memory %s: %s", shm_name, error)
                if error_callback and image_id:
                    error_callback(
                        image_id,
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
        ack_host: str = "localhost",
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
        self._ack_host = ack_host
        self.ack_socket = None
        self._setup_ack_socket()

    def _setup_ack_socket(self):
        """Setup PUSH socket for sending acknowledgments."""
        try:
            ack_url = get_zmq_transport_url(
                self.config.shared_ack_port,
                host=self._ack_host,
                mode=self.transport_mode,
                config=self.config,
            )
            context = zmq.Context.instance()
            self.ack_socket = context.socket(zmq.PUSH)
            self.ack_socket.connect(ack_url)
            logger.info("Connected ack socket to %s", ack_url)
        except Exception as e:
            logger.warning("Failed to setup ack socket: %s", e)
            self.ack_socket = None

    def send_ack(self, image_id: str, status: str = "success", error: str | None = None):
        """Send acknowledgment that an image was processed."""
        if not self.ack_socket:
            return
        try:
            ack = ImageAck(
                image_id=image_id,
                viewer_port=self.port,
                viewer_type=self.viewer_type,
                status=status,
                timestamp=time.time(),
                error=error,
            )
            self.ack_socket.send_json(ack.to_dict())
        except Exception as e:
            logger.warning("Failed to send ack for %s: %s", image_id, e)

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
