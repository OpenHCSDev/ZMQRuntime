"""Execution client with submit/poll/wait and progress streaming."""
from __future__ import annotations

import pickle
import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Any

import zmq

from zmqruntime.client import ZMQClient
from zmqruntime.messages import ControlMessageType, ExecutionStatus, MessageFields
from zmqruntime.transport import get_zmq_transport_url

logger = logging.getLogger(__name__)


class ExecutionClient(ZMQClient, ABC):
    """Execution client with progress streaming."""

    def __init__(self, port: int, host: str = "localhost", persistent: bool = True,
                 progress_callback=None, transport_mode=None, config=None):
        super().__init__(port, host, persistent, transport_mode=transport_mode, config=config)
        self.progress_callback = progress_callback
        self._progress_thread: threading.Thread | None = None
        self._progress_stop_event = threading.Event()

    def _start_progress_listener(self):
        if self._progress_thread and self._progress_thread.is_alive():
            logger.info("Progress listener already running")
            return
        if not self.progress_callback:
            logger.info("No progress callback, skipping listener")
            return
        logger.info("Starting progress listener thread")
        self._progress_stop_event.clear()
        self._progress_thread = threading.Thread(target=self._progress_listener_loop, daemon=True)
        self._progress_thread.start()

    def _stop_progress_listener(self):
        if not self._progress_thread:
            return
        self._progress_stop_event.set()
        if self._progress_thread.is_alive():
            self._progress_thread.join(timeout=2)
        self._progress_thread = None

    def _progress_listener_loop(self):
        logger.info("Progress listener loop started")
        try:
            while not self._progress_stop_event.is_set():
                if not self.data_socket:
                    time.sleep(0.1)
                    continue
                try:
                    message = self.data_socket.recv_string(zmq.NOBLOCK)
                    data = json.loads(message)
                    if self.progress_callback and data.get("type") == "progress":
                        required_fields = [
                            MessageFields.EXECUTION_ID,
                            MessageFields.PLATE_ID,
                            MessageFields.AXIS_ID,
                            MessageFields.STEP_NAME,
                            MessageFields.STEP_INDEX,
                            MessageFields.TOTAL_STEPS,
                            MessageFields.PHASE,
                            MessageFields.STATUS,
                            MessageFields.COMPLETED,
                            MessageFields.TOTAL,
                            MessageFields.PERCENT,
                            MessageFields.TIMESTAMP,
                        ]
                        for field in required_fields:
                            if field not in data:
                                raise KeyError(f"Missing progress field: {field}")
                        self.progress_callback(data)
                except zmq.Again:
                    time.sleep(0.05)
                except Exception as e:
                    logger.warning("Progress listener error: %s", e)
                    time.sleep(0.1)
        except Exception as e:
            logger.error("Progress listener loop crashed: %s", e, exc_info=True)
        finally:
            logger.info("Progress listener loop exited")

    def submit_execution(self, task: Any, config: Any = None):
        if not self._connected and not self.connect():
            raise RuntimeError("Failed to connect to execution server")
        if self.progress_callback:
            self._start_progress_listener()
        request = self.serialize_task(task, config)
        if MessageFields.TYPE not in request:
            request[MessageFields.TYPE] = ControlMessageType.EXECUTE.value
        response = self._send_control_request(request)
        return response

    def poll_status(self, execution_id: str | None = None):
        request = {MessageFields.TYPE: ControlMessageType.STATUS.value}
        if execution_id:
            request[MessageFields.EXECUTION_ID] = execution_id
        return self._send_control_request(request)

    def wait_for_completion(self, execution_id, poll_interval=0.5, max_consecutive_errors=5):
        logger.info("Waiting for execution %s to complete", execution_id)
        consecutive_errors = 0
        poll_count = 0

        while True:
            time.sleep(poll_interval)
            poll_count += 1
            try:
                status_response = self.poll_status(execution_id)
                consecutive_errors = 0

                if status_response.get(MessageFields.STATUS) == "ok":
                    execution = status_response.get("execution", {})
                    exec_status = execution.get(MessageFields.STATUS)
                    if exec_status == ExecutionStatus.COMPLETE.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.COMPLETE.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            "results": execution.get(MessageFields.RESULTS_SUMMARY, {}),
                        }
                    if exec_status == ExecutionStatus.FAILED.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.FAILED.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            MessageFields.MESSAGE: execution.get(MessageFields.ERROR),
                        }
                    if exec_status == ExecutionStatus.CANCELLED.value:
                        return {
                            MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
                            MessageFields.EXECUTION_ID: execution_id,
                            MessageFields.MESSAGE: "Execution was cancelled",
                        }
                elif status_response.get(MessageFields.STATUS) == "error":
                    error_msg = status_response.get(MessageFields.MESSAGE, "Unknown error")
                    return {
                        MessageFields.STATUS: "error",
                        MessageFields.EXECUTION_ID: execution_id,
                        MessageFields.MESSAGE: error_msg,
                    }

            except Exception as e:
                consecutive_errors += 1
                logger.warning(
                    "Error checking execution status (attempt %s/%s): %s",
                    consecutive_errors,
                    max_consecutive_errors,
                    e,
                )

                if consecutive_errors >= max_consecutive_errors:
                    return {
                        MessageFields.STATUS: ExecutionStatus.CANCELLED.value,
                        MessageFields.EXECUTION_ID: execution_id,
                        MessageFields.MESSAGE: "Lost connection to server",
                    }

                time.sleep(1.0)

    def execute(self, task: Any, config: Any = None):
        response = self.submit_execution(task, config)
        if response.get(MessageFields.STATUS) == "accepted":
            execution_id = response.get(MessageFields.EXECUTION_ID)
            return self.wait_for_completion(execution_id)
        return response

    def cancel_execution(self, execution_id):
        return self._send_control_request(
            {MessageFields.TYPE: ControlMessageType.CANCEL.value, MessageFields.EXECUTION_ID: execution_id}
        )

    def ping(self):
        try:
            pong = self.get_server_info()
            return pong.get(MessageFields.TYPE) == "pong" and pong.get(MessageFields.READY)
        except Exception:
            return False

    def get_server_info(self):
        try:
            if not self._connected and not self.connect():
                return {MessageFields.STATUS: "error", MessageFields.MESSAGE: "Not connected"}
            ctx = zmq.Context()
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, 1000)
            control_url = get_zmq_transport_url(
                self.control_port,
                host=self.host,
                mode=self.transport_mode,
                config=self.config,
            )
            sock.connect(control_url)
            sock.send(pickle.dumps({MessageFields.TYPE: ControlMessageType.PING.value}))
            response = pickle.loads(sock.recv())
            sock.close()
            ctx.term()
            return response
        except Exception:
            return {MessageFields.STATUS: "error", MessageFields.MESSAGE: "Failed"}

    def _send_control_request(self, request, timeout_ms=5000):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        control_url = get_zmq_transport_url(
            self.control_port,
            host=self.host,
            mode=self.transport_mode,
            config=self.config,
        )
        sock.connect(control_url)
        try:
            sock.send(pickle.dumps(request))
            response = sock.recv()
            return pickle.loads(response)
        except zmq.Again:
            raise TimeoutError(
                f"Server did not respond to {request.get(MessageFields.TYPE)} request within {timeout_ms}ms"
            )
        finally:
            sock.close()
            ctx.term()

    def disconnect(self):
        self._stop_progress_listener()
        super().disconnect()

    @abstractmethod
    def serialize_task(self, task: Any, config: Any) -> dict:
        """Serialize task for transmission. Subclass provides serialization logic."""
        raise NotImplementedError
