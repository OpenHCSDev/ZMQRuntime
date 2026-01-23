"""Generic ZMQ execution server with queue-based processing."""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures.process import BrokenProcessPool
from typing import Any

from zmqruntime.messages import (
    CancelRequest,
    ControlMessageType,
    ExecuteRequest,
    ExecuteResponse,
    ExecutionStatus,
    MessageFields,
    PongResponse,
    ResponseType,
    StatusRequest,
)
from zmqruntime.config import ZMQConfig
from zmqruntime.server import ZMQServer

logger = logging.getLogger(__name__)


class ExecutionServer(ZMQServer, ABC):
    """Queue-based execution server with progress streaming."""

    _server_type = "execution"

    def __init__(self, port: int | None = None, host: str = "*", log_file_path: str | None = None,
                 transport_mode=None, config: ZMQConfig | None = None):
        config = config or ZMQConfig()
        if port is None:
            port = config.default_port
        super().__init__(port, host, log_file_path, transport_mode=transport_mode, config=config)
        self.active_executions: dict[str, dict] = {}
        self.start_time = None
        self.progress_queue: queue.Queue = queue.Queue()

        self.execution_queue: queue.Queue = queue.Queue()
        self.queue_worker_thread: threading.Thread | None = None

    def start(self):
        super().start()
        self.start_time = self.start_time or time.time()
        self._start_queue_worker()

    def _create_pong_response(self):
        running = [
            (eid, r)
            for eid, r in self.active_executions.items()
            if r.get(MessageFields.STATUS) == ExecutionStatus.RUNNING.value
        ]
        queued = [
            (eid, r)
            for eid, r in self.active_executions.items()
            if r.get(MessageFields.STATUS) == ExecutionStatus.QUEUED.value
        ]
        return (
            PongResponse(
                port=self.port,
                control_port=self.control_port,
                ready=self._ready,
                server=self.__class__.__name__,
                log_file_path=self.log_file_path,
                active_executions=len(running) + len(queued),
                running_executions=[
                    {
                        MessageFields.EXECUTION_ID: eid,
                        MessageFields.PLATE_ID: r.get(MessageFields.PLATE_ID, "unknown"),
                        MessageFields.START_TIME: r.get(MessageFields.START_TIME, 0),
                        MessageFields.ELAPSED: time.time() - r.get(MessageFields.START_TIME, 0)
                        if r.get(MessageFields.START_TIME)
                        else 0,
                    }
                    for eid, r in running
                ],
                workers=self._get_worker_info(),
                uptime=time.time() - self.start_time if self.start_time else 0,
            ).to_dict()
        )

    def process_messages(self):
        super().process_messages()
        while not self.progress_queue.empty():
            try:
                if self.data_socket:
                    self.data_socket.send_string(json.dumps(self.progress_queue.get_nowait()))
            except (queue.Empty, Exception) as e:
                if not isinstance(e, queue.Empty):
                    logger.warning("Failed to send progress: %s", e)
                break

    def get_status_info(self):
        status = super().get_status_info()
        status.update(
            {
                "active_executions": len(self.active_executions),
                "uptime": time.time() - self.start_time if self.start_time else 0,
                "executions": list(self.active_executions.values()),
            }
        )
        return status

    def handle_control_message(self, message):
        try:
            return ControlMessageType(message.get(MessageFields.TYPE)).dispatch(self, message)
        except ValueError:
            return ExecuteResponse(
                ResponseType.ERROR,
                error=f"Unknown message type: {message.get(MessageFields.TYPE)}",
            ).to_dict()

    def handle_data_message(self, message):
        pass

    def _validate_and_parse(self, msg, request_class):
        try:
            request = request_class.from_dict(msg)
            if hasattr(request, "validate") and (error := request.validate()):
                return None, ExecuteResponse(ResponseType.ERROR, error=error).to_dict()
            return request, None
        except KeyError as e:
            return None, ExecuteResponse(ResponseType.ERROR, error=f"Missing field: {e}").to_dict()

    def _start_queue_worker(self):
        if self.queue_worker_thread is None or not self.queue_worker_thread.is_alive():
            self.queue_worker_thread = threading.Thread(target=self._queue_worker, daemon=True)
            self.queue_worker_thread.start()
            logger.info("Started execution queue worker thread")

    def _queue_worker(self):
        logger.info("Queue worker thread started - will process executions sequentially")
        try:
            while self._running:
                try:
                    try:
                        execution_id, request, record = self.execution_queue.get(timeout=1.0)
                    except queue.Empty:
                        continue

                    logger.info(
                        "[%s] Dequeued for execution (queue size: %s)",
                        execution_id,
                        self.execution_queue.qsize(),
                    )

                    if not self._running:
                        logger.info("[%s] Server shutting down, skipping execution", execution_id)
                        record[MessageFields.STATUS] = ExecutionStatus.CANCELLED.value
                        self.execution_queue.task_done()
                        break

                    if record[MessageFields.STATUS] == ExecutionStatus.CANCELLED.value:
                        logger.info("[%s] Execution was cancelled while queued, skipping", execution_id)
                        self.execution_queue.task_done()
                        continue

                    self._run_execution(execution_id, request, record)
                    self.execution_queue.task_done()
                except Exception as e:
                    logger.error("Queue worker error: %s", e, exc_info=True)
        finally:
            remaining = 0
            while not self.execution_queue.empty():
                try:
                    execution_id, request, record = self.execution_queue.get_nowait()
                    record[MessageFields.STATUS] = ExecutionStatus.CANCELLED.value
                    record[MessageFields.END_TIME] = time.time()
                    logger.info("[%s] Cancelled (was queued when server shut down)", execution_id)
                    self.execution_queue.task_done()
                    remaining += 1
                except queue.Empty:
                    break

            if remaining > 0:
                logger.info("Cancelled %s queued executions during shutdown", remaining)
            logger.info("Queue worker thread exiting")

    def _handle_execute(self, msg):
        request, error = self._validate_and_parse(msg, ExecuteRequest)
        if error:
            return error
        execution_id = str(uuid.uuid4())
        record = {
            MessageFields.EXECUTION_ID: execution_id,
            MessageFields.PLATE_ID: request.plate_id,
            MessageFields.CLIENT_ADDRESS: request.client_address,
            MessageFields.STATUS: ExecutionStatus.QUEUED.value,
            MessageFields.START_TIME: None,
            MessageFields.END_TIME: None,
            MessageFields.ERROR: None,
        }
        self.active_executions[execution_id] = record

        self.execution_queue.put((execution_id, request, record))
        queue_position = self.execution_queue.qsize()
        logger.info("[%s] Queued for execution (position: %s)", execution_id, queue_position)

        return ExecuteResponse(
            ResponseType.ACCEPTED,
            execution_id=execution_id,
            message=f"Execution queued (position: {queue_position})",
        ).to_dict()

    def _run_execution(self, execution_id, request, record):
        try:
            record[MessageFields.STATUS] = ExecutionStatus.RUNNING.value
            record[MessageFields.START_TIME] = time.time()
            logger.info("[%s] Starting execution (was queued)", execution_id)

            results = self.execute_task(execution_id, request)
            logger.info("[%s] Execution returned, updating status to COMPLETE", execution_id)
            record[MessageFields.STATUS] = ExecutionStatus.COMPLETE.value
            record[MessageFields.END_TIME] = time.time()
            record[MessageFields.RESULTS_SUMMARY] = {
                MessageFields.WELL_COUNT: len(results) if isinstance(results, dict) else 0,
                MessageFields.WELLS: list(results.keys()) if isinstance(results, dict) else [],
            }
            logger.info(
                "[%s] ✓ Completed in %.1fs",
                execution_id,
                record[MessageFields.END_TIME] - record[MessageFields.START_TIME],
            )
        except Exception as e:
            if isinstance(e, BrokenProcessPool) and record[MessageFields.STATUS] == ExecutionStatus.CANCELLED.value:
                logger.info("[%s] Cancelled", execution_id)
            else:
                import traceback
                full_traceback = traceback.format_exc()
                record[MessageFields.STATUS] = ExecutionStatus.FAILED.value
                record[MessageFields.END_TIME] = time.time()
                record[MessageFields.ERROR] = str(e)
                record['traceback'] = full_traceback  # Add full traceback to record
                logger.error("[%s] ✗ Failed: %s", execution_id, e, exc_info=True)
        finally:
            record.pop("orchestrator", None)
            killed = self._kill_worker_processes()
            if killed > 0:
                logger.info("[%s] Killed %s worker processes during cleanup", execution_id, killed)
            logger.info("[%s] Execution cleanup complete", execution_id)

    def _handle_status(self, msg):
        execution_id = StatusRequest.from_dict(msg).execution_id
        if execution_id:
            if execution_id not in self.active_executions:
                return ExecuteResponse(
                    ResponseType.ERROR,
                    error=f"Execution {execution_id} not found",
                ).to_dict()
            r = self.active_executions[execution_id]
            return {
                MessageFields.STATUS: ResponseType.OK.value,
                "execution": {
                    k: r.get(k)
                    for k in [
                        MessageFields.EXECUTION_ID,
                        MessageFields.PLATE_ID,
                        MessageFields.STATUS,
                        MessageFields.START_TIME,
                        MessageFields.END_TIME,
                        MessageFields.ERROR,
                        MessageFields.RESULTS_SUMMARY,
                    ]
                },
            }
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.ACTIVE_EXECUTIONS: len(self.active_executions),
            MessageFields.UPTIME: time.time() - self.start_time if self.start_time else 0,
            MessageFields.EXECUTIONS: list(self.active_executions.keys()),
        }

    def _handle_cancel(self, msg):
        request, error = self._validate_and_parse(msg, CancelRequest)
        if error:
            return error
        if request.execution_id not in self.active_executions:
            return ExecuteResponse(
                ResponseType.ERROR,
                error=f"Execution {request.execution_id} not found",
            ).to_dict()

        self._cancel_all_executions()
        killed = self._kill_worker_processes()
        logger.info("[%s] Cancelled - killed %s workers", request.execution_id, killed)
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.MESSAGE: f"Cancelled - killed {killed} workers",
            MessageFields.WORKERS_KILLED: killed,
        }

    def _cancel_all_executions(self):
        for eid, r in self.active_executions.items():
            if r[MessageFields.STATUS] in (
                ExecutionStatus.RUNNING.value,
                ExecutionStatus.QUEUED.value,
            ):
                r[MessageFields.STATUS] = ExecutionStatus.CANCELLED.value
                r[MessageFields.END_TIME] = time.time()
                logger.info("[%s] Cancelled", eid)

    def _shutdown_workers(self, force=False):
        self._cancel_all_executions()
        killed = self._kill_worker_processes()
        if force:
            self.request_shutdown()
        msg = f"Workers killed ({killed}), server {'shutting down' if force else 'alive'}"
        logger.info(msg)
        return {
            MessageFields.TYPE: ResponseType.SHUTDOWN_ACK.value,
            MessageFields.STATUS: "success",
            MessageFields.MESSAGE: msg,
        }

    def _handle_shutdown(self, msg):
        return self._shutdown_workers(force=False)

    def _handle_force_shutdown(self, msg):
        return self._shutdown_workers(force=True)

    def send_progress_update(self, well_id, step, status):
        try:
            self.progress_queue.put_nowait(
                {
                    "type": "progress",
                    "well_id": well_id,
                    "step": step,
                    "status": status,
                    "timestamp": time.time(),
                }
            )
        except queue.Full:
            logger.warning("Progress queue full, dropping %s", well_id)

    def _get_worker_info(self):
        try:
            import psutil

            workers = []
            for child in psutil.Process(os.getpid()).children(recursive=True):
                try:
                    cmdline = child.cmdline()
                    if not (cmdline and "python" in cmdline[0].lower()):
                        continue
                    cmdline_str = " ".join(cmdline)
                    if any(
                        x in cmdline_str.lower()
                        for x in ["napari", "fiji", "resource_tracker", "semaphore_tracker"]
                    ) or child.pid == os.getpid():
                        continue
                    workers.append(
                        {
                            "pid": child.pid,
                            "status": child.status(),
                            "cpu_percent": child.cpu_percent(interval=0),
                            "memory_mb": child.memory_info().rss / 1024 / 1024,
                            "create_time": child.create_time(),
                        }
                    )
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            return workers
        except (ImportError, Exception) as e:
            logger.warning("Cannot get worker info: %s", e)
            return []

    def _kill_worker_processes(self) -> int:
        """Kill all worker processes and return the number killed."""
        try:
            import psutil

            all_children = psutil.Process(os.getpid()).children(recursive=False)
            zombies = []
            workers = []

            for child in all_children:
                try:
                    if child.status() == psutil.STATUS_ZOMBIE:
                        zombies.append(child)
                        logger.info("Found zombie process PID %s", child.pid)
                        continue

                    cmd = child.cmdline()
                    if cmd and "python" in cmd[0].lower():
                        cmdline_str = " ".join(cmd)
                        if "napari" not in cmdline_str.lower() and "fiji" not in cmdline_str.lower():
                            workers.append(child)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue

            if zombies:
                logger.info("Reaping %s zombie processes", len(zombies))
                for zombie in zombies:
                    try:
                        zombie.wait(timeout=0.1)
                    except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                        pass

            if not workers:
                logger.info("No live worker processes found to kill")
                return len(zombies)

            logger.info("Found %s live worker processes to kill", len(workers))

            for w in workers:
                try:
                    w.terminate()
                except Exception:
                    pass

            gone, alive = psutil.wait_procs(workers, timeout=3)
            if alive:
                for w in alive:
                    try:
                        w.kill()
                    except Exception:
                        pass
                psutil.wait_procs(alive, timeout=1)

            total_killed = len(workers) + len(zombies)
            logger.info("Killed %s worker processes and reaped %s zombies", len(workers), len(zombies))
            return total_killed

        except (ImportError, Exception) as e:
            logger.error("Failed to kill worker processes: %s", e, exc_info=True)
            return 0

    @abstractmethod
    def execute_task(self, execution_id: str, request: ExecuteRequest) -> Any:
        """Execute a task. Subclass provides actual execution logic."""
        raise NotImplementedError
