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
    ExecutionRecord,
    ExecutionStatus,
    MessageFields,
    PongResponse,
    QueuedExecutionInfo,
    ResponseType,
    RunningExecutionInfo,
    StatusRequest,
    ProgressRegistrationRequest,
    ProgressUnregistrationRequest,
)
from zmqruntime.config import ZMQConfig
from zmqruntime.execution.lifecycle import InMemoryExecutionLifecycleEngine
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
        self._lifecycle = InMemoryExecutionLifecycleEngine()
        self.active_executions: dict[str, ExecutionRecord] = self._lifecycle.records()
        self.start_time = None
        self.progress_queue: queue.Queue = queue.Queue()

        self.execution_queue: queue.Queue = queue.Queue()
        self.queue_worker_thread: threading.Thread | None = None
        self._progress_subscribers: set[str] = set()

    def start(self):
        super().start()
        self.start_time = self.start_time or time.time()
        self._start_queue_worker()

    def _create_pong_response(self):
        snapshot = self._lifecycle.snapshot(
            uptime=time.time() - self.start_time if self.start_time else 0.0
        )
        running = snapshot.running_executions or tuple()
        queued = snapshot.queued_executions or tuple()
        return (
            PongResponse(
                port=self.port,
                control_port=self.control_port,
                ready=self._ready,
                server=self.__class__.__name__,
                log_file_path=self.log_file_path,
                active_executions=snapshot.active_executions,
                running_executions=tuple(
                    RunningExecutionInfo(
                        execution_id=info.execution_id,
                        plate_id=info.plate_id,
                        start_time=info.start_time,
                        elapsed=info.elapsed,
                    )
                    for info in running
                ),
                queued_executions=tuple(
                    QueuedExecutionInfo(
                        execution_id=info.execution_id,
                        plate_id=info.plate_id,
                        queue_position=info.queue_position,
                    )
                    for info in queued
                ),
                workers=self._get_worker_info(),
                uptime=time.time() - self.start_time if self.start_time else 0,
                progress_subscribers=len(self._progress_subscribers),
            ).to_dict()
        )

    def process_messages(self):
        super().process_messages()
        if not self.data_socket:
            return
        logger = logging.getLogger(__name__)
        count = 0
        while True:
            try:
                progress_update = self.progress_queue.get_nowait()
            except queue.Empty:
                if count > 0:
                    logger.info(f"Published {count} progress update(s) to ZMQ")
                break
            logger.info(
                f"Publishing to ZMQ: step={progress_update.get('step')}, "
                f"axis={progress_update.get('axis_id')}, plate_id={progress_update.get('plate_id')}, "
                f"percent={progress_update.get('percent')}"
            )
            self.data_socket.send_string(json.dumps(progress_update))
            count += 1

    def get_status_info(self):
        status = super().get_status_info()
        status.update(
            {
                "active_executions": len(self.active_executions),
                "uptime": time.time() - self.start_time if self.start_time else 0,
                "executions": [record.to_dict() for record in self.active_executions.values()],
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
            error = request.validate()
            if error:
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
                        execution_id, request = self.execution_queue.get(timeout=1.0)
                    except queue.Empty:
                        continue

                    logger.info(
                        "[%s] Dequeued for execution (queue size: %s)",
                        execution_id,
                        self.execution_queue.qsize(),
                    )

                    if not self._running:
                        logger.info("[%s] Server shutting down, skipping execution", execution_id)
                        self._lifecycle.mark_cancelled(execution_id)
                        self.execution_queue.task_done()
                        break

                    record = self.active_executions[execution_id]
                    if record.status == ExecutionStatus.CANCELLED.value:
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
                    execution_id, request = self.execution_queue.get_nowait()
                    self._lifecycle.mark_cancelled(execution_id)
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
        record = ExecutionRecord(
            execution_id=execution_id,
            plate_id=request.plate_id,
            client_address=request.client_address,
            status=ExecutionStatus.QUEUED.value,
            compile_only=request.compile_only,
        )
        queue_position = self._lifecycle.enqueue(record)
        self.execution_queue.put((execution_id, request))
        logger.info("[%s] Queued for execution (position: %s)", execution_id, queue_position)

        return ExecuteResponse(
            ResponseType.ACCEPTED,
            execution_id=execution_id,
            message=f"Execution queued (position: {queue_position})",
        ).to_dict()

    def _run_execution(self, execution_id, request, record):
        try:
            self._lifecycle.mark_running(execution_id)
            logger.info("[%s] Starting execution (was queued)", execution_id)

            results = self.execute_task(execution_id, request)
            logger.info("[%s] Execution returned, updating status to COMPLETE", execution_id)
            self._lifecycle.mark_complete(execution_id)
            record = self.active_executions[execution_id]
            record.results_summary = {
                MessageFields.WELL_COUNT: len(results) if isinstance(results, dict) else 0,
                MessageFields.WELLS: list(results.keys()) if isinstance(results, dict) else [],
            }
            logger.info(
                "[%s] ✓ Completed in %.1fs",
                execution_id,
                (record.end_time or 0.0) - (record.start_time or 0.0),
            )
        except Exception as e:
            if isinstance(e, BrokenProcessPool) and record.status == ExecutionStatus.CANCELLED.value:
                logger.info("[%s] Cancelled", execution_id)
            else:
                import traceback
                full_traceback = traceback.format_exc()
                self._lifecycle.mark_failed(execution_id, str(e))
                record = self.active_executions[execution_id]
                record.traceback = full_traceback
                logger.error("[%s] ✗ Failed: %s", execution_id, e, exc_info=True)
        finally:
            record.pop_extra("orchestrator", None)
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
            snapshot = self._lifecycle.snapshot(
                uptime=time.time() - self.start_time if self.start_time else 0.0,
                execution_id=execution_id,
            )
            return snapshot.to_dict()
        snapshot = self._lifecycle.snapshot(
            uptime=time.time() - self.start_time if self.start_time else 0.0
        )
        return snapshot.to_dict()

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
        self._lifecycle.cancel_all_active(end_time=time.time())
        for execution_id, record in self.active_executions.items():
            if record.status == ExecutionStatus.CANCELLED.value:
                logger.info("[%s] Cancelled", execution_id)

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

    def _handle_register_progress(self, msg):
        request, error = self._validate_and_parse(msg, ProgressRegistrationRequest)
        if error:
            return error
        self._progress_subscribers.add(request.client_id)
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.MESSAGE: "Progress subscriber registered",
            MessageFields.CLIENT_ID: request.client_id,
            MessageFields.PROGRESS_SUBSCRIBERS: len(self._progress_subscribers),
        }

    def _handle_unregister_progress(self, msg):
        request, error = self._validate_and_parse(msg, ProgressUnregistrationRequest)
        if error:
            return error
        self._progress_subscribers.discard(request.client_id)
        return {
            MessageFields.STATUS: ResponseType.OK.value,
            MessageFields.MESSAGE: "Progress subscriber unregistered",
            MessageFields.CLIENT_ID: request.client_id,
            MessageFields.PROGRESS_SUBSCRIBERS: len(self._progress_subscribers),
        }

    def send_progress_update(self, progress_update: dict) -> None:
        from zmqruntime.messages import validate_progress_payload

        validate_progress_payload(progress_update)
        self.progress_queue.put(progress_update)

    def _get_worker_info(self):
        try:
            import psutil
            from zmqruntime.messages import WorkerState

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
                        WorkerState(
                            pid=child.pid,
                            status=child.status(),
                            cpu_percent=child.cpu_percent(interval=0),
                            memory_mb=child.memory_info().rss / 1024 / 1024,
                            metadata={"create_time": child.create_time()},
                        )
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
