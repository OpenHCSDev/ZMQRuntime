"""ZMQ client base class."""
from __future__ import annotations

import logging
import multiprocessing
import pickle
import platform
import socket
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, wait

import zmq

from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.messages import ControlMessageType, MessageFields, ResponseType
from zmqruntime.transport import (
    get_control_port,
    get_default_transport_mode,
    get_ipc_socket_path,
    get_zmq_transport_url,
    is_port_in_use,
    ping_control_port,
    remove_ipc_socket,
    request_control_ping,
    wait_for_server_ready,
)


class ZMQClient(ABC):
    """ABC for ZMQ clients - dual-channel pattern with auto-spawning."""

    def __init__(
        self,
        port: int,
        host: str = "localhost",
        persistent: bool = True,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
    ):
        self.config = config or ZMQConfig()
        self.port = port
        self.host = host
        self.control_port = port + self.config.control_port_offset
        self.persistent = persistent
        self.transport_mode = transport_mode or get_default_transport_mode()
        self.zmq_context = None
        self.data_socket = None
        self.control_socket = None
        self.server_process = None
        self._connected = False
        self._connected_to_existing = False
        self._lock = threading.Lock()

    def connect(self, timeout: float = 10.0):
        with self._lock:
            if self._connected:
                return True
            if self._is_port_in_use(self.port):
                if self._try_connect_to_existing(
                    self.port,
                    timeout_ms=self._existing_endpoint_probe_timeout_ms(timeout),
                ):
                    self._setup_client_sockets()  # ← FIX: Set up data socket even when connecting to existing server
                    self._connected = self._connected_to_existing = True
                    return True
                if self._should_preserve_unresponsive_endpoint(self.port):
                    return False
                self._kill_processes_on_port(self.port)
                self._kill_processes_on_port(self.control_port)
                time.sleep(0.5)
            self.server_process = self._spawn_server_process()
            if not self._wait_for_server_ready(timeout):
                return False
            self._setup_client_sockets()
            self._connected = True
            return True

    def disconnect(self):
        with self._lock:
            if not self._connected:
                return
            self._cleanup_sockets()
            if not self._connected_to_existing and self.server_process and not self.persistent:
                self._stop_owned_server_process(self.server_process)
            self._connected = False

    def is_connected(self):
        return self._connected

    @staticmethod
    def _stop_owned_server_process(server_process):
        if isinstance(server_process, multiprocessing.Process):
            if server_process.is_alive():
                server_process.terminate()
                server_process.join(timeout=5)
                if server_process.is_alive():
                    server_process.kill()
            return

        if isinstance(server_process, subprocess.Popen):
            if server_process.poll() is None:
                server_process.terminate()
                try:
                    server_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server_process.kill()
            return

        raise TypeError(
            f"Unsupported ZMQ server process handle: {type(server_process).__name__}"
        )

    def _setup_client_sockets(self):
        import zmq

        logger = logging.getLogger(__name__)
        self.zmq_context = zmq.Context()
        data_url = get_zmq_transport_url(
            self.port,
            host=self.host,
            mode=self.transport_mode,
            config=self.config,
        )

        self.data_socket = self.zmq_context.socket(zmq.SUB)
        self.data_socket.setsockopt(zmq.LINGER, 0)
        self.data_socket.connect(data_url)
        self.data_socket.setsockopt(zmq.SUBSCRIBE, b"")
        time.sleep(0.1)
        logger.info(f"Set up ZMQ SUB socket connected to {data_url}")

    def _cleanup_sockets(self):
        if self.data_socket:
            self.data_socket.close()
            self.data_socket = None
        if self.control_socket:
            self.control_socket.close()
            self.control_socket = None

        if self.zmq_context:
            self.zmq_context.term()
            self.zmq_context = None

    def _try_connect_to_existing(self, port: int, timeout_ms: int = 500) -> bool:
        return ping_control_port(
            port,
            self.transport_mode,
            host=self.host,
            config=self.config,
            timeout_ms=timeout_ms,
            require_ready=True,
        )

    @staticmethod
    def _existing_endpoint_probe_timeout_ms(timeout: float) -> int:
        return max(500, min(int(timeout * 1000), 5000))

    def _should_preserve_unresponsive_endpoint(self, port: int) -> bool:
        if self.transport_mode != TransportMode.IPC:
            return False
        return self._ipc_server_process_exists(port)

    @staticmethod
    def _ipc_server_process_exists(port: int) -> bool:
        try:
            import psutil
        except Exception:
            return False

        port_flags = (f"--port {port}", f"--port={port}")
        for proc in psutil.process_iter(["cmdline"]):
            try:
                cmdline = proc.info.get("cmdline") or []
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            cmdline_str = " ".join(cmdline)
            if any(flag in cmdline_str for flag in port_flags):
                return True
        return False

    def _wait_for_server_ready(self, timeout: float = 10.0) -> bool:
        return wait_for_server_ready(
            self.port,
            self.transport_mode,
            host=self.host,
            config=self.config,
            timeout=timeout,
        )

    def _is_port_in_use(self, port: int) -> bool:
        return is_port_in_use(
            port,
            self.transport_mode,
            host=self.host,
            config=self.config,
        )

    def _find_free_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def _kill_processes_on_port(self, port: int):
        try:
            if self.transport_mode == TransportMode.IPC:
                remove_ipc_socket(port, self.config)
                return

            system = platform.system()
            if system in ["Linux", "Darwin"]:
                result = subprocess.run(
                    ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                if result.returncode == 0 and result.stdout.strip():
                    for pid in result.stdout.strip().split("\n"):
                        try:
                            subprocess.run(["kill", "-9", pid], timeout=1)
                        except Exception:
                            pass
            elif system == "Windows":
                result = subprocess.run(
                    ["netstat", "-ano"], capture_output=True, text=True, timeout=2
                )
                for line in result.stdout.split("\n"):
                    if f":{port}" in line and "LISTENING" in line:
                        try:
                            subprocess.run(["taskkill", "/PID", line.split()[-1]], timeout=1)
                        except Exception:
                            pass
        except Exception:
            pass

    @staticmethod
    def scan_servers(
        ports,
        host: str = "localhost",
        timeout_ms: int = 200,
        transport_mode: TransportMode | None = None,
        config: ZMQConfig | None = None,
    ):
        config = config or ZMQConfig()
        transport_mode = transport_mode or get_default_transport_mode()
        ports = tuple(ports)
        if not ports:
            return []

        def scan_port(port):
            pong = request_control_ping(
                port,
                transport_mode,
                host=host,
                config=config,
                timeout_ms=timeout_ms,
            )
            if pong is None or pong.get(MessageFields.TYPE) != ResponseType.PONG.value:
                return None
            return {
                **pong,
                "port": port,
                "control_port": get_control_port(port, config),
            }

        servers = []
        worker_count = min(len(ports), 32)
        executor = ThreadPoolExecutor(max_workers=worker_count)
        try:
            futures = tuple(executor.submit(scan_port, port) for port in ports)
            done, _ = wait(futures, timeout=max(timeout_ms / 1000, 0.001))
            for future in done:
                server = future.result()
                if server is not None:
                    servers.append(server)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return sorted(servers, key=lambda server: ports.index(server["port"]))

    @staticmethod
    def kill_server_on_port(
        port: int,
        graceful: bool = True,
        timeout: float = 5.0,
        transport_mode: TransportMode | None = None,
        host: str = "localhost",
        config: ZMQConfig | None = None,
    ):
        config = config or ZMQConfig()
        transport_mode = transport_mode or get_default_transport_mode()
        msg_type = "shutdown" if graceful else "force_shutdown"

        def kill_ipc_server_processes(port: int) -> int:
            """Kill server processes in IPC mode by finding them via command line."""
            import psutil
            killed = 0
            try:
                for proc in psutil.process_iter(['pid', 'cmdline']):
                    try:
                        cmdline = proc.cmdline()
                        if not cmdline:
                            continue
                        cmdline_str = ' '.join(cmdline)
                        # Look for server process with this port
                        if f"--port {port}" in cmdline_str or f"--port={port}" in cmdline_str:
                            proc.kill()
                            killed += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except Exception:
                pass
            return killed

        def is_port_free(port: int) -> bool:
            if transport_mode == TransportMode.IPC:
                socket_path = get_ipc_socket_path(port, config)
                return not (socket_path and socket_path.exists())
            sock_test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock_test.settimeout(0.1)
            try:
                sock_test.bind(("localhost", port))
                sock_test.close()
                return True
            except OSError:
                return False
            finally:
                try:
                    sock_test.close()
                except Exception:
                    pass

        try:
            control_port = port + config.control_port_offset
            control_url = get_zmq_transport_url(
                control_port,
                host=host,
                mode=transport_mode,
                config=config,
            )

            ctx = zmq.Context.instance()
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(control_url)

            if graceful:
                sock.setsockopt(zmq.RCVTIMEO, int(timeout * 1000))
                sock.send(pickle.dumps({MessageFields.TYPE: msg_type}))
                ack = pickle.loads(sock.recv())
                if ack[MessageFields.TYPE] == ResponseType.SHUTDOWN_ACK.value:
                    return True
            else:
                sock.setsockopt(zmq.SNDTIMEO, 1000)
                try:
                    sock.send(pickle.dumps({MessageFields.TYPE: msg_type}))
                except Exception:
                    pass

                if transport_mode == TransportMode.IPC:
                    # Kill the actual server processes, not just the socket files
                    killed = kill_ipc_server_processes(port)
                    # Also clean up socket files
                    remove_ipc_socket(port, config)
                    remove_ipc_socket(control_port, config)
                    return killed > 0

                from zmqruntime.server import ZMQServer

                killed = sum(ZMQServer.kill_processes_on_port(p) for p in [port, control_port])
                return killed > 0

        except Exception:
            if not graceful:
                if transport_mode == TransportMode.IPC:
                    # Kill the actual server processes, not just the socket files
                    killed = kill_ipc_server_processes(port)
                    # Also clean up socket files
                    remove_ipc_socket(port, config)
                    remove_ipc_socket(control_port, config)
                    return killed > 0
                from zmqruntime.server import ZMQServer

                killed = sum(ZMQServer.kill_processes_on_port(p) for p in [port, control_port])
                return killed > 0
            return False
        finally:
            try:
                sock.close(linger=0)
            except Exception:
                pass

        return False

    @abstractmethod
    def _spawn_server_process(self):
        pass

    @abstractmethod
    def send_data(self, data):
        pass
