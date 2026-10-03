"""Exact resource discovery includes children forked by nonleader threads."""

import json
import subprocess
import sys
import threading
from pathlib import Path

import psutil
import pytest

from zmqruntime.messages import ProcessIdentity


@pytest.fixture
def thread_owned_process_tree():
    started = threading.Event()
    release = threading.Event()
    processes = []

    def launch():
        process = subprocess.Popen(
            [sys.executable, "-u", "-c", """
import json, subprocess, sys
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
print(json.dumps({'grandchild': child.pid}), flush=True)
try:
    sys.stdin.readline()
finally:
    child.terminate()
    child.wait(timeout=5)
"""],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        processes.append(process)
        started.set()
        release.wait(timeout=15)

    thread = threading.Thread(target=launch)
    thread.start()
    try:
        assert started.wait(timeout=5)
        process = processes[0]
        grandchild = json.loads(process.stdout.readline())["grandchild"]
        yield process, grandchild
    finally:
        release.set()
        thread.join(timeout=5)
        if processes:
            process = processes[0]
            process.communicate("stop\n", timeout=5)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux per-thread children interface")
def test_real_thread_children_and_grandchildren_need_no_host_process_scan(
    monkeypatch, thread_owned_process_tree
):
    process, grandchild = thread_owned_process_tree
    identity = ProcessIdentity.current()
    expected = {process.pid, grandchild}
    leader_children = Path(
        f"{psutil.PROCFS_PATH}/{identity.pid}/task/{identity.pid}/children"
    ).read_text().split()
    assert str(process.pid) not in leader_children
    assert expected <= {child.pid for child in psutil.Process().children(recursive=True)}

    def reject_host_scan(*args, **kwargs):
        pytest.fail("Linux resource discovery scanned every host process")

    monkeypatch.setattr(psutil.Process, "children", reject_host_scan)
    recursive = {child.pid for child in identity.descendants()}
    direct = {child.pid for child in identity.descendants(recursive=False)}
    assert expected <= recursive
    assert process.pid in direct and grandchild not in direct
    assert expected <= {child.pid for child in identity.work_snapshot()}


def test_reused_root_is_not_an_ancestry_owner():
    identity = ProcessIdentity.current()
    reused = ProcessIdentity(identity.pid, identity.create_time - 1)
    with pytest.raises(psutil.NoSuchProcess):
        reused.descendants()
    assert reused.work_snapshot() == {}


@pytest.mark.parametrize("unavailable", [FileNotFoundError, PermissionError])
def test_unavailable_children_interface_preserves_native_discovery(
    monkeypatch, thread_owned_process_tree, unavailable
):
    process, grandchild = thread_owned_process_tree
    identity = ProcessIdentity.current()

    def fail_interface(*args, **kwargs):
        raise unavailable()

    monkeypatch.setattr(ProcessIdentity, "_linux_descendants", fail_interface)
    assert {process.pid, grandchild} <= {child.pid for child in identity.descendants()}


def test_portable_discovery_uses_original_native_parent_law(
    monkeypatch, thread_owned_process_tree
):
    process, grandchild = thread_owned_process_tree
    import zmqruntime.messages as messages

    monkeypatch.setattr(messages.sys, "platform", "portable")
    assert {process.pid, grandchild} <= {
        child.pid for child in ProcessIdentity.current().descendants()
    }
