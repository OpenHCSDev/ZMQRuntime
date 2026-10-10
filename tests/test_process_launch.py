"""Contracts for noninteractive background subprocess creation."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from zmqruntime.process_launch import (
    BackgroundProcessLaunchPolicy,
    BackgroundProcessLaunchSpec,
    BackgroundProcessPlatform,
    SessionBackgroundProcesses,
    WindowsBackgroundProcesses,
)

NO_WINDOW = 0x08000000
NEW_PROCESS_GROUP = 0x00000200


@pytest.fixture
def windows_flags(monkeypatch):
    monkeypatch.setattr(subprocess, "CREATE_NO_WINDOW", NO_WINDOW, raising=False)
    monkeypatch.setattr(
        subprocess, "CREATE_NEW_PROCESS_GROUP", NEW_PROCESS_GROUP, raising=False
    )


@pytest.mark.parametrize(
    ("platform", "attached", "detached"),
    [
        (
            WindowsBackgroundProcesses,
            BackgroundProcessLaunchSpec(creationflags=NO_WINDOW),
            BackgroundProcessLaunchSpec(creationflags=NO_WINDOW | NEW_PROCESS_GROUP),
        ),
        (
            SessionBackgroundProcesses,
            BackgroundProcessLaunchSpec(),
            BackgroundProcessLaunchSpec(start_new_session=True),
        ),
    ],
)
def test_each_platform_owns_its_launch_spec(
    windows_flags, platform, attached, detached
) -> None:
    assert BackgroundProcessLaunchPolicy(platform=platform).resolve() == attached
    assert BackgroundProcessLaunchPolicy(platform=platform, detached=True).resolve() == detached
    assert BackgroundProcessLaunchPolicy(platform=platform, detached=True).popen_arguments() == (
        detached.popen_arguments()
    )


def test_spec_projects_only_non_default_popen_arguments() -> None:
    assert BackgroundProcessLaunchSpec().popen_arguments() == {}
    assert BackgroundProcessLaunchSpec(start_new_session=True).popen_arguments() == {
        "start_new_session": True
    }


@pytest.mark.parametrize(
    ("platform", "windowed_present", "expected_name"),
    [
        (WindowsBackgroundProcesses, True, "pythonw.exe"),
        (WindowsBackgroundProcesses, False, "python.exe"),
        (SessionBackgroundProcesses, True, "python.exe"),
    ],
)
def test_background_python_interpreter(
    tmp_path: Path, platform, windowed_present, expected_name
) -> None:
    python_executable = tmp_path / "python.exe"
    python_executable.touch()
    if windowed_present:
        (tmp_path / "pythonw.exe").touch()

    policy = BackgroundProcessLaunchPolicy(platform=platform)

    assert policy.python_executable(str(python_executable)) == str(tmp_path / expected_name)


def test_current_host_selects_registered_family(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    assert BackgroundProcessPlatform.current() is WindowsBackgroundProcesses
    monkeypatch.setattr(sys, "platform", "linux")
    assert BackgroundProcessPlatform.current() is SessionBackgroundProcesses
    assert BackgroundProcessLaunchPolicy.current(detached=True) == BackgroundProcessLaunchPolicy(
        platform=SessionBackgroundProcesses, detached=True
    )
