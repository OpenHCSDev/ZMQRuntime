"""Platform policy for noninteractive background subprocesses.

Each host family is one :class:`BackgroundProcessPlatform` subclass that owns
how a background process is launched there: console suppression, detached
process groups and the console-free interpreter. The module imports only the
standard library and ``metaclass_registry``, so early startup paths can use it
before any GUI or runtime machinery loads.
"""

from __future__ import annotations

import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from metaclass_registry import AutoRegisterMeta


@dataclass(frozen=True, slots=True)
class BackgroundProcessLaunchSpec:
    """Resolved keyword arguments for one subprocess launch."""

    creationflags: int = 0
    start_new_session: bool = False

    def popen_arguments(self) -> dict[str, bool | int]:
        """Return only non-default ``subprocess.Popen`` arguments."""

        arguments: dict[str, bool | int] = {}
        if self.creationflags:
            arguments["creationflags"] = self.creationflags
        if self.start_new_session:
            arguments["start_new_session"] = True
        return arguments


class BackgroundProcessPlatform(ABC, metaclass=AutoRegisterMeta):
    """How background processes launch on one host family.

    Families register under the ``sys.platform`` value they own; a host with
    no registered family launches as :class:`SessionBackgroundProcesses`.
    """

    __registry_key__ = "sys_platform"
    __skip_if_no_key__ = True

    sys_platform: ClassVar[str | None] = None

    @classmethod
    def current(cls) -> type[BackgroundProcessPlatform]:
        """Return the family of the running host."""

        return cls.__registry__.get(sys.platform, SessionBackgroundProcesses)

    @classmethod
    @abstractmethod
    def launch_spec(cls, *, detached: bool) -> BackgroundProcessLaunchSpec:
        """Launch arguments for a background process, detached or attached."""

    @classmethod
    def python_executable(cls, executable: str) -> str:
        """The interpreter for a background Python process family."""

        return executable


class SessionBackgroundProcesses(BackgroundProcessPlatform):
    """Hosts without consoles per process: detaching starts a new session."""

    @classmethod
    def launch_spec(cls, *, detached: bool) -> BackgroundProcessLaunchSpec:
        return BackgroundProcessLaunchSpec(start_new_session=detached)


class WindowsBackgroundProcesses(BackgroundProcessPlatform):
    """Windows: suppress consoles, and detach into a new process group."""

    sys_platform = "win32"

    @classmethod
    def launch_spec(cls, *, detached: bool) -> BackgroundProcessLaunchSpec:
        creationflags = subprocess.CREATE_NO_WINDOW
        if detached:
            creationflags |= subprocess.CREATE_NEW_PROCESS_GROUP
        return BackgroundProcessLaunchSpec(creationflags=creationflags)

    @classmethod
    def python_executable(cls, executable: str) -> str:
        """Use the colocated ``pythonw.exe`` when it exists.

        Creation flags suppress the first child console, but a child launched
        through ``python.exe`` can still create visible consoles when it
        starts multiprocessing descendants. ``pythonw.exe`` makes the
        console-free interpreter identity transitive.
        """

        windowed_executable = Path(executable).with_name("pythonw.exe")
        if windowed_executable.is_file():
            return str(windowed_executable)
        return executable


@dataclass(frozen=True, slots=True)
class BackgroundProcessLaunchPolicy:
    """Launch policy for one background process on one host family."""

    platform: type[BackgroundProcessPlatform]
    detached: bool = False

    @classmethod
    def current(cls, *, detached: bool = False) -> BackgroundProcessLaunchPolicy:
        """Construct the policy for the current host."""

        return cls(platform=BackgroundProcessPlatform.current(), detached=detached)

    def resolve(self) -> BackgroundProcessLaunchSpec:
        """Resolve the platform policy to a transportable launch spec."""

        return self.platform.launch_spec(detached=self.detached)

    def popen_arguments(self) -> dict[str, bool | int]:
        """Resolve directly to ``subprocess.Popen`` keyword arguments."""

        return self.resolve().popen_arguments()

    def python_executable(self, executable: str) -> str:
        """Return the interpreter for a background Python process family."""

        return self.platform.python_executable(executable)
