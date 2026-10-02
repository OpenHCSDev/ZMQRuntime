"""Run the original publisher case, intercepting only OS termination admission."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

import pytest
from zmqruntime.messages import ProcessIdentity


def intercept_termination(identity, timeout=5.0):
    print(
        "INTERCEPTED_TERMINATION",
        identity,
        "CALLER",
        ProcessIdentity.current(),
        "SAME_INCARNATION",
        identity == ProcessIdentity.current(),
        "TIMEOUT",
        timeout,
        flush=True,
    )
    return False


ProcessIdentity.terminate = intercept_termination
raise SystemExit(pytest.main([
    "--noconftest", "-q", "-s", "-o", "addopts=", "-p", "no:cacheprovider",
    "--basetemp", sys.argv[1],
    "tests/test_execution.py::test_shutdown_result_distinguishes_worker_stop_from_endpoint_termination",
]))
