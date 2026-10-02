"""Execute the full original source candidate with owned filesystem fixtures."""

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

import pytest
import zmqruntime


@pytest.fixture(autouse=True)
def owned_test_files(tmp_path, monkeypatch):
    # Existing tests use HOME as their supported per-test filesystem boundary.
    # Extend that same fixture isolation to every case and spawned fixture child;
    # no source algorithms, sockets, assertions or test selection are replaced.
    # Unix-domain socket addresses have a real 107-byte boundary. The owned
    # persistent basetemp parent is deliberately short; do not widen that limit
    # or replace the real transport with a test implementation.
    fixture_root = Path(tempfile.mkdtemp(prefix="h", dir=Path(sys.argv[1]).parent))
    monkeypatch.setenv("HOME", str(fixture_root))


if __name__ == "__main__":
    print("SOURCE_IMPORT", zmqruntime.__file__, flush=True)
    raise SystemExit(pytest.main([
        "--noconftest", "-q", "-o", "addopts=", "-p", "no:cacheprovider",
        "--basetemp", sys.argv[1], *(sys.argv[2:] or ["tests"]),
    ], plugins=[sys.modules[__name__]]))
