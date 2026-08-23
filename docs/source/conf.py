"""Sphinx configuration for zmqruntime."""

import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

project = "zmqruntime"
author = "Tristan Simas"
project_metadata = tomllib.loads(
    (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text(encoding="utf-8")
)["project"]
release = project_metadata["version"]
version = release.rsplit(".", maxsplit=1)[0]

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path: list[str] = []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_theme = "sphinx_rtd_theme"
html_static_path: list[str] = []
master_doc = "index"
