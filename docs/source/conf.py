"""Sphinx configuration for zmqruntime."""

from importlib.metadata import version as distribution_version
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

project = "zmqruntime"
author = "Tristan Simas"
release = distribution_version("zmqruntime")
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
