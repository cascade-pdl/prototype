"""The ``cascade`` command line.

``main`` is re-exported here so the packaged entry point declared in
pyproject.toml (``cascade = "cascade.cli:main"``) resolves.
"""
from cascade.cli.main import main, build_parser

__all__ = ["main", "build_parser"]