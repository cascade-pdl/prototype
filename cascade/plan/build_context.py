"""Build contexts: what a ``build:`` ref's image is made from, and its content hash.

A ``build:`` ref names a directory holding a Dockerfile. Compile hashes everything that
goes into the image, and the image is named by that hash (``cascade.substrate.naming``),
so "is it built?" is "does that name exist?": there is nothing to record, commit or go
stale, and the same directory gives the same name on every machine.

What the hash covers:

- **every file in the directory**, with its executable bit. ``.dockerignore`` is not
  consulted: hashing an ignored file can only cause an unneeded rebuild, while ignoring
  one Docker does copy could reuse a stale image.
- **cascade's own source, when the Dockerfile uses it.** cascade is not on a package
  index, so an image that installs it gets it from this checkout, passed to the build as
  the named context ``cascade`` (``COPY --from=cascade . /src/cascade``). A change to
  cascade then rebuilds exactly those images. ``cascade_source`` is the one list of those
  files, used both to hash them here and to stage them for the build. The checkout's
  README is not among them: ``pyproject.toml`` names one, so a fixed stub stands in, and
  editing the docs does not rename and rebuild every image that installs cascade.

What it cannot cover: a ``FROM`` image by moving tag (``python:3.13-slim``) can change
under the same hash. Pin bases by digest (``FROM python:3.13-slim@sha256:...``).
"""
from __future__ import annotations

import fnmatch
import hashlib
import re
from pathlib import Path

import cascade


CASCADE_CONTEXT = "cascade"
"""The named build context carrying cascade's source: ``COPY --from=cascade ...``."""

_USES_CASCADE = re.compile(rf"--from={CASCADE_CONTEXT}\b")

_EDITOR_FILES = (".#*", "#*#", "*~")
"""Emacs lock links, auto-saves and backups: never part of cascade's source."""


STUB_README = b"""# cascade

Installed into this image from a source checkout by cascade's image builder. The
documentation lives in the checkout.
"""
"""What ``README.md`` is in the staged source: only so ``pip install`` finds the file
``pyproject.toml`` names. Fixed bytes, so it never changes an image's hash."""

Source = Path | bytes
"""A file to copy, or the bytes to write in its place."""


class BuildContextError(Exception):
    """A ``build:`` directory that cannot be built from."""


def check_context(context: Path) -> list[str]:
    """Problems with ``context`` as a build directory, or none."""
    if not context.is_dir():
        return [f"build directory {str(context)!r} does not exist"]
    if not (context / "Dockerfile").is_file():
        return [f"build directory {str(context)!r} has no Dockerfile"]
    if uses_cascade(context):
        try:
            cascade_source()
        except BuildContextError as e:
            return [f"build directory {str(context)!r}: {e}"]
    return []


def uses_cascade(context: Path) -> bool:
    """Whether the Dockerfile copies from the ``cascade`` named context."""
    return bool(_USES_CASCADE.search((context / "Dockerfile").read_text()))


def cascade_source() -> list[tuple[str, Source]]:
    """cascade's installable source, as ``(path in the context, file or bytes)`` pairs.

    The minimum ``pip install`` needs: ``pyproject.toml``, a README where it names one
    (:data:`STUB_README`), and the package without bytecode or editor files (an unsaved
    edit would otherwise ship in the image and change its hash). Only available when
    cascade runs from a source checkout.
    """
    root = Path(cascade.__file__).resolve().parent.parent
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file() or 'name = "cascade-pdl"' not in pyproject.read_text():
        raise BuildContextError(
            "the Dockerfile copies from the `cascade` context, but cascade is not running "
            f"from a source checkout (no cascade pyproject.toml at {root})"
        )
    files: list[tuple[str, Source]] = [("pyproject.toml", pyproject), ("README.md", STUB_README)]
    package = root / "cascade"
    files += [
        (str(p.relative_to(root)), p)
        for p in sorted(package.rglob("*"))
        if p.is_file()
        and "__pycache__" not in p.parts
        and p.suffix != ".pyc"
        and not any(fnmatch.fnmatch(p.name, pattern) for pattern in _EDITOR_FILES)
    ]
    return files


def context_hash(context: Path) -> str:
    """The hex sha256 of everything the image is built from (see the module docstring)."""
    digest = hashlib.sha256()
    files = [(str(p.relative_to(context)), p) for p in sorted(context.rglob("*")) if p.is_file()]
    _add(digest, "context", files)
    if uses_cascade(context):
        _add(digest, CASCADE_CONTEXT, cascade_source())
    return digest.hexdigest()


def _add(digest, label: str, files: list[tuple[str, Source]]) -> None:
    """Paths, executable bits and contents, length-prefixed so no two different inputs
    can produce the same byte stream."""
    digest.update(f"{label}\0{len(files)}\0".encode())
    for name, source in files:
        if isinstance(source, bytes):
            data, executable = source, False
        else:
            data = source.read_bytes()
            executable = source.stat().st_mode & 0o111 != 0
        digest.update(f"{name}\0{int(executable)}\0{len(data)}\0".encode())
        digest.update(data)
