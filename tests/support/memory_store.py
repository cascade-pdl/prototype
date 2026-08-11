"""An in-memory :class:`~cascade.store.base.Store` double.

Keys live in a dict; scope and ``at`` fragments compose exactly as the real
backends do (prefix segments joined by ``/``, ``list`` returning keys relative to
the scoped base). It exists to exercise the ABC's own concrete behaviour — the
``copy`` primitive, the JSON helpers, scope/``at`` composition — without touching
a filesystem or S3, and to give the in-process ``RunnerCoro`` a store to run
against in engine tests.

Deliberately **not** registry-serialisable: it is a test double, never a
registered backend, so it stays out of the production ``STORE_REG``. Tests that
need the ``{kind, config}`` envelope use a registered backend (``FileStore``).
"""
from __future__ import annotations

from cascade.store.base import Store


class MemoryStore(Store):
    def __init__(self, scope: tuple[str, ...] = ()) -> None:
        self.scope = tuple(scope)
        self._data: dict[str, bytes] = {}

    def _key(self, key: str | None, at: tuple[str, ...]) -> str:
        parts = [p for p in (*self.scope, *at, key) if p]
        return "/".join(parts)

    def put(self, key: str, data: bytes, at: tuple[str, ...] = ()) -> str:
        self._data[self._key(key, at)] = data
        return key

    def get(self, key: str, at: tuple[str, ...] = ()) -> bytes:
        return self._data[self._key(key, at)]

    def has(self, key: str, at: tuple[str, ...] = ()) -> bool:
        return self._key(key, at) in self._data

    def list(self, at: tuple[str, ...] = ()) -> list[str]:
        """Keys under the scoped base, returned relative to it (mirrors the real
        backends, which strip scope+prefix back off)."""
        base = self._key(None, at)
        strip = (base + "/") if base else ""
        out = [
            k[len(strip):] if strip else k
            for k in self._data
            if not strip or k.startswith(strip)  # only keys *under* the base
        ]
        return [p for p in out if p]