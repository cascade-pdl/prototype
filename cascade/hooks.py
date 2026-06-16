"""Node-side hooks: the translation layer between the canonical data plane and
a container's local format + field names.

This is the code that runs *node-side* (conceptually inside the container's
entrypoint wrapper). It performs only **type-preserving representation changes**:

  - field mapping  — relabel canonical field names <-> the container's local
                     names (a pure rename; never computes or restructures)
  - encoding       — serialize/deserialize between canonical (JSON in the store)
                     and the container's local encoding (json | csv)

It never performs value computation, unit conversion, or restructuring — those
are nodes, not hooks. The line: a hook may change *representation* (names,
format), never *meaning* (values, structure).

Flow (input hook):  canonical bytes -> parse -> relabel(canonical->local)
                                    -> encode(local fmt) -> file for container
Flow (output hook): container file -> decode(local fmt) -> relabel(local->canonical)
                                    -> encode canonical (JSON) -> store
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any


# --------------------------------------------------------------------------- #
# Codecs — (de)serialize between bytes and Python structures.
# A codec is a fixed, total, value-preserving transform: it changes only the
# *encoding*, never the logical content. Adding one (parquet, avro) is local.
# --------------------------------------------------------------------------- #
def decode(data: bytes, encoding: str) -> Any:
    """bytes -> Python structure (list[dict] | dict | scalar)."""
    if encoding == "json":
        return json.loads(data.decode("utf-8"))
    if encoding == "csv":
        text = data.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = [dict(r) for r in reader]
        return [_coerce_csv_row(r) for r in rows]
    raise ValueError(f"unknown encoding '{encoding}'")


def encode(obj: Any, encoding: str) -> bytes:
    """Python structure -> bytes."""
    if encoding == "json":
        return json.dumps(obj).encode("utf-8")
    if encoding == "csv":
        rows = obj if isinstance(obj, list) else [obj]
        if not rows:
            return b""
        # union of keys, stable order from the first row then any extras
        fieldnames: list[str] = list(rows[0].keys())
        for r in rows[1:]:
            for k in r:
                if k not in fieldnames:
                    fieldnames.append(k)
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: _csv_scalar(v) for k, v in r.items()})
        return buf.getvalue().encode("utf-8")
    raise ValueError(f"unknown encoding '{encoding}'")


def _coerce_csv_row(row: dict[str, str]) -> dict[str, Any]:
    """CSV values are strings; best-effort coerce numerics/bools so that a
    round-trip through CSV preserves the logical types. (Representation change
    only — we are recovering the value CSV flattened to text, not computing.)"""
    out: dict[str, Any] = {}
    for k, v in row.items():
        out[k] = _coerce_scalar(v)
    return out


def _coerce_scalar(v: str) -> Any:
    if v == "":
        return None
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        if "." in v or "e" in low:
            return float(v)
        return int(v)
    except ValueError:
        return v


def _csv_scalar(v: Any) -> Any:
    """Flatten a value for a CSV cell. Lists/dicts in a CSV cell are JSON-encoded
    (a representation choice, still no computation on the logical value)."""
    if isinstance(v, (list, dict)):
        return json.dumps(v)
    if v is None:
        return ""
    return v


# --------------------------------------------------------------------------- #
# Field mapping — pure, type-preserving rename.
# mapping is {canonical_name: local_name}. Applying it relabels keys; it never
# adds, drops, computes, or restructures.
# --------------------------------------------------------------------------- #
def relabel_canonical_to_local(obj: Any, mapping: dict[str, str]) -> Any:
    """Rename canonical field names -> container-local names."""
    return _relabel(obj, mapping)


def relabel_local_to_canonical(obj: Any, mapping: dict[str, str]) -> Any:
    """Rename container-local names -> canonical names (inverse mapping)."""
    inverse = {local: canon for canon, local in mapping.items()}
    return _relabel(obj, inverse)


def _relabel(obj: Any, rename: dict[str, str]) -> Any:
    if not rename:
        return obj
    if isinstance(obj, list):
        return [_relabel(x, rename) for x in obj]
    if isinstance(obj, dict):
        return {rename.get(k, k): v for k, v in obj.items()}
    return obj


# --------------------------------------------------------------------------- #
# The two hooks, composed. These are what the engine/runner invoke at the
# data-plane boundary (node-side).
#
# Binary blobs are opaque: bytes + a media type, no fields. So for a binary
# port the hooks SKIP codec and field-mapping entirely and pass bytes through
# unchanged — there is no JSON/CSV for a PNG, and no field to rename. The media
# type travels as metadata, not in the bytes.
# --------------------------------------------------------------------------- #
def to_container(canonical_bytes: bytes, local_encoding: str, mapping: dict[str, str],
                 is_binary: bool = False) -> bytes:
    """Input hook: canonical bytes from the store -> bytes for the container.

    For a structured port: decode canonical JSON, relabel canonical->local
    names, re-encode to the container's local format.
    For a binary port: pass the bytes through unchanged (no decode, no relabel,
    no re-encode) — the container reads the raw blob (e.g. the PNG) directly.
    """
    if is_binary:
        return canonical_bytes
    obj = decode(canonical_bytes, "json")           # store is always canonical JSON
    obj = relabel_canonical_to_local(obj, mapping)  # type-preserving rename
    return encode(obj, local_encoding)              # re-encode to the node's format


def from_container(local_bytes: bytes, local_encoding: str, mapping: dict[str, str],
                   is_binary: bool = False) -> bytes:
    """Output hook: bytes the container produced -> canonical bytes for the store.

    For a structured port: parse the node's format, relabel local->canonical,
    re-encode to canonical JSON.
    For a binary port: pass the bytes through unchanged — the blob is stored
    as-is, its media type carried as metadata.
    """
    if is_binary:
        return local_bytes
    obj = decode(local_bytes, local_encoding)       # parse the node's format
    obj = relabel_local_to_canonical(obj, mapping)  # rename back to canonical
    return encode(obj, "json")                       # store is always canonical JSON
