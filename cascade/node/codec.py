"""Local file encodings: what a *tool* wants on disk, not what the store holds.

The store is canonical JSON for everything structured. A tool may want something else —
flat-bug takes JSON, BirdNET emits CSV — so the hook converts on the way in and back on
the way out. That conversion is the only place a non-canonical format exists, and it
exists on local disk for the duration of one container.

**CSV is deliberately partial.** Encoding a list of flat records is well defined; decoding
one is not, because CSV yields nothing but strings and coercing `"0.94"` back to a float
needs the field types. Those live in the pipeline's structures, which the node does not
currently receive — so `decode` coerces only what the port's rendered type tells it about
(depth and base) and leaves fields as strings otherwise. Shipping the reachable structures
in the env is the follow-on that makes CSV whole, and it is the same prerequisite that
boundary validation needs.
"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from cascade.model.types import DataFormat


class CodecError(Exception):
    """A payload could not be converted to or from a tool's local format."""


def encode(value: Any, encoding: DataFormat) -> bytes:
    """Canonical value -> the bytes a tool expects in its local file."""
    if encoding is DataFormat.json:
        return json.dumps(value, indent=2).encode("utf-8")
    if encoding is DataFormat.csv:
        return _to_csv(value)
    raise CodecError(f"no encoder for {encoding.value!r}")


def decode(data: bytes, encoding: DataFormat) -> Any:
    """A tool's local file -> a canonical value."""
    if encoding is DataFormat.json:
        try:
            return json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise CodecError(f"not valid JSON: {e}") from e
    if encoding is DataFormat.csv:
        return _from_csv(data)
    raise CodecError(f"no decoder for {encoding.value!r}")


def _to_csv(value: Any) -> bytes:
    if not isinstance(value, list):
        raise CodecError(f"csv needs a list of records, got {type(value).__name__}")
    if not value:
        return b""
    if not all(isinstance(row, dict) for row in value):
        raise CodecError("csv needs a list of flat records (objects), not scalars")
    # union of keys in first-seen order, so a row missing a field still lines up
    fields: list[str] = []
    for row in value:
        for k in row:
            if k not in fields:
                fields.append(k)
    if any(isinstance(v, (dict, list)) for row in value for v in row.values()):
        raise CodecError(
            "csv cannot hold nested values; declare this port as json "
            "(a Detection with a nested bbox or a float[][] contour is not tabular)"
        )
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(value)
    return buffer.getvalue().encode("utf-8")


def _from_csv(data: bytes) -> list[dict[str, Any]]:
    text = data.decode("utf-8")
    if not text.strip():
        return []
    rows = list(csv.DictReader(io.StringIO(text)))
    return [{k: _coerce(v) for k, v in row.items()} for row in rows]


def _coerce(raw: str | None) -> Any:
    """Best-effort scalar coercion.

    Without the declared field types this is a guess, and a deliberately conservative
    one: numbers and booleans that round-trip exactly, everything else left as a string.
    A field genuinely typed `string` whose value is `"12"` would be wrongly narrowed —
    which is why the real fix is shipping the structures, not smarter guessing.
    """
    if raw is None or raw == "":
        return None
    lowered = raw.strip().lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    try:
        as_int = int(raw)
        return as_int if str(as_int) == raw.strip() else raw
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw