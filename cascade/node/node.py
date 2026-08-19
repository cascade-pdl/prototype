import json

from cascade.engine.binding import InputBindings, OutputDecls
from cascade.model.types import DataFormat
from cascade.node.codec import decode, encode
from cascade.store.base import Store


import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Iterable

from cascade.store.registry import decode as decode_store


class NodeError(Exception):
    """The node could not do what a ref asked of it."""



ELEMENTS_PREFIX = "$elements."
"""Where ``write_collection`` puts a distributed collection's elements — beside the
descriptor, not beneath its key, since a file store cannot have one name be both a file
and a directory."""

DONE_MARKER = "$done"
"""Written last, after every output. Its presence means "this node completed", which is
what makes resume possible: a node whose outputs exist but whose marker does not was
interrupted mid-write. ``$`` is reserved for engine-owned names, so no port can collide."""


@dataclass
class Node:
    """One instance's view of the world: its identity, its data, and nothing else."""

    name: str = ""
    run_id: str = ""
    node_id: str | None = None
    instance_id: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    inputs: InputBindings = field(default_factory=InputBindings)
    outputs: OutputDecls = field(default_factory=OutputDecls)
    store_in: Store | None = None
    store_out: Store | None = None
    root: Path | None = None
    _written: list[str] = field(default_factory=list)
    _scratch: Path | None = None

    # ------------------------------------------------------------------ inputs
    def has(self, port: str) -> bool:
        return self.inputs.input_for(port) is not None

    def read(self, port: str) -> Any:
        """The canonical value of an input port, resolving a collection descriptor.

        Whether the collection was materialised or left in place is invisible here, which
        is the point of ``Store.read_json``.
        """
        binding = self._binding(port)
        if self.store_in is None:
            raise NodeError("no reader store: CASCADE_STORE_IN was not set")
        value = self.store_in.read_json(binding.key, at=binding.scope)
        self._check_shape(port, value, binding.depth)
        return value

    def read_bytes(self, port: str) -> Any:
        """Raw bytes, or a list of them for a collection. For blob ports."""
        binding = self._binding(port)
        if self.store_in is None:
            raise NodeError("no reader store: CASCADE_STORE_IN was not set")
        return self.store_in.read(binding.key, at=binding.scope)

    def path(self, port: str) -> Path:
        """Stage one input to a local file and return its path.

        For tools that take a filename. The file is written in the tool's declared
        encoding, not the canonical one.
        """
        binding = self._binding(port)
        target = self._staged(port)
        target.parent.mkdir(parents=True, exist_ok=True)
        if binding.depth == 0 or binding.encoding is not DataFormat.json:
            target.write_bytes(encode(self.read(port), binding.encoding))
        else:
            target.write_bytes(encode(self.read(port), DataFormat.json))
        return target

    def dir(self, port: str, suffix: str = ".json") -> Path:
        """Stage a *collection* input as a directory of one file per element.

        The idiom batch tools need: ``fb_predict -i <dir>``. Filenames are the element
        index zero-padded, so lexicographic order is element order — directory listings
        are not otherwise ordered, and a tool that sorts its inputs would silently
        scramble the correspondence the gather depends on.
        """
        binding = self._binding(port)
        elements = self.read(port)
        if not isinstance(elements, list):
            raise NodeError(
                f"port {port!r} is not a collection (depth {binding.depth}); "
                "use path() for a single value"
            )
        target = self._staged(port)
        target.mkdir(parents=True, exist_ok=True)
        width = max(len(str(len(elements) - 1)), 1)
        for index, element in enumerate(elements):
            name = f"{index:0{width}d}{suffix}"
            (target / name).write_bytes(encode(element, binding.encoding))
        return target

    # ----------------------------------------------------------------- outputs
    def write(self, port: str, value: Any) -> None:
        """Write a canonical value to an output port."""
        self._out().put_json(self._declared(port).port, value)
        self._written.append(port)

    def write_bytes(self, port: str, data: bytes) -> None:
        self._out().put(self._declared(port).port, data)
        self._written.append(port)

    def write_file(self, port: str, path: Path | str) -> None:
        """Take a file the tool produced and store it canonically."""
        decl = self._declared(port)
        raw = Path(path).read_bytes()
        if decl.encoding is DataFormat.json and decl.depth == 0 and not decl.type:
            self._out().put(decl.port, raw)
        else:
            self._out().put_json(decl.port, decode(raw, decl.encoding))
        self._written.append(port)

    def write_dir(self, port: str, path: Path | str, pattern: str = "*") -> None:
        """Collect every matching file in a directory into a collection.

        Sorted by filename, which is why ``dir()`` zero-pads: order has to be recoverable,
        and a directory does not carry one.
        """
        decl = self._declared(port)
        files = sorted(p for p in Path(path).glob(pattern) if p.is_file())
        if not files:
            raise NodeError(f"port {port!r}: no files matching {pattern!r} under {path}")
        self._out().put_json(decl.port, [decode(f.read_bytes(), decl.encoding) for f in files])
        self._written.append(port)

    def write_collection(self, port: str, items: Iterable[Any]) -> None:
        """Write a collection in *distributed* form: N elements plus a descriptor.

        Nothing is concatenated, so this is what to use when the elements are large.
        """
        decl = self._declared(port)
        store = self._out()
        # elements go *beside* the descriptor, never beneath its own key: a file store
        # cannot have `dets` be both the descriptor and a directory of elements. The `$`
        # prefix is reserved for engine-owned names, so this cannot collide with a port.
        holder = (f"{ELEMENTS_PREFIX}{decl.port}",)
        elements = []
        for index, item in enumerate(items):
            store.put_json("element", item, at=(*holder, str(index)))
            elements.append(((*holder, str(index)), "element"))
        store.write_collection(decl.port, elements, element_type=decl.type or None)
        self._written.append(port)

    # --------------------------------------------------------------- lifecycle
    def done(self) -> None:
        """Write the completion marker, last.

        Outputs then either all exist with a marker, or the node is plainly unfinished.
        Writing it in the same call as the outputs is what stops the two disagreeing.
        """
        missing = [p for p in self.outputs.ports if p not in self._written]
        if missing:
            raise NodeError(
                f"declared output port(s) never written: {missing} — "
                f"wrote {self._written or 'nothing'}"
            )
        self._out().put_json(
            DONE_MARKER,
            {"instance": self.instance_id, "ports": list(self.outputs.ports)},
        )

    def tempdir(self) -> Path:
        """A scratch directory, removed when the context exits."""
        if self._scratch is None:
            self._scratch = Path(tempfile.mkdtemp(prefix="cascade-"))
        return self._scratch

    def log(self, message: str) -> None:
        print(f"[{self.instance_id or self.name}] {message}", flush=True)

    # --------------------------------------------------------------- internals
    def _binding(self, port: str):
        binding = self.inputs.input_for(port)
        if binding is None:
            raise NodeError(
                f"no input bound for port {port!r} (bound: {list(self.inputs.ports)})"
            )
        return binding

    def _declared(self, port: str):
        decl = self.outputs.decl_for(port)
        if decl is None:
            raise NodeError(
                f"{port!r} is not a declared output port "
                f"(declared: {list(self.outputs.ports)})"
            )
        return decl

    def _out(self) -> Store:
        if self.store_out is None:
            raise NodeError("no writer store: CASCADE_STORE_OUT was not set")
        return self.store_out

    def _staged(self, port: str) -> Path:
        return self.root / "inputs" / port

    def _check_shape(self, port: str, value: Any, depth: int) -> None:
        """The declared depth is what turns a silent misread into an error."""
        if depth == 0 and isinstance(value, list):
            raise NodeError(
                f"port {port!r} is declared scalar but received a list of {len(value)} — "
                "the pipeline and the stored data disagree"
            )
        if depth > 0 and not isinstance(value, list):
            raise NodeError(
                f"port {port!r} is declared {depth}-dimensional but received "
                f"{type(value).__name__}"
            )

    # ------------------------------------------------------- context behaviour
    def __enter__(self) -> "Node":
        return self

    def __exit__(self, exc_type, exc, tb: TracebackType | None) -> bool:
        if self._scratch is not None:
            shutil.rmtree(self._scratch, ignore_errors=True)
            self._scratch = None
        # the marker is written only on a clean exit -- a failed node must not look done
        if exc_type is None:
            self.done()
        return False


def from_env(env: dict[str, str]) -> Node:
    """Build a ``Node`` from the environment.

    The whole contract in one call: no arguments, because everything comes from the env the
    runner set. Use as a context manager so the completion marker is written on a clean
    exit and *not* written on failure.
    """
    spec = json.loads(env.get("CASCADE_RUN_SPEC", "{}"))
    reader = env.get("CASCADE_STORE_IN")
    writer = env.get("CASCADE_STORE_OUT")
    return Node(
        name=spec.get("name", ""),
        run_id=spec.get("run_id", ""),
        node_id=spec.get("node_id"),
        instance_id=spec.get("instance_id"),
        args=json.loads(env.get("CASCADE_ARGS", "{}")),
        inputs=InputBindings.decode(json.loads(env.get("CASCADE_INPUTS", "[]"))),
        outputs=OutputDecls.decode(json.loads(env.get("CASCADE_OUTPUTS", "[]"))),
        root=Path(env.get("CASCADE_ROOT", "/cascade")),
        store_in=decode_store(json.loads(reader)) if reader else None,
        store_out=decode_store(json.loads(writer)) if writer else None,
    )
