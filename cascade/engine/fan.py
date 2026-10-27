"""``FanRunner`` — runs one node once per element, and closes the fan.

A wrapper, not a kind: it takes whatever the node runs — a ref, or a whole dag — and
runs it once per element of the scattered input. That is why per-element *pipelines*
need no new machinery: wrap the stages in a subdag and scatter that, and
``FanRunner(DagRunner(...))`` fans it opaquely.

**The fan closes here.** Lanes write into ``<node>/<i>/`` and this runner then writes a
collection descriptor at the node's *own* scope, under the output port's name. So
downstream sees exactly what it would see from an unfanned node — one key at the
producer's scope, resolved by ``store.read`` — which is why nothing else in the engine
knows fans exist. Nothing is copied: gathering costs one small object however large the
elements are.

**A lane is bound to its element where it already is.** A distributed collection
(what a fan gathers) is a descriptor naming one object per element, each inside the
dag's slot, so lane ``i`` is bound straight to element ``i``: the fan reads the
descriptor and nothing else, and copies nothing. Only a *monolithic* collection, one
object holding every element, has no per-element object to point at; then each element
is staged to ``<node>/<i>/$in/<port>`` and bound there. A fanned *subdag* still copies
its element into its own ``$in`` on entry, as every dag boundary does: its inner nodes
cannot read outside the lane. Every other input is bound unchanged, still pointing at
its sibling's scope.

Every lane starts at once. How many *run* at once is not decided here: a cap on one
fan bounds that node's breadth, never the product across nested fans or the sum over
sibling fans. The run's limits are slots its leaf runners take (``cascade.runners.limiter``),
so a lane waiting for one is just a coroutine waiting inside its leaf.
"""
from __future__ import annotations

import json
from typing import Any, Awaitable, Iterator, Mapping

from cascade.engine.concurrency import run_all
from cascade.protocol.binding import INPUTS_SCOPE, InputBinding, InputBindings
from cascade.protocol.instance_path import InstancePath
from cascade.store.collection import CollectionDescriptor
from cascade.protocol.run_spec import RunSpec
from cascade.protocol.runner import Runner
from cascade.protocol.coro import RunnerCoroBase
from cascade.store.base import Store
from cascade.store.registry import from_config


SCATTER_INPUT = INPUTS_SCOPE

WIDEST_LANE = 99_999
"""The lane index ``leaves`` assumes, since a fan's width is known only once it runs. A
lane's index is in its identity and its store scope, so a wider fan's specs are a few
characters longer; the check at spawn still catches those."""


class FanError(Exception):
    """The fan could not be run."""


def _store_at(store: Store, scope: tuple[str, ...]) -> Store:
    config = store.config.subscope(scope)
    _kind, store_cls, _config_cls = from_config(config)
    return store_cls(config)


class FanRunner(RunnerCoroBase):
    def __init__(
        self,
        child: Runner,
        scatter_port: str,
        outputs: Mapping[str, tuple[tuple[str, ...], str]] | None = None,
        merge: str = "concat",
    ):
        self.child = child
        self.scatter_port = scatter_port
        # port -> where that port lands *within* a lane. For a ref child that is
        # ((), port); for a dag child it is the subdag's own output alias, since a
        # dag writes through its inner nodes rather than at its root.
        self.outputs = dict(outputs or {})
        self.merge = merge

    def _invoke(self, spec: RunSpec) -> Awaitable[int]:
        return self._run_fan(spec)

    # ------------------------------------------------------------------ lanes
    def _lane_spec(
        self,
        spec: RunSpec,
        path: InstancePath,
        index: int,
        element: tuple[tuple[str, ...], str],
    ) -> RunSpec:
        """One lane: every input bound as the node's own, except the scattered port,
        which points at that lane's element, ``(scope, key)`` in the lanes' reader."""
        source = self._scatter_binding(spec)
        element_scope, element_key = element
        scattered = InputBinding(
            port=self.scatter_port,
            scope=element_scope,
            key=element_key,
            # one element of the scattered collection: same config, one array level less
            type=source.type.element() if source.type.depth > 0 else source.type,
            config=source.config,
        )
        others = tuple(
            b for b in (spec.inputs or InputBindings()).inputs
            if b.port != self.scatter_port
        )
        return RunSpec(
            name=spec.name,
            run_id=spec.run_id,
            node_id=spec.node_id,
            instance_id=str(path.lane(index)),
            store_in=spec.store_in,
            store_out=_store_at(spec.store_out, (str(index),)),
            inputs=InputBindings(inputs=(*others, scattered)),
            outputs=spec.outputs,  # every lane declares what the node declares
            args=dict(spec.args),
        )

    def _scatter_binding(self, spec: RunSpec) -> InputBinding:
        binding = (spec.inputs or InputBindings()).input_for(self.scatter_port)
        if binding is None:
            raise FanError(
                f"node scatters over {self.scatter_port!r} but no input is bound to it"
            )
        return binding

    async def _run_lane(self, spec: RunSpec) -> None:
        # a non-zero lane raises here, not after the fan: that is what lets run_all
        # stop the other lanes at the first failure
        code = await self.child.run(spec)
        if code != 0:
            raise FanError(f"{spec.instance_id}: exited {code}")

    def _elements(self, spec: RunSpec) -> list[tuple[tuple[str, ...], str]]:
        """Where each lane's element is, as ``(scope, key)`` in the lanes' reader.

        A descriptor's element scopes are relative to the descriptor, so prefixing the
        binding's scope makes them relative to the reader. A monolithic collection is
        split here, one staged object per element: one read, ``N`` writes.
        """
        binding = self._scatter_binding(spec)
        raw = spec.store_in.get(binding.key, at=binding.scope)
        if descriptor := CollectionDescriptor.try_decode(raw):
            return [((*binding.scope, *scope), key) for scope, key in descriptor.elements]
        values = json.loads(raw.decode("utf-8"))
        if not isinstance(values, list):
            raise FanError(
                f"node scatters over {self.scatter_port!r}, but "
                f"{'/'.join((*binding.scope, binding.key))} is not a collection"
            )
        for index, value in enumerate(values):
            spec.store_out.put_json(self.scatter_port, value, at=(str(index), SCATTER_INPUT))
        return [self._staged_element(spec, index) for index in range(len(values))]

    def _staged_element(self, spec: RunSpec, index: int) -> tuple[tuple[str, ...], str]:
        """Where lane ``index``'s element is staged, relative to the lanes' reader."""
        node_prefix = (spec.node_id,) if spec.node_id else ()
        return (*node_prefix, str(index), SCATTER_INPUT), self.scatter_port

    @staticmethod
    def _require_stores(spec: RunSpec) -> None:
        if spec.store_in is None or spec.store_out is None:
            raise FanError("a fanned node needs both a reader and a writer store")

    def leaves(self, spec: RunSpec) -> Iterator[tuple[Runner, RunSpec]]:
        """One lane's leaves, at the widest lane index assumed: every lane's spec has
        the same shape, so one stands for all.

        Where the element will be is known only once the upstream has run, so the lane
        is shown bound to a staged element. An element bound in place has a scope of
        the same order (the producer's node and lane index rather than this node's);
        the check at spawn still catches one that is longer."""
        self._require_stores(spec)
        path = InstancePath.parse(spec.instance_id or spec.run_id)
        lane = self._lane_spec(spec, path, WIDEST_LANE, self._staged_element(spec, WIDEST_LANE))
        yield from self.child.leaves(lane)

    # -------------------------------------------------------------------- run
    async def _run_fan(self, spec: RunSpec) -> int:
        self._require_stores(spec)

        path = InstancePath.parse(spec.instance_id or spec.run_id)
        lanes = [
            self._lane_spec(spec, path, index, element)
            for index, element in enumerate(self._elements(spec))
        ]

        # lane i stays element i because each lane writes beneath its own index, not
        # because of completion order; run_all decides what a failure does (fail fast)
        await run_all(self._run_lane(s) for s in lanes)

        self._gather(spec, len(lanes))
        return 0

    def _gather(self, spec: RunSpec, width: int) -> None:
        """Close the fan: one artifact per output port, at the node's own scope.

        Written as a **descriptor** — the lanes' outputs stay where they are and the
        collection is a small object referencing them, so gathering costs the same
        whether the elements are integers or image crops. A consumer calling
        ``store.read`` cannot tell the difference, which is the point.
        """
        for port, (inner_scope, inner_key) in self.outputs.items():
            lanes = [((str(i), *inner_scope), inner_key) for i in range(width)]
            if self.merge == "flatten":
                self._flatten(spec, port, lanes)
            else:
                spec.store_out.write_collection(port, lanes)

    def _flatten(
        self,
        spec: RunSpec,
        port: str,
        lanes: list[tuple[tuple[str, ...], str]],
    ) -> None:
        """Concatenate lane collections into one, at the node's own scope.

        When every lane wrote a descriptor this is **metadata only**: the merged
        descriptor simply lists all the lanes' elements, with each element's scope
        prefixed by its lane so it stays relative to the new descriptor's location. No
        payload moves, which is what makes the expensive-looking reshape the cheap one.

        A lane that wrote a monolithic collection has no elements to reference, so those
        fall back to reading and concatenating values — the one case where flattening
        costs what it looks like it costs.
        """
        merged: list[tuple[tuple[str, ...], str]] = []
        for scope, key in lanes:
            descriptor = CollectionDescriptor.try_decode(spec.store_out.get(key, at=scope))
            if descriptor is None:
                # at least one lane is monolithic: concatenate values instead
                values: list[Any] = []
                for s, k in lanes:
                    values.extend(spec.store_out.read_json(k, at=s))
                spec.store_out.put_json(port, values)
                return
            merged.extend(
                ((*scope, *element_scope), element_key)
                for element_scope, element_key in descriptor.elements
            )
        spec.store_out.write_collection(port, merged)