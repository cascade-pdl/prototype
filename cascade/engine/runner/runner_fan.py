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

**The one place a slice is materialised.** A scatter element does not exist as an
addressable artifact until someone creates it: the upstream wrote *a collection*, not
N objects. So each lane's element is staged to ``<node>/<i>/$in/<port>`` and bound
there. Every other input is bound unchanged, still pointing at its sibling's scope, so
this is the only copy in the system and it is one element wide.

Concurrency is bounded here because this is the only component that knows N. Left
unset it is unbounded *by design*: under a broker (celery, ECS) the scheduler owns the
budget, and a local semaphore would throttle work the scheduler cannot then see. Note
that a cap here bounds one node's breadth, never the product across nested fans —
global scheduling is deliberately out of scope.
"""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Mapping

from cascade.engine.binding import InputBinding, InputBindings
from cascade.engine.instance_path import InstancePath
from cascade.store.collection import CollectionDescriptor
from cascade.engine.run_spec import RunSpec
from cascade.engine.runner.runner import Runner
from cascade.engine.runner.runner_coro import RunnerCoroBase
from cascade.store.base import Store
from cascade.store.registry import from_config


SCATTER_INPUT = "$in"


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
        max_concurrent: int | None = None,
    ):
        self.child = child
        self.scatter_port = scatter_port
        # port -> where that port lands *within* a lane. For a ref child that is
        # ((), port); for a dag child it is the subdag's own output alias, since a
        # dag writes through its inner nodes rather than at its root.
        self.outputs = dict(outputs or {})
        self.merge = merge
        self.max_concurrent = max_concurrent

    def _invoke(self, spec: RunSpec) -> Awaitable[int]:
        return self._run_fan(spec)

    # ------------------------------------------------------------------ lanes
    def _lane_spec(
        self,
        spec: RunSpec,
        path: InstancePath,
        index: int,
        element_scope: tuple[str, ...],
    ) -> RunSpec:
        """One lane: every input bound as the node's own, except the scattered port,
        which points at that lane's staged element."""
        scattered = InputBinding(
            port=self.scatter_port,
            scope=element_scope,
            key=self.scatter_port,
            encoding=self._scatter_binding(spec).encoding,
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
            args=dict(spec.args),
        )

    def _scatter_binding(self, spec: RunSpec) -> InputBinding:
        binding = (spec.inputs or InputBindings()).input_for(self.scatter_port)
        if binding is None:
            raise FanError(
                f"node scatters over {self.scatter_port!r} but no input is bound to it"
            )
        return binding

    async def _run_lane(self, spec: RunSpec, sem: asyncio.Semaphore | None) -> int:
        if sem is None:
            return await self.child.run(spec)
        async with sem:
            return await self.child.run(spec)

    # -------------------------------------------------------------------- run
    async def _run_fan(self, spec: RunSpec) -> int:
        if spec.store_in is None or spec.store_out is None:
            raise FanError("a fanned node needs both a reader and a writer store")

        path = InstancePath.parse(spec.instance_id or spec.run_id)
        binding = self._scatter_binding(spec)
        elements = spec.store_in.read_json(binding.key, at=binding.scope)

        # stage one element per lane -- the only copy in the system, and the only
        # way a slice becomes addressable
        node_prefix = (spec.node_id,) if spec.node_id else ()
        lanes = []
        for index, element in enumerate(elements):
            spec.store_out.put_json(self.scatter_port, element, at=(str(index), SCATTER_INPUT))
            element_scope = (*node_prefix, str(index), SCATTER_INPUT)
            lanes.append(self._lane_spec(spec, path, index, element_scope))

        sem = asyncio.Semaphore(self.max_concurrent) if self.max_concurrent else None
        # gather preserves order, so lane i stays element i -- as_completed would
        # silently destroy the correspondence the gathered collection depends on
        codes = await asyncio.gather(*(self._run_lane(s, sem) for s in lanes))
        for index, code in enumerate(codes):
            if code != 0:
                raise FanError(f"{path.lane(index)}: exited {code}")

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