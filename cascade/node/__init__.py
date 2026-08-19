"""The node-side SDK — what a ref needs to talk to Cascade.

**This is one implementation of a protocol, not the protocol itself.** Everything here is
expressible as "read five env vars, GET and PUT objects", and that is the property worth
protecting: a Rust or Go ref reimplements it in a hundred lines rather than being locked
out. Consequently there are no SDK-only capabilities — no decorators, no registration, no
coercion into user-declared classes, and no way for a node to see the dag it belongs to.

The env contract, produced by ``cascade.engine.run_spec.to_env``:

- ``CASCADE_STORE_IN``  — dag-scoped store; sibling node outputs are visible.
- ``CASCADE_STORE_OUT`` — instance-scoped store; write bare port names here.
- ``CASCADE_INPUTS``    — per port: ``(scope, key, encoding, depth, type)``.
- ``CASCADE_OUTPUTS``   — per port: ``(encoding, depth, type)``. No location: the writer
  store's scope already fixes that.
- ``CASCADE_ARGS``      — static kwargs from the dag node.
- ``CASCADE_RUN_SPEC``  — identity: run, node, instance.

Note how little a ref has to know. It never computes a path, and it *cannot* write outside
its own slot, because the writer store is already scoped to it — containment is structural
rather than a convention the container is trusted to honour.

**The file and directory helpers matter more than ``read``/``write``.** Most real refs are
tools that read and write files: ``fb_predict -i DIR -o DIR`` will never speak to an object
store. So ``dir()``/``write_dir()`` — materialise a collection as a directory, collect a
directory back — are what make wrapping an unmodified tool a ten-line entrypoint. Batch
tools also argue against scattering them: one invocation over N images beats N model loads.
"""

from cascade.node.codec import encode, decode, CodecError
from cascade.node.node import NodeError, Node, from_env


__all__ = [
    "encode",
    "decode",
    "NodeError",
    "CodecError",
    "Node",
    "from_env",
]
