"""Package layering, asserted rather than assumed.

Three silent restructuring failures this session — a circular import that only surfaced at
test collection, imports left pointing at moved modules, typing names stranded by a file
split — argue for checking the shape mechanically rather than by eye.
"""
import ast
import pathlib


LAYERS = [
    "cascade.types",       # the type system: depends on nothing
    "cascade.graph",
    "cascade.model",       # the pipeline document
    "cascade.store",       # the data plane
    "cascade.plan",        # compilation
    "cascade.protocol",    # the runtime contract: RunSpec, bindings, Runner/Handle ABCs
    "cascade.runners",     # substrate implementations
    "cascade.engine",      # coordination: DagRunner, FanRunner, resolve
    "cascade.node",        # the node-side SDK
    "cascade.deployment",  # substrate configuration
    "cascade.project",
    "cascade.cli",
]
RANK = {name: index for index, name in enumerate(LAYERS)}

KNOWN_VIOLATIONS = {
    # the executor is the top shell sitting inside a middle layer; moving it out is the
    # next step, and this entry should go with it
    ("cascade.engine", "cascade.deployment"),
}


def _package(module: str) -> str:
    return ".".join(module.split(".")[:2])


def _source_root() -> pathlib.Path:
    """The `cascade` package, found by walking up to the project root.

    Not `Path("cascade")`: that is relative to the working directory, so running pytest
    from anywhere else would find no modules and pass every test below vacuously — a green
    check asserting nothing.
    """
    for parent in pathlib.Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent / "cascade"
    raise RuntimeError("no pyproject.toml above this test; cannot locate the source")


def _imports() -> dict[str, set[str]]:
    edges: dict[str, set[str]] = {}
    root = _source_root()
    for path in root.rglob("*.py"):
        if "__pycache__" in str(path):
            continue
        # `.parts`, not a string replace: the separator is `\\` on Windows, so replacing
        # "/" leaves the path untouched and every module looks like its own package
        module = ".".join(path.relative_to(root.parent).with_suffix("").parts)
        source = _package(module)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("cascade."):
                target = _package(node.module)
                if target != source:
                    edges.setdefault(source, set()).add(target)
    if not edges:
        raise RuntimeError(f"no imports found under {root}; the scan is broken")
    return edges


def test_every_package_is_ranked():
    """A new top-level package must be placed deliberately, not discovered later."""
    unranked = {p for p in _imports() if p not in RANK}
    assert unranked == set(), f"unranked package(s): {sorted(unranked)}"


def test_imports_only_go_downward():
    """A layer may use what is below it and nothing above."""
    upward = {
        (source, target)
        for source, targets in _imports().items()
        for target in targets
        if RANK.get(target, -1) > RANK.get(source, -1)
    } - KNOWN_VIOLATIONS
    assert upward == set(), f"upward imports: {sorted(upward)}"


def test_no_two_packages_depend_on_each_other():
    """The failure this replaced: `engine` and `engine.runner` imported each other because
    one package held the contract, the coordination *and* the substrates."""
    edges = _imports()
    mutual = {
        tuple(sorted((a, b)))
        for a, targets in edges.items()
        for b in targets
        if a in edges.get(b, set())
    }
    assert mutual == set(), f"mutually dependent: {sorted(mutual)}"


def test_the_protocol_names_no_substrate():
    """`protocol` is vocabulary — the set a ref in another language would reimplement — so
    it must not reach into any implementation of itself."""
    forbidden = {"cascade.runners", "cascade.engine", "cascade.deployment", "cascade.cli"}
    assert _imports().get("cascade.protocol", set()) & forbidden == set()


def test_the_node_sdk_stays_thin():
    """A ref should not drag in the compiler or the coordinator: the node depends only on
    the contract, the data plane and the type system."""
    assert _imports().get("cascade.node", set()) <= {
        "cascade.protocol",
        "cascade.store",
        "cascade.types",
    }