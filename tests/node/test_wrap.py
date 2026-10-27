"""``run_wrapped`` called in-process: what the CLI tests cannot see from outside.

The behaviour itself (staging, exit codes, signal forwarding, marker) is tested end to end
through the real command in ``tests/cli/test_node_wrap.py``.
"""
import asyncio
import signal
import sys

import pytest

from cascade.node import Node, run_wrapped
from cascade.node.node import DONE_MARKER
from cascade.protocol.binding import OutputDecl, OutputDecls
from cascade.store.file_store import FileConfig, FileStore
from cascade.types import TypeExpr


@pytest.fixture
def node(tmp_path):
    store = FileStore(FileConfig(root=str(tmp_path / "store"), scope=("r1",)))
    return Node(
        name="n",
        outputs=OutputDecls((OutputDecl("out", TypeExpr.parse("string")),)),
        store_out=store,
        root=tmp_path / "cascade",
    )


def write_out(node: Node) -> list[str]:
    return [sys.executable, "-c",
            f"open({str(node.outputs_dir / 'out')!r}, 'w').write('\"done\"')"]


def test_a_successful_run_collects_and_marks_done(node):
    assert run_wrapped(node, write_out(node)) == 0
    assert asyncio.run(node.store_out.get_json("out")) == "done"
    assert asyncio.run(node.store_out.has(DONE_MARKER))


def test_the_previous_signal_handlers_are_restored(node):
    """A library call must not leave process-wide state behind: after the run, SIGTERM
    and SIGINT behave as they did before it."""
    before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    run_wrapped(node, write_out(node))
    assert {s: signal.getsignal(s) for s in before} == before


def test_the_handlers_are_restored_after_a_failure_too(node):
    before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    assert run_wrapped(node, [sys.executable, "-c", "raise SystemExit(3)"]) == 3
    assert {s: signal.getsignal(s) for s in before} == before


def test_nothing_to_run_is_refused(node):
    with pytest.raises(ValueError, match="nothing to run"):
        run_wrapped(node, [])
