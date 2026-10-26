"""One arithmetic step on one number. The node's ``args`` pick it: ``op`` (add, sub, mul,
div, pow, mod) and ``by``, the right-hand operand. One ref, several nodes."""
import operator
import os

import cascade.node as cn

OPS = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "div": operator.truediv,
    "pow": operator.pow,
    "mod": operator.mod,
}


def main() -> int:
    with cn.from_env(os.environ) as n:
        op, by = n.args["op"], float(n.args["by"])
        if op not in OPS:
            raise cn.NodeError(f"unknown op {op!r}; one of {sorted(OPS)}")
        x = float(n.read("x"))
        y = OPS[op](x, by)
        n.log(f"{x} {op} {by} = {y}")
        n.write("y", y)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
