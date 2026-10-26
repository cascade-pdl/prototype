"""Sums the gathered fan: reads the whole collection, writes one number."""
import os

import cascade.node as cn


def main() -> int:
    with cn.from_env(os.environ) as n:
        n.write("total", sum(float(x) for x in n.read("xs")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
