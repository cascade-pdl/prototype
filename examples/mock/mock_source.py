"""Mock source ref: emits a range of integers.

Reads nothing, writes one port. ``count`` comes from the dag node's ``args``.
"""
import os

import cascade.node as cn


def main() -> int:
    with cn.from_env(os.environ) as n:
        count = int(n.args.get("count", 10))
        n.log(f"emitting {count} integers")
        n.write("numbers", list(range(count)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())