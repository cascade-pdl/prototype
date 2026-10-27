"""Source ref: emits 1.0, 2.0, ... ``count`` (from the node's ``args``)."""
import os

import cascade.node as cn


def main() -> int:
    with cn.from_env(os.environ) as n:
        count = int(n.args.get("count", 4))
        n.write("numbers", [float(i) for i in range(1, count + 1)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
